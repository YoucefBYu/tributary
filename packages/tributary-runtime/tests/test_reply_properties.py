"""W2-8 property tests (proposal section 7, extended to `AE` by the architect's amendment).

**Scope: this proves the rules on the model, not on a receiver.** The answer property runs against
`tributary.runtime.sync_answer.SyncRequest`, a pure state machine. It fixes what the answers must be. It
does not show that any receiver gives them: that holds only for a receiver that delegates to
`SyncRequest`, or one this property is run against (`violations` takes a factory, so a receiver's
pending-map entry can be adapted to the same five events) with a broken-receiver control that goes red.
Until then the invariant-1 risk row for W2-8 stays open.

**Answer property.** Over random interleavings of the receiver's `raw` delivery callback, reply records
(`accepted`, `rejected`, absent status, unknown status), the reply timer, a revoke of the advertised
partition and a sender disconnect, every request gets **exactly one** resolution, and neither `AA` nor
`AE` is ever given before the successful `raw` callback. Also: `AR` never follows a successful callback;
the answer is exactly what an independently written oracle derives from its cause, so `AA` needs the
**first** reply to be `accepted`, a reply-derived `AE` carries the first reply's code, a timer `AE` is
`REPLY_TIMEOUT` and a revoke `AE` is `REPLY_ROUTE_LOST`. Seven broken implementations are the controls,
and each must be caught: one that answers on the reply alone (the proposal's control), one that answers
`AE` on a revoke before the callback (what the amendment forbids), one that trusts an absent status, one
that lets the last held reply win, one that swaps the timer and revoke codes, one that answers `AA` for
an unrecognised status, and one that answers a failed commit from a held reply instead of `AR`.

**Worker property.** Over random pipeline and per-destination outcomes (ok, drop, `Reject`, raise), a
sync record gets exactly one reply; it is produced after every `out` and `dlq` record and after a flush
that served their callbacks; its status and code are those of the first failure in spec order; and no
non-reply record carries a reply-routing header.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st
from tributary.runtime.reply import PRODUCE_FAILED
from tributary.runtime.sync_answer import Resolution, SyncRequest, answer_for_reply
from tributary.sdk import Drop, Message, Reject, step
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT, TRANSFORM_ERROR

from .reply_harness import DLQ, REPLY, make_spec, rig, sync_request

# -- the answer state machine ----------------------------------------------------------------------

REPLIES = {
    "reply_accepted": ("accepted", None),
    "reply_rejected": ("rejected", "LAB_QUERY_UNSUPPORTED"),
    "reply_failed": ("failed", TRANSFORM_ERROR),
    "reply_absent": (None, None),
    "reply_unknown": ("partially-accepted", None),
}
NOISE = [*REPLIES, "timeout", "revoke", "disconnect"]


@st.composite
def traces(draw: st.DrawFn) -> list[str]:
    """Noise events in any order, exactly one `raw` callback somewhere, and a final timer firing."""
    noise = draw(st.lists(st.sampled_from(NOISE), max_size=8))
    commit = draw(st.sampled_from(["commit_ok", "commit_failed"]))
    position = draw(st.integers(min_value=0, max_value=len(noise)))
    # The timer always fires eventually unless something resolved first: it closes every trace.
    return [*noise[:position], commit, *noise[position:], "timeout"]


def drive(request: SyncRequest, event: str) -> Resolution | None:
    if event == "commit_ok":
        return request.commit_succeeded()
    if event == "commit_failed":
        return request.commit_failed()
    if event in REPLIES:
        status, code = REPLIES[event]
        return request.reply_received(status, code)
    if event == "timeout":
        return request.reply_timed_out()
    if event == "revoke":
        return request.partition_revoked()
    if event == "disconnect":
        return request.disconnected()
    raise AssertionError(event)


def oracle(cause: str, first_reply: str | None, revoked: bool) -> Resolution | None:
    """The one right answer, written from channel-spec 3.2.6's table without reading `SyncRequest`.

    `cause` is the event that resolved the request. A reply-derived answer comes from the **first**
    reply (held before the callback or received after it): `AA` only for `accepted`, else `AE` with that
    reply's own code. A revoke answers `REPLY_ROUTE_LOST`, the timer `REPLY_TIMEOUT`. `None`: nothing
    justifies an answer at this event.
    """
    if cause == "disconnect":
        return Resolution(answer=None)
    if cause == "commit_failed":
        return Resolution("AR")
    if cause == "timeout":
        return Resolution("AE", REPLY_TIMEOUT)
    if cause == "revoke":
        return Resolution("AE", REPLY_ROUTE_LOST)
    if first_reply is not None:  # cause is a reply, or the callback releasing a held one
        status, code = REPLIES[first_reply]
        return Resolution("AA") if status == "accepted" else Resolution("AE", code)
    if cause == "commit_ok" and revoked:  # the callback releasing a held revoke
        return Resolution("AE", REPLY_ROUTE_LOST)
    return None


def violations(trace: list[str], factory: Callable[[], SyncRequest]) -> list[str]:
    """Every broken rule in one run of `trace`. Empty means the implementation held."""
    request = factory()
    found: list[str] = []
    resolutions: list[Resolution] = []
    committed = False
    disconnected = False
    revoked = False
    first_reply: str | None = None
    for event in trace:
        committed = committed or event == "commit_ok"
        if first_reply is None and event in REPLIES and not resolutions:
            first_reply = event
        revoked = revoked or (event == "revoke" and not resolutions)
        resolution = drive(request, event)
        disconnected = disconnected or event == "disconnect"
        if resolution is None:
            continue
        resolutions.append(resolution)
        if resolution.answer in ("AA", "AE") and not committed:
            found.append(f"{resolution.answer} before the raw callback succeeded (invariant 1)")
        if resolution.answer == "AR" and committed:
            found.append("AR after a confirmed commit: tells the sender a resend is safe when it is not")
        if resolution.answer == "AA" and first_reply != "reply_accepted":
            found.append("AA without the first reply being accepted")
        if resolution.answer is not None and disconnected:
            found.append("an answer written to a sender that had gone")
        expected = oracle(event, first_reply, revoked)
        if len(resolutions) == 1 and resolution != expected:
            found.append(f"{resolution} where the rules give {expected} (cause {event})")
    if len(resolutions) != 1:
        found.append(f"{len(resolutions)} resolutions, expected exactly one")
    return found


@given(traces())
@settings(max_examples=2000, deadline=None)
def test_every_request_gets_exactly_one_answer_and_never_ahead_of_the_commit(trace: list[str]) -> None:
    assert violations(trace, SyncRequest) == []


class AnswersOnReplyAlone(SyncRequest):
    """The proposal's control: resolves on the reply without waiting for the raw callback."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = answer_for_reply(status, code)
        return self.resolution


class AnswersRevokeEagerly(SyncRequest):
    """The amendment's control: `AE` / `REPLY_ROUTE_LOST` on a revoke before the callback."""

    def partition_revoked(self) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = Resolution("AE", REPLY_ROUTE_LOST)
        return self.resolution


class TrustsAbsentStatus(SyncRequest):
    """Envelope 2.3.3's control: reads an absent `tb-reply-status` as accepted."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        return super().reply_received("accepted" if status is None else status, code)


class LastReplyWins(SyncRequest):
    """A later reply overwrites the one held before the callback (the rule is: the first one counts)."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved or self._committed:
            return super().reply_received(status, code)
        self._reply = None  # forget the held one, so the base class holds this one instead
        return super().reply_received(status, code)


class SwapsAeCodes(SyncRequest):
    """The timer answers `REPLY_ROUTE_LOST` and a revoke `REPLY_TIMEOUT`: right answer, wrong code."""

    def reply_timed_out(self) -> Resolution | None:
        resolved = super().reply_timed_out()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_ROUTE_LOST))

    def partition_revoked(self) -> Resolution | None:
        resolved = super().partition_revoked()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_TIMEOUT))


class TrustsUnrecognisedStatus(SyncRequest):
    """`AA` for any status that is not `rejected` or `failed`, so `partially-accepted` becomes `AA`."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        trusted = "accepted" if status not in (None, "rejected", "failed") else status
        return super().reply_received(trusted, code)


class AnswersHeldReplyOnFailedCommit(SyncRequest):
    """A reply held before the callback is answered even when the callback then fails (never `AR`)."""

    def commit_failed(self) -> Resolution | None:
        if not self.resolved and self._committed is None and self._reply is not None:
            self._committed = False
            return self._resolve(answer_for_reply(self._reply.status, self._reply.code))
        return super().commit_failed()


BROKEN = [
    AnswersOnReplyAlone,
    AnswersRevokeEagerly,
    TrustsAbsentStatus,
    LastReplyWins,
    SwapsAeCodes,
    TrustsUnrecognisedStatus,
    AnswersHeldReplyOnFailedCommit,
]


@pytest.mark.parametrize("broken", BROKEN)
def test_each_broken_receiver_is_caught(broken: type[SyncRequest]) -> None:
    """A property that cannot go red is not evidence (PHASE-0-DECISION section 4)."""
    counterexample = find(traces(), lambda trace: bool(violations(trace, broken)), settings=settings(max_examples=5000))
    assert violations(counterexample, broken)


def test_the_documented_answers() -> None:
    """Spot checks of the table in channel-spec 3.2.6, so a reader can see the rules, not only a property."""
    early_reply = SyncRequest()
    assert early_reply.reply_received("accepted", None) is None, "held until the callback"
    assert early_reply.commit_succeeded() == Resolution("AA")

    revoked_first = SyncRequest()
    assert revoked_first.partition_revoked() is None, "waits for the callback"
    assert revoked_first.commit_failed() == Resolution("AR"), "AR on a failed callback, never AE"

    revoked_then_committed = SyncRequest()
    revoked_then_committed.partition_revoked()
    assert revoked_then_committed.commit_succeeded() == Resolution("AE", REPLY_ROUTE_LOST)

    timed_out = SyncRequest()
    assert timed_out.reply_timed_out() is None, "the clock starts at the callback"
    timed_out.commit_succeeded()
    assert timed_out.reply_timed_out() == Resolution("AE", REPLY_TIMEOUT)

    unknown = SyncRequest()
    unknown.commit_succeeded()
    assert unknown.reply_received("partially-accepted", "SOME_CODE") == Resolution("AE", "SOME_CODE")

    first_counts = SyncRequest()
    assert first_counts.reply_received("rejected", "LAB_QUERY_UNSUPPORTED") is None
    assert first_counts.reply_received("accepted", None) is None, "a second reply is ignored"
    assert first_counts.commit_succeeded() == Resolution("AE", "LAB_QUERY_UNSUPPORTED")


@pytest.mark.parametrize(
    ("broken", "trace"),
    [
        (LastReplyWins, ["reply_rejected", "reply_accepted", "commit_ok", "timeout"]),
        (SwapsAeCodes, ["commit_ok", "timeout"]),
        (SwapsAeCodes, ["commit_ok", "revoke", "timeout"]),
        (TrustsUnrecognisedStatus, ["commit_ok", "reply_unknown", "timeout"]),
        (AnswersHeldReplyOnFailedCommit, ["reply_accepted", "commit_failed", "timeout"]),
    ],
)
def test_the_new_controls_fail_on_their_minimal_traces(broken: type[SyncRequest], trace: list[str]) -> None:
    """Each named trace breaks exactly the rule its control is about, and the model passes it."""
    assert violations(trace, broken)
    assert violations(trace, SyncRequest) == []


# -- the worker ------------------------------------------------------------------------------------

OUTCOMES = ["ok", "drop", "reject", "raise"]


def _node(outcome: str, code: str) -> Any:
    if outcome == "ok":

        @step
        def ok(message: Message) -> None:
            return None

        return ok
    if outcome == "drop":

        @step
        def dropped(message: Message) -> Drop:
            return Drop(reason="filtered on purpose")

        return dropped
    if outcome == "reject":

        @step
        def rejected(message: Message) -> None:
            raise Reject(code)

        return rejected

    @step
    def raised(message: Message) -> None:
        raise RuntimeError("synthetic failure")

    return raised


def expected_outcome(pipeline: str, destinations: list[str]) -> tuple[str, str | None]:
    """The status rule written independently of the implementation: first failure in spec order."""
    if pipeline == "drop":
        return ("accepted", None)
    if pipeline == "reject":
        return ("rejected", "PIPE_REJECT")
    if pipeline == "raise":
        return ("failed", TRANSFORM_ERROR)
    for index, outcome in enumerate(destinations):
        if outcome == "reject":
            return ("rejected", f"DEST_REJECT_{index}")
        if outcome == "raise":
            return ("failed", TRANSFORM_ERROR)
    return ("accepted", None)


@given(
    pipeline=st.sampled_from(OUTCOMES),
    destinations=st.lists(st.sampled_from(OUTCOMES), min_size=1, max_size=3),
    fail_reply=st.booleans(),
)
@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_the_worker_answers_once_after_every_callback_with_the_first_failure_in_spec_order(
    pipeline: str, destinations: list[str], fail_reply: bool
) -> None:
    names = [f"d{index}" for index in range(len(destinations))]
    table = {
        f"{name}-step": _node(outcome, f"DEST_REJECT_{index}")
        for index, (name, outcome) in enumerate(zip(names, destinations, strict=True))
    }
    channel_spec = make_spec(reply={}, destinations=[(name, (f"{name}-step",)) for name in names])
    r = rig(channel_spec=channel_spec, steps=(_node(pipeline, "PIPE_REJECT"),), resolve=table)
    if fail_reply:
        r.producer.fail_delivery_topics.add(REPLY)

    loop = r.run(sync_request())
    r.replies.poll()

    replies = r.producer.replies()
    assert len(replies) == 1, "exactly one reply per sync record"
    headers = replies[0].headers
    assert (headers[h.TB_REPLY_STATUS], headers.get(h.TB_ERROR_CODE)) == expected_outcome(pipeline, destinations)

    ops = r.producer.ops
    reply_at = ops.index(f"produce:{REPLY}")
    others = [index for index, op in enumerate(ops) if op.startswith("produce:") and op != f"produce:{REPLY}"]
    if others:
        assert max(others) < reply_at, "nothing is produced for the message after its answer"
        assert "flush" in ops[max(others) : reply_at], "the answer follows the out/dlq delivery callbacks"
    for record in r.producer.produced:
        if record.topic != REPLY:
            assert not {h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE, h.TB_REPLY_STATUS} & set(record.headers)

    assert loop.stats.retried == 0, "a reply, delivered or not, never retries the message"
    assert loop.stats.handled + loop.stats.dead_lettered == 1
    if pipeline in ("reject", "raise"):
        assert r.producer.records(DLQ), "a pipeline failure is dead-lettered before it is answered"
    events = [event for event in r.emitter.events if event.stage == "replied"]
    assert len(events) == 1
    if fail_reply:
        assert (events[0].status, events[0].error_code) == ("error", PRODUCE_FAILED)
