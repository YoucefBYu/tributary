"""W2-8 property tests (proposal section 7, extended to `AE` by the architect's amendment).

**Scope: this proves the rules on the model, not on a receiver.** The answer property runs against
`tributary.runtime.sync_answer.SyncRequest`, a pure state machine. It fixes what the answers must be. It
does not show that any receiver gives them: that holds only for a receiver that delegates to
`SyncRequest`, or one this property is run against (`violations` takes a factory, so a receiver's
pending-map entry can be adapted to the same six events) with a broken-receiver control that goes red.
Until then the invariant-1 risk row for W2-8 stays open.

**The oracle is shared (W3-20).** `traces`, `violations`, the oracle and the seven broken controls live in
`tests/support/sync_reply_oracle.py`, so this suite and the receivers' suite judge with one copy of the
rules. Its own controls are in `tests/support/test_sync_reply_oracle.py`.

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

from typing import Any

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st
from tributary.runtime.reply import PRODUCE_FAILED
from tributary.runtime.sync_answer import Resolution, SyncRequest
from tributary.sdk import Drop, Message, Reject, step
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT, TRANSFORM_ERROR

from tests.support.sync_reply_oracle import (
    BROKEN_REQUESTS,
    AnswersHeldReplyOnFailedCommit,
    LastReplyWins,
    SwapsAeCodes,
    TrustsUnrecognisedStatus,
    traces,
    violations,
)

from .reply_harness import DLQ, REPLY, make_spec, rig, sync_request

# -- the answer state machine ----------------------------------------------------------------------


@given(traces())
@settings(max_examples=2000, deadline=None)
def test_every_request_gets_exactly_one_answer_and_never_ahead_of_the_commit(trace: list[str]) -> None:
    assert violations(trace, SyncRequest) == []


@pytest.mark.parametrize("broken", BROKEN_REQUESTS)
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
