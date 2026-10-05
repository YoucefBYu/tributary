"""W2-8 answer property, run against the receiver's real pending map.

`packages/tributary-runtime/tests/test_reply_properties.py` proves the answer rules on `SyncRequest`, a pure
model, and says so: it holds for a receiver only if the receiver delegates to `SyncRequest` or the property is
run against an adapter of the receiver's own entry. This is that adapter. `MapEntry` drives one
`tributary.receivers.reply.pending.PendingMap` entry through the same six events (the raw callback succeeding
or failing, a reply, the timer, a revoke, a disconnect), with a fake clock for the timer, and `violations`
(mirrored below from the runtime's test) must find nothing over random interleavings: exactly one resolution
per request, neither `AA` nor `AE` before the successful raw callback, `AA` only for a first `accepted` reply,
and the answer the independent oracle derives from its cause.

The strategy, oracle and broken `SyncRequest`s are a **copy** of the runtime test's (importing a test module
across packages is not possible under `--import-mode=importlib`; a shared `tests/support` module would remove
the copy, which is a request to `platform-engineer`). `test_the_mirror_agrees_with_the_model` runs both traces
against the model so a drift in this copy shows as a failure here rather than as a quiet divergence.

Controls (a property that cannot go red is not evidence): the seven broken `SyncRequest`s injected as the
map's request factory must each be caught **through the map**, and three bugs of the map itself, one that never arms
the reply clock, one that ignores a revoke and one that answers on the reply alone, must be caught
too, because they are bugs a correct `SyncRequest` cannot hide.
"""

from __future__ import annotations

import asyncio
import atexit
from collections.abc import Callable

import pytest
from hypothesis import find, given, settings
from hypothesis import strategies as st
from tributary.receivers.backpressure import ByteBudget
from tributary.receivers.metrics import Metrics
from tributary.receivers.reply.feed import ReplyRecord
from tributary.receivers.reply.pending import Pending, PendingMap
from tributary.receivers.reply.settings import ReplySettings
from tributary.runtime.sync_answer import Resolution, SyncRequest, answer_for_reply
from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT, TRANSFORM_ERROR
from tributary.sdk.ids import uuid7

from .support import FakeClock, FakePartitions

LOOP = asyncio.new_event_loop()  # never run: futures and `call_later` handles are created, nothing is awaited
atexit.register(LOOP.close)

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
    noise = draw(st.lists(st.sampled_from(NOISE), max_size=8))
    commit = draw(st.sampled_from(["commit_ok", "commit_failed"]))
    position = draw(st.integers(min_value=0, max_value=len(noise)))
    return [*noise[:position], commit, *noise[position:], "timeout"]


# -- the adapter ---------------------------------------------------------------------------------------


class MapEntry:
    """One entry of a real `PendingMap`, seen through the six events of `SyncRequest`'s interface."""

    def __init__(
        self,
        request_factory: Callable[[], SyncRequest] = SyncRequest,
        map_class: type[PendingMap] = PendingMap,
    ) -> None:
        self.clock = FakeClock()
        self.connection = object()
        self.map = map_class(
            ReplySettings(),
            feed=FakePartitions(0),
            loop=LOOP,
            budget=ByteBudget(1_000_000),
            metrics=Metrics(),
            schedule=self.clock.schedule,
            request_factory=request_factory,
        )
        opened = self.map.open(sender_control_id="SYNTH-CTRL", connection=self.connection)
        assert isinstance(opened, Pending)
        self.entry = opened
        self.entry.bind(str(uuid7()))

    def _settled(self, was_done: bool) -> Resolution | None:
        if was_done or not self.entry.outcome.done():
            return None
        return self.entry.outcome.result().resolution

    def commit_succeeded(self) -> Resolution | None:
        was_done = self.entry.outcome.done()
        self.map.commit(self.entry, ok=True)
        return self._settled(was_done)

    def commit_failed(self) -> Resolution | None:
        was_done = self.entry.outcome.done()
        self.map.commit(self.entry, ok=False)
        return self._settled(was_done)

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        was_done = self.entry.outcome.done()
        record = ReplyRecord(
            partition=0,
            correlation_id=self.entry.correlation_id,
            msg_id=self.entry.msg_id or "",
            status=status,
            code=code,
            body=b"",
        )
        self.map.deliver_reply(record)
        return self._settled(was_done)

    def reply_timed_out(self) -> Resolution | None:
        was_done = self.entry.outcome.done()
        self.clock.fire_all()  # fires only a timer the map armed; before the callback there is none
        return self._settled(was_done)

    def partition_revoked(self) -> Resolution | None:
        was_done = self.entry.outcome.done()
        self.map.partitions_revoked(frozenset({self.entry.partition}))
        return self._settled(was_done)

    def disconnected(self) -> Resolution | None:
        was_done = self.entry.outcome.done()
        self.map.abandon(self.connection)
        return self._settled(was_done)


# -- the mirror of the runtime's oracle ----------------------------------------------------------------


def drive(request: MapEntry | SyncRequest, event: str) -> Resolution | None:
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
    if cause == "disconnect":
        return Resolution(answer=None)
    if cause == "commit_failed":
        return Resolution("AR")
    if cause == "timeout":
        return Resolution("AE", REPLY_TIMEOUT)
    if cause == "revoke":
        return Resolution("AE", REPLY_ROUTE_LOST)
    if first_reply is not None:
        status, code = REPLIES[first_reply]
        return Resolution("AA") if status == "accepted" else Resolution("AE", code)
    if cause == "commit_ok" and revoked:
        return Resolution("AE", REPLY_ROUTE_LOST)
    return None


def violations(trace: list[str], factory: Callable[[], MapEntry | SyncRequest]) -> list[str]:
    request = factory()
    found: list[str] = []
    resolutions: list[Resolution] = []
    committed = disconnected = revoked = False
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
            found.append("AR after a confirmed commit")
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


# -- the property --------------------------------------------------------------------------------------


@given(traces())
@settings(max_examples=2000, deadline=None)
def test_the_real_pending_map_answers_once_and_never_ahead_of_the_commit(trace: list[str]) -> None:
    assert violations(trace, MapEntry) == []


@given(traces())
@settings(max_examples=500, deadline=None)
def test_the_mirror_agrees_with_the_model(trace: list[str]) -> None:
    assert violations(trace, SyncRequest) == []


@given(traces())
@settings(max_examples=500, deadline=None)
def test_a_settled_entry_leaves_the_map_and_its_timer_is_cancelled(trace: list[str]) -> None:
    rig = MapEntry()
    for event in trace:
        drive(rig, event)
    assert len(rig.map) == 0, "every trace ends in a resolution, so no entry may outlive it"
    assert rig.clock.live() == [], "no timer outlives its entry"


# -- controls: the broken SyncRequests (copied from the runtime test), caught through the map -----------


class AnswersOnReplyAlone(SyncRequest):
    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = answer_for_reply(status, code)
        return self.resolution


class AnswersRevokeEagerly(SyncRequest):
    def partition_revoked(self) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = Resolution("AE", REPLY_ROUTE_LOST)
        return self.resolution


class TrustsAbsentStatus(SyncRequest):
    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        return super().reply_received("accepted" if status is None else status, code)


class LastReplyWins(SyncRequest):
    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved or self._committed:
            return super().reply_received(status, code)
        self._reply = None
        return super().reply_received(status, code)


class SwapsAeCodes(SyncRequest):
    def reply_timed_out(self) -> Resolution | None:
        resolved = super().reply_timed_out()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_ROUTE_LOST))

    def partition_revoked(self) -> Resolution | None:
        resolved = super().partition_revoked()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_TIMEOUT))


class TrustsUnrecognisedStatus(SyncRequest):
    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        trusted = "accepted" if status not in (None, "rejected", "failed") else status
        return super().reply_received(trusted, code)


class AnswersHeldReplyOnFailedCommit(SyncRequest):
    def commit_failed(self) -> Resolution | None:
        if not self.resolved and self._committed is None and self._reply is not None:
            self._committed = False
            return self._resolve(answer_for_reply(self._reply.status, self._reply.code))
        return super().commit_failed()


#: `LastReplyWins` is not here: the map forwards only a request's first reply (`reply_seen`), so that bug of the
#: model is invisible through it. The guard is what holds the rule; `ForwardsEveryReply` below is its control.
BROKEN_REQUESTS = [
    AnswersOnReplyAlone,
    AnswersRevokeEagerly,
    TrustsAbsentStatus,
    SwapsAeCodes,
    TrustsUnrecognisedStatus,
    AnswersHeldReplyOnFailedCommit,
]


@pytest.mark.parametrize("broken", BROKEN_REQUESTS)
def test_each_broken_request_is_caught_through_the_map(broken: type[SyncRequest]) -> None:
    def factory() -> MapEntry:
        return MapEntry(request_factory=broken)

    counterexample = find(
        traces(), lambda trace: bool(violations(trace, factory)), settings=settings(max_examples=5000)
    )
    assert violations(counterexample, factory)


# -- controls: bugs of the map itself ------------------------------------------------------------------


class NeverArmsTheClock(PendingMap):
    """No reply clock at all: a request whose reply never comes would wait forever."""

    def commit(self, entry: Pending, *, ok: bool) -> Resolution | None:
        resolution = entry.request.commit_succeeded() if ok else entry.request.commit_failed()
        return self._settle(entry, resolution)


class IgnoresRevoke(PendingMap):
    """A revoke does not reach the entries advertised on the lost partition (they wait out the timeout)."""

    def partitions_revoked(self, partitions: frozenset[int]) -> list[Resolution]:
        return []


class ForwardsEveryReply(PendingMap):
    """No first-reply guard: every reply reaches the request (with `LastReplyWins`, the last one counts)."""

    def deliver_reply(self, record: ReplyRecord) -> tuple[bool, Resolution | None]:
        entry = self._by_id.get(record.correlation_id)
        if entry is None or entry.msg_id != record.msg_id:
            return False, None
        return True, self._settle(entry, entry.request.reply_received(record.status, record.code))


class SettlesOnTheReplyAlone(PendingMap):
    """Resolves the entry from the reply without consulting the raw callback."""

    def deliver_reply(self, record: ReplyRecord) -> tuple[bool, Resolution | None]:
        entry = self._by_id.get(record.correlation_id)
        if entry is None:
            return False, None
        return True, self._settle(entry, answer_for_reply(record.status, record.code))


@pytest.mark.parametrize("broken_map", [NeverArmsTheClock, SettlesOnTheReplyAlone])
def test_each_broken_map_is_caught(broken_map: type[PendingMap]) -> None:
    def factory() -> MapEntry:
        return MapEntry(map_class=broken_map)

    counterexample = find(
        traces(), lambda trace: bool(violations(trace, factory)), settings=settings(max_examples=5000)
    )
    assert violations(counterexample, factory)


def revoke_gaps(trace: list[str], factory: Callable[[], MapEntry]) -> list[str]:
    """After the callback, a revoke of the advertised partition answers `AE` / `REPLY_ROUTE_LOST` **at once**
    (the oracle only judges answers that were given, so a revoke that is ignored needs its own check)."""
    request, committed, settled = factory(), False, False
    found: list[str] = []
    for event in trace:
        resolution = drive(request, event)
        if resolution is not None:
            settled = True
        elif event == "revoke" and committed and not settled:
            found.append("a revoke after the callback did not answer")
        committed = committed or event == "commit_ok"
    return found


@given(traces())
@settings(max_examples=1000, deadline=None)
def test_a_revoke_after_the_callback_answers_at_once(trace: list[str]) -> None:
    assert revoke_gaps(trace, MapEntry) == []


def test_the_revoke_control_is_caught() -> None:
    def broken() -> MapEntry:
        return MapEntry(map_class=IgnoresRevoke)

    counterexample = find(
        traces(), lambda trace: bool(revoke_gaps(trace, broken)), settings=settings(max_examples=5000)
    )
    assert revoke_gaps(counterexample, broken)


def test_the_first_reply_guard_is_what_holds_the_rule() -> None:
    def broken() -> MapEntry:
        return MapEntry(request_factory=LastReplyWins, map_class=ForwardsEveryReply)

    counterexample = find(traces(), lambda trace: bool(violations(trace, broken)), settings=settings(max_examples=5000))
    assert violations(counterexample, broken)
    assert violations(["reply_rejected", "reply_accepted", "commit_ok", "timeout"], MapEntry) == []


@pytest.mark.parametrize(
    ("broken_map", "trace"),
    [
        (NeverArmsTheClock, ["commit_ok", "timeout"]),
        (SettlesOnTheReplyAlone, ["reply_accepted", "commit_failed", "timeout"]),
    ],
)
def test_the_map_controls_fail_on_their_minimal_traces_and_the_real_map_passes_them(
    broken_map: type[PendingMap], trace: list[str]
) -> None:
    assert violations(trace, lambda: MapEntry(map_class=broken_map))
    assert violations(trace, MapEntry) == []
