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
from tributary.receivers.backpressure import ByteBudget
from tributary.receivers.metrics import Metrics
from tributary.receivers.reply.feed import ReplyRecord
from tributary.receivers.reply.pending import Pending, PendingMap
from tributary.receivers.reply.settings import ReplySettings
from tributary.runtime.sync_answer import Resolution, SyncRequest, answer_for_reply
from tributary.sdk.ids import uuid7

from tests.support.sync_reply_oracle import (
    BROKEN_REQUESTS as SHARED_BROKEN_REQUESTS,
)
from tests.support.sync_reply_oracle import (
    LastReplyWins,
    drive,
    revoke_gaps,
    traces,
    violations,
)

from .support import FakeClock, FakePartitions

LOOP = asyncio.new_event_loop()  # never run: futures and `call_later` handles are created, nothing is awaited
atexit.register(LOOP.close)

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


BROKEN_REQUESTS = [b for b in SHARED_BROKEN_REQUESTS if b is not LastReplyWins]


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
