"""The pending map with a fake clock and a fake partition source (no Kafka, no sockets).

Every answer asserted here is `SyncRequest`'s (the runtime's pure state machine, already property-tested); what
these tests pin is the map around it: admission, the reply clock, matching, `body: step` accounting, revoke and
disconnect, and that no answer precedes the receiver's own `raw` callback."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import replace

import pytest
from tributary.receivers.backpressure import ByteBudget
from tributary.receivers.reply.pending import Outcome
from tributary.receivers.reply.settings import ReplySettings
from tributary.runtime.sync_answer import Resolution
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_MISSING, REPLY_BUSY, REPLY_ROUTE_LOST, REPLY_TIMEOUT, REPLY_UNAVAILABLE

from .support import CONTROL_ID_MARKER, FakePartitions, new_map, open_entry, record


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    event_loop = asyncio.new_event_loop()
    yield event_loop
    event_loop.close()


def test_aa_needs_the_raw_callback_and_an_accepted_reply(loop: asyncio.AbstractEventLoop) -> None:
    pending, clock, metrics, _ = new_map(loop)
    entry = open_entry(pending)
    assert pending.deliver_reply(record(entry)) == (True, None), "a reply before the callback is held, never answered"
    assert not entry.outcome.done()
    assert clock.live() == [], "the reply clock has not started"
    assert pending.commit(entry, ok=True) == Resolution("AA")
    assert entry.outcome.result().resolution == Resolution("AA")
    assert len(pending) == 0 and metrics.reply_pending == 0


def test_the_reply_clock_starts_at_the_callback_and_expiry_answers_ae_reply_timeout(
    loop: asyncio.AbstractEventLoop,
) -> None:
    pending, clock, _, _ = new_map(loop, settings=ReplySettings(timeout_s=3.0))
    entry = open_entry(pending)
    assert clock.timers == [], "nothing scheduled before the raw callback"
    assert pending.commit(entry, ok=True) is None
    assert [t.delay for t in clock.live()] == [3.0]
    clock.fire_all()
    assert entry.outcome.result().resolution == Resolution("AE", REPLY_TIMEOUT)
    assert len(pending) == 0


def test_a_failed_commit_answers_ar_and_never_starts_a_clock(loop: asyncio.AbstractEventLoop) -> None:
    pending, clock, _, _ = new_map(loop)
    entry = open_entry(pending)
    assert pending.commit(entry, ok=False) == Resolution("AR")
    assert clock.timers == []


def test_a_reply_after_settlement_is_unmatched_and_dropped(loop: asyncio.AbstractEventLoop) -> None:
    pending, clock, _, _ = new_map(loop)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    clock.fire_all()  # timed out
    assert pending.deliver_reply(record(entry)) == (False, None)
    assert entry.outcome.result().resolution == Resolution("AE", REPLY_TIMEOUT), "a late reply changes nothing"


def test_a_reply_with_a_different_msg_id_or_correlation_id_is_dropped(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, _, _ = new_map(loop)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    wrong_msg = replace(record(entry), msg_id="01980000-0000-7000-8000-000000000000")
    assert pending.deliver_reply(wrong_msg)[0] is False
    foreign = replace(record(entry), correlation_id="11111111-1111-4111-8111-111111111111")
    assert pending.deliver_reply(foreign)[0] is False
    assert not entry.outcome.done()


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        ("accepted", None, Resolution("AA")),
        ("rejected", "LAB_QUERY_UNSUPPORTED", Resolution("AE", "LAB_QUERY_UNSUPPORTED")),
        ("failed", "TRANSFORM_ERROR", Resolution("AE", "TRANSFORM_ERROR")),
        (None, None, Resolution("AE", None)),  # a set 1.0-shaped reply: never AA
        ("partially-accepted", None, Resolution("AE", None)),  # a newer producer's status: tolerated, not trusted
    ],
)
def test_status_to_answer(
    loop: asyncio.AbstractEventLoop, status: str | None, code: str | None, expected: Resolution
) -> None:
    pending, _, _, _ = new_map(loop)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    assert pending.deliver_reply(record(entry, status=status, code=code)) == (True, expected)


@pytest.mark.parametrize("hostile", ["AE|MSA", "lower_case", "X" * 65, "A B", "CODE\r", ""])
def test_a_malformed_reply_code_never_reaches_the_sender(loop: asyncio.AbstractEventLoop, hostile: str) -> None:
    """The code lands in MSA-3: only a registered-shape code passes (envelope 2.3.3: tolerated, never trusted)."""
    pending, _, _, _ = new_map(loop)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    matched, resolution = pending.deliver_reply(record(entry, status="rejected", code=hostile))
    assert matched and resolution == Resolution("AE", None)


def test_the_first_reply_counts(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, _, _ = new_map(loop)
    entry = open_entry(pending)
    pending.deliver_reply(record(entry, status="rejected", code="FIRST_CODE"))
    pending.deliver_reply(record(entry, status="accepted"))
    assert pending.commit(entry, ok=True) == Resolution("AE", "FIRST_CODE")


# -- admission ---------------------------------------------------------------------------------------


def test_no_reply_partition_refuses_before_anything_is_opened(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, metrics, _ = new_map(loop, partitions=FakePartitions())
    assert pending.open(sender_control_id=CONTROL_ID_MARKER, connection=object()) == REPLY_UNAVAILABLE
    assert len(pending) == 0 and metrics.nacked_reply_unavailable == 1


def test_max_pending_refuses_with_reply_busy_and_a_slot_frees_on_settlement(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, metrics, _ = new_map(loop, settings=ReplySettings(max_pending=2))
    first, second = open_entry(pending), open_entry(pending)
    assert pending.open(sender_control_id="c", connection=object()) == REPLY_BUSY
    assert len(pending) == 2 and metrics.nacked_reply_busy == 1
    pending.commit(first, ok=False)
    assert not isinstance(pending.open(sender_control_id="c", connection=object()), str)
    assert len(pending) == 2 and not second.outcome.done()


def test_a_live_update_changes_the_cap_for_the_next_request(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, _, _ = new_map(loop, settings=ReplySettings(max_pending=1))
    open_entry(pending)
    assert pending.open(sender_control_id="c", connection=object()) == REPLY_BUSY
    pending.configure(ReplySettings(max_pending=5))
    assert not isinstance(pending.open(sender_control_id="c", connection=object()), str)


# -- revoke, disconnect -------------------------------------------------------------------------------


def test_revoke_answers_route_lost_only_after_the_callback(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, _, _ = new_map(loop, partitions=FakePartitions(1))
    entry = open_entry(pending)
    assert pending.partitions_revoked(frozenset({1})) == [], "AE would claim a commit that may still fail"
    assert not entry.outcome.done()
    assert pending.commit(entry, ok=True) == Resolution("AE", REPLY_ROUTE_LOST)


def test_revoke_before_a_failed_callback_is_ar(loop: asyncio.AbstractEventLoop) -> None:
    pending, _, _, _ = new_map(loop, partitions=FakePartitions(1))
    entry = open_entry(pending)
    pending.partitions_revoked(frozenset({1}))
    assert pending.commit(entry, ok=False) == Resolution("AR")


def test_revoke_after_the_callback_answers_at_once_and_leaves_other_partitions_alone(
    loop: asyncio.AbstractEventLoop,
) -> None:
    partitions = FakePartitions(1)
    pending, clock, _, _ = new_map(loop, partitions=partitions)
    moved = open_entry(pending)
    partitions.held = [2]
    kept = open_entry(pending)
    pending.commit(moved, ok=True)
    pending.commit(kept, ok=True)
    assert pending.partitions_revoked(frozenset({1})) == [Resolution("AE", REPLY_ROUTE_LOST)]
    assert not kept.outcome.done() and len(pending) == 1
    assert [t.cancelled for t in clock.timers] == [True, False], "the settled entry's timer is cancelled"


def test_a_disconnected_sender_is_abandoned_not_answered(loop: asyncio.AbstractEventLoop) -> None:
    pending, clock, _, _ = new_map(loop)
    gone, stays = object(), object()
    a, b = open_entry(pending, connection=gone), open_entry(pending, connection=stays)
    pending.commit(a, ok=True)
    pending.abandon(gone)
    assert a.outcome.result().resolution == Resolution(None)
    assert len(pending) == 1 and not b.outcome.done()
    pending.abandon_all()
    assert len(pending) == 0 and len(clock.live()) == 0


# -- body: step ----------------------------------------------------------------------------------------

BODY = b"MSH|^~\\&|ENGINE|LAB|TRIBUTARY-SYNTH|LAB|20260928||RSP^K11|ACK-1|P|2.5.1\rMSA|AA|X\r"


def test_a_step_body_is_charged_to_the_budget_until_the_answer_is_written(loop: asyncio.AbstractEventLoop) -> None:
    budget = ByteBudget(10_000)
    pending, _, _, _ = new_map(loop, settings=ReplySettings(body="step"), budget=budget)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    pending.deliver_reply(record(entry, body=BODY))
    assert budget.used_bytes == len(BODY)
    outcome = entry.outcome.result()
    assert outcome.body == BODY and outcome.resolution == Resolution("AA")
    assert outcome.take_charge() == len(BODY) and outcome.take_charge() == 0, "released once, idempotently"


def test_a_body_that_does_not_fit_the_budget_answers_ae_reply_busy(loop: asyncio.AbstractEventLoop) -> None:
    budget = ByteBudget(len(BODY) - 1)
    pending, _, _, _ = new_map(loop, settings=ReplySettings(body="step"), budget=budget)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    matched, resolution = pending.deliver_reply(record(entry, body=BODY))
    assert matched and resolution == Resolution("AE", REPLY_BUSY), "committed, so AE; the body is not held"
    assert budget.used_bytes == 0


def test_waiting_bodies_count_against_the_same_budget_as_one_another(loop: asyncio.AbstractEventLoop) -> None:
    budget = ByteBudget(len(BODY) * 2)
    pending, _, _, _ = new_map(loop, settings=ReplySettings(body="step"), budget=budget)
    answers = []
    for _ in range(3):
        entry = open_entry(pending)
        pending.commit(entry, ok=True)
        answers.append(pending.deliver_reply(record(entry, body=BODY))[1])
    assert answers == [Resolution("AA"), Resolution("AA"), Resolution("AE", REPLY_BUSY)]
    assert budget.used_bytes == len(BODY) * 2


@pytest.mark.parametrize("framing_byte", [b"\x0b", b"\x1c"])
def test_a_body_that_would_break_mllp_framing_is_refused(loop: asyncio.AbstractEventLoop, framing_byte: bytes) -> None:
    budget = ByteBudget(10_000)
    pending, _, _, _ = new_map(loop, settings=ReplySettings(body="step"), budget=budget)
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    _, resolution = pending.deliver_reply(record(entry, body=b"MSH|x" + framing_byte + b"y"))
    assert resolution == Resolution("AE", REPLY_BODY_MISSING) and budget.used_bytes == 0


def test_a_body_is_ignored_in_ack_mode_and_for_a_refusal(loop: asyncio.AbstractEventLoop) -> None:
    budget = ByteBudget(10_000)
    pending, _, _, _ = new_map(loop, budget=budget)  # body: ack
    entry = open_entry(pending)
    pending.commit(entry, ok=True)
    pending.deliver_reply(record(entry, body=BODY))
    assert budget.used_bytes == 0 and entry.outcome.result().body == b""
    stepped, _, _, budget2 = new_map(loop, settings=ReplySettings(body="step"), budget=ByteBudget(10_000))
    other = open_entry(stepped)
    stepped.commit(other, ok=True)
    stepped.deliver_reply(record(other, status="rejected", code="LAB_QUERY_UNSUPPORTED", body=BODY))
    assert budget2.used_bytes == 0, "a rejected reply's body is never held"


def test_a_held_body_is_returned_when_the_commit_then_fails(loop: asyncio.AbstractEventLoop) -> None:
    budget = ByteBudget(10_000)
    pending, _, _, _ = new_map(loop, settings=ReplySettings(body="step"), budget=budget)
    entry = open_entry(pending)
    pending.deliver_reply(record(entry, body=BODY))  # before the callback
    assert budget.used_bytes == len(BODY)
    assert pending.commit(entry, ok=False) == Resolution("AR")
    outcome: Outcome = entry.outcome.result()
    assert outcome.take_charge() == len(BODY), "the writer hands it back even though the answer is AR"
    assert outcome.body == b""


# -- routing headers ----------------------------------------------------------------------------------


def test_the_routing_headers_carry_ids_and_a_deadline_never_the_control_id(loop: asyncio.AbstractEventLoop) -> None:
    from datetime import UTC, datetime

    pending, _, _, _ = new_map(loop, settings=ReplySettings(timeout_s=7.0), partitions=FakePartitions(2))
    entry = open_entry(pending)
    now = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)
    headers = entry.routing_headers(ack_timeout_s=10.0, now=now)
    assert set(headers) == {h.TB_CORRELATION_ID, h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE}
    assert headers[h.TB_REPLY_PARTITION] == "2"
    assert headers[h.TB_REPLY_DEADLINE] == "2026-10-01T12:00:17.000Z", "produce + ack.timeoutMs + reply.timeoutMs"
    assert CONTROL_ID_MARKER not in "".join(headers.values())
    assert h.is_well_formed(h.TB_CORRELATION_ID, headers[h.TB_CORRELATION_ID])
