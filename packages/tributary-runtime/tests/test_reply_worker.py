"""The worker reply side of synchronous reply mode, with no Kafka (channel-spec README 3.2.6, W2-8).

What is pinned here, each against the contract rule it comes from:

* the record decides: a reply iff the `raw` record carries `tb-reply-partition`;
* the reply follows every `out` delivery callback, and the `dlq` callback when there was one;
* status rules: `Reject` -> `rejected`, other failures -> `failed`, whole-message `Drop` -> `accepted`,
  partial failure -> the first failure in **spec order**;
* fail-fast: a lost reply neither raises, nor retries, nor fails the next record's flush;
* a bad routing header disables the reply, not the message (envelope 2.3.3);
* expired, transactional and split records produce no reply and are counted; an expired one is also a
  `replied` / `dropped` event;
* an unknown or stale `reply` partition count leaves a route unverified (reply attempted), never
  `REPLY_ROUTE_INVALID`, and metadata is never fetched on the consumer thread;
* `ctx.reply`: pipeline steps only, a failed attempt's body discarded, `REPLY_BODY_MISSING`;
* PHI: neither the record key nor MSH-10 reaches a reply header, the reply key, a log line or an event.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.events import MessageEvent
from tributary.runtime.reply import (
    ACCEPTED,
    EXPIRED_MESSAGE,
    PIPELINE_RANK,
    FailureLedger,
    ReplyOutcome,
    ReplyPartitionCount,
    ReplyRoute,
    RouteInvalid,
    destination_rank,
    parse_route,
)
from tributary.runtime.worker import dlq_message, out_message
from tributary.sdk import Drop, Message, Reject, splitter, step
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_MISSING, REPLY_ROUTE_INVALID, TRANSFORM_ERROR
from tributary.sdk.projection import record_projection

from .reply_harness import (
    CONTRACTS,
    CONTROL_ID_MARKER,
    DLQ,
    KEY_MARKER,
    NOW,
    REPLY,
    Rig,
    make_spec,
    rig,
    sync_request,
)

OUT = "dev.adt-passthrough.out.http-stub"
HL7 = "application/hl7-v2+er7"
EVENT_SCHEMA = json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
ACK_BODY = (
    f"MSH|^~\\&|ENGINE|LAB|TRIBUTARY-SYNTH|LAB|20260928||ACK^Q11|ACK-1|P|2.5.1\rMSA|AA|{CONTROL_ID_MARKER}\r".encode()
)


@step
def passthrough(message: Message) -> None:
    return None


@step
def drop_all(message: Message) -> Drop:
    return Drop(reason="filtered on purpose")


@step
def reject_lab(message: Message) -> None:
    raise Reject("LAB_QUERY_UNSUPPORTED")


@step
def explode(message: Message) -> None:
    raise ValueError("SYNTH^JANE mrn 12345")


@step
def answer(message: Message, ctx: Any) -> None:
    ctx.reply(ACK_BODY, content_type=HL7)


@step
def reply_in_destination(message: Message, ctx: Any) -> None:
    ctx.reply(ACK_BODY, content_type=HL7)


def validator(name: str) -> Draft202012Validator:
    return Draft202012Validator(json.loads((CONTRACTS / name).read_text(encoding="utf-8")))


def replied(r: Rig) -> list[MessageEvent]:
    return [event for event in r.emitter.events if isinstance(event, MessageEvent) and event.stage == "replied"]


def reply_headers(r: Rig) -> dict[str, str]:
    (reply,) = r.producer.replies()
    return reply.headers


# -- the record decides ----------------------------------------------------------------------------


def test_an_async_record_gets_no_reply_even_on_a_sync_spec() -> None:
    r = rig(steps=(passthrough,))
    r.run(sync_request(partition=None))
    assert r.producer.records(OUT), "the message is routed"
    assert r.producer.replies() == []
    assert replied(r) == []


def test_a_sync_record_gets_a_reply_even_after_a_hot_reload_to_async() -> None:
    """The record, not the spec, decides: a spec without `reply` still answers a record that asks."""
    r = rig(channel_spec=make_spec(reply=None), steps=(passthrough,))
    r.run(sync_request())
    assert reply_headers(r)[h.TB_REPLY_STATUS] == "accepted"


def test_an_accepted_reply_lands_on_the_named_partition_after_the_out_callback() -> None:
    request = sync_request(partition="2")
    r = rig(steps=(passthrough,))
    r.run(request)

    (reply,) = r.producer.replies()
    assert reply.partition == 2
    assert reply.key == request.require(h.TB_CORRELATION_ID).encode()
    assert reply.value == b""
    assert reply.headers[h.TB_REPLY_STATUS] == "accepted"
    assert h.TB_ERROR_CODE not in reply.headers
    assert reply.headers[h.TB_CONTENT_TYPE] == "application/octet-stream"
    assert reply.headers[h.TB_MSG_ID] == request.msg_id
    assert reply.headers[h.TB_RECEIVED_AT] == request.received_at, "copied unchanged (envelope 6 rule 1)"
    ops = r.producer.ops
    assert ops.index(f"produce:{REPLY}") > ops.index(f"produce:{OUT}")
    assert "flush" in ops[ops.index(f"produce:{OUT}") : ops.index(f"produce:{REPLY}")], (
        "AA means routed: the out record's delivery callback comes first"
    )
    (event,) = replied(r)
    assert event.status == "ok" and event.error_code is None
    assert event.correlation_id == request.correlation_id


def test_the_reply_record_and_its_event_satisfy_the_contracts() -> None:
    request = sync_request()
    r = rig(steps=(reject_lab,))
    r.run(request)
    reply = r.producer.replies()[0]
    message = Message(headers=reply.headers, payload=reply.value, key=reply.key)
    projection = record_projection(message, topic_class="reply", topic=REPLY)
    assert list(validator("envelope/v0/envelope.schema.json").iter_errors(projection)) == []
    events = validator("events/v0/event.schema.json")
    for event in replied(r):
        assert list(events.iter_errors(event.to_dict())) == []


def test_routing_headers_never_leave_raw() -> None:
    """Envelope 6 rule 7: a reprocessed out or dlq record must never answer on an unwatched partition."""
    request = sync_request()
    out = out_message(request, "http-stub", usr={})
    dead = dlq_message(request, error_code=TRANSFORM_ERROR, stage="transformed")
    for projected in (out, dead):
        assert not {h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE, h.TB_REPLY_STATUS} & set(projected.headers)
        assert projected.correlation_id == request.correlation_id, "the correlation id still propagates"


# -- status rules ----------------------------------------------------------------------------------


def test_a_pipeline_reject_answers_rejected_after_the_dlq_callback() -> None:
    r = rig(steps=(reject_lab,))
    r.run(sync_request())
    headers = reply_headers(r)
    assert headers[h.TB_REPLY_STATUS] == "rejected"
    assert headers[h.TB_ERROR_CODE] == "LAB_QUERY_UNSUPPORTED"
    ops = r.producer.ops
    assert ops.index(f"produce:{REPLY}") > ops.index(f"produce:{DLQ}")
    assert "flush" in ops[ops.index(f"produce:{DLQ}") : ops.index(f"produce:{REPLY}")]


def test_an_exhausted_step_answers_failed_with_transform_error() -> None:
    r = rig(steps=(explode,))
    r.run(sync_request())
    headers = reply_headers(r)
    assert headers[h.TB_REPLY_STATUS] == "failed"
    assert headers[h.TB_ERROR_CODE] == TRANSFORM_ERROR


def test_a_whole_message_drop_answers_accepted() -> None:
    r = rig(steps=(drop_all,))
    r.run(sync_request())
    assert reply_headers(r)[h.TB_REPLY_STATUS] == "accepted"
    assert r.producer.records(OUT) == []


def test_a_header_reject_answers_failed_with_header_parse_error() -> None:
    request = sync_request(extra={h.TB_ATTEMPT: "1_0"})
    r = rig(steps=(passthrough,))
    r.run(request)
    headers = reply_headers(r)
    assert (headers[h.TB_REPLY_STATUS], headers[h.TB_ERROR_CODE]) == ("failed", "HEADER_PARSE_ERROR")


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("reject_lab", "explode", ("rejected", "LAB_QUERY_UNSUPPORTED")),
        ("explode", "reject_lab", ("failed", TRANSFORM_ERROR)),
        ("passthrough", "reject_lab", ("rejected", "LAB_QUERY_UNSUPPORTED")),
        ("passthrough", "drop_all", ("accepted", None)),
    ],
)
def test_a_partial_failure_answers_with_the_first_failure_in_spec_order(
    first: str, second: str, expected: tuple[str, str | None]
) -> None:
    steps = {"passthrough": passthrough, "reject_lab": reject_lab, "explode": explode, "drop_all": drop_all}
    channel_spec = make_spec(reply={}, destinations=(("lis", (first,)), ("archive", (second,))))
    r = rig(channel_spec=channel_spec, steps=(passthrough,), resolve=steps)
    r.run(sync_request())
    headers = reply_headers(r)
    assert (headers[h.TB_REPLY_STATUS], headers.get(h.TB_ERROR_CODE)) == expected


def test_the_ledger_ranks_by_spec_order_not_arrival() -> None:
    ledger = FailureLedger()
    ledger.record(destination_rank(1), ReplyOutcome.failure("SECOND", rejected=False))
    ledger.record(destination_rank(0), ReplyOutcome.failure("FIRST", rejected=True))
    ledger.record(destination_rank(0), ReplyOutcome.failure("LATER_SAME_RANK", rejected=False))
    assert ledger.outcome() == ReplyOutcome("rejected", "FIRST")
    ledger.record(PIPELINE_RANK, ReplyOutcome.failure("PIPELINE", rejected=False))
    assert ledger.outcome().error_code == "PIPELINE"
    assert FailureLedger().outcome() == ACCEPTED


def test_an_outcome_cannot_be_built_inconsistent() -> None:
    with pytest.raises(ValueError):
        ReplyOutcome("accepted", "TRANSFORM_ERROR")
    with pytest.raises(ValueError):
        ReplyOutcome("failed", None)


# -- fail-fast -------------------------------------------------------------------------------------


def test_a_failed_reply_delivery_neither_raises_nor_fails_the_next_record() -> None:
    r = rig(steps=(passthrough,))
    r.producer.fail_delivery_topics.add(REPLY)
    first = r.run(sync_request(), offset=0)
    second = r.run(sync_request(), offset=1)  # its flush serves the failed callback; must not raise

    assert first.stats.handled == 1 and second.stats.handled == 1
    assert first.stats.retried == 0 and second.stats.retried == 0, "a lost reply never retries the message"
    assert len(r.producer.records(OUT)) == 2
    assert r.replies.stats.failed == 2
    assert all(event.status == "error" and event.error_code == "PRODUCE_FAILED" for event in replied(r))
    assert "_MSG_TIMED_OUT" in r.log.getvalue()
    assert "free text" not in r.log.getvalue(), "the error name only, never str(err)"


def test_a_refused_reply_produce_is_counted_and_not_retried() -> None:
    r = rig(steps=(passthrough,))
    r.producer.refuse_topics.add(REPLY)
    loop = r.run(sync_request())
    assert loop.stats.handled == 1 and loop.stats.retried == 0
    assert r.replies.stats.failed == 1
    assert [event.error_code for event in replied(r)] == ["PRODUCE_FAILED"]


def test_an_expired_reply_is_skipped_but_the_message_is_processed() -> None:
    past = NOW - timedelta(seconds=6)
    r = rig(steps=(passthrough,))
    r.run(sync_request(deadline=h.format_timestamp(past)))
    assert r.producer.records(OUT), "sync mode never cancels work"
    assert r.producer.replies() == []
    assert r.replies.stats.expired == 1
    (event,) = replied(r)
    assert (event.status, event.error_code) == ("dropped", None), "a deliberate skip: no code, like a Drop"
    assert event.error_message == EXPIRED_MESSAGE and event.correlation_id is not None
    projected = event.to_dict()
    assert list(Draft202012Validator(EVENT_SCHEMA).iter_errors(projected)) == [], "valid under events/v0"
    assert "MARKER" not in json.dumps(projected)


def test_the_skew_allowance_is_five_seconds() -> None:
    r = rig(steps=(passthrough,))
    r.run(sync_request(deadline=h.format_timestamp(NOW - timedelta(seconds=4))))
    assert len(r.producer.replies()) == 1


# -- routing headers: disable the reply, not the message (envelope 2.3.3) -------------------------


@pytest.mark.parametrize(
    ("partition", "deadline", "reason"),
    [
        ("64", None, "partition-malformed"),
        ("x", None, "partition-malformed"),
        ("02", None, "partition-malformed"),
        ("SYNTH-PART-MARKER", None, "partition-malformed"),
        ("5", None, "partition-out-of-range"),
        ("1", "SYNTH-DEADLINE-MARKER", "deadline-malformed"),
    ],
)
def test_a_bad_routing_header_disables_the_reply_not_the_message(
    partition: str, deadline: str | None, reason: str
) -> None:
    request = sync_request(partition=partition, deadline=deadline)
    assert parse_route(request, partition_count=3) == RouteInvalid(reason)
    r = rig(steps=(passthrough,))
    loop = r.run(request)
    assert loop.stats.dead_lettered == 0 and r.producer.records(DLQ) == []
    assert r.producer.records(OUT), "the message is processed normally"
    assert r.producer.replies() == []
    (event,) = replied(r)
    assert (event.status, event.error_code) == ("error", REPLY_ROUTE_INVALID)
    assert "MARKER" not in r.log.getvalue(), "a header value never reaches a log line"
    assert "MARKER" not in json.dumps(event.to_dict())


def test_a_deadline_without_a_partition_is_invalid_and_no_route_is_async() -> None:
    lone_deadline = sync_request(partition=None).with_header(h.TB_REPLY_DEADLINE, h.format_timestamp(NOW))
    assert parse_route(lone_deadline, partition_count=3) == RouteInvalid("deadline-without-partition")
    lone_partition = sync_request().without_header(h.TB_REPLY_DEADLINE)
    assert parse_route(lone_partition, partition_count=3) == RouteInvalid("partition-without-deadline")
    assert parse_route(sync_request(partition=None), partition_count=3) is None
    no_correlation = sync_request().without_header(h.TB_CORRELATION_ID)
    assert parse_route(no_correlation, partition_count=3) == RouteInvalid("correlation-id-invalid")


def test_an_unknown_partition_count_leaves_the_route_unverified_not_invalid() -> None:
    """W2-8 review: a transient metadata failure was reported as `REPLY_ROUTE_INVALID`, which says the
    *header* is wrong (envelope 2.3.3). Unknown proves nothing about the header, so the range check is
    skipped and the reply attempted; a partition that does not exist fails its produce instead."""
    route = parse_route(sync_request(partition="9"), partition_count=None)
    assert isinstance(route, ReplyRoute) and route.partition == 9
    assert parse_route(sync_request(partition="9"), partition_count=3) == RouteInvalid("partition-out-of-range")
    route = parse_route(sync_request(partition="2"), partition_count=3)
    assert isinstance(route, ReplyRoute) and route.partition == 2


def test_the_worker_answers_when_metadata_is_unavailable() -> None:
    r = rig(steps=(passthrough,), partitions=None)
    r.run(sync_request(partition="2"))
    (reply,) = r.producer.replies()
    assert reply.partition == 2
    assert [event.error_code for event in replied(r)] == [None]
    assert r.replies.stats.route_invalid == 0


def test_an_unverified_partition_that_does_not_exist_fails_as_produce_failed() -> None:
    r = rig(steps=(passthrough,), partitions=None)
    r.producer.refuse_topics.add(REPLY)  # what librdkafka does with an unknown partition
    r.run(sync_request(partition="9"))
    (event,) = replied(r)
    assert (event.status, event.error_code) == ("error", "PRODUCE_FAILED")
    assert r.replies.stats.route_invalid == 0


class _Topic:
    error = None

    def __init__(self, count: int) -> None:
        self.partitions = dict.fromkeys(range(count))


class _Metadata:
    def __init__(self, count: int) -> None:
        self.topics = {REPLY: _Topic(count)}


class _Deferred:
    """A `spawn` that queues the fetch, so a test controls exactly when the 'background' fetch runs."""

    def __init__(self) -> None:
        self.pending: list[Any] = []

    def __call__(self, target: Any) -> None:
        self.pending.append(target)

    def run_all(self) -> None:
        pending, self.pending = self.pending, []
        for target in pending:
            target()


def test_the_partition_count_is_fetched_off_the_callers_thread() -> None:
    calls: list[str] = []
    now = [0.0]
    spawn = _Deferred()

    def list_topics(*, topic: str, timeout: float) -> _Metadata:
        calls.append(topic)
        return _Metadata(3)

    count = ReplyPartitionCount(list_topics, REPLY, refresh_s=30.0, clock=lambda: now[0], spawn=spawn)
    assert count(1) is None and calls == [], "unknown: answered at once, fetched later"
    assert count(1) is None and len(spawn.pending) == 1, "one fetch in flight at most"
    spawn.run_all()
    assert calls == [REPLY]
    assert count(1) == 3 and count(2) == 3
    assert count(4) == 3, "a fresh count proves 4 out of range"
    assert spawn.pending == [], "fresh: nothing to fetch"


def test_prime_fetches_once_on_the_callers_thread_for_start_up() -> None:
    now = [0.0]
    spawn = _Deferred()
    calls: list[str] = []

    def list_topics(*, topic: str, timeout: float) -> _Metadata:
        calls.append(topic)
        return _Metadata(3)

    count = ReplyPartitionCount(list_topics, REPLY, refresh_s=30.0, clock=lambda: now[0], spawn=spawn)
    assert count.prime() == 3 and calls == [REPLY]
    assert count(9) == 3 and spawn.pending == [], "fresh from the start: 9 is provably out of range"

    def broken(*, topic: str, timeout: float) -> _Metadata:
        raise RuntimeError("synthetic metadata timeout")

    assert ReplyPartitionCount(broken, REPLY, spawn=spawn).prime() is None, "a failed prime is unknown"


def test_a_stale_count_is_refreshed_and_does_not_prove_a_miss() -> None:
    counts = iter([3, 6])
    now = [0.0]
    spawn = _Deferred()

    def list_topics(*, topic: str, timeout: float) -> _Metadata:
        return _Metadata(next(counts))

    count = ReplyPartitionCount(list_topics, REPLY, refresh_s=30.0, clock=lambda: now[0], spawn=spawn)
    count(0)
    spawn.run_all()
    now[0] = 31.0
    assert count(1) == 3, "below any known count is valid: Kafka cannot remove partitions"
    assert len(spawn.pending) == 1, "stale: a refresh started in the background"
    assert count(4) is None, "beyond a stale count: unverified (an operator may have added partitions)"
    assert len(spawn.pending) == 1, "still one in flight"
    spawn.run_all()
    assert count(4) == 6


def test_a_failing_fetch_leaves_routes_unverified_and_is_retried_once_per_interval() -> None:
    now = [0.0]
    calls: list[float] = []
    spawn = _Deferred()

    def broken(*, topic: str, timeout: float) -> _Metadata:
        calls.append(now[0])
        raise RuntimeError("synthetic metadata timeout")

    count = ReplyPartitionCount(broken, REPLY, refresh_s=30.0, clock=lambda: now[0], spawn=spawn)
    assert count(9) is None
    spawn.run_all()
    assert count(9) is None and spawn.pending == [], "no retry inside the interval: a flood is one request"
    now[0] = 30.0
    assert count(9) is None and len(spawn.pending) == 1
    spawn.run_all()
    assert calls == [0.0, 30.0]


def test_a_blocked_metadata_request_does_not_block_the_caller() -> None:
    """The default spawn is a real thread: a `list_topics` stuck for its whole timeout costs the caller
    nothing (it used to run on the consumer thread with a 5 s timeout)."""
    import threading
    import time

    release = threading.Event()
    entered = threading.Event()

    def stuck(*, topic: str, timeout: float) -> _Metadata:
        entered.set()
        release.wait(5.0)
        return _Metadata(3)

    count = ReplyPartitionCount(stuck, REPLY)
    started = time.monotonic()
    assert count(1) is None
    assert time.monotonic() - started < 0.5
    assert entered.wait(2.0), "the fetch ran on another thread"
    release.set()
    deadline = time.monotonic() + 2.0
    while count(1) is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert count(1) == 3


# -- specs that cannot reply -----------------------------------------------------------------------


def test_a_transactional_worker_skips_the_reply_and_counts_it() -> None:
    r = rig(channel_spec=make_spec(reply=None), steps=(passthrough,), transactional=True)
    r.handler.handle(_consumed(sync_request()))
    assert r.producer.replies() == []
    assert r.replies.stats.skipped == 1


@splitter
def halves(message: Message) -> list[Message]:
    return [message, message]


def test_a_sync_spec_with_a_splitter_is_refused_at_load() -> None:
    with pytest.raises(ChannelSpecError, match="splitter"):
        rig(channel_spec=make_spec(reply={}), steps=(halves,))


def test_a_sync_spec_on_a_transactional_pipeline_is_refused_at_load() -> None:
    with pytest.raises(ChannelSpecError, match="transactional"):
        rig(channel_spec=make_spec(reply={}, transactional=True), steps=(passthrough,))


def test_a_split_worker_skips_a_sync_record() -> None:
    """A worker still running an async split spec gets a record from a receiver already in sync mode."""
    r = rig(channel_spec=make_spec(reply=None), steps=(halves,))
    r.run(sync_request())
    assert len(r.producer.records(OUT)) == 2
    assert r.producer.replies() == []
    assert r.replies.stats.skipped == 1


# -- ctx.reply and body: step ----------------------------------------------------------------------


def test_body_step_carries_the_step_body() -> None:
    r = rig(channel_spec=make_spec(reply={"body": "step"}), steps=(answer,))
    r.run(sync_request())
    (reply,) = r.producer.replies()
    assert reply.value == ACK_BODY
    assert reply.headers[h.TB_CONTENT_TYPE] == HL7
    assert reply.headers[h.TB_REPLY_STATUS] == "accepted"


def test_body_step_without_a_call_fails_the_reply_only() -> None:
    r = rig(channel_spec=make_spec(reply={"body": "step"}), steps=(passthrough,))
    loop = r.run(sync_request())
    headers = reply_headers(r)
    assert (headers[h.TB_REPLY_STATUS], headers[h.TB_ERROR_CODE]) == ("failed", REPLY_BODY_MISSING)
    assert r.producer.records(OUT) and r.producer.records(DLQ) == [], "routed; no dlq record"
    assert loop.stats.dead_lettered == 0
    (event,) = replied(r)
    assert (event.status, event.error_code) == ("error", REPLY_BODY_MISSING)


def test_a_drop_under_body_step_answers_accepted_without_a_body() -> None:
    r = rig(channel_spec=make_spec(reply={"body": "step"}), steps=(answer, drop_all))
    r.run(sync_request())
    (reply,) = r.producer.replies()
    assert reply.headers[h.TB_REPLY_STATUS] == "accepted" and reply.value == b""
    assert r.replies.stats.body_discarded == 1


def test_a_body_under_body_ack_is_discarded_and_counted() -> None:
    r = rig(channel_spec=make_spec(reply={"body": "ack"}), steps=(answer,))
    r.run(sync_request())
    assert r.producer.replies()[0].value == b""
    assert r.replies.stats.body_discarded == 1


def test_a_body_on_an_async_record_is_discarded_and_counted() -> None:
    r = rig(channel_spec=make_spec(reply={"body": "step"}), steps=(answer,))
    r.run(sync_request(partition=None))
    assert r.producer.replies() == []
    assert r.replies.stats.body_discarded == 1


def test_a_failed_attempts_body_is_discarded_before_the_retry() -> None:
    attempts: list[int] = []

    @step
    def flaky(message: Message, ctx: Any) -> None:
        attempts.append(1)
        ctx.reply(b"MSA|AE|first-attempt\r" if len(attempts) == 1 else ACK_BODY, content_type=HL7)
        if len(attempts) == 1:
            raise RuntimeError("transient")

    r = rig(channel_spec=make_spec(reply={"body": "step"}), steps=(flaky,))
    r.run(sync_request())
    assert len(attempts) == 2, "the retry called ctx.reply again without tripping at-most-once"
    assert r.producer.replies()[0].value == ACK_BODY


def test_ctx_reply_in_a_destination_step_fails_that_destination() -> None:
    channel_spec = make_spec(reply={}, destinations=(("lis", ("reply_in_destination",)),))
    r = rig(channel_spec=channel_spec, steps=(passthrough,), resolve={"reply_in_destination": reply_in_destination})
    r.run(sync_request())
    headers = reply_headers(r)
    assert (headers[h.TB_REPLY_STATUS], headers[h.TB_ERROR_CODE]) == ("failed", TRANSFORM_ERROR)
    assert r.producer.records(DLQ), "the destination's copy was dead-lettered"


# -- PHI (invariant 6) -----------------------------------------------------------------------------


@pytest.mark.parametrize("steps", [(passthrough,), (reject_lab,), (explode,), (drop_all,)])
def test_neither_the_key_nor_msh_10_reaches_a_reply_a_log_or_an_event(steps: tuple[Any, ...]) -> None:
    r = rig(steps=steps)
    r.run(sync_request())
    (reply,) = r.producer.replies()
    rendered = [json.dumps(reply.headers), reply.key.decode() if reply.key else "", r.log.getvalue()]
    rendered += [json.dumps(event.to_dict()) for event in r.emitter.events]
    for text in rendered:
        assert KEY_MARKER.decode() not in text
        assert CONTROL_ID_MARKER not in text
    assert reply.value == b"", "under body: ack the reply carries no payload at all"


def _consumed(message: Message) -> Any:
    from tributary.runtime.records import ConsumedRecord

    return ConsumedRecord(topic="dev.adt-passthrough.raw", partition=0, offset=0, message=message)


def test_the_worker_context_is_a_reply_context() -> None:
    from tributary.runtime.context import WorkerContext
    from tributary.runtime.logs import JsonLogger
    from tributary.sdk import ReplyContext

    ctx = WorkerContext(channel="c", channel_version=1, instance="worker:t@h", log=JsonLogger("t"))
    assert isinstance(ctx, ReplyContext)
