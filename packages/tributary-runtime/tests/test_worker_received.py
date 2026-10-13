"""The `received` stage event: the worker emits it once per consumed `raw` record, before any step runs.

Events contract 2.1 (set 1.8) names the pipeline worker as the emitter, because a receiver holds no WRITE on
the events topic and must not put a produce on its ACK path (invariants 1 and 9). No Kafka here: the
handler is driven with a recording emitter and a fake producer.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import orjson
import pytest
from jsonschema import Draft202012Validator
from tributary.runtime.errors import DeliveryError
from tributary.runtime.events import EventError, LifecycleEvent, MessageEvent, RecordingEmitter, serialize
from tributary.runtime.executor import StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import KafkaEventEmitter, RecordProducer
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec
from tributary.runtime.worker import RECEIVED_MEMORY, PipelineHandler
from tributary.sdk import Drop, Message, step
from tributary.sdk.testing import make_message

from .fakes import FakeProducer
from .test_consumer_loop import Clock
from .test_consumer_loop import record as txn_record
from .test_transactions import TxnConsumer
from .test_transactions import build as build_txn

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
EXAMPLES = CONTRACTS / "channel-spec" / "v0" / "examples" / "valid"
EVENT_SCHEMA = CONTRACTS / "events" / "v0" / "event.schema.json"
RAW = "dev.adt-passthrough.raw"
PAYLOAD = b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|||20260101000000||ADT^A01|SYNTH-1|P|2.5\r"
KEY = b"SYNTH-KEY-1"


def spec(version: int = 3) -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": version}
    return ChannelSpec.from_document(document)


def consumed(message: Message | None = None, *, offset: int = 7) -> ConsumedRecord:
    return ConsumedRecord(
        topic=RAW, partition=2, offset=offset, message=message or make_message(PAYLOAD, key=KEY, attempt=0)
    )


def handler(
    producer: FakeProducer,
    emitter: Any,
    *,
    steps: tuple[Any, ...],
    log: io.StringIO,
    transactional: bool = False,
    received_memory: int = RECEIVED_MEMORY,
) -> PipelineHandler:
    return PipelineHandler(
        env="dev",
        spec_provider=spec,
        producer=RecordProducer(producer, log=JsonLogger("test", stream=log)),
        emitter=emitter,
        log=JsonLogger("worker", stream=log),
        instance="worker:test@localhost",
        steps_override=steps,
        transactional=transactional,
        received_memory=received_memory,
    )


def received(events: list[LifecycleEvent]) -> list[MessageEvent]:
    return [event for event in events if isinstance(event, MessageEvent) and event.stage == "received"]


@step
def passthrough(msg: Message) -> None:
    return None


@step
def drop_all(msg: Message) -> Drop:
    return Drop(reason="unsupported trigger event")


@step
def explode(msg: Message) -> Message:
    raise ValueError("synthetic failure")


def test_received_is_emitted_first_with_the_raw_coordinates_and_no_payload() -> None:
    emitter, log = RecordingEmitter(), io.StringIO()
    seen_before_step: list[list[str]] = []

    @step
    def observe(msg: Message) -> None:
        seen_before_step.append(emitter.stages())

    record = consumed()
    handler(FakeProducer(), emitter, steps=(observe,), log=log).handle(record)

    assert seen_before_step == [["received"]], "received is emitted before the first step runs"
    assert emitter.stages() == ["received", "transformed", "routed"]
    event = emitter.events[0]
    assert isinstance(event, MessageEvent)
    assert (event.stage, event.status) == ("received", "ok")
    assert (event.channel, event.channel_version) == ("adt-passthrough", 3)
    assert event.message_id == record.message.msg_id
    assert (event.topic, event.partition, event.offset) == (RAW, 2, 7)
    assert event.attempt == 0
    assert event.size_bytes == len(PAYLOAD)
    assert event.content_type == record.message.get("tb-content-type")
    assert event.traceparent == record.message.traceparent
    assert event.destination is None and event.attrs is None and event.error_code is None

    document = orjson.loads(serialize(event))
    errors = list(Draft202012Validator(json.loads(EVENT_SCHEMA.read_text(encoding="utf-8"))).iter_errors(document))
    assert errors == [], "the received event satisfies events/v0 with existing fields only"
    blob = serialize(event)
    assert PAYLOAD not in blob and b"SYNTH-KEY-1" not in blob and b"TRIBUTARY-SYNTH" not in blob, "no PHI"
    assert b"TRIBUTARY-SYNTH" not in log.getvalue().encode()


@pytest.mark.parametrize(
    ("steps", "raises", "after"),
    [((drop_all,), None, "filtered"), ((explode,), StepFailure, None)],
    ids=["filtered", "step-failure"],
)
def test_received_precedes_every_outcome(
    steps: tuple[Any, ...], raises: type[Exception] | None, after: str | None
) -> None:
    emitter = RecordingEmitter()
    run = handler(FakeProducer(), emitter, steps=steps, log=io.StringIO())
    if raises is None:
        run.handle(consumed())
    else:
        with pytest.raises(raises):
            run.handle(consumed())
    assert emitter.stages() == ["received"] + ([after] if after else [])


def test_a_blocking_retry_of_the_same_record_emits_received_once() -> None:
    """The loop re-hands the same offset after a retryable failure; `received` is not one per attempt."""
    producer, emitter = FakeProducer(), RecordingEmitter()
    run = handler(producer, emitter, steps=(passthrough,), log=io.StringIO())
    record = consumed()

    producer.remaining = 1  # first flush times out: the loop would pause and retry this record
    with pytest.raises(DeliveryError):
        run.handle(record)
    producer.remaining = 0
    run.handle(record)

    assert emitter.stages() == ["received", "transformed", "routed"]


def test_a_replay_another_process_sees_re_emits_with_the_same_dedup_key() -> None:
    """A rebalance or restart hands uncommitted records to a process that never saw them: the copy is
    identical on the archive's key. (Records the same process re-handles are covered below.)"""
    emitter = RecordingEmitter()
    first = consumed(offset=7)
    handler(FakeProducer(), emitter, steps=(passthrough,), log=io.StringIO()).handle(first)
    handler(FakeProducer(), emitter, steps=(passthrough,), log=io.StringIO()).handle(first)  # the successor

    copies = [event for event in received(emitter.events) if event.offset == 7]
    assert len(copies) == 2
    keys = {(event.message_id, event.stage, event.destination, event.attempt) for event in copies}
    assert len(keys) == 1, "tb-msg-id and attempt come from the record, so the archive collapses the copies"


def test_a_full_events_queue_never_fails_or_delays_the_record() -> None:
    """Invariant 9: the archive path is off the critical path, `received` included."""
    events_producer, data_producer = FakeProducer(fail_next=True), FakeProducer()
    log = io.StringIO()
    emitter = KafkaEventEmitter(events_producer, "_tb.dev.events", log=JsonLogger("events", stream=log))

    handler(data_producer, emitter, steps=(passthrough,), log=log).handle(consumed())

    assert emitter.dropped == 1, "the received event was dropped, not waited for"
    assert data_producer.topics() == ["dev.adt-passthrough.out.http-stub"], "the record was still delivered"


class RefusingEmitter(RecordingEmitter):
    """Refuses the first `refusals` received events, then accepts."""

    def __init__(self, refusals: int = 1) -> None:
        super().__init__()
        self.refusals = refusals

    def emit(self, event: LifecycleEvent) -> None:
        if event.stage == "received" and self.refusals > 0:
            self.refusals -= 1
            raise EventError("refused by contract")
        super().emit(event)


def test_a_refused_received_event_is_logged_by_id_and_the_record_still_runs() -> None:
    emitter, producer, log = RefusingEmitter(), FakeProducer(), io.StringIO()

    handler(producer, emitter, steps=(passthrough,), log=log).handle(consumed())

    assert emitter.stages() == ["transformed", "routed"]
    assert producer.topics() == ["dev.adt-passthrough.out.http-stub"]
    lines = [json.loads(line) for line in log.getvalue().splitlines()]
    failed = [line for line in lines if line["event"] == "event_emit_failed"]
    assert len(failed) == 1
    assert failed[0]["stage"] == "received" and failed[0]["offset"] == 7
    assert "refused by contract" not in log.getvalue(), "exception text is never logged"


def test_a_refused_received_event_is_not_remembered_so_the_next_attempt_retries_it() -> None:
    emitter, producer = RefusingEmitter(), FakeProducer()
    run = handler(producer, emitter, steps=(passthrough,), log=io.StringIO())
    record = consumed()

    producer.remaining = 1  # the first attempt fails after the refused event; the loop re-hands the record
    with pytest.raises(DeliveryError):
        run.handle(record)
    producer.remaining = 0
    run.handle(record)

    assert [event.offset for event in received(emitter.events)] == [7], "the retry emitted the missed event"


def test_memory_is_bounded_and_a_record_that_aged_out_re_emits() -> None:
    emitter = RecordingEmitter()
    run = handler(FakeProducer(), emitter, steps=(passthrough,), log=io.StringIO(), received_memory=2)
    for offset in (1, 2, 3, 1):  # 1 is evicted by 3, so its replay emits again (archive dedups it)
        run.handle(consumed(offset=offset))
    assert [event.offset for event in received(emitter.events)] == [1, 2, 3, 1]


class FailOnce:
    """The real handler, then a retryable failure once for one record: a produce that did not land."""

    def __init__(self, inner: PipelineHandler, fail: tuple[int, int]) -> None:
        self.inner, self.fail = inner, fail
        self.seen: list[tuple[int, int]] = []

    def handle(self, rec: ConsumedRecord) -> None:
        self.seen.append((rec.partition, rec.offset))
        self.inner.handle(rec)
        if (rec.partition, rec.offset) == self.fail:
            self.fail = (-1, -1)
            raise DeliveryError("PRODUCE_FAILED", retryable=True)


def test_a_transactional_abort_rewinds_the_window_but_emits_received_once_per_record() -> None:
    """The loop aborts and rewinds every partition in the window to its start (consumer.py, ADR-003 D6).

    Every record in the window is handed to the pipeline again, so `transformed` and `routed` repeat
    (those copies belong to the aborted transaction); `received` does not, because the record was
    received once.
    """
    consumer = TxnConsumer()
    for partition in (0, 1):
        for offset in (0, 1):
            consumer.append(txn_record(offset, partition=partition))
    emitter = RecordingEmitter()
    pipeline = handler(FakeProducer(), emitter, steps=(passthrough,), log=io.StringIO(), transactional=True)
    failing = FailOnce(pipeline, fail=(1, 1))
    clock = Clock()
    loop, producer, _ = build_txn(consumer, failing, clock=clock)

    loop.run(max_iterations=4)
    assert "abort" in producer.calls
    clock.advance(1.0)
    loop.run(max_iterations=8)

    replayed = failing.seen[4:]
    assert sorted(replayed) == [(0, 0), (0, 1), (1, 0), (1, 1)], "the whole window really was re-handed"
    assert emitter.stages().count("transformed") == 8
    coordinates = [(event.partition, event.offset) for event in received(emitter.events)]
    assert sorted(coordinates) == [(0, 0), (0, 1), (1, 0), (1, 1)], "one received per record, not per attempt"
