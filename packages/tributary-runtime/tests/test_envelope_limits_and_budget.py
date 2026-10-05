"""Security review of W2-5: a sender must not be able to crash the worker into a replay loop.

HIGH: an HL7 message of exactly 1,000,000 bytes with no final CR became a 1,000,001-byte split child,
the produce raised `EnvelopeError`, nothing caught it, and the worker died before the commit and replayed
the same record for ever. The same happens when `tb-parent-id` or a destination step pushes a record
over a header or payload limit. What must hold now, each tested through the real consumer loop:

* the split adds no byte its parent did not have;
* any record the pipeline built that breaks the envelope dead-letters the **consumed** record with
  `TB_OUTPUT_ENVELOPE_INVALID`, before anything is produced, and the offset is committed;
* the loop's dead letter itself always fits (registered headers only, when it must).

MEDIUM 1: splitting stops costing work at child 10,001. MEDIUM 2: one `handle` has a time budget.
All payloads are synthetic: MSH-3 is TRIBUTARY-SYNTH.
"""

from __future__ import annotations

import io
import json
import time
import tracemalloc
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from tributary.runtime.builtins.split import _hl7_batch, _json_array
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.core_steps import split
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.executor import StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import ConsumedRecord, to_kafka_headers
from tributary.runtime.spec import ChannelSpec, RetryPolicy
from tributary.runtime.worker import KafkaDlqWriter, PipelineHandler, handle_budget_s
from tributary.sdk import Message, Reject, splitter, step
from tributary.sdk import headers as h
from tributary.sdk.codes import SPLIT_TOO_LARGE, TB_OUTPUT_ENVELOPE_INVALID, TB_PIPELINE_TIME_BUDGET
from tributary.sdk.projection import MAX_INLINE_PAYLOAD_BYTES
from tributary.sdk.testing import make_message, run_splitter

from .fakes import FakeConsumer, FakeProducer, FakeRecord
from .log_line_schema import assert_lines_conform

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
CHANNEL = "adt-passthrough"
RAW = f"dev.{CHANNEL}.raw"
HL7 = "application/hl7-v2+er7"
MSH = b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A01|M1|P|2.5.1"


def hl7_of_size(size: int, *, terminated: bool) -> bytes:
    """One synthetic HL7 message of exactly `size` bytes."""
    tail = b"\r" if terminated else b""
    filler = size - len(MSH) - 1 - len(b"NTE|1||") - len(tail)
    return MSH + b"\r" + b"NTE|1||" + b"x" * filler + tail


# -- harness -------------------------------------------------------------------------------------


def spec_with(
    pipeline: list[dict[str, Any]], destinations: dict[str, list[dict[str, Any]]], *, step_retries: int = 3
) -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    template = document["spec"]["destinations"][0]
    document["metadata"]["version"] = 1
    document["spec"]["pipeline"]["steps"] = pipeline
    document["spec"]["pipeline"]["stepRetries"] = step_retries
    built = []
    for name, steps in destinations.items():
        destination = json.loads(json.dumps(template))
        destination["name"] = name
        if steps:
            destination["steps"] = steps
        built.append(destination)
    document["spec"]["destinations"] = built
    return ChannelSpec.from_document(document)


CUSTOM: dict[str, Any] = {}


def resolve_custom(reference: Any) -> Any:
    from tributary.runtime.steps import load_step

    if reference.uses in CUSTOM:
        return CUSTOM[reference.uses].bind(reference.params)
    return load_step(reference)


def handler(
    spec: ChannelSpec,
    producer: FakeProducer,
    *,
    time_budget_s: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PipelineHandler:
    built = PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=RecordProducer(producer, log=JsonLogger("test", stream=io.StringIO())),
        emitter=RecordingEmitter(),
        log=JsonLogger("worker", stream=io.StringIO()),
        instance="worker:test@localhost",
        resolve=resolve_custom,
        time_budget_s=time_budget_s,
        clock=clock,
    )
    built.prepare(spec)
    return built


def raw_record(message: Message, offset: int = 0) -> FakeRecord:
    return FakeRecord(
        _topic=RAW,
        _partition=0,
        _offset=offset,
        _key=message.key,
        _value=message.payload,
        _headers=to_kafka_headers(message),
    )


def run_through_the_loop(spec: ChannelSpec, message: Message) -> tuple[FakeConsumer, FakeProducer, io.StringIO]:
    """The real ConsumerLoop and KafkaDlqWriter over fakes: what the worker process actually runs."""
    consumer, producer, stream = FakeConsumer(), FakeProducer(), io.StringIO()
    consumer.append(raw_record(message))
    log = JsonLogger("worker", stream=stream)
    records = RecordProducer(producer, log=log)
    pipeline = PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=records,
        emitter=RecordingEmitter(),
        log=log,
        instance="worker:test@localhost",
        resolve=resolve_custom,
    )
    pipeline.prepare(spec)
    loop = ConsumerLoop(
        consumer,
        pipeline,
        topics=[RAW],
        retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=log,
        channel=CHANNEL,
        channel_version=1,
        instance="worker:test@localhost",
        dlq=KafkaDlqWriter(records, f"dev.{CHANNEL}.dlq", log=log, source="worker:test@localhost"),
    )
    loop.run(max_iterations=3)  # returns: the process did not die
    return consumer, producer, stream


def produced(producer: FakeProducer, suffix: str) -> list[tuple[bytes | None, dict[str, str]]]:
    return [
        (value, {name: blob.decode() for name, blob in headers or []})
        for topic, _key, value, headers in producer.produced
        if topic.endswith(suffix)
    ]


# -- HIGH (b): the split adds nothing ------------------------------------------------------------


@pytest.mark.parametrize("terminated", [False, True])
def test_a_one_megabyte_message_splits_to_a_child_of_the_same_size(terminated: bool) -> None:
    payload = hl7_of_size(MAX_INLINE_PAYLOAD_BYTES, terminated=terminated)
    assert len(payload) == MAX_INLINE_PAYLOAD_BYTES
    children = run_splitter(split, make_message(payload, content_type=HL7), with_={"by": "hl7-batch"})
    assert isinstance(children, tuple)
    [child] = children
    assert child.payload == payload.replace(b"\r\n", b"\r"), "no terminator the parent did not have"
    assert child.payload_size == MAX_INLINE_PAYLOAD_BYTES


@pytest.mark.parametrize("separator", [b"\r", b"\n", b"\r\n", b"\r\r"])
def test_a_child_is_never_longer_than_its_span_in_the_parent(separator: bytes) -> None:
    body = separator.join([MSH, b"PID|1", MSH, b"PID|2"])
    children = run_splitter(split, make_message(body, content_type=HL7), with_={"by": "hl7-batch"})
    assert isinstance(children, tuple)
    assert [child.payload for child in children] == [MSH + b"\rPID|1\r", MSH + b"\rPID|2"]
    assert sum(child.payload_size for child in children) <= len(body)


def test_the_reproduced_crash_now_delivers_the_child() -> None:
    """The reviewer's reproduction, end to end: exactly 1,000,000 bytes, no final CR."""
    spec = spec_with([{"name": "unbatch", "uses": "tributary.core.split", "with": {"by": "hl7-batch"}}], {"fhir": []})
    message = make_message(hl7_of_size(MAX_INLINE_PAYLOAD_BYTES, terminated=False), content_type=HL7, channel=CHANNEL)
    consumer, producer, _stream = run_through_the_loop(spec, message)
    [(value, headers)] = produced(producer, ".out.fhir")
    assert value is not None and len(value) == MAX_INLINE_PAYLOAD_BYTES
    assert headers[h.TB_PARENT_ID] == message.msg_id
    assert consumer.committed, "the offset was committed"


# -- HIGH (a) and (c): a record over the envelope dead-letters the consumed record -----------------


@splitter
def oversized_child(msg: Message) -> list[Message]:
    return [msg.with_payload(b"x" * 10), msg.with_payload(b"x" * (MAX_INLINE_PAYLOAD_BYTES + 1))]


@step
def grow(msg: Message) -> Message:
    return msg.with_payload((msg.payload or b"") + b"x" * MAX_INLINE_PAYLOAD_BYTES)


CUSTOM["t.oversized_child"] = oversized_child
CUSTOM["t.grow"] = grow


def test_an_oversized_child_dead_letters_the_parent_and_produces_nothing_else() -> None:
    spec = spec_with([{"name": "fan", "uses": "t.oversized_child"}], {"fhir": []})
    producer = FakeProducer()
    with pytest.raises(StepFailure) as caught:
        handler(spec, producer).handle(ConsumedRecord(RAW, 0, 0, make_message(b"x", channel=CHANNEL)))
    assert caught.value.error_code == TB_OUTPUT_ENVELOPE_INVALID
    assert producer.produced == [], "validated before any produce: the first, valid child was not sent"


def test_a_destination_step_that_grows_the_payload_past_the_limit_is_not_a_crash() -> None:
    spec = spec_with(
        [{"name": "p", "uses": "tributary.core.passthrough"}],
        {"fhir": [], "rows": [{"name": "grow", "uses": "t.grow"}]},
    )
    message = make_message(b"synthetic", channel=CHANNEL)
    consumer, producer, stream = run_through_the_loop(spec, message)

    assert produced(producer, ".out.fhir") == [], "no destination is half-delivered"
    [(value, headers)] = produced(producer, ".dlq")
    assert headers[h.TB_ERROR_CODE] == TB_OUTPUT_ENVELOPE_INVALID
    assert headers[h.TB_MSG_ID] == message.msg_id
    assert value == b"synthetic", "the consumed record, which fits, is what is dead-lettered"
    assert consumer.committed
    assert "x" * 64 not in stream.getvalue()


def at_header_cap(payload: bytes, **kwargs: Any) -> Message:
    """A raw record with exactly 32 headers: the core set plus channel-defined tb-usr-* headers."""
    message = make_message(payload, channel=CHANNEL, **kwargs)
    for index in range(h.MAX_HEADERS - len(message.headers)):
        message = message.with_usr(f"tag-{index}", "v")
    assert len(message.headers) == h.MAX_HEADERS
    return message


def test_too_many_headers_on_the_out_record_dead_letters_the_consumed_record() -> None:
    """`out` adds tb-destination and tb-stage to a record already at the cap. The dlq projection would
    add two more, so the loop's writer falls back to registered headers only - and does not crash."""
    spec = spec_with([{"name": "p", "uses": "tributary.core.passthrough"}], {"fhir": []})
    message = at_header_cap(b"synthetic")
    consumer, producer, stream = run_through_the_loop(spec, message)

    assert produced(producer, ".out.fhir") == []
    [(value, headers)] = produced(producer, ".dlq")
    assert headers[h.TB_ERROR_CODE] == TB_OUTPUT_ENVELOPE_INVALID
    assert value == b"synthetic"
    assert not any(name.startswith("tb-usr-") for name in headers)
    assert consumer.committed
    logged = [json.loads(line) for line in stream.getvalue().splitlines() if '"event":"dead_lettered"' in line]
    assert logged[-1]["headers_dropped"] == h.MAX_HEADERS - 8
    assert_lines_conform(stream.getvalue())  # observability/v0: no null parent_id or destination


def test_tb_parent_id_pushing_a_child_over_the_cap_is_handled_the_same_way() -> None:
    spec = spec_with([{"name": "unbatch", "uses": "tributary.core.split", "with": {"by": "hl7-batch"}}], {"fhir": []})
    message = at_header_cap(MSH + b"\r", content_type=HL7)
    consumer, producer, _stream = run_through_the_loop(spec, message)
    assert produced(producer, ".out.fhir") == []
    [(_value, headers)] = produced(producer, ".dlq")
    assert headers[h.TB_ERROR_CODE] == TB_OUTPUT_ENVELOPE_INVALID
    assert consumer.committed


# -- MEDIUM 1: the cap stops the work ------------------------------------------------------------


def test_json_array_children_are_produced_lazily() -> None:
    """The first child is available before the rest of the document has been read at all."""
    message = make_message(b'[{"a": 1}, ' + b"garbage" * 1000, content_type="application/json")
    parts = _json_array(message, "")
    assert next(parts) == b'{"a": 1}'
    with pytest.raises(Reject):
        list(parts)  # the defect is still found when the generator is drained


def test_hl7_batch_children_are_produced_lazily() -> None:
    parts = _hl7_batch(make_message(MSH + b"\r" + MSH + b"\rnot a segment", content_type=HL7))
    assert next(parts) == MSH + b"\r"
    with pytest.raises(Reject):
        list(parts)


@pytest.mark.parametrize(
    ("payload", "content_type", "with_"),
    [
        (b"[" + b"0," * 480_000 + b"0]", "application/json", {"by": "json-array"}),
        ((MSH + b"\r") * 17_000, HL7, {"by": "hl7-batch"}),
    ],
)
def test_an_oversized_split_is_rejected_with_bounded_memory_and_time(
    payload: bytes, content_type: str, with_: dict[str, Any]
) -> None:
    """The review measured 173-307 MB RSS and 1.5-2.2 s for a 998 KB array. The bound here is loose on
    purpose (CI machines vary); it fails by an order of magnitude if every child is built first."""
    message = make_message(payload, content_type=content_type)
    tracemalloc.start()
    started = time.perf_counter()
    try:
        with pytest.raises(Reject) as caught:
            run_splitter(split, message, with_=with_)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    elapsed = time.perf_counter() - started
    assert caught.value.code == SPLIT_TOO_LARGE
    assert peak < 40 * 1024 * 1024, f"peak {peak} bytes"
    assert elapsed < 5.0


# -- MEDIUM 2: a time budget per handle ----------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


CLOCK = Clock()


@step
def slow(msg: Message) -> None:
    CLOCK.now += 2.0


@splitter
def many(msg: Message) -> Iterator[Message]:
    return (msg.with_payload(b"c") for _ in range(50))


CUSTOM["t.slow"] = slow
CUSTOM["t.many"] = many


def test_a_fan_out_that_outlasts_its_budget_dead_letters_the_parent_before_producing() -> None:
    CLOCK.now = 0.0
    spec = spec_with([{"name": "fan", "uses": "t.many"}, {"name": "slow", "uses": "t.slow"}], {"fhir": []})
    producer = FakeProducer()
    with pytest.raises(StepFailure) as caught:
        handler(spec, producer, time_budget_s=30.0, clock=CLOCK).handle(
            ConsumedRecord(RAW, 0, 0, make_message(b"x", channel=CHANNEL))
        )
    assert caught.value.error_code == TB_PIPELINE_TIME_BUDGET
    assert CLOCK.now <= 34.0, "the budget stopped the work, not the end of the fan-out"
    assert producer.produced == []


def test_within_budget_everything_is_produced() -> None:
    CLOCK.now = 0.0
    spec = spec_with([{"name": "fan", "uses": "t.many"}, {"name": "slow", "uses": "t.slow"}], {"fhir": []})
    producer = FakeProducer()
    handler(spec, producer, time_budget_s=1000.0, clock=CLOCK).handle(
        ConsumedRecord(RAW, 0, 0, make_message(b"x", channel=CHANNEL))
    )
    assert len(produced(producer, ".out.fhir")) == 50


def test_the_budget_is_sized_under_the_poll_interval() -> None:
    assert handle_budget_s(300_000, 30.0) == 120.0
    assert handle_budget_s(600_000, 30.0) == 270.0
    assert handle_budget_s(10_000, 30.0) == 10.0, "never below 10 s"


# -- a dead letter that cannot be written halts its partition, not the process -------------------


def two_partition_loop(
    first: FakeRecord, spec: ChannelSpec
) -> tuple[FakeConsumer, FakeProducer, RecordingEmitter, str]:
    """Partition 0 carries `first`; partition 1 carries an ordinary record."""
    consumer, producer, stream, events = FakeConsumer(), FakeProducer(), io.StringIO(), RecordingEmitter()
    consumer.append(first)
    ordinary = make_message(b"synthetic", channel=CHANNEL)
    consumer.append(FakeRecord(RAW, 1, 0, None, b"synthetic", to_kafka_headers(ordinary)))
    log = JsonLogger("worker", stream=stream)
    records = RecordProducer(producer, log=log)
    pipeline = PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=records,
        emitter=events,
        log=log,
        instance="worker:test@localhost",
        resolve=resolve_custom,
    )
    pipeline.prepare(spec)
    loop = ConsumerLoop(
        consumer,
        pipeline,
        topics=[RAW],
        retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=events,
        log=log,
        channel=CHANNEL,
        channel_version=1,
        instance="worker:test@localhost",
        dlq=KafkaDlqWriter(records, f"dev.{CHANNEL}.dlq", log=log, source="worker:test@localhost"),
    )
    loop.run(max_iterations=6)  # returns: the process did not die
    return consumer, producer, events, stream.getvalue()


def halted(events: RecordingEmitter) -> list[Any]:
    return [event for event in events.events if event.stage == "partition_halted"]


def test_a_raw_record_over_the_inline_limit_halts_its_partition_and_the_others_flow() -> None:
    """Security review (reproduced): 1,000,001 bytes through a passthrough. The broker accepts it
    (`max.message.bytes` 1,048,576), no `out` or `dlq` projection fits, and the worker crash-looped."""
    spec = spec_with([{"name": "p", "uses": "tributary.core.passthrough"}], {"fhir": []})
    oversized = make_message(b"x" * (MAX_INLINE_PAYLOAD_BYTES + 1), channel=CHANNEL)
    consumer, producer, events, logged = two_partition_loop(raw_record(oversized), spec)

    [event] = halted(events)
    assert (event.partition, event.offset, event.error_code) == (0, 0, "TB_DLQ_UNWRITABLE")
    assert event.error_message == "dead letter exceeds the envelope; partition halted, operator action required"
    assert (RAW, 0) in consumer.paused and (RAW, 0, 0) in consumer.seek_calls, "held at the record"
    assert not any(partition == 0 for batch in consumer.committed for _t, partition, _o in batch), "not dropped"
    assert produced(producer, ".dlq") == []
    assert len(produced(producer, ".out.fhir")) == 1, "partition 1 still flows"
    assert any(partition == 1 for batch in consumer.committed for _t, partition, _o in batch)
    assert "x" * 64 not in logged


def test_an_undecodable_record_too_large_to_quarantine_halts_instead_of_crashing() -> None:
    blob = b"x" * MAX_INLINE_PAYLOAD_BYTES
    undecodable = FakeRecord(RAW, 0, 0, None, blob, [("tb-channel", b"\xff\xfe")])
    spec = spec_with([{"name": "p", "uses": "tributary.core.passthrough"}], {"fhir": []})
    _consumer, producer, events, _logged = two_partition_loop(undecodable, spec)
    [event] = halted(events)
    assert event.error_code == "TB_DLQ_UNWRITABLE"
    assert event.message_id is None, "nothing from the undecodable record is used"
    assert len(produced(producer, ".out.fhir")) == 1


# -- the time budget is checked between step retries ---------------------------------------------


@step
def always_fails_slowly(msg: Message) -> None:
    CLOCK.now += 2.0
    CALLS.append("attempt")
    raise ValueError("synthetic failure")


CALLS: list[str] = []
CUSTOM["t.always_fails_slowly"] = always_fails_slowly


@pytest.mark.parametrize("where", ["pipeline", "destination"])
def test_the_budget_stops_in_place_retries_and_is_not_swallowed_by_a_branch(where: str) -> None:
    CLOCK.now = 0.0
    CALLS.clear()
    failing = [{"name": "fail", "uses": "t.always_fails_slowly"}]
    spec = spec_with(
        failing if where == "pipeline" else [{"name": "p", "uses": "tributary.core.passthrough"}],
        {"fhir": [] if where == "pipeline" else failing},
        step_retries=8,
    )
    producer = FakeProducer()
    with pytest.raises(StepFailure) as caught:
        handler(spec, producer, time_budget_s=3.0, clock=CLOCK).handle(
            ConsumedRecord(RAW, 0, 0, make_message(b"x", channel=CHANNEL))
        )
    assert caught.value.error_code == TB_PIPELINE_TIME_BUDGET, "the whole record, not a destination dead letter"
    assert len(CALLS) == 2, "stopped between retries, not after all nine attempts"
    assert producer.produced == []
