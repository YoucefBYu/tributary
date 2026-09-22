"""The pipeline worker's handler: raw -> steps -> `out.<dest>`, with no Kafka.

The ordering-relevant assertions here are about what the outgoing record looks like — same key, reset
attempt, new span — and about the sequence: every destination record is flushed before the handler
returns, because the loop commits the input offset the moment it does.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from tributary.runtime.errors import DeliveryError, PartitionMismatchError
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.executor import StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec
from tributary.runtime.worker import (
    KafkaDlqWriter,
    PipelineHandler,
    check_partition_counts,
    dlq_message,
    out_message,
    quarantine_message,
)
from tributary.sdk import Drop, Message, step
from tributary.sdk import headers as h
from tributary.sdk.projection import record_projection
from tributary.sdk.testing import make_message

from .fakes import FakeProducer

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
EXAMPLES = CONTRACTS / "channel-spec" / "v0" / "examples" / "valid"
ENVELOPE_SCHEMA = CONTRACTS / "envelope" / "v0" / "envelope.schema.json"


def spec(name: str = "walking-skeleton-loopback.json", version: int = 3) -> ChannelSpec:
    document = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": version}
    return ChannelSpec.from_document(document)


@step
def passthrough(msg: Message) -> None:
    return None


@step
def annotate(msg: Message, ctx: Any) -> Message:
    ctx.set_attr("trigger", "a01")
    ctx.set_usr("batch-id", "b-4471")
    return msg


@step
def filter_everything(msg: Message) -> Drop:
    return Drop(reason="unsupported trigger event")


@step
def explode(msg: Message) -> Message:
    raise ValueError("SYNTH^JANE mrn 12345")


def consumed(message: Message | None = None, *, offset: int = 7) -> ConsumedRecord:
    return ConsumedRecord(
        topic="dev.adt-passthrough.raw",
        partition=2,
        offset=offset,
        message=message or make_message(b"synthetic", key=b"SYNTH-KEY-1"),
    )


def handler(
    producer: FakeProducer,
    emitter: RecordingEmitter,
    *,
    steps: tuple[Any, ...] = (passthrough,),
    channel_spec: ChannelSpec | None = None,
    stream: io.StringIO | None = None,
) -> PipelineHandler:
    return PipelineHandler(
        env="dev",
        spec_provider=lambda: channel_spec or spec(),
        producer=RecordProducer(producer, log=JsonLogger("test", stream=stream or io.StringIO())),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream or io.StringIO()),
        instance="worker:test@localhost",
        steps_override=steps,
    )


def test_a_message_flows_from_raw_to_the_destination_topic() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()
    record = consumed()

    handler(producer, emitter).handle(record)

    assert producer.topics() == ["dev.adt-passthrough.out.http-stub"]
    _topic, key, value, headers = producer.produced[0]
    assert key == b"SYNTH-KEY-1", "the ordering key is carried through unchanged (invariant 5)"
    assert value == b"synthetic"
    sent = {name: blob.decode() for name, blob in headers or []}
    assert sent[h.TB_DESTINATION] == "http-stub"
    assert sent[h.TB_ATTEMPT] == "0"
    assert sent[h.TB_MSG_ID] == record.message.msg_id
    assert emitter.stages() == ["transformed", "routed"]


def test_every_destination_record_is_flushed_before_the_handler_returns() -> None:
    """The loop commits as soon as this returns, so an unflushed record would be a lost message."""
    producer, emitter = FakeProducer(), RecordingEmitter()
    handler(producer, emitter).handle(consumed())
    assert producer.flushes == 1


def test_a_produce_that_never_lands_fails_the_handler_rather_than_committing() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()
    producer.remaining = 1  # flush() times out with a record still queued

    with pytest.raises(DeliveryError) as caught:
        handler(producer, emitter).handle(consumed())

    assert caught.value.error_code == "PRODUCE_TIMEOUT"
    assert caught.value.retryable is True
    assert "routed" not in emitter.stages(), "nothing is reported as routed unless it was delivered"


def test_a_dropped_message_is_filtered_and_nothing_is_produced() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()

    handler(producer, emitter, steps=(filter_everything,)).handle(consumed())

    assert producer.produced == []
    assert emitter.stages() == ["filtered"]
    event = emitter.events[0]
    assert event.status == "dropped"
    assert event.error_message == "unsupported trigger event"


def test_a_failing_step_raises_for_the_loop_to_dead_letter() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()

    with pytest.raises(StepFailure):
        handler(producer, emitter, steps=(explode,)).handle(consumed())

    assert producer.produced == []


def test_attrs_and_usr_headers_set_by_a_step_reach_the_event_and_the_record() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()

    handler(producer, emitter, steps=(annotate,)).handle(consumed())

    _topic, _key, _value, headers = producer.produced[0]
    sent = {name: blob.decode() for name, blob in headers or []}
    assert sent["tb-usr-batch-id"] == "b-4471"
    assert emitter.events[0].attrs == {"trigger": "a01"}
    # Envelope contract 2.2: tb-usr-* are propagated in headers and NOT projected into events.
    assert "batch-id" not in (emitter.events[0].attrs or {})


def test_fan_out_produces_one_record_per_destination() -> None:
    producer, emitter = FakeProducer(), RecordingEmitter()
    fanout = spec("two-destinations-fanout.json")

    handler(producer, emitter, channel_spec=fanout).handle(consumed())

    assert len(producer.produced) == len(fanout.destinations)
    assert sorted(producer.topics()) == sorted(
        f"dev.{fanout.name}.out.{destination.name}" for destination in fanout.destinations
    )
    assert emitter.stages().count("routed") == len(fanout.destinations)


def test_the_outgoing_record_gets_a_new_span_in_the_same_trace() -> None:
    original = make_message(b"synthetic", key=b"K")
    outgoing = out_message(original, "http-stub", usr={})
    before = (original.traceparent or "").split("-")
    after = (outgoing.traceparent or "").split("-")
    assert after[1] == before[1], "same trace id across hops"
    assert after[2] != before[2], "new span id per hop (envelope contract 6 rule 2)"


def test_the_dlq_record_is_keyed_by_message_id_and_keeps_the_payload() -> None:
    original = make_message(b"synthetic-payload", key=b"SYNTH-KEY-1")
    dead = dlq_message(original, error_code="STEP_ERROR", stage="transformed")

    assert dead.key == original.msg_id.encode("utf-8")
    assert dead.payload == b"synthetic-payload", "reprocess after a code fix needs the original payload"
    assert dead.error_code == "STEP_ERROR"
    assert dead.stage == "transformed"

    validator = Draft202012Validator(json.loads(ENVELOPE_SCHEMA.read_text(encoding="utf-8")))
    assert list(validator.iter_errors(record_projection(dead, topic_class="dlq"))) == []


def test_the_dlq_record_carries_a_code_and_never_exception_text() -> None:
    """ADR-003 D4: parser messages quote the offending field value, which is PHI."""
    original = make_message(b"synthetic", key=b"K")
    dead = dlq_message(original, error_code="STEP_ERROR", stage="transformed")
    rendered = " ".join(f"{name}={value}" for name, value in dead.headers.items())
    assert "SYNTH^JANE" not in rendered
    assert "ValueError" not in rendered


def test_the_quarantine_wrapper_is_a_new_conformant_record() -> None:
    """Envelope contract 5.2: a new record the engine constructs, not the original moved."""
    blob = b"TBQ1-original-bytes"
    wrapper = quarantine_message(
        blob=blob,
        channel="adt-passthrough",
        channel_version=3,
        source="worker:pipeline-1@localhost",
    )

    assert wrapper.payload == blob, "the original bytes live in the payload, the opaque PHI-bearing part"
    assert wrapper.key == wrapper.msg_id.encode("utf-8"), "keyed by tb-msg-id like every dlq record"
    assert wrapper.msg_id[14] == "7", "a freshly minted UUIDv7, never a value recovered from the record"
    assert wrapper.content_type == "application/octet-stream"
    assert wrapper.error_code == "RECORD_UNDECODABLE", "the schema keys its quarantine rules on this code"
    assert wrapper.stage == "quarantined", (
        "never the stage that read it: the record never reached queued or pending, so claiming it did "
        "would be false rather than merely inconsistent"
    )
    assert wrapper.source == "worker:pipeline-1@localhost", "this component, not the unknown original peer"

    validator = Draft202012Validator(json.loads(ENVELOPE_SCHEMA.read_text(encoding="utf-8")))
    assert list(validator.iter_errors(record_projection(wrapper, topic_class="dlq"))) == []


def test_two_quarantine_wrappers_never_share_an_identity() -> None:
    """A forged or reused id would collide with a real message in the browser and in loader idempotency."""
    first = quarantine_message(blob=b"a", channel="c", channel_version=1, source="s")
    second = quarantine_message(blob=b"a", channel="c", channel_version=1, source="s")
    assert first.msg_id != second.msg_id


def test_the_dlq_writer_quarantines_a_conformant_wrapper_and_returns_its_id() -> None:
    producer = FakeProducer()
    writer = KafkaDlqWriter(
        RecordProducer(producer, log=JsonLogger("test", stream=io.StringIO())),
        "dev.adt-passthrough.dlq",
        log=JsonLogger("test", stream=io.StringIO()),
        source="worker:pipeline-1@localhost",
    )

    class Broken:
        def key(self) -> bytes:
            return b"SYNTH-KEY-1"

        def value(self) -> bytes:
            return b"unreadable"

        def headers(self) -> list[tuple[str, bytes]]:
            return [("tb-channel", bytes([0xFF, 0xFE]))]

    minted = writer.quarantine(Broken(), channel="adt-passthrough", channel_version=1)

    topic, key, value, _headers = producer.produced[0]
    assert topic == "dev.adt-passthrough.dlq"
    assert key == minted.encode("utf-8")
    assert value is not None and value.startswith(b"TBQ1"), "the original bytes are framed into the payload"
    assert b"unreadable" in value
    assert b"SYNTH-KEY-1" in value, "the original key lives in the blob (envelope 5.2)"
    assert key != b"SYNTH-KEY-1", "and must not become the wrapper's key, which is the minted id"
    assert producer.flushes == 1, "the DLQ write is confirmed before the loop commits the input offset"


def test_worker_logs_never_contain_the_record_key_or_payload() -> None:
    stream = io.StringIO()
    producer, emitter = FakeProducer(), RecordingEmitter()
    message = make_message(b"PAYLOAD-MARKER", key=b"KEY-MARKER-9f2a")

    handler(producer, emitter, stream=stream).handle(consumed(message))

    written = stream.getvalue()
    assert "KEY-MARKER" not in written
    assert "PAYLOAD-MARKER" not in written


class FakeMetadata:
    def __init__(self, topics: dict[str, int]) -> None:
        self.topics = {
            name: type("T", (), {"partitions": dict.fromkeys(range(count)), "error": None})()
            for name, count in topics.items()
        }


class FakeAdmin:
    def __init__(self, topics: dict[str, int]) -> None:
        self._topics = topics

    def list_topics(self, timeout: float = -1) -> FakeMetadata:
        return FakeMetadata(self._topics)


def test_a_partition_count_mismatch_refuses_to_start_an_ordered_channel() -> None:
    ordered = spec("adt-to-fhir-per-key-mtls.json")
    destination = ordered.destinations[0].name
    matching = FakeAdmin({f"dev.{ordered.name}.raw": 3, f"dev.{ordered.name}.out.{destination}": 3})
    mismatched = FakeAdmin({f"dev.{ordered.name}.raw": 3, f"dev.{ordered.name}.out.{destination}": 6})

    check_partition_counts(matching, "dev", ordered)
    with pytest.raises(PartitionMismatchError):
        check_partition_counts(mismatched, "dev", ordered)


def test_an_unordered_channel_does_not_care_about_partition_counts() -> None:
    """`ordering: none` already round-robins, so there is no key-to-partition mapping to break."""
    unordered = spec()
    check_partition_counts(FakeAdmin({"dev.adt-passthrough.raw": 3}), "dev", unordered)
