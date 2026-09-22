"""Gate requirement 10 for the events side: the real emitter validated against the real schema.

The S5 spike's event shape would have failed this schema — epoch-float `ts`, no `v`, no `kind`, a
`worker` field (gate section 6, G2) — and nothing caught it, because nothing validated. So these tests
build events with `MessageEvent`/`PartitionEvent` exactly as the worker does and run
`contracts/events/v0/event.schema.json` over the result, each with a mutation control.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import orjson
import pytest
from jsonschema import Draft202012Validator
from tributary.runtime.events import EventError, MessageEvent, PartitionEvent, RecordingEmitter, serialize
from tributary.sdk.ids import uuid7

SCHEMA_PATH = Path(__file__).resolve().parents[3] / "contracts" / "events" / "v0" / "event.schema.json"


@pytest.fixture(scope="module")
def validator() -> Draft202012Validator:
    assert SCHEMA_PATH.is_file(), "events schema not found: the suite would pass vacuously"
    return Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def failures(validator: Draft202012Validator, instance: Any) -> set[tuple[str, str]]:
    """(json_path, keyword) pairs only: an error message embeds the instance, and events carry ids."""
    return {(error.json_path, str(error.validator)) for error in validator.iter_errors(instance)}


def message_event(**overrides: Any) -> MessageEvent:
    fields: dict[str, Any] = {
        "stage": "sent",
        "status": "ok",
        "channel": "adt-passthrough",
        "channel_version": 2,
        "instance": "worker:pipeline-2@worker-4",
        "message_id": uuid7(),
        "destination": "http-stub",
        "attempt": 1,
        "content_type": "application/hl7-v2+er7",
        "size_bytes": 465,
        "topic": "dev.adt-passthrough.out.http-stub",
        "partition": 2,
        "offset": 17,
        "duration_ms": 12.5,
        "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        "attrs": {"trigger": "a01", "step": "passthrough"},
    }
    fields.update(overrides)
    return MessageEvent(**fields)


def partition_event(**overrides: Any) -> PartitionEvent:
    fields: dict[str, Any] = {
        "stage": "partition_paused",
        "status": "error",
        "channel": "adt-passthrough",
        "channel_version": 2,
        "instance": "worker:loader-1@worker-4",
        "topic": "dev.adt-passthrough.out.http-stub",
        "partition": 2,
        "offset": 17,
        "destination": "http-stub",
        "attempt": 3,
        "error_code": "HTTP_503",
        "error_message": "destination returned 503",
    }
    fields.update(overrides)
    return PartitionEvent(**fields)


@pytest.mark.parametrize(
    "stage", ["received", "queued", "filtered", "transformed", "routed", "pending", "sent", "replied"]
)
def test_message_events_validate(validator: Draft202012Validator, stage: str) -> None:
    assert failures(validator, message_event(stage=stage).to_dict()) == set()


@pytest.mark.parametrize("stage", ["retrying", "error", "dlq"])
def test_failure_events_validate(validator: Draft202012Validator, stage: str) -> None:
    event = message_event(stage=stage, status="error", error_code="HTTP_503", error_message="destination returned 503")
    assert failures(validator, event.to_dict()) == set()


@pytest.mark.parametrize("stage", ["partition_paused", "partition_resumed", "partition_halted"])
def test_partition_events_validate(validator: Draft202012Validator, stage: str) -> None:
    status = "ok" if stage == "partition_resumed" else "error"
    event = partition_event(stage=stage, status=status, error_code=None if status == "ok" else "HTTP_503")
    assert failures(validator, event.to_dict()) == set()


def test_a_filtered_message_is_dropped_not_failed(validator: Draft202012Validator) -> None:
    event = message_event(stage="filtered", status="dropped", error_message="unsupported trigger event")
    assert failures(validator, event.to_dict()) == set()
    assert "error_code" not in event.to_dict()


def test_absent_fields_are_omitted_not_nulled(validator: Draft202012Validator) -> None:
    """`additionalProperties: false` plus a null is a validation failure for no information gained."""
    document = message_event(destination=None, attrs=None, claim_check=None).to_dict()
    assert "destination" not in document and "attrs" not in document and "claim_check" not in document
    assert failures(validator, document) == set()


def test_the_serialized_event_is_one_line_of_json() -> None:
    line = serialize(message_event())
    assert b"\n" not in line
    assert orjson.loads(line)["kind"] == "message"


def test_keys_follow_the_contract() -> None:
    event = message_event()
    assert event.key == event.message_id.encode("utf-8")
    assert partition_event().key == b"dev.adt-passthrough.out.http-stub:2"


# -- mutation controls ----------------------------------------------------------------------------


def test_a_message_event_without_a_message_id_fails_the_schema(validator: Draft202012Validator) -> None:
    document = message_event().to_dict()
    del document["message_id"]
    assert ("$", "required") in failures(validator, document)


def test_the_spike_event_shape_fails_the_schema(validator: Draft202012Validator) -> None:
    """Gate section 6 G2: epoch-float ts, no v, no kind, a `worker` field. It must not come back."""
    spike_shape = {"ts": 1758365000.123, "stage": "sent", "worker": "s5-loader-1", "msg_id": "abc"}
    assert failures(validator, spike_shape)


def test_a_payload_field_cannot_be_smuggled_into_an_event(validator: Draft202012Validator) -> None:
    document = message_event().to_dict()
    document["payload"] = "MSH|^~\\&|TRIBUTARY-SYNTH"
    assert ("$", "additionalProperties") in failures(validator, document)


@pytest.mark.parametrize(
    "overrides",
    [
        {"stage": "partition_paused"},  # a partition stage on a message event
        {"status": "error", "error_code": None},  # a failure with no code to alert on
        {"error_message": "x" * 513},
        {"error_message": "two\nlines"},
        {"attrs": {"mrn": "123456"}},
        {"attrs": {"reason": "admitted for observation"}},
        {"claim_check": "s3://b/dev/ch/2026/09/20/id", "size_bytes": 465},
    ],
)
def test_contract_violations_are_refused_at_construction(overrides: dict[str, Any]) -> None:
    """Constructing the event is where this fails, so a bad event never reaches the topic at all."""
    with pytest.raises(EventError):
        message_event(**overrides)


def test_a_message_stage_on_a_partition_event_is_refused() -> None:
    with pytest.raises(EventError):
        partition_event(stage="sent")


def test_the_schema_check_itself_can_go_red(validator: Draft202012Validator) -> None:
    assert failures(validator, {})


def test_the_recording_emitter_keeps_the_stage_sequence() -> None:
    emitter = RecordingEmitter()
    emitter.emit(message_event(stage="received"))
    emitter.emit(message_event(stage="transformed"))
    emitter.emit(partition_event())
    assert emitter.stages() == ["received", "transformed", "partition_paused"]
