"""Fast tests (no Kafka, no ClickHouse): the reprocess id rule, the header rule and the publisher."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from tributary.archiver.errors import PublishError
from tributary.archiver.republish import DROPPED_HEADERS, RawPublisher, reinjected, reprocess_id
from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.ids import derived_uuid7, uuid7

ORIGINAL = "0194f5a0-0000-7000-8000-000000000001"


def raw_message(**over: str) -> Message:
    headers = {
        h.TB_MSG_ID: ORIGINAL,
        h.TB_CHANNEL: "adt",
        h.TB_CHANNEL_VERSION: "3",
        h.TB_SOURCE: "mllp-receiver:r-1@localhost:9092",
        h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
        h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
        h.TB_ATTEMPT: "2",
        h.TRACEPARENT: "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        h.TB_REPLY_PARTITION: "3",
        h.TB_REPLY_DEADLINE: "2026-01-01T00:01:00.000Z",
        h.TB_CORRELATION_ID: str(uuid.uuid4()),
        h.TB_STAGE: "transformed",
        h.TB_ERROR_CODE: "TRANSFORM_ERROR",
        h.TB_DESTINATION: "d1",
    }
    headers.update(over)
    return Message(headers=headers, payload=b"SYNTH-BYTES", key=b"SYNTH-KEY")


def test_reprocess_id_is_deterministic_v7_and_request_scoped() -> None:
    request = uuid7()
    first = reprocess_id(ORIGINAL, request)
    assert first == reprocess_id(ORIGINAL, request)
    assert h.is_well_formed(h.TB_MSG_ID, first)
    assert first != ORIGINAL
    assert first != reprocess_id(ORIGINAL, uuid7())
    assert first != reprocess_id(str(uuid7()), request)
    # a distinct domain from split children
    assert first != derived_uuid7(ORIGINAL, 0)


def test_reprocess_id_refuses_a_non_v7_request_id() -> None:
    with pytest.raises(ValueError, match="UUIDv7"):
        reprocess_id(ORIGINAL, str(uuid.uuid4()))


def test_reinjected_sets_parent_new_id_and_drops_what_raw_must_not_carry() -> None:
    new_id = reprocess_id(ORIGINAL, uuid7())
    out = reinjected(raw_message(), new_id=new_id, parent_id=ORIGINAL)
    assert out.get(h.TB_MSG_ID) == new_id
    assert out.get(h.TB_PARENT_ID) == ORIGINAL
    assert out.get(h.TB_ATTEMPT) == "0"
    assert all(name not in out.headers for name in DROPPED_HEADERS)
    assert out.get(h.TB_RECEIVED_AT) == "2026-01-01T00:00:00.000Z"  # copied unchanged
    assert out.get(h.TB_CORRELATION_ID) is not None  # propagates (envelope 6 rule 7)
    assert out.payload == b"SYNTH-BYTES" and out.key == b"SYNTH-KEY"
    original_trace = raw_message().traceparent
    assert original_trace is not None and out.traceparent is not None
    assert out.traceparent.split("-")[1] == original_trace.split("-")[1]  # same trace
    assert out.traceparent.split("-")[2] != original_trace.split("-")[2]  # new span


def test_reinjected_shadow_target_rewrites_the_channel() -> None:
    out = reinjected(raw_message(), new_id=reprocess_id(ORIGINAL, uuid7()), parent_id=ORIGINAL, channel="adt-shadow")
    assert out.get(h.TB_CHANNEL) == "adt-shadow"


def test_producer_profile_is_acks_all_and_idempotent() -> None:
    config = producer_config(KafkaSettings(bootstrap_servers="localhost:9092"), load_topic_defaults(), client_id="x")
    assert str(config["acks"]) in ("all", "-1")
    assert config["enable.idempotence"] in (True, "true")


class FakeProducer:
    def __init__(self, *, fail: bool = False, full_times: int = 0) -> None:
        self.fail = fail
        self.full = full_times
        self.pending: list[Any] = []
        self.produced = 0

    def produce(self, topic: str, *, value: Any, key: Any, headers: Any, on_delivery: Any) -> None:
        if self.full:
            self.full -= 1
            raise BufferError
        self.produced += 1
        self.pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        class Msg:
            def partition(self) -> int:
                return 0

            def offset(self) -> int:
                return 7

        for i, callback in enumerate(self.pending):
            callback("ERR" if self.fail else None, Msg())
            del i
        self.pending = []
        return 0


def test_publisher_reports_deliveries_and_refuses_on_a_failed_callback() -> None:
    message = reinjected(raw_message(), new_id=reprocess_id(ORIGINAL, uuid7()), parent_id=ORIGINAL)
    ok = RawPublisher(FakeProducer(full_times=2))  # back-pressure is waited out, not dropped
    ok.publish("dev.adt.raw", message)
    assert [(d.partition, d.offset) for d in ok.flush()] == [(0, 7)]
    bad = RawPublisher(FakeProducer(fail=True))
    bad.publish("dev.adt.raw", message)
    with pytest.raises(PublishError) as raised:
        bad.flush()
    assert raised.value.code == "PRODUCE_FAILED" and "ERR" not in str(raised.value)


def test_publisher_refuses_an_envelope_violation_without_naming_values() -> None:
    broken = reinjected(raw_message(), new_id=reprocess_id(ORIGINAL, uuid7()), parent_id=ORIGINAL).without_header(
        h.TB_CONTENT_TYPE
    )
    with pytest.raises(PublishError) as raised:
        RawPublisher(FakeProducer()).publish("dev.adt.raw", broken)
    assert raised.value.code == "ENVELOPE_INVALID"
    assert "SYNTH" not in str(raised.value)
