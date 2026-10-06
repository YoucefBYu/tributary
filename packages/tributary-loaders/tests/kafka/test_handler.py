"""`KafkaRecordHandler`: translates `KafkaLoader.deliver` outcomes into the
`tributary.runtime.consumer.RecordHandler` protocol (return on success, raise `DeliveryError`
otherwise). Mirrors `tests/postgresql/test_handler.py`."""

from __future__ import annotations

from typing import Any

import pytest
from tributary.loaders.kafka.config import KafkaConnectionSpec, KafkaDestinationConfig, KafkaSecuritySpec
from tributary.loaders.kafka.handler import KafkaRecordHandler
from tributary.loaders.kafka.loader import KafkaLoader
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import Destination, Idempotency, RetryPolicy
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

from .test_loader import FakeKafkaError, FakeProducer


class FakeSpecSource:
    def __init__(self, channel: str, destination: Destination) -> None:
        self.name = channel
        self._destination = destination
        self.closed = False

    def current(self) -> FakeSpecSource:
        return self

    def destination(self, name: str) -> Destination:
        assert name == self._destination.name
        return self._destination

    def close(self) -> None:
        self.closed = True


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "partner-feed",
        "type": "kafka",
        "params": {
            "connection": {
                "bootstrapServers": ["kafka.example.org:9093"],
                "security": {"protocol": "SSL"},
            },
            "topic": "partner.events",
            "key": "none",
            "timeoutMs": 2000,
        },
        "idempotency": Idempotency(technique="idempotency-key-header", header="tb-msg-id"),
        "retry": RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def make_config(**overrides: object) -> KafkaDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "partner-feed",
        "connection": KafkaConnectionSpec(("kafka.example.org:9093",), KafkaSecuritySpec("SSL")),
        "topic": "partner.events",
        "key_mode": "none",
        "timeout_s": 2.0,
        "idempotency_technique": "idempotency-key-header",
        "idempotency_header": "tb-msg-id",
        "circuit_breaker": None,
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return KafkaDestinationConfig(**base)  # type: ignore[arg-type]


def make_record() -> ConsumedRecord:
    message = Message(
        headers={
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "partner-feed",
        },
        payload=b'{"value": 1}',
    )
    return ConsumedRecord(topic="dev.c.out.partner-feed", partition=0, offset=1, message=message)


def make_handler(loader: KafkaLoader, *, destination: Destination | None = None) -> KafkaRecordHandler:
    dest = destination if destination is not None else make_destination()
    return KafkaRecordHandler(loader, spec_source=FakeSpecSource("c", dest), destination=dest.name)


def test_handle_returns_normally_on_success() -> None:
    loader = KafkaLoader(make_config(), FakeProducer())
    handler = make_handler(loader)
    handler.handle(make_record())  # must not raise
    handler.close()


def test_handle_raises_retryable_delivery_error_on_timeout() -> None:
    loader = KafkaLoader(make_config(), FakeProducer(flush_remaining=1))
    handler = make_handler(loader)
    with pytest.raises(DeliveryError) as exc_info:
        handler.handle(make_record())
    assert exc_info.value.error_code == "TIMEOUT"
    assert exc_info.value.retryable is True
    handler.close()


def test_handle_raises_non_retryable_delivery_error_on_a_kafka_error() -> None:
    err = FakeKafkaError(10, "MSG_SIZE_TOO_LARGE", False)
    loader = KafkaLoader(make_config(), FakeProducer(respond=lambda args: err))
    handler = make_handler(loader)
    with pytest.raises(DeliveryError) as exc_info:
        handler.handle(make_record())
    assert exc_info.value.error_code == "KAFKA_MSG_SIZE_TOO_LARGE"
    assert exc_info.value.retryable is False
    handler.close()


def test_handle_can_be_called_more_than_once_on_the_same_handler() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    handler = make_handler(loader)
    handler.handle(make_record())
    handler.handle(make_record())
    handler.close()
    assert len(producer.produced) == 2


def test_close_closes_the_spec_source() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    source = FakeSpecSource("c", make_destination())
    handler = KafkaRecordHandler(loader, spec_source=source, destination="partner-feed")
    handler.close()
    assert source.closed is True


def test_metrics_snapshot_reports_delivery_counts() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    handler = make_handler(loader)
    handler.handle(make_record())
    handler.close()
    snapshot: dict[str, Any] = handler.metrics_snapshot()
    assert snapshot["delivery_success"] == 1
