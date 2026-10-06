"""`ClickhouseRecordHandler`: translates `ClickhouseLoader.deliver` outcomes into the
`tributary.runtime.consumer.RecordHandler` protocol. Mirrors `tests/postgresql/test_handler.py`."""

from __future__ import annotations

import json
from typing import Any

import pytest
from tributary.loaders.clickhouse.client import Response, TransportError
from tributary.loaders.clickhouse.config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig
from tributary.loaders.clickhouse.handler import ClickhouseRecordHandler
from tributary.loaders.clickhouse.loader import ClickhouseLoader
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import Destination, Idempotency, RetryPolicy
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

from .test_loader import FakeTransport, ok


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
        "name": "adt-analytics",
        "type": "clickhouse",
        "params": {
            "host": "clickhouse.example.org",
            "database": "analytics",
            "table": "adt_events",
            "auth": {"type": "password", "secretRef": "clickhouse-adt"},
            "tls": {"mode": "verify-full"},
            "timeoutMs": 2000,
        },
        "idempotency": Idempotency(technique="sink-native"),
        "retry": RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def make_config(**overrides: object) -> ClickhouseDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "adt-analytics",
        "host": "clickhouse.example.org",
        "port": 8443,
        "database": "analytics",
        "table": "adt_events",
        "id_column": "tb_msg_id",
        "auth": ChAuthSpec("password", "clickhouse-adt"),
        "tls": ChTlsSpec("verify-full", None),
        "timeout_s": 2.0,
        "circuit_breaker": None,
        "idempotency_technique": "sink-native",
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return ClickhouseDestinationConfig(**base)  # type: ignore[arg-type]


def make_record(row: dict[str, Any] | None = None) -> ConsumedRecord:
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
            h.TB_DESTINATION: "adt-analytics",
        },
        payload=json.dumps(row if row is not None else {"value": 1}).encode("utf-8"),
    )
    return ConsumedRecord(topic="dev.c.out.adt-analytics", partition=0, offset=1, message=message)


def make_handler(loader: ClickhouseLoader, *, destination: Destination | None = None) -> ClickhouseRecordHandler:
    dest = destination if destination is not None else make_destination()
    return ClickhouseRecordHandler(loader, spec_source=FakeSpecSource("c", dest), destination=dest.name)


def test_handle_returns_normally_on_success() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ok()))
    handler = make_handler(loader)
    handler.handle(make_record())  # must not raise
    handler.close()


def test_handle_raises_retryable_delivery_error_on_connect_failed() -> None:
    def raiser(sql: bytes) -> Response:
        raise TransportError("down")

    loader = ClickhouseLoader(make_config(), FakeTransport(raiser))
    handler = make_handler(loader)
    with pytest.raises(DeliveryError) as exc_info:
        handler.handle(make_record())
    assert exc_info.value.error_code == "CONNECT_FAILED"
    assert exc_info.value.retryable is True
    handler.close()


def test_handle_raises_non_retryable_delivery_error_on_payload_shape_invalid() -> None:
    loader = ClickhouseLoader(make_config(), FakeTransport(lambda sql: ok()))
    handler = make_handler(loader)
    record = make_record()
    bad_record = ConsumedRecord(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        message=record.message.with_payload(b"not json"),
    )
    with pytest.raises(DeliveryError) as exc_info:
        handler.handle(bad_record)
    assert exc_info.value.error_code == "PAYLOAD_SHAPE_INVALID"
    assert exc_info.value.retryable is False
    handler.close()


def test_handle_can_be_called_more_than_once_on_the_same_handler() -> None:
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(), transport)
    handler = make_handler(loader)
    handler.handle(make_record())
    handler.handle(make_record())
    handler.close()
    assert len(transport.calls) == 2


def test_close_closes_the_spec_source() -> None:
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(), transport)
    source = FakeSpecSource("c", make_destination())
    handler = ClickhouseRecordHandler(loader, spec_source=source, destination="adt-analytics")
    handler.close()
    assert source.closed is True


def test_metrics_snapshot_reports_delivery_counts() -> None:
    transport = FakeTransport(lambda sql: ok())
    loader = ClickhouseLoader(make_config(), transport)
    handler = make_handler(loader)
    handler.handle(make_record())
    handler.close()
    assert handler.metrics_snapshot()["delivery_success"] == 1
