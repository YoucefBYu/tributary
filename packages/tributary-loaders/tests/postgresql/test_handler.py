"""`PostgresqlRecordHandler`: translates `PostgresqlLoader.deliver` outcomes into the
`tributary.runtime.consumer.RecordHandler` protocol (return on success, raise `DeliveryError`
otherwise) that `ConsumerLoop` drives. Mirrors `tests/http/test_handler.py`."""

from __future__ import annotations

import json
from typing import Any

import pytest
from tributary.loaders.postgresql.config import PgAuthSpec, PgTlsSpec, PostgresqlDestinationConfig
from tributary.loaders.postgresql.handler import PostgresqlRecordHandler
from tributary.loaders.postgresql.loader import PostgresqlLoader
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import Destination, Idempotency, RetryPolicy
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

from .test_loader import FakePool


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
        "name": "warehouse",
        "type": "postgresql",
        "params": {
            "host": "db.example.org",
            "database": "clinical",
            "table": "observations",
            "auth": {"type": "password", "secretRef": "results-db"},
            "tls": {"mode": "verify-full"},
            "timeoutMs": 2000,
        },
        "idempotency": Idempotency(technique="sink-native"),
        "retry": RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def make_config(**overrides: object) -> PostgresqlDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "warehouse",
        "host": "db.example.org",
        "port": 5432,
        "database": "clinical",
        "table": "observations",
        "id_column": "tb_msg_id",
        "auth": PgAuthSpec("password", "results-db"),
        "tls": PgTlsSpec("verify-full", None),
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
    return PostgresqlDestinationConfig(**base)  # type: ignore[arg-type]


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
            h.TB_DESTINATION: "warehouse",
        },
        payload=json.dumps(row if row is not None else {"value": 1}).encode("utf-8"),
    )
    return ConsumedRecord(topic="dev.c.out.warehouse", partition=0, offset=1, message=message)


def make_handler(loader: PostgresqlLoader, *, destination: Destination | None = None) -> PostgresqlRecordHandler:
    dest = destination if destination is not None else make_destination()
    return PostgresqlRecordHandler(loader, spec_source=FakeSpecSource("c", dest), destination=dest.name)


def test_handle_returns_normally_on_success() -> None:
    loader = PostgresqlLoader(make_config(), FakePool(lambda args: "INSERT 0 1"))
    handler = make_handler(loader)
    try:
        handler.handle(make_record())  # must not raise
    finally:
        handler.close()


def test_handle_treats_a_conflict_no_op_as_success_too() -> None:
    loader = PostgresqlLoader(make_config(), FakePool(lambda args: "INSERT 0 0"))
    handler = make_handler(loader)
    try:
        handler.handle(make_record())  # must not raise: the row already existed, which is fine
    finally:
        handler.close()


def test_handle_raises_retryable_delivery_error_on_connect_failed() -> None:
    from tributary.loaders.postgresql.client import TransportError

    def raiser(args: tuple[Any, ...]) -> str:
        raise TransportError("down")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    handler = make_handler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "CONNECT_FAILED"
        assert exc_info.value.retryable is True
    finally:
        handler.close()


def test_handle_raises_non_retryable_delivery_error_on_pg_error_code() -> None:
    import asyncpg  # type: ignore[import-untyped]

    def raiser(args: tuple[Any, ...]) -> str:
        raise asyncpg.exceptions.UndefinedTableError("relation does not exist")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    handler = make_handler(loader)
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(make_record())
        assert exc_info.value.error_code == "PG_42P01"
        assert exc_info.value.retryable is False
    finally:
        handler.close()


def test_handle_raises_non_retryable_delivery_error_on_payload_shape_invalid() -> None:
    loader = PostgresqlLoader(make_config(), FakePool(lambda args: "INSERT 0 1"))
    handler = make_handler(loader)
    record = make_record()
    bad_record = ConsumedRecord(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        message=record.message.with_payload(b"not json"),
    )
    try:
        with pytest.raises(DeliveryError) as exc_info:
            handler.handle(bad_record)
        assert exc_info.value.error_code == "PAYLOAD_SHAPE_INVALID"
        assert exc_info.value.retryable is False
    finally:
        handler.close()


def test_handle_can_be_called_more_than_once_on_the_same_handler() -> None:
    pool = FakePool(lambda args: "INSERT 0 1")
    loader = PostgresqlLoader(make_config(), pool)
    handler = make_handler(loader)
    try:
        handler.handle(make_record())
        handler.handle(make_record())
    finally:
        handler.close()
    assert len(pool.calls) == 2


def test_close_closes_the_spec_source_and_the_pool() -> None:
    pool = FakePool(lambda args: "INSERT 0 1")
    loader = PostgresqlLoader(make_config(), pool)
    source = FakeSpecSource("c", make_destination())
    handler = PostgresqlRecordHandler(loader, spec_source=source, destination="warehouse")
    handler.close()
    assert source.closed is True
    assert pool.closed is True


def test_metrics_snapshot_includes_duplicates_absorbed() -> None:
    applied: set[str] = set()

    def stub_table(args: tuple[Any, ...]) -> str:
        msg_id = args[0]
        if msg_id in applied:
            return "INSERT 0 0"
        applied.add(msg_id)
        return "INSERT 0 1"

    loader = PostgresqlLoader(make_config(), FakePool(stub_table))
    handler = make_handler(loader)
    record = make_record()
    try:
        handler.handle(record)
        handler.handle(record)  # redelivery of the identical record
    finally:
        handler.close()
    snapshot = handler.metrics_snapshot()
    assert snapshot["duplicates_absorbed"] == 1
