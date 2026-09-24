"""`PostgresqlLoader.deliver`: the row-shape check, classification, the circuit breaker, the startup
idempotency-constraint proof, and the sink-native duplicate-delivery test (invariant 4: every loader
documents and tests its idempotency technique keyed by `tb-msg-id`). `FakePool` stands in for
`asyncpg.Pool`; `test_integration_postgresql_loader.py` proves the same behaviour against a real
server.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Callable
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from tributary.loaders.errors import IdempotencyConstraintError
from tributary.loaders.postgresql.client import TransportError
from tributary.loaders.postgresql.config import CircuitBreakerSpec, PgAuthSpec, PgTlsSpec, PostgresqlDestinationConfig
from tributary.loaders.postgresql.loader import PostgresqlLoader, parse_row
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_message(
    row: dict[str, Any] | None = None, *, payload: bytes | None = None, msg_id: str | None = None
) -> Message:
    if payload is None:
        payload = json.dumps(row if row is not None else {"patient_ref": "SYNTH-1", "value": 98.6}).encode("utf-8")
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "warehouse",
        },
        payload=payload,
    )


def make_config(**overrides: object) -> PostgresqlDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "warehouse",
        "host": "db.example.org",
        "port": 5432,
        "database": "clinical",
        "table": "observations",
        "id_column": "tb_msg_id",
        "auth": PgAuthSpec("password", "db-creds"),
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


class FakePool:
    """Simulates just enough of `asyncpg.Pool` to drive the loader's classification: `respond`
    decides the command tag or raises, `fetchval_result` answers `verify_idempotency_constraint`, and
    every call's arguments are recorded so a test can assert what was actually sent."""

    def __init__(self, respond: Callable[[tuple[Any, ...]], str], *, fetchval_result: Any = True) -> None:
        self._respond = respond
        self.fetchval_result = fetchval_result
        self.calls: list[tuple[Any, ...]] = []
        self.fetchval_calls: list[tuple[Any, ...]] = []
        self.closed = False

    async def execute(self, query: str, /, *args: Any, timeout: float | None = None) -> str:
        self.calls.append(args)
        return self._respond(args)

    async def fetchval(self, query: str, /, *args: Any, timeout: float | None = None) -> Any:
        self.fetchval_calls.append(args)
        return self.fetchval_result

    async def close(self) -> None:
        self.closed = True


# -- row shape (README 3.4.3) ---------------------------------------------------------------------


def test_parse_row_accepts_scalars_and_null() -> None:
    row = parse_row(json.dumps({"a": "x", "b": 1, "c": 1.5, "d": True, "e": None}).encode(), "tb_msg_id")
    assert row == {"a": "x", "b": 1, "c": 1.5, "d": True, "e": None}


def test_parse_row_rejects_non_object_payload() -> None:
    with pytest.raises(Exception, match="not a JSON object"):
        parse_row(json.dumps([1, 2, 3]).encode(), "tb_msg_id")


def test_parse_row_rejects_invalid_json() -> None:
    with pytest.raises(Exception, match="not valid JSON"):
        parse_row(b"not json{", "tb_msg_id")


def test_parse_row_rejects_a_payload_that_sets_id_column_itself() -> None:
    with pytest.raises(Exception, match="tb_msg_id"):
        parse_row(json.dumps({"tb_msg_id": "attacker-supplied"}).encode(), "tb_msg_id")


def test_parse_row_rejects_a_bad_column_name() -> None:
    with pytest.raises(Exception, match="column name"):
        parse_row(json.dumps({"Bad-Column": "x"}).encode(), "tb_msg_id")


def test_parse_row_rejects_a_nested_value() -> None:
    with pytest.raises(Exception, match="non-scalar"):
        parse_row(json.dumps({"nested": {"a": 1}}).encode(), "tb_msg_id")


def test_deliver_with_malformed_payload_is_payload_shape_invalid_and_not_retryable() -> None:
    loader = PostgresqlLoader(make_config(), FakePool(lambda args: "INSERT 0 1"))
    outcome = asyncio.run(loader.deliver(make_message(payload=b"not json")))
    assert outcome.success is False
    assert outcome.reason == "PAYLOAD_SHAPE_INVALID"
    assert outcome.retryable is False


# -- delivery and classification -------------------------------------------------------------------


def test_insert_success_returns_success_and_inserted_true() -> None:
    pool = FakePool(lambda args: "INSERT 0 1")
    loader = PostgresqlLoader(make_config(), pool)
    outcome = asyncio.run(loader.deliver(make_message({"value": 1})))
    assert outcome.success is True
    assert outcome.inserted is True
    assert loader.duplicates_absorbed == 0


def test_query_sends_tb_msg_id_first_then_sorted_row_columns() -> None:
    pool = FakePool(lambda args: "INSERT 0 1")
    loader = PostgresqlLoader(make_config(table="clinical.observations"), pool)
    message = make_message({"value": 98.6, "patient_ref": "SYNTH-1"})
    asyncio.run(loader.deliver(message))
    assert pool.calls[0][0] == message.msg_id
    # sorted() column order: patient_ref before value
    assert pool.calls[0][1:] == ("SYNTH-1", 98.6)


def test_timeout_is_failure_and_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise TimeoutError("query timed out")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.success is False
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True


def test_transport_error_is_connect_failed_and_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise TransportError("connection refused")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "CONNECT_FAILED"
    assert outcome.retryable is True


def test_postgres_connection_error_is_connect_failed_and_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise asyncpg.exceptions.ConnectionDoesNotExistError("pool exhausted")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "CONNECT_FAILED"
    assert outcome.retryable is True


def test_ssl_error_is_tls_verify_failed_and_not_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise ssl.SSLError("certificate verify failed")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "TLS_VERIFY_FAILED"
    assert outcome.retryable is False


def test_deadlock_is_a_pg_error_code_and_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise asyncpg.exceptions.DeadlockDetectedError("deadlock detected")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "PG_40P01"
    assert outcome.retryable is True


def test_undefined_table_is_a_pg_error_code_and_not_retryable() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise asyncpg.exceptions.UndefinedTableError("relation does not exist")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.success is False
    assert outcome.reason == "PG_42P01"
    assert outcome.retryable is False


def test_other_unique_violation_is_a_pg_error_code_and_not_retryable() -> None:
    """A violation of some constraint OTHER than idColumn (the one ON CONFLICT already handles) is a
    genuine data problem, not something a retry fixes."""

    def raiser(args: tuple[Any, ...]) -> str:
        raise asyncpg.exceptions.UniqueViolationError("duplicate key value violates unique constraint")

    loader = PostgresqlLoader(make_config(), FakePool(raiser))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "PG_23505"
    assert outcome.retryable is False


def test_circuit_breaker_opens_after_repeated_failures_and_skips_the_query() -> None:
    def raiser(args: tuple[Any, ...]) -> str:
        raise TransportError("down")

    pool = FakePool(raiser)
    loader = PostgresqlLoader(make_config(circuit_breaker=CircuitBreakerSpec(2, 60.0)), pool)
    asyncio.run(loader.deliver(make_message()))
    asyncio.run(loader.deliver(make_message()))
    outcome = asyncio.run(loader.deliver(make_message()))
    assert outcome.reason == "circuit-open"
    assert len(pool.calls) == 2  # the third call never reached the pool


# -- idempotency -------------------------------------------------------------------------------------


def test_duplicate_delivery_is_harmless() -> None:
    """The sink-native technique: `ON CONFLICT (idColumn) DO NOTHING` is what actually absorbs a
    redelivery. `FakePool` here applies the same rule a real unique constraint does — first call for a
    given `tb-msg-id` "inserts" (tag `INSERT 0 1`), every later call for the same id is a no-op
    (`INSERT 0 0`) — which is exactly what blocking retry redelivering the identical record produces."""
    applied: set[str] = set()
    effects = {"count": 0}

    def stub_table(args: tuple[Any, ...]) -> str:
        msg_id = args[0]
        if msg_id not in applied:
            applied.add(msg_id)
            effects["count"] += 1
            return "INSERT 0 1"
        return "INSERT 0 0"

    loader = PostgresqlLoader(make_config(), FakePool(stub_table))
    message = make_message({"value": 1})  # one message, one tb-msg-id, delivered twice (a blocking retry)

    first = asyncio.run(loader.deliver(message))
    second = asyncio.run(loader.deliver(message))  # simulates a redelivery of the identical record

    assert first.success is True
    assert first.inserted is True
    assert second.success is True
    assert second.inserted is False
    assert effects["count"] == 1  # the underlying row exists once, even though deliver() ran twice
    assert loader.duplicates_absorbed == 1


def test_two_different_messages_both_insert() -> None:
    loader = PostgresqlLoader(make_config(), FakePool(lambda args: "INSERT 0 1"))
    first = asyncio.run(loader.deliver(make_message({"value": 1})))
    second = asyncio.run(loader.deliver(make_message({"value": 2})))
    assert first.inserted is True
    assert second.inserted is True
    assert loader.duplicates_absorbed == 0


def test_verify_idempotency_constraint_passes_when_the_pool_reports_it_exists() -> None:
    pool = FakePool(lambda args: "INSERT 0 1", fetchval_result=True)
    loader = PostgresqlLoader(make_config(), pool)
    asyncio.run(loader.verify_idempotency_constraint())  # must not raise
    assert pool.fetchval_calls[0] == ("public", "observations", "tb_msg_id")


def test_verify_idempotency_constraint_splits_schema_qualified_tables() -> None:
    pool = FakePool(lambda args: "INSERT 0 1", fetchval_result=True)
    loader = PostgresqlLoader(make_config(table="clinical.observations"), pool)
    asyncio.run(loader.verify_idempotency_constraint())
    assert pool.fetchval_calls[0] == ("clinical", "observations", "tb_msg_id")


def test_verify_idempotency_constraint_refuses_to_start_without_a_unique_index() -> None:
    pool = FakePool(lambda args: "INSERT 0 1", fetchval_result=False)
    loader = PostgresqlLoader(make_config(), pool)
    with pytest.raises(IdempotencyConstraintError, match="no unique index or primary key"):
        asyncio.run(loader.verify_idempotency_constraint())


# -- metrics -------------------------------------------------------------------------------------


def test_metrics_snapshot_counts_by_reason() -> None:
    responses = iter(["INSERT 0 1", "INSERT 0 0", "error"])

    def respond(args: tuple[Any, ...]) -> str:
        value = next(responses)
        if value == "error":
            raise asyncpg.exceptions.UndefinedTableError("boom")
        return value

    loader = PostgresqlLoader(make_config(), FakePool(respond))
    asyncio.run(loader.deliver(make_message({"value": 1})))
    asyncio.run(loader.deliver(make_message({"value": 2})))
    outcome = asyncio.run(loader.deliver(make_message({"value": 3})))
    assert outcome.reason == "PG_42P01"
    snapshot = loader.metrics.snapshot()
    assert snapshot["delivery_success"] == 2
    assert snapshot["delivery_PG_42P01"] == 1
