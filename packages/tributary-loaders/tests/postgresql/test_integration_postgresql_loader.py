"""`PostgresqlLoader` against a real PostgreSQL server (`testcontainers`), proving invariant 4 and
README 3.4.3 for real: a duplicate delivery of the same `tb-msg-id` produces exactly one row, and a
table with no unique index on `idColumn` refuses to start rather than silently duplicate.
`uv run pytest -m integration packages/tributary-loaders/tests/postgresql`.

`driver=None` on `PostgresContainer`: this loader talks to the server with `asyncpg` directly, never
through a SQLAlchemy URL, so the container's readiness probe (`psql` run inside the container itself,
`testcontainers.community.postgres.PostgresContainer._connect`) is all that is needed — no `psycopg2`
dependency reaches this package for that.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import asyncpg  # type: ignore[import-untyped]
import pytest
from testcontainers.community.postgres import PostgresContainer
from tributary.loaders.errors import IdempotencyConstraintError
from tributary.loaders.postgresql.client import PgConnectionSettings, PgPoolLike, create_pool
from tributary.loaders.postgresql.config import PgAuthSpec, PgTlsSpec, PostgresqlDestinationConfig
from tributary.loaders.postgresql.loader import PostgresqlLoader
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

pytestmark = pytest.mark.integration


def _as_asyncpg_pool(pool: PgPoolLike) -> asyncpg.Pool:
    """Verification-only widening: `PgPoolLike` (what the loader depends on) has no `fetch`, but the
    real object `create_pool` returns is a real `asyncpg.Pool`, and a test needs to independently read
    back what landed in the table rather than trust the loader's own report of what it did."""
    return cast("asyncpg.Pool", pool)


def make_message(row: dict[str, Any] | None = None, *, msg_id: str | None = None) -> Message:
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
        payload=json.dumps(row if row is not None else {"value": 1}).encode("utf-8"),
    )


def make_config(**overrides: object) -> PostgresqlDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "warehouse",
        "host": "unused",  # the pool is built directly from PgConnectionSettings below, not this
        "port": 5432,
        "database": "unused",
        "table": "observations",
        "id_column": "tb_msg_id",
        "auth": PgAuthSpec("password", "unused"),
        "tls": PgTlsSpec("disable", None),
        "timeout_s": 5.0,
        "circuit_breaker": None,
        "idempotency_technique": "sink-native",
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return PostgresqlDestinationConfig(**base)  # type: ignore[arg-type]


@asynccontextmanager
async def _open_pool(container: PostgresContainer) -> AsyncIterator[PgPoolLike]:
    settings = PgConnectionSettings(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(container.port)),
        database=container.dbname,
        user=container.username,
        password=container.password,
        ssl_context=None,  # test container, loopback-published port; see PgConnectionSettings
    )
    pool = await create_pool(settings, min_size=1, max_size=2, command_timeout=5.0)
    try:
        yield pool
    finally:
        await pool.close()


def test_duplicate_delivery_produces_one_row() -> None:
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> tuple[bool, bool, int]:
            async with _open_pool(container) as pool:
                await pool.execute("CREATE TABLE observations (tb_msg_id TEXT PRIMARY KEY, value INTEGER NOT NULL)")
                loader = PostgresqlLoader(make_config(), pool)
                await loader.verify_idempotency_constraint()  # must not raise: PRIMARY KEY covers it
                message = make_message({"value": 42})

                first = await loader.deliver(message)
                second = await loader.deliver(message)  # simulates a blocking-retry redelivery

                row_count = await _as_asyncpg_pool(pool).fetchval(
                    "SELECT count(*) FROM observations WHERE tb_msg_id = $1", message.msg_id
                )
                return first.success and first.inserted, second.success and not second.inserted, int(row_count)

        first_ok, second_ok, row_count = asyncio.run(run())
        assert first_ok is True
        assert second_ok is True
        assert row_count == 1  # the duplicate delivery never became a second row


def test_two_different_messages_produce_two_rows() -> None:
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> int:
            async with _open_pool(container) as pool:
                await pool.execute("CREATE TABLE observations (tb_msg_id TEXT PRIMARY KEY, value INTEGER NOT NULL)")
                loader = PostgresqlLoader(make_config(), pool)
                await loader.deliver(make_message({"value": 1}))
                await loader.deliver(make_message({"value": 2}))
                count = await _as_asyncpg_pool(pool).fetchval("SELECT count(*) FROM observations")
                return int(count)

        assert asyncio.run(run()) == 2  # two distinct tb-msg-ids: no conflict, both rows land


def test_verify_idempotency_constraint_refuses_a_table_with_no_unique_index() -> None:
    """The startup half of README 3.4.3's proof: a table that CAN silently duplicate must stop the
    loader before it ever delivers, not be trusted because the spec says `sink-native`."""
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> None:
            async with _open_pool(container) as pool:
                await pool.execute("CREATE TABLE observations (tb_msg_id TEXT NOT NULL, value INTEGER NOT NULL)")
                loader = PostgresqlLoader(make_config(), pool)
                await loader.verify_idempotency_constraint()

        with pytest.raises(IdempotencyConstraintError):
            asyncio.run(run())


def test_verify_idempotency_constraint_accepts_a_unique_index_not_just_a_primary_key() -> None:
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> None:
            async with _open_pool(container) as pool:
                await pool.execute("CREATE TABLE observations (tb_msg_id TEXT NOT NULL, value INTEGER NOT NULL)")
                await pool.execute("CREATE UNIQUE INDEX observations_tb_msg_id_uq ON observations (tb_msg_id)")
                loader = PostgresqlLoader(make_config(), pool)
                await loader.verify_idempotency_constraint()  # must not raise

        asyncio.run(run())  # no exception


def test_undefined_table_is_a_pg_error_code_not_retryable() -> None:
    """Real `asyncpg.exceptions.UndefinedTableError`, not a hand-picked fixture: the classification in
    `loader.py` catches what the driver actually raises."""
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> tuple[bool, str, bool]:
            async with _open_pool(container) as pool:
                loader = PostgresqlLoader(make_config(table="does_not_exist"), pool)
                outcome = await loader.deliver(make_message())
                return outcome.success, outcome.reason, outcome.retryable

        success, reason, retryable = asyncio.run(run())
        assert success is False
        assert reason == "PG_42P01"
        assert retryable is False


def test_row_columns_land_in_the_right_table_columns() -> None:
    with PostgresContainer("postgres:16", driver=None) as container:

        async def run() -> tuple[str, int]:
            async with _open_pool(container) as pool:
                await pool.execute(
                    "CREATE TABLE observations (tb_msg_id TEXT PRIMARY KEY, patient_ref TEXT NOT NULL, value INTEGER NOT NULL)"
                )
                loader = PostgresqlLoader(make_config(), pool)
                message = make_message({"patient_ref": "SYNTH-1", "value": 99})
                outcome = await loader.deliver(message)
                assert outcome.success is True
                row = await _as_asyncpg_pool(pool).fetchrow(
                    "SELECT patient_ref, value FROM observations WHERE tb_msg_id = $1", message.msg_id
                )
                assert row is not None
                return row["patient_ref"], row["value"]

        patient_ref, value = asyncio.run(run())
        assert patient_ref == "SYNTH-1"
        assert value == 99


def test_asyncpg_error_hierarchy_matches_the_loader_classification() -> None:
    """A cheap sanity check that the classes `loader.py` catches actually exist in this installed
    `asyncpg` version under the names it expects — a version bump renaming one of these would
    otherwise fail silently into the generic `PostgresError` branch."""
    assert issubclass(asyncpg.exceptions.UndefinedTableError, asyncpg.exceptions.PostgresError)
    assert issubclass(asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.TransactionRollbackError)
    assert issubclass(asyncpg.exceptions.UniqueViolationError, asyncpg.exceptions.IntegrityConstraintViolationError)
