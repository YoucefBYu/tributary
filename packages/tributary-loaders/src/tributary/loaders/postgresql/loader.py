"""`PostgresqlLoader`: one delivery attempt of one `Message`, inserted as one row.
`contracts/channel-spec/v0/README.md` section 3.4.3 is the source of truth this follows.

## The row

**The payload is a row**: `application/json`, a JSON object whose keys are column names matching
`^[a-z_][a-z0-9_]{0,62}$` and whose values are scalars (`str`, `int`, `float`, `bool`) or `null`. A step
upstream builds it (README 3.4.3: "There is no column-mapping language in the spec, because that is
where configuration becomes a program the loader has to interpret", ADR-007 D13) — this loader never
shapes the payload, it only inserts it. A payload that is not such an object, or that sets
`idColumn` itself, is `PAYLOAD_SHAPE_INVALID` and non-retryable: retrying an unfixable shape produces
the identical failure forever, same as the HTTP loader's 4xx.

## Idempotency

Keyed by `tb-msg-id`, never configurable (invariant 4). Every insert is

```sql
INSERT INTO <table> (<idColumn>, <row's own columns...>)
VALUES ($1, $2, ...)
ON CONFLICT (<idColumn>) DO NOTHING
```

so a retried delivery of the identical record (blocking retry redelivers the same `tb-msg-id`; a
sender's own resend mints a *new* one, which is a different logical delivery on purpose) either inserts
the row once or, on every later attempt, matches the existing `idColumn` value and does nothing — the
same row, never a second one, no matter how many attempts this loader makes.

**This loader proves the constraint exists rather than trusting the declaration** (README 3.4.3):
`verify_idempotency_constraint` — called once at startup, before the `ConsumerLoop` starts consuming —
queries `information_schema` for a unique index or primary key covering exactly `idColumn`, and raises
`tributary.loaders.errors.IdempotencyConstraintError` if none exists. Without it, `ON CONFLICT` has
nothing to conflict on: the query would still succeed, would still return an `INSERT` tag, and every
duplicate delivery would silently become a second row — the exact failure mode a startup check exists
to make loud instead. `tests/postgresql/test_loader.py::test_duplicate_delivery_is_harmless` demonstrates
the delivery-time half of the proof against a fake pool that applies the same rule a real unique
constraint does; `test_integration_postgresql_loader.py` proves both halves — the startup check AND the
duplicate-delivery behaviour — against a real server.

## Timeout, backoff, circuit breaker

One `deliver()` call is one attempt, bounded by `config.timeout_s` as the query's `timeout=`. It does
not retry: `retry.policy: blocking` (the only value the schema allows) means the caller — the runtime's
`ConsumerLoop`, through `PostgresqlRecordHandler` — holds the partition, waits out a backoff, and calls
`deliver` again with the same message. `config.circuit_breaker`, when configured, is consulted first,
same as the HTTP loader; unlike the HTTP loader, an unconfigured breaker still gets a finite default
(`DEFAULT_FAILURE_THRESHOLD`/`DEFAULT_OPEN_SECONDS`, not `CircuitBreaker.disabled()`), because a wedged
connection pool is a narrower, more mechanical failure mode than an arbitrary third-party HTTP
destination and there is little reason to ever let it retry with no circuit at all.

## Error codes (README 3.4 table)

* `TIMEOUT` — the query did not finish within `timeoutMs`. Retryable.
* `CONNECT_FAILED` — the connection could not be made or was lost: `postgresql.client.TransportError`,
  `asyncpg.exceptions.PostgresConnectionError`, `asyncpg.exceptions.InterfaceError`,
  `asyncpg.exceptions.InternalClientError` (asyncpg's own protocol state machine, not the server, is
  confused — for example a connection the outer `asyncio.wait_for` timeout above cut off mid-operation
  against a backend a peer already killed), `OSError`. Retryable.
* `TLS_VERIFY_FAILED` — `ssl.SSLError` (checked before the broader `OSError`, which it subclasses): the
  server's certificate or hostname failed verification. Not retryable — a bad CA or hostname mismatch
  fails identically on every attempt.
* `PAYLOAD_SHAPE_INVALID` — checked before any query is attempted. Not retryable.
* `PG_<SQLSTATE>` — the query reached PostgreSQL and it returned an error. SQLSTATE class `40`
  (`asyncpg.exceptions.TransactionRollbackError`: deadlock, serialization failure) is retryable — the
  query was fine, a concurrent transaction won a race; every other `asyncpg.exceptions.PostgresError`
  (a constraint violation on some column *other* than `idColumn`, a missing table or column, an
  insufficient-privilege error, ...) is not — retrying repeats the identical failure.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import ssl
from dataclasses import dataclass
from typing import Any

import asyncpg  # type: ignore[import-untyped]  # see client.py's module docstring
from tributary.loaders.errors import IdempotencyConstraintError
from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .client import PgPoolLike, TransportError
from .config import PostgresqlDestinationConfig

#: A wedged connection pool is a narrower failure mode than an arbitrary HTTP destination (module
#: docstring); these apply whenever `config.circuit_breaker` is not set.
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

#: `$defs/sqlIdentifier` / the column-name half of README 3.4.3's row-shape rule.
_COLUMN_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_SCALAR_TYPES = (str, int, float, bool)

#: One unique index or primary key covering exactly `idColumn`, or nothing. `pg_catalog`, not
#: `information_schema`: README 3.4.3 says "a unique index or primary key", and a bare
#: `CREATE UNIQUE INDEX` (no backing `UNIQUE`/`PRIMARY KEY` constraint) is exactly what `ON CONFLICT`
#: needs but does NOT appear in `information_schema.table_constraints` at all — found by the
#: integration test against a real server (`test_verify_idempotency_constraint_accepts_a_unique_index_
#: not_just_a_primary_key`), not by a unit test against a fake pool. `indpred IS NULL` excludes a
#: partial index (does not cover every row) and `indkey[0] <> 0` excludes an expression index (the
#: key is not a plain column) — neither is a target `ON CONFLICT (idColumn)` can use.
_UNIQUE_ID_COLUMN_QUERY = """
SELECT EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = i.indkey[0]
    WHERE n.nspname = $1
      AND t.relname = $2
      AND i.indisunique
      AND cardinality(i.indkey) = 1
      AND i.indpred IS NULL
      AND i.indkey[0] <> 0
      AND a.attname = $3
)
"""


class PayloadShapeError(Exception):
    """The payload is not `README 3.4.3`'s row shape. `str(exc)` never contains a payload value —
    only column names (developer-chosen, part of the destination's schema, not sender data) and a
    fixed reason phrase."""


def _quote_identifier(name: str) -> str:
    return f'"{name}"'


def _quote_table(table: str) -> str:
    """`table` is already `config._TABLE_RE`-validated, so this can only ever quote an identifier
    this loader itself validated — not an injection point — but quoting it means a reserved word
    (`"order"`, `"user"`) still works as a table name."""
    return ".".join(_quote_identifier(part) for part in table.split("."))


def parse_row(payload: bytes | None, id_column: str) -> dict[str, Any]:
    """`README 3.4.3`'s row-shape check. Raises `PayloadShapeError`; never returns something this
    loader would go on to insert unchecked."""
    if payload is None:
        raise PayloadShapeError("payload is absent (a claim-checked record is not a row this loader can insert)")
    try:
        row = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PayloadShapeError("payload is not valid JSON") from exc
    if not isinstance(row, dict):
        raise PayloadShapeError("payload is not a JSON object")
    if id_column in row:
        raise PayloadShapeError(f"payload sets {id_column!r} itself; this loader fills it from tb-msg-id")
    for key, value in row.items():
        # Never interpolate `key` into the message (code review on the clickhouse loader's identical
        # pattern, Medium): a payload's JSON keys are sender-controlled and can themselves carry PHI,
        # unlike `id_column` above (this loader's own config). Only a fixed phrase and the key's
        # length are safe to say anything about.
        if not isinstance(key, str) or not _COLUMN_RE.match(key):
            raise PayloadShapeError(f"payload has a key that is not a valid column name (length {len(key)})")
        if value is not None and not isinstance(value, _SCALAR_TYPES):
            raise PayloadShapeError(f"payload key {len(key)} chars long has a non-scalar value")
    return row


def _insert_query(table: str, id_column: str, columns: list[str]) -> str:
    all_columns = [id_column, *columns]
    quoted = ", ".join(_quote_identifier(c) for c in all_columns)
    placeholders = ", ".join(f"${i}" for i in range(1, len(all_columns) + 1))
    # Flagged by pattern, not by substance: every identifier here is `_COLUMN_RE`/`config._TABLE_RE`
    # validated and every value is a bound `$n` parameter — nothing is interpolated from raw content.
    return f"INSERT INTO {_quote_table(table)} ({quoted}) VALUES ({placeholders}) ON CONFLICT ({_quote_identifier(id_column)}) DO NOTHING"  # noqa: S608


@dataclass(frozen=True, slots=True)
class PgDeliveryOutcome(DeliveryOutcome):
    inserted: bool = False  # True: this attempt inserted the row. False + success: ON CONFLICT no-op.


class PostgresqlLoader:
    def __init__(self, config: PostgresqlDestinationConfig, pool: PgPoolLike, *, log: Logger | None = None) -> None:
        self.config = config
        self._pool = pool
        self._log = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        self.duplicates_absorbed = 0  # ON CONFLICT no-ops: ADR-003 D7's "an idempotent loader absorbs it"
        breaker_spec = config.circuit_breaker
        self.breaker = CircuitBreaker(
            breaker_spec.failure_threshold if breaker_spec else DEFAULT_FAILURE_THRESHOLD,
            breaker_spec.open_seconds if breaker_spec else DEFAULT_OPEN_SECONDS,
        )

    async def verify_idempotency_constraint(self) -> None:
        """README 3.4.3: refuse to start rather than deliver into a table where `ON CONFLICT` has
        nothing to conflict on. Call once, after the pool is open and before the `ConsumerLoop` starts
        (`main.py`)."""
        schema, _, table_name = self.config.table.partition(".")
        if not table_name:
            schema, table_name = "public", schema
        # Bounded the same way `deliver()`'s query is (asyncio.wait_for around the call, not just the
        # `timeout=` kwarg): `asyncpg.pool.Pool.fetchval`'s `timeout=` only bounds the query once a
        # connection is acquired, never the `pool.acquire()` wait itself (`Pool.execute`/`fetchval`'s
        # own source: `async with self.acquire() as con:` with no timeout on that `acquire()`) -- a
        # pool with every connection checked out (or stuck reconnecting) would otherwise hang this
        # startup check forever. Root cause of the intermittent 6-hour CI hang (b36b740 follow-up):
        # this was the one call in the module with no bound at all.
        exists = await asyncio.wait_for(
            self._pool.fetchval(
                _UNIQUE_ID_COLUMN_QUERY, schema, table_name, self.config.id_column, timeout=self.config.timeout_s
            ),
            self.config.timeout_s + 1,
        )
        if not exists:
            raise IdempotencyConstraintError(
                f"destination {self.config.destination!r}: table {self.config.table!r} has no unique "
                f"index or primary key on {self.config.id_column!r}; ON CONFLICT would have nothing to "
                "conflict on and duplicate deliveries would silently become extra rows (README 3.4.3)"
            )

    async def deliver(self, message: Message) -> PgDeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return PgDeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        try:
            row = parse_row(message.payload, self.config.id_column)
        except PayloadShapeError as exc:
            self.metrics.record("PAYLOAD_SHAPE_INVALID")
            self._log.warning(
                "delivery_failed",
                destination=self.config.destination,
                message_id=msg_id,
                reason="PAYLOAD_SHAPE_INVALID",
                detail=str(exc),
            )
            return PgDeliveryOutcome(success=False, reason="PAYLOAD_SHAPE_INVALID", retryable=False)

        columns = sorted(row)  # a fixed, deterministic order for the same row shape every time
        query = _insert_query(self.config.table, self.config.id_column, columns)
        values = [row[column] for column in columns]

        try:
            tag = await asyncio.wait_for(
                self._pool.execute(query, msg_id, *values, timeout=self.config.timeout_s),
                self.config.timeout_s + 1,
            )
        except TimeoutError:
            return self._record_failure(msg_id, "TIMEOUT", retryable=True)
        except TransportError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)
        except asyncpg.exceptions.PostgresConnectionError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)
        except asyncpg.exceptions.InterfaceError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)
        except asyncpg.exceptions.InternalClientError:
            # Not a `PostgresError` (server never gets to answer) and not one of the connection classes
            # above either: the outer `asyncio.wait_for` above cancelling `execute()` mid-flight on a
            # connection whose backend a peer already killed (`test_a_dropped_connection_is_retried_
            # without_crashing_the_shared_loop`'s `pg_terminate_backend`) can leave asyncpg's own
            # protocol state machine unable to tell what state the connection is in ("cannot switch to
            # state N; another operation is in progress") rather than raising a normal connection error.
            # Same shape as any other transport loss from this loader's point of view: retryable, and
            # the next attempt gets a connection the pool has had a chance to notice is unusable.
            # Safe to retry even if THIS attempt's INSERT actually committed before the connection
            # broke: module docstring's Idempotency section -- `ON CONFLICT (idColumn) DO NOTHING`
            # absorbs the redelivery into a no-op rather than a second row.
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)
        except ssl.SSLError:  # before OSError: ssl.SSLError is one
            return self._record_failure(msg_id, "TLS_VERIFY_FAILED", retryable=False)
        except OSError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)
        except asyncpg.exceptions.TransactionRollbackError as exc:
            return self._record_failure(msg_id, f"PG_{exc.sqlstate}", retryable=True)
        except asyncpg.exceptions.PostgresError as exc:
            return self._record_failure(msg_id, f"PG_{exc.sqlstate}", retryable=False)

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record("success")
        inserted = tag != "INSERT 0 0"
        if not inserted:
            self.duplicates_absorbed += 1
        return PgDeliveryOutcome(success=True, reason="success", retryable=False, inserted=inserted)

    def _record_failure(self, msg_id: str, reason: str, *, retryable: bool) -> PgDeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        self.breaker.record_failure()
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning("delivery_failed", destination=self.config.destination, message_id=msg_id, reason=reason)
        return PgDeliveryOutcome(success=False, reason=reason, retryable=retryable)

    async def close(self) -> None:
        """Bounded: `asyncpg.pool.Pool.close()`'s own docstring says "It is advisable to use
        `asyncio.wait_for()` to set a timeout" -- `close()` waits for every checked-out connection to
        be released before it closes anything, with no timeout of its own. A connection left stuck
        (for example the one `test_a_dropped_connection_is_retried_without_crashing_the_shared_loop`
        deliberately kills server-side, or one still mid-reconnect) would otherwise hang this forever,
        the second half of the same root cause as `verify_idempotency_constraint`'s fix above. On
        timeout, `Pool.close()` has *already* called `Pool.terminate()` itself -- its own
        `except (Exception, asyncio.CancelledError)` branch, triggered by the cancellation
        `asyncio.wait_for` raises into it on timeout -- so the pool is forcefully torn down rather than
        left half-closed; nothing further to do here."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._pool.close(), self.config.timeout_s + 1)
