"""`ClickhouseLoader`: one delivery attempt of one `Message`, inserted as one row.
`contracts/channel-spec/v0/README.md` section 3.4.3 is the source of truth this follows — see
`postgresql/loader.py`'s docstring for the row-shape and idempotency reasoning this repeats almost word
for word; only the transport and the SQL dialect differ.

## The row

Identical rule to `postgresql`: a JSON object, keys matching `^[a-z_][a-z0-9_]{0,62}$`, values scalars
or `null`, built by a step upstream. A payload that is not such an object, or that sets `idColumn`
itself, is `PAYLOAD_SHAPE_INVALID`, non-retryable.

## Idempotency

Keyed by `tb-msg-id`. Every insert is `FORMAT JSONEachRow` against a `ReplacingMergeTree`-family table
sorted by `idColumn` — **deduplication is eventual** (at merge time), so a plain `count()` after a
duplicate delivery can still show two rows for a while. README 3.4.3 is explicit that the
duplicate-delivery test must assert a `FINAL` count, never a plain one, or it proves nothing;
`tests/clickhouse/test_integration_clickhouse_loader.py` does exactly that. `verify_idempotency_constraint`
— called once at startup, before the `ConsumerLoop` starts consuming — queries `system.tables` for the
table's `engine`/`sorting_key` and raises `tributary.loaders.errors.IdempotencyConstraintError` unless
the engine is a `ReplacingMergeTree` family (`"Replacing" in engine`, which also matches the replicated
variant, `ReplicatedReplacingMergeTree`) **and** the sorting key's first column is `idColumn` — a table
sorted by anything else cannot deduplicate on `idColumn` no matter what engine it uses.

## Timeout, backoff, circuit breaker

One `deliver()` call is one attempt, bounded by `config.timeout_s` as the HTTP request timeout. It does
not retry: `retry.policy: blocking` means the caller (`ConsumerLoop`, through `ClickhouseRecordHandler`)
holds the partition and calls `deliver` again with the same message. `config.circuit_breaker` is
consulted first, same as `postgresql`.

## Error codes (README 3.4 table: `CH_<numeric code>`)

* `TIMEOUT` — `client.py`'s `send` raised `TransportTimeout` (a socket timeout or the overall deadline), or the request timed
  out establishing a connection. Retryable. (A query that ClickHouse itself judges too slow comes back
  as an ordinary HTTP error with `X-ClickHouse-Exception-Code: 159` — `CH_159` below, not this.)
* `CONNECT_FAILED` — `client.py`'s `TransportError` from anything else (DNS failure, connection
  refused, an I/O error reading a response). Retryable.
* `TLS_VERIFY_FAILED` — `client.py`'s `TlsVerificationError`: the TLS handshake failed (certificate or
  hostname verification). Not retryable: a bad CA or hostname mismatch fails identically every time.
  **Kept as a distinct exception from `TransportError`** (code review, Low) so this classification is
  reachable at all — folding a TLS failure into the generic transport case made it silently
  `CONNECT_FAILED`/retryable instead.
* `PAYLOAD_SHAPE_INVALID` — checked before any request is sent. Not retryable.
* `CH_<code>` — a response carrying `X-ClickHouse-Exception-Code` (or, failing that, the `Code: <n>.`
  prefix — both read in `client.py`), **checked on every response regardless of HTTP status** (code
  review, Critical: ClickHouse can send `200` and only fail once the body is written, so a `2xx` alone
  is never sufficient — see `client.py`'s `wait_end_of_query=1` and this module's `_classify_response`).
  Retryable only for a small, named set of codes that are ClickHouse's own way of saying "try again",
  found by reading the server's `ErrorCodes.cpp` naming rather than guessed: `159` `TIMEOUT_EXCEEDED`,
  `164` `READONLY` (a replica temporarily demoted), `202` `TOO_MANY_SIMULTANEOUS_QUERIES`, `209`
  `SOCKET_TIMEOUT`, `210` `NETWORK_ERROR`, `225` `NO_ZOOKEEPER`, `242` `TABLE_IS_READ_ONLY`, `279`
  `ALL_CONNECTION_TRIES_FAILED`, `999` `KEEPER_EXCEPTION`. Every other code — `60` `UNKNOWN_TABLE`, `81`
  `UNKNOWN_DATABASE`, `117` `BAD_ARGUMENTS`, an access-denied code, a type mismatch — repeats identically
  on retry and is not.
* A non-2xx response with no `X-ClickHouse-Exception-Code` header and no parseable `Code: <n>.` body
  prefix (should not happen against a real server, but a proxy or load balancer between this loader and
  ClickHouse could return its own error page) is `CH_UNKNOWN`, not retryable — the safe default, since
  retrying a shape this loader cannot even classify is retrying blind. A **2xx** with neither is success.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from tributary.loaders.errors import IdempotencyConstraintError
from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .client import ClickhouseTransport, Response, TlsVerificationError, TransportError, TransportTimeout
from .config import ClickhouseDestinationConfig

DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

_COLUMN_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")
_SCALAR_TYPES = (str, int, float, bool)

#: ClickHouse's own "try again" codes (loader.py module docstring). Every other `CH_<code>` is
#: non-retryable: repeating the identical query against the identical schema produces the identical
#: failure.
_RETRYABLE_EXCEPTION_CODES = frozenset({159, 164, 202, 209, 210, 225, 242, 279, 999})

#: `Code: <n>. DB::Exception: ...` — the fallback when `X-ClickHouse-Exception-Code` is absent
#: (`client.py`'s module docstring: a proxy between this loader and ClickHouse could swallow it).
_CODE_PREFIX_RE = re.compile(rb"^Code:\s*(\d+)\.")


class PayloadShapeError(Exception):
    """The payload is not README 3.4.3's row shape. `str(exc)` never contains a payload value — only
    column names (developer-chosen) and a fixed reason phrase."""


def _quote_identifier(name: str) -> str:
    return f"`{name}`"


def _quote_table(table: str) -> str:
    """`table` is already `config._TABLE_RE`-validated (schema-qualified, lower-case) — not an
    injection point, same reasoning as `postgresql/loader.py`'s `_quote_table`."""
    return ".".join(_quote_identifier(part) for part in table.split("."))


def parse_row(payload: bytes | None, id_column: str) -> dict[str, Any]:
    """README 3.4.3's row-shape check — identical rule to `postgresql.loader.parse_row`."""
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
        # Never interpolate `key` into the message (code review, Medium): a payload's JSON keys are
        # sender-controlled and can themselves carry PHI, unlike `id_column` above (this loader's own
        # config). Only a fixed phrase and the key's length are safe to say anything about.
        if not isinstance(key, str) or not _COLUMN_RE.fullmatch(key):
            raise PayloadShapeError(f"payload has a key that is not a valid column name (length {len(key)})")
        if value is not None and not isinstance(value, _SCALAR_TYPES):
            raise PayloadShapeError(f"payload key {len(key)} chars long has a non-scalar value")
    return row


def _insert_statement(table: str, id_column: str, row: dict[str, Any], msg_id: str) -> bytes:
    columns = [id_column, *sorted(row)]  # a fixed, deterministic order for the same row shape every time
    quoted = ", ".join(_quote_identifier(c) for c in columns)
    values = {id_column: msg_id, **row}
    line = json.dumps({c: values[c] for c in columns}, separators=(",", ":"))
    header = f"INSERT INTO {_quote_table(table)} ({quoted}) FORMAT JSONEachRow\n"
    return (header + line + "\n").encode("utf-8")


def _verify_query(database: str, table_name: str, id_column: str) -> bytes:
    # database/table_name/id_column are all `config._IDENTIFIER_RE`/`_TABLE_RE`-validated identifiers
    # from this loader's own config, never message content — not an injection point.
    query = (
        "SELECT engine, sorting_key FROM system.tables "  # noqa: S608 - database/table_name are validated
        f"WHERE database = '{database}' AND name = '{table_name}' FORMAT TabSeparated"
    )
    return query.encode()


def _classify_response(response: Response) -> tuple[str, bool] | None:
    """`(reason, retryable)` if `response` is a ClickHouse exception, else `None` for a genuine
    success. Checked the same way regardless of HTTP status (code review, Critical) — the header is
    tried first, the `Code: <n>.` body prefix is the fallback, matching `client.py`'s module docstring
    on why a `2xx` alone is never sufficient."""
    code: int | None = None
    for name, value in response.headers.items():
        if name.lower() == "x-clickhouse-exception-code":
            try:
                code = int(value)
            except ValueError:
                code = None
            break
    if code is None:
        match = _CODE_PREFIX_RE.match(response.body)
        if match is not None:
            code = int(match.group(1))
    if code is None:
        return ("CH_UNKNOWN", False) if response.status_code >= 300 else None
    return f"CH_{code}", code in _RETRYABLE_EXCEPTION_CODES


@dataclass(frozen=True, slots=True)
class ChDeliveryOutcome(DeliveryOutcome):
    inserted: bool = False  # True: this attempt sent the row (ClickHouse accepted the insert)


class ClickhouseLoader:
    def __init__(
        self, config: ClickhouseDestinationConfig, transport: ClickhouseTransport, *, log: Logger | None = None
    ) -> None:
        self.config = config
        self._transport = transport
        self._log = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        breaker_spec = config.circuit_breaker
        self.breaker = CircuitBreaker(
            breaker_spec.failure_threshold if breaker_spec else DEFAULT_FAILURE_THRESHOLD,
            breaker_spec.open_seconds if breaker_spec else DEFAULT_OPEN_SECONDS,
        )

    def verify_idempotency_constraint(self) -> None:
        """README 3.4.3: refuse to start rather than deliver into a table that cannot actually
        deduplicate. Call once, before the `ConsumerLoop` starts (`main.py`)."""
        schema, _, table_name = self.config.table.partition(".")
        if not table_name:
            schema, table_name = self.config.database, schema
        response = self._transport.send(
            _verify_query(schema, table_name, self.config.id_column), timeout_s=self.config.timeout_s
        )
        failure = _classify_response(response)
        if failure is not None:
            reason, _ = failure
            raise IdempotencyConstraintError(
                f"destination {self.config.destination!r}: could not read system.tables for "
                f"{self.config.table!r} ({reason}); cannot prove sink-native idempotency"
            )
        line = response.body.decode("utf-8", errors="replace").strip()
        if not line:
            raise IdempotencyConstraintError(
                f"destination {self.config.destination!r}: table {self.config.table!r} does not exist; "
                "cannot prove sink-native idempotency (README 3.4.3)"
            )
        engine, _, sorting_key = line.partition("\t")
        first_sort_column = sorting_key.split(",")[0].strip() if sorting_key else ""
        if "Replacing" not in engine or first_sort_column != self.config.id_column:
            raise IdempotencyConstraintError(
                f"destination {self.config.destination!r}: table {self.config.table!r} has engine "
                f"{engine!r} sorted by {sorting_key!r}; sink-native deduplication needs a "
                f"ReplacingMergeTree-family engine sorted first by {self.config.id_column!r} (README 3.4.3)"
            )

    def deliver(self, message: Message) -> ChDeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return ChDeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        try:
            row = parse_row(message.payload, self.config.id_column)
        except PayloadShapeError as exc:
            self.metrics.record("PAYLOAD_SHAPE_INVALID")
            # `str(exc)` is safe to log: `parse_row` never interpolates a payload's own key or value
            # into its message (code review, Medium — a JSON key is sender-controlled and can be PHI).
            self._log.warning(
                "delivery_failed",
                destination=self.config.destination,
                message_id=msg_id,
                reason="PAYLOAD_SHAPE_INVALID",
                detail=str(exc),
            )
            return ChDeliveryOutcome(success=False, reason="PAYLOAD_SHAPE_INVALID", retryable=False)

        statement = _insert_statement(self.config.table, self.config.id_column, row, msg_id)
        try:
            response = self._transport.send(statement, timeout_s=self.config.timeout_s)
        except (TimeoutError, TransportTimeout):
            return self._record_failure(msg_id, "TIMEOUT", retryable=True)
        except TlsVerificationError:
            return self._record_failure(msg_id, "TLS_VERIFY_FAILED", retryable=False)
        except TransportError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)

        failure = _classify_response(response)
        if failure is not None:
            reason, retryable = failure
            return self._record_failure(msg_id, reason, retryable=retryable)

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record("success")
        return ChDeliveryOutcome(success=True, reason="success", retryable=False, inserted=True)

    def _record_failure(self, msg_id: str, reason: str, *, retryable: bool) -> ChDeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        self.breaker.record_failure()
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning("delivery_failed", destination=self.config.destination, message_id=msg_id, reason=reason)
        return ChDeliveryOutcome(success=False, reason=reason, retryable=retryable)

    def close(self) -> None:
        pass  # no persistent connection to close: every request is Connection: close (client.py)
