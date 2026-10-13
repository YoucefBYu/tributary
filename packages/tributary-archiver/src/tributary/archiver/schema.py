"""`message_events` DDL (ADR-001 6.6, `contracts/events/v0` README 4) and its idempotent migration.

**Idempotency.** `ReplacingMergeTree(ts)`; the sorting key is the dedup key, which is the contract's
`(message_id, stage, destination, attempt)` (README 2.2: events are at-least-once and a worker restart
re-emits) plus `channel` and `scope`. For message events `ts` is deliberately NOT in the key: a re-emitted
event carries a new `ts`, and a key containing it would never collapse. `scope` is empty for message events and
`topic:partition:offset:ts` for partition and record events, which have no message identity. `ts` IS part
of `scope` for those kinds, so two pauses of one partition are two rows, and a re-emitted partition or
record event carries its original `ts` only if it is the same Kafka record redelivered (a new emission
is a new row). Count with `FINAL` (merges are eventual).

Known edge: a replacing merge only collapses rows inside one partition, and the partition is
`toYYYYMM(ts)`. A re-emitted event whose `ts` falls in the next month from the original is not collapsed.
Queries that must be exact across that boundary use `argMax(..., ts) GROUP BY` the key, not `FINAL`.

**TTL.** `ttl_days` is an operator setting (ADR-004 R2: events 90, content 30, audit 2190 days, range
1-36500). The DDL here sets it at creation; changing it on an existing table is `retention.py` (R6).
`ttl_only_drop_parts` makes expiry a cheap part drop (monthly partitions).

The `_hmac` columns/attributes: none. `attrs` holds only what `event.clean_attrs` lets through.
"""

from __future__ import annotations

from typing import Any, Final, Protocol

TABLE: Final = "message_events"
DEFAULT_TTL_DAYS: Final = 90  # ADR-004 R2 default for archived events
MAX_TTL_DAYS: Final = 36_500
DEFAULT_CONTENT_TTL_DAYS: Final = 30  # ADR-004 R2; read only when TB_ARCHIVE_CONTENT is on (W3-2b)
DEFAULT_AUDIT_TTL_DAYS: Final = 2190  # ADR-004 R2: an engineering default, not a compliance determination


class CommandClient(Protocol):
    def command(self, cmd: str, **kwargs: Any) -> Any: ...


def ddl(*, database: str | None = None, ttl_days: int = DEFAULT_TTL_DAYS) -> str:
    _check_days(ttl_days)
    return f"""CREATE TABLE IF NOT EXISTS {qualified(TABLE, database)} (
  ts               DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
  kind             LowCardinality(String),
  channel          LowCardinality(String),
  channel_version  UInt32,
  stage            LowCardinality(String),
  status           LowCardinality(String),
  instance         LowCardinality(String),
  message_id       UUID,
  parent_id        Nullable(UUID),
  correlation_id   Nullable(UUID),
  destination      LowCardinality(String),
  attempt          UInt16,
  content_type     LowCardinality(String),
  size_bytes       UInt64,
  claim_check      String CODEC(ZSTD(3)),
  topic            String,
  `partition`      Nullable(Int32),
  `offset`         Nullable(Int64),
  error_code       LowCardinality(String),
  error_message    String CODEC(ZSTD(3)),
  duration_ms      Nullable(Float32),
  traceparent      String CODEC(ZSTD(3)),
  attrs            Map(String, String) CODEC(ZSTD(3)),
  scope            String CODEC(ZSTD(3)),
  ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3),
  INDEX ix_ts ts TYPE minmax GRANULARITY 4
) ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (channel, message_id, stage, destination, attempt, scope)
TTL toDateTime(ts) + INTERVAL {ttl_days} DAY
SETTINGS ttl_only_drop_parts = 1"""


AUDIT_TABLE: Final = "audit_log"
SETTINGS_TABLE: Final = "archive_settings"


def qualified(table: str, database: str | None) -> str:
    return f"`{database}`.{table}" if database else table


def _check_days(ttl_days: int) -> None:
    if not isinstance(ttl_days, int) or isinstance(ttl_days, bool) or not 1 <= ttl_days <= MAX_TTL_DAYS:
        raise ValueError("ttl_days must be an integer from 1 to 36500")


def audit_ddl(*, database: str | None = None, ttl_days: int = DEFAULT_AUDIT_TTL_DAYS) -> str:
    """`audit_log`, the audit system of record (`contracts/audit/v0` README 4; ADR-004 R7 rev 3).

    **Append-only: plain `MergeTree`, never `ReplacingMergeTree`.** With a replacing engine, anyone holding INSERT
    could overwrite an audit row by inserting the same sorting key with a later version. Here an INSERT can only
    add a row. Columns and types are the contract's. Identifiers, actor, action, codes and counts only: never a
    payload, a key or a header value (invariant 6). Retention is R2 (`TB_ARCHIVE_AUDIT_TTL_DAYS`, default 2190),
    applied by the migrate step only.

    **`inserted_at` is MATERIALIZED, not DEFAULT.** The dedup keeps the earliest copy, so a column an INSERT-only
    principal can set would let a forged copy (`inserted_at` 1970) win every read. ClickHouse refuses an explicit
    value for a MATERIALIZED column (integration test). `source_partition` and `source_offset` are client-set
    and only break ties between rows archived in the same millisecond; that residual is documented, not closed.
    `SELECT *` omits a MATERIALIZED column: name `inserted_at` explicitly when it is needed.

    **Read rule (`AUDIT_DEDUP_ORDER`, `audit_dedup_sql`).** Redelivered duplicates stay on disk and collapse at
    query time on `audit_id`, keeping the earliest-archived copy. No `FINAL`. Two rows with one `audit_id` and
    different fields are a tamper signal (`audit_tamper_sql`), not a redelivery.
    """
    _check_days(ttl_days)
    return f"""CREATE TABLE IF NOT EXISTS {qualified(AUDIT_TABLE, database)} (
  audit_id         UUID,
  ts               DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
  environment      LowCardinality(String),
  action           LowCardinality(String),
  phase            LowCardinality(String),
  actor            String CODEC(ZSTD(3)),
  actor_roles      Array(LowCardinality(String)),
  request_id       Nullable(UUID),
  operation_id     Nullable(UUID),
  message_id       Nullable(UUID),
  deployment_id    Nullable(UUID),
  channel          String DEFAULT '',
  destination      String DEFAULT '',
  target_topic     String DEFAULT '',
  stage            LowCardinality(String) DEFAULT '',
  error_code       LowCardinality(String) DEFAULT '',
  version          Nullable(UInt32),
  previous_version Nullable(UInt32),
  allow_reorder    Nullable(Bool),
  range_start      Nullable(DateTime64(3, 'UTC')),
  range_end        Nullable(DateTime64(3, 'UTC')),
  `count`          Nullable(UInt64),
  source_partition UInt32,
  source_offset    UInt64,
  inserted_at      DateTime64(3, 'UTC') MATERIALIZED now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (actor, ts, audit_id)
TTL toDateTime(ts) + INTERVAL {ttl_days} DAY
SETTINGS ttl_only_drop_parts = 1"""


#: Columns that are the record itself. Provenance (`source_*`) and `inserted_at` are ours and may differ
#: between honest redeliveries; everything here may not.
AUDIT_CONTENT_COLUMNS: Final = (
    "ts, environment, action, phase, actor, actor_roles, request_id, operation_id, message_id, deployment_id, "
    "channel, destination, target_topic, stage, error_code, version, previous_version, allow_reorder, "
    "range_start, range_end, `count`"
)
#: The dedup order of every audit read: the earliest-archived copy of an `audit_id` wins.
AUDIT_DEDUP_ORDER: Final = "ORDER BY inserted_at, source_partition, source_offset LIMIT 1 BY audit_id"


def audit_dedup_sql(*, database: str | None = None) -> str:
    """The deduplicated view of `audit_log` for a `FROM (...)` clause. Never use `FINAL` on this table."""
    return f"(SELECT * FROM {qualified(AUDIT_TABLE, database)} {AUDIT_DEDUP_ORDER})"  # noqa: S608


def audit_tamper_sql(*, database: str | None = None) -> str:
    """Count of `audit_id`s that have two or more differing copies: something other than a redelivery wrote
    the table. The number to export and alert on; honest redeliveries (identical content) are not counted."""
    table = qualified(AUDIT_TABLE, database)
    return (
        f"SELECT count() FROM (SELECT audit_id FROM {table} GROUP BY audit_id "  # noqa: S608
        f"HAVING uniqExact(tuple({AUDIT_CONTENT_COLUMNS})) > 1)"
    )


def settings_ddl(*, database: str | None = None) -> str:
    """`archive_settings`: one row per applied retention change (ADR-004 R6 (3)); the API reads it (R3).
    No TTL: it is the record of what retention was. `source` is `applied` (this run altered the TTL) or
    `backfilled` (an earlier run altered it and its row was lost). `enabled` is NULL on TTL rows; the pseudo-table
    row `content_storage` carries the content on/off flag the API reads (`query.ArchiveQueries.content_storage_enabled`)."""
    return f"""CREATE TABLE IF NOT EXISTS {qualified(SETTINGS_TABLE, database)} (
  table_name     LowCardinality(String),
  days           UInt32,
  previous_days  Nullable(UInt32),
  applied_at     DateTime64(3, 'UTC') DEFAULT now64(3),
  release        String,
  applied_by     String DEFAULT currentUser(),
  decrease_allowed Bool DEFAULT false,
  source         LowCardinality(String) DEFAULT 'applied',
  enabled        Nullable(Bool)
) ENGINE = MergeTree
ORDER BY (table_name, applied_at)"""


def settings_upgrade_commands(*, database: str | None = None, has_applied_by: bool) -> list[str]:
    """Adds the columns a pre-W3-30 `archive_settings` lacks. Rows that already exist were written by an unknown
    user, so `applied_by` is added with DEFAULT '' and materialised (old rows read ''), and only then does the
    default become `currentUser()`. Adding it with `currentUser()` directly would stamp the migrating user on
    rows it did not write."""
    name = qualified(SETTINGS_TABLE, database)
    commands = [
        f"ALTER TABLE {name} ADD COLUMN IF NOT EXISTS decrease_allowed Bool DEFAULT false, "
        "ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'applied', "
        "ADD COLUMN IF NOT EXISTS enabled Nullable(Bool)"
    ]
    if not has_applied_by:
        commands += [
            f"ALTER TABLE {name} ADD COLUMN IF NOT EXISTS applied_by String DEFAULT ''",
            f"ALTER TABLE {name} MATERIALIZE COLUMN applied_by",
            f"ALTER TABLE {name} MODIFY COLUMN applied_by String DEFAULT currentUser()",
        ]
    return commands


def ensure_schema(client: CommandClient, *, database: str | None = None, ttl_days: int = DEFAULT_TTL_DAYS) -> None:
    """Create `message_events` if absent. **Never alters an existing table's TTL**: the archivers do not apply
    retention (ADR-004 R6); `retention.apply_retention` does, in the migrate step, with the decrease guard."""
    client.command(ddl(database=database, ttl_days=ttl_days))
