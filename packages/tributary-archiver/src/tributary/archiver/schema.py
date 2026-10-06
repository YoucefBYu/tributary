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

**TTL.** `ttl_days` is a parameter: the retention figure is not decided for the events table (ADR-001
6.6 gives 90 days for `message_content` only). `DEFAULT_TTL_DAYS` is a placeholder, not a policy.
`ttl_only_drop_parts` makes expiry a cheap part drop (monthly partitions).

The `_hmac` columns/attributes: none. `attrs` holds only what `event.clean_attrs` lets through.
"""

from __future__ import annotations

from typing import Any, Final, Protocol

TABLE: Final = "message_events"
DEFAULT_TTL_DAYS: Final = 90  # placeholder: retention policy for events is not decided (see module doc)
MAX_TTL_DAYS: Final = 36_500


class CommandClient(Protocol):
    def command(self, cmd: str, **kwargs: Any) -> Any: ...


def ddl(*, database: str | None = None, ttl_days: int = DEFAULT_TTL_DAYS) -> str:
    if not isinstance(ttl_days, int) or isinstance(ttl_days, bool) or not 1 <= ttl_days <= MAX_TTL_DAYS:
        raise ValueError("ttl_days must be an integer from 1 to 36500")
    name = f"`{database}`.{TABLE}" if database else TABLE
    return f"""CREATE TABLE IF NOT EXISTS {name} (
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


def ttl_clause(ttl_days: int) -> str:
    return f"TTL toDateTime(ts) + toIntervalDay({ttl_days})"


def ensure_schema(client: CommandClient, *, database: str | None = None, ttl_days: int = DEFAULT_TTL_DAYS) -> None:
    """Create the table if absent; re-apply the TTL only when it differs (a `MODIFY TTL` can rewrite parts)."""
    client.command(ddl(database=database, ttl_days=ttl_days))
    name = f"`{database}`.{TABLE}" if database else TABLE
    current = str(
        client.command(
            "SELECT create_table_query FROM system.tables WHERE name = {t:String} "
            "AND database = if({db:String} = '', currentDatabase(), {db:String})",
            parameters={"t": TABLE, "db": database or ""},
        )
    )
    if ttl_clause(ttl_days) not in current:
        client.command(f"ALTER TABLE {name} MODIFY TTL toDateTime(ts) + INTERVAL {ttl_days} DAY")
