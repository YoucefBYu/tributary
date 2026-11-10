"""`message_content`: per-stage payload storage (ADR-004 A2, A3, A6; W3-2b). **This is a PHI store.**

One row per `(message_id, stage, destination)`, with the stage decided by the TOPIC the record came from, never
by a header a producer could set:

| Topic | `stage` | `destination` |
|---|---|---|
| `<env>.<ch>.raw` | `received` | `''` |
| `<env>.<ch>.out.<dest>` | `routed` | `<dest>` (a conflicting `tb-destination` header rejects the record) |
| `<env>.<ch>.dlq` | `dlq` | the `tb-destination` header when valid, else `''` |

Stages inside a pipeline run in process (invariant 2), so there is no separate `transformed` content.

**Off by default, on per channel.** Nothing is stored unless `TB_ARCHIVE_CONTENT=true` and the channel is listed in
`TB_ARCHIVE_CONTENT_CHANNELS` (`config.py`). The consumer subscribes only to those channels' topics, and
`parse_content_record` rejects any other channel again (defence in depth).

**Claim-check (invariant 8, A3).** A record with a `tb-claim-check` header is stored as the locator in `claim_ref`
with an empty `payload`: the object is never fetched into ClickHouse. A record with NO claim-check header and an
inline payload over 1 MiB violates invariant 8 and is rejected, not stored.

**Idempotency (invariant 4).** `ReplacingMergeTree(ts)` keyed `(channel, message_id, stage, destination)`, where
`ts` is the Kafka record timestamp, so a redelivery or a second backfill run carries the same `ts` and collapses
(count with `FINAL`). Offsets are committed only after the insert, as in the events archiver.

**What is stored.** The payload bytes as received, `content_type`, `size_bytes`, and the envelope facts needed to
show a message (ids, channel version, attempt, error code, topic/partition/offset). **Never `tb-usr-*` headers**
(`envelope/v0` 2.2) and never a header value that is not in the column list.

**Invariant 6.** Payload bytes are never logged. Rejections carry a fixed code plus topic, partition and offset.
Anything reading these rows is an auditable action (the API emits the audit event; `audit.py`).

**Give up.** An INSERT-only principal can overwrite a content row by inserting the same key with a later `ts`
(unlike `audit_log`, content is not an audit control). Retention: `TB_ARCHIVE_CONTENT_TTL_DAYS` (default 30),
applied by the migrate step (`retention.py`, same decrease guard and `archive_settings` record).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from tributary.runtime.errors import TopicNameError
from tributary.runtime.topics import parse_data_topic
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError

from .errors import EventRejected
from .schema import MAX_TTL_DAYS, qualified

CONTENT_TABLE: Final = "message_content"
MAX_INLINE_BYTES: Final = 1_048_576  # invariant 8: larger payloads are claim-checked
MAX_UINT32: Final = 2**32 - 1  # `channel_version` column; the header allows more (envelope/v0 INTEGER_HEADERS)
#: A record timestamp outside this window is not trusted as a row version or TTL clock and is rejected.
EARLIEST_TS: Final = datetime(2000, 1, 1, tzinfo=UTC)
MAX_FUTURE_SKEW: Final = timedelta(days=1)

COLUMNS: Final = (
    "ts",
    "channel",
    "channel_version",
    "message_id",
    "stage",
    "destination",
    "content_type",
    "size_bytes",
    "payload",
    "claim_ref",
    "parent_id",
    "correlation_id",
    "attempt",
    "error_code",
    "topic",
    "partition",
    "offset",
)

_STAGE_OF_KIND: Final = {"raw": "received", "out": "routed", "dlq": "dlq"}


def content_ddl(*, database: str | None = None, ttl_days: int = 30) -> str:
    """`CREATE TABLE IF NOT EXISTS message_content`. Run by the migrate step only (A5)."""
    if not isinstance(ttl_days, int) or isinstance(ttl_days, bool) or not 1 <= ttl_days <= MAX_TTL_DAYS:
        raise ValueError("ttl_days must be an integer from 1 to 36500")
    return f"""CREATE TABLE IF NOT EXISTS {qualified(CONTENT_TABLE, database)} (
  ts               DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
  channel          LowCardinality(String),
  channel_version  UInt32,
  message_id       UUID,
  stage            LowCardinality(String),
  destination      LowCardinality(String),
  content_type     LowCardinality(String),
  size_bytes       UInt64,
  payload          String CODEC(ZSTD(3)),
  claim_ref        String CODEC(ZSTD(3)),
  parent_id        Nullable(UUID),
  correlation_id   Nullable(UUID),
  attempt          UInt16,
  error_code       LowCardinality(String),
  topic            String,
  `partition`      Int32,
  `offset`         Int64,
  ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (channel, message_id, stage, destination)
TTL toDateTime(ts) + INTERVAL {ttl_days} DAY
SETTINGS ttl_only_drop_parts = 1"""


@dataclass(frozen=True, slots=True)
class ContentRow:
    """One row in `COLUMNS` order (the loop's `RowLike`)."""

    values: tuple[Any, ...]
    ts: datetime
    attrs_dropped: int = 0
    attrs_dropped_hmac: int = 0
    fields_blanked: int = 0


def _optional(headers: dict[str, str], name: str) -> tuple[str | None, int]:
    """The header when it is well formed; else `(None, 1)` (blanked and counted), never an error."""
    value = headers.get(name)
    if value is None:
        return None, 0
    return (value, 0) if h.is_well_formed(name, value) else (None, 1)


def parse_content_record(
    *,
    topic: str,
    raw_headers: Any,
    value: bytes | None,
    timestamp_ms: int | None,
    partition: int,
    offset: int,
    enabled_channels: frozenset[str],
    now: datetime | None = None,
) -> ContentRow:
    """The row for one consumed record, or `EventRejected(code)`. Codes are fixed; nothing quotes the record."""
    try:
        parsed = parse_data_topic(topic)
    except TopicNameError:
        raise EventRejected("CONTENT_TOPIC_INVALID") from None
    stage = _STAGE_OF_KIND.get(parsed.kind)
    if stage is None:
        raise EventRejected("CONTENT_TOPIC_UNSUPPORTED")
    if parsed.channel not in enabled_channels:
        raise EventRejected("CONTENT_CHANNEL_DISABLED")
    if value is None:
        raise EventRejected("CONTENT_EMPTY")  # a tombstone carries no content; claim-checked records use b""
    moment = datetime.fromtimestamp(timestamp_ms / 1000.0, UTC) if timestamp_ms and timestamp_ms > 0 else None
    if moment is None or moment < EARLIEST_TS or moment > (now or datetime.now(UTC)) + MAX_FUTURE_SKEW:
        raise EventRejected("CONTENT_TIMESTAMP_INVALID")  # no now() fallback: ts is the version and the TTL clock
    try:
        headers = h.decode_headers(raw_headers or [])
    except HeaderError:
        raise EventRejected("CONTENT_HEADERS_INVALID") from None
    message_id = headers.get(h.TB_MSG_ID, "")
    if not message_id or not h.is_well_formed(h.TB_MSG_ID, message_id):
        raise EventRejected("CONTENT_MESSAGE_ID_INVALID")
    if headers.get(h.TB_CHANNEL) != parsed.channel:
        raise EventRejected("CONTENT_CHANNEL_MISMATCH")  # the topic says one channel, the record another
    destination = parsed.destination or ""
    claimed_destination, blanked = _optional(headers, h.TB_DESTINATION)
    if parsed.kind == "out":
        if claimed_destination is not None and claimed_destination != destination:
            raise EventRejected("CONTENT_DESTINATION_MISMATCH")
    elif parsed.kind == "dlq":
        destination = claimed_destination or ""
    claim_ref, b1 = _optional(headers, h.TB_CLAIM_CHECK)
    content_type, b2 = _optional(headers, h.TB_CONTENT_TYPE)
    error_code, b3 = _optional(headers, h.TB_ERROR_CODE)
    version, b4 = _optional(headers, h.TB_CHANNEL_VERSION)
    attempt, b5 = _optional(headers, h.TB_ATTEMPT)
    parent, b6 = _optional(headers, h.TB_PARENT_ID)
    correlation, b7 = _optional(headers, h.TB_CORRELATION_ID)
    if version is not None and int(version) > MAX_UINT32:
        version, b4 = None, b4 + 1  # does not fit UInt32: blank and count, as an invalid optional header
    payload = value
    if claim_ref is None and len(payload) > MAX_INLINE_BYTES:
        raise EventRejected("CONTENT_OVERSIZE_WITHOUT_CLAIM_CHECK")
    stored = b"" if claim_ref is not None else payload  # a claim-checked record keeps the locator only (A3)
    when = moment
    values = (
        when,
        parsed.channel,
        int(version) if version is not None else 0,
        uuid.UUID(message_id),
        stage,
        destination,
        content_type or "",
        len(payload),
        stored,
        claim_ref or "",
        uuid.UUID(parent) if parent else None,
        uuid.UUID(correlation) if correlation else None,
        int(attempt) if attempt is not None else 0,
        error_code or "",
        topic,
        partition,
        offset,
    )
    return ContentRow(values=values, ts=when, fields_blanked=blanked + b1 + b2 + b3 + b4 + b5 + b6 + b7)
