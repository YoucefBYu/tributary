"""Read side of the archive: a message timeline and a keyset-paginated search over `message_events`.

**Parameterised SQL only.** Every caller-supplied value reaches ClickHouse through a typed server-side
parameter (`{name:Type}`), never through string formatting. The only text interpolated into SQL is the table
identifier (validated against an identifier pattern at construction) and fixed fragments chosen by this
module. A test asserts no caller value appears in the SQL text.

**Dedup at read (events README 4.1 rule 2).** Both queries `ORDER BY <dedup key>, ts DESC LIMIT 1 BY <dedup
key>`, which keeps the newest copy of each event whether or not a background merge has collapsed it and
whether or not a re-emit crossed a month partition. Filters on non-key columns (`status`, `error_code`,
`attrs`) are applied AFTER the dedup, so a stale copy that happens to match cannot resurface. The dedup key is
the archive's `(channel, scope, message_id, stage, destination, attempt)`; search is `kind = 'message'` only,
where `scope` is empty.

**Keyset pagination.** Order is `(ts, channel, message_id, stage, destination, attempt)` descending, a total
order on message events. The cursor is that tuple of the last row returned; the next page asks for rows
strictly below it. No OFFSET, so a deep page costs what the first costs and a concurrent insert never shifts a
page. The cursor is opaque base64 and is NOT signed: it holds ids and a timestamp (no payload, no PHI), and
tampering can only change the caller's own position, because every filter still applies.

**Time window.** `since`/`until` apply to the stored copies' `ts`. A re-emitted event whose copies straddle a
window edge is returned if any copy is inside it (which copy survives a background merge is not deterministic).

**Cost.** The dedup sorts the rows that pass the key-column filters and the time window, so `channel` and
`since` are required: they are the sorting key prefix and the partition/minmax prune. An unbounded search of
every channel is not offered.

**Search attributes are non-identifying.** An `attrs` filter takes a short-code name and value only. A name
ending `_hmac` or on the identifier deny-list is refused (`QUERY_INVALID`): how identifier-derived search
works is human-reserved (events README 3.1). Rows are returned without any `_hmac` attribute even if one
were stored.

Errors never carry a parameter value or the driver's text (invariant 6).
"""

from __future__ import annotations

import base64
import binascii
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import orjson

from .content import CONTENT_TABLE
from .errors import QueryError
from .event import IDENTIFIER_NAMES, STAGES, STATUSES
from .schema import TABLE

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_IDENT: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_NAME: Final = re.compile(r"[a-z][a-z0-9-]{0,63}")
_ERROR_CODE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
_ATTR_NAME: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
_SHORT_CODE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}")
MAX_PAGE: Final = 500
DEFAULT_PAGE: Final = 100
MAX_ATTR_FILTERS: Final = 4
MAX_TIMELINE_ROWS: Final = 1000
#: Margin below a UUIDv7's embedded time when pruning a timeline by `ts` (clock skew between receiver and
#: producer of the id, and event ts being taken after the id was minted).
_TIMELINE_MARGIN_MS: Final = 5 * 60 * 1000

_KEY: Final = "channel, scope, message_id, stage, destination, attempt"
_VIEW_COLUMNS: Final = (
    "ts, kind, channel, channel_version, stage, status, instance, message_id, parent_id, correlation_id, "
    "destination, attempt, content_type, size_bytes, claim_check, topic, `partition`, `offset`, error_code, "
    "error_message, duration_ms, traceparent, attrs"
)
_VIEW_NAMES: Final = tuple(c.strip().strip("`") for c in _VIEW_COLUMNS.split(","))


@dataclass(frozen=True, slots=True)
class QueryLimits:
    """Per-query ClickHouse limits. Applied to EVERY query this module runs. `readonly=1` goes last: ClickHouse
    refuses a settings change after it. Defaults are bounds, not measurements: the 300k-row volume test uses a
    few MB and well under a second; tune from the real table size."""

    max_execution_time_s: int = 30
    max_memory_usage_bytes: int = 4_000_000_000
    max_rows_to_read: int = 500_000_000

    def settings(self) -> dict[str, Any]:
        return {
            "max_execution_time": self.max_execution_time_s,
            "max_memory_usage": self.max_memory_usage_bytes,
            "max_rows_to_read": self.max_rows_to_read,
            "max_result_rows": MAX_TIMELINE_ROWS + MAX_PAGE + 1,
            "readonly": 1,
        }


@dataclass(frozen=True, slots=True)
class EventView:
    """One deduplicated event. Metadata only: the archive holds no payload (and `error_message` is the
    producer's redacted static text)."""

    ts: datetime
    kind: str
    channel: str
    channel_version: int
    stage: str
    status: str
    instance: str
    message_id: uuid.UUID
    parent_id: uuid.UUID | None
    correlation_id: uuid.UUID | None
    destination: str
    attempt: int
    content_type: str
    size_bytes: int
    claim_check: str
    topic: str
    partition: int | None
    offset: int | None
    error_code: str
    error_message: str
    duration_ms: float | None
    traceparent: str
    attrs: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class ReprocessFacts:
    found: bool
    #: Any `dlq` event naming a destination (destination-scoped failure: reprocess refuses).
    destination_dlq: bool
    #: Any event with a `parent_id` (split child or earlier reprocess: reprocess refuses).
    has_parent: bool
    #: Earliest event that places the message on the given `raw` topic with partition and offset.
    raw_partition: int | None
    raw_offset: int | None


@dataclass(frozen=True, slots=True)
class Page:
    items: tuple[EventView, ...]
    #: Opaque; pass back as `cursor`. `None` when there is no further row.
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class SearchFilter:
    channel: str
    since: datetime
    until: datetime | None = None  # exclusive
    stages: Sequence[str] = ()
    status: str | None = None
    destination: str | None = None
    error_code: str | None = None
    attrs: Mapping[str, str] | None = None


def _bad(name: str) -> QueryError:
    return QueryError("QUERY_INVALID", f"invalid query parameter: {name}")


def _ms(value: datetime, name: str) -> int:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _bad(name)
    return int(value.astimezone(UTC).timestamp() * 1000)


def _uuid_text(value: object, name: str) -> str:
    try:
        parsed = value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except ValueError:
        raise _bad(name) from None
    return str(parsed)


def _pattern(value: object, pattern: re.Pattern[str], name: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise _bad(name)
    return value


def _validate_attrs(attrs: Mapping[str, str]) -> list[tuple[str, str]]:
    if len(attrs) > MAX_ATTR_FILTERS:
        raise _bad("attrs")
    out: list[tuple[str, str]] = []
    for name, value in attrs.items():
        _pattern(name, _ATTR_NAME, "attrs")
        if name.endswith("_hmac") or name in IDENTIFIER_NAMES:
            raise _bad("attrs")
        out.append((name, _pattern(value, _SHORT_CODE, "attrs")))
    return out


@dataclass(frozen=True, slots=True)
class _Cursor:
    ts_ms: int
    channel: str
    message_id: str
    stage: str
    destination: str
    attempt: int

    def encode(self) -> str:
        raw = orjson.dumps([self.ts_ms, self.channel, self.message_id, self.stage, self.destination, self.attempt])
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @classmethod
    def decode(cls, token: str) -> _Cursor:
        try:
            if not isinstance(token, str) or len(token) > 512:
                raise ValueError
            data = orjson.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
            ts_ms, channel, message_id, stage, destination, attempt = data
            if (
                isinstance(ts_ms, bool)
                or not isinstance(ts_ms, int)
                or isinstance(attempt, bool)
                or not isinstance(attempt, int)
                or not 0 <= attempt <= 9999
                or stage not in STAGES
                or (destination and _NAME.fullmatch(destination) is None)
                or _NAME.fullmatch(channel) is None
            ):
                raise ValueError
            return cls(ts_ms, channel, _uuid_text(message_id, "cursor"), stage, destination, attempt)
        except (ValueError, TypeError, binascii.Error, orjson.JSONDecodeError, QueryError):
            raise QueryError("CURSOR_INVALID", "invalid cursor") from None


def _row(names: Sequence[str], values: Sequence[Any]) -> EventView:
    row = dict(zip(names, values, strict=True))
    attrs = {k: v for k, v in dict(row["attrs"] or {}).items() if not str(k).endswith("_hmac")}
    row["attrs"] = attrs
    stamp = row["ts"]  # the column is UTC; the driver may hand back a naive datetime for it
    row["ts"] = stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
    for name in ("message_id", "parent_id", "correlation_id"):
        if row[name] is not None and not isinstance(row[name], uuid.UUID):
            row[name] = uuid.UUID(str(row[name]))
    return EventView(**row)


def uuid7_ms(message_id: str) -> int | None:
    """The embedded millisecond timestamp of a UUIDv7, or `None` for any other version."""
    parsed = uuid.UUID(message_id)
    return parsed.int >> 80 if parsed.version == 7 else None


STATS_STAGES: Final = ("received", "filtered", "routed", "sent", "retrying", "error", "dlq")
CONTENT_STAGES: Final = frozenset({"received", "routed", "dlq"})
_CHANNEL: Final = re.compile(r"[a-z][a-z0-9-]{0,63}")
_DEST: Final = re.compile(r"([a-z][a-z0-9-]{0,63})?")
_LOCATOR: Final = re.compile(r"(s3|gs|az|https)://[A-Za-z0-9._-]{1,253}(:[0-9]{2,5})?(/[A-Za-z0-9._-]{1,128}){1,16}")
_UNKNOWN_TABLE: Final = 60  # ClickHouse error code; read from the driver's text or an `ch_code` attribute
_CH_CODE: Final = re.compile(r"Code: (\d{1,4})")
CONTENT_FLAG_ROW: Final = "content_storage"  # `archive_settings.table_name` of the content on/off flag rows


@dataclass(frozen=True, slots=True)
class DestinationStats:
    destination: str
    sent: int
    retrying: int
    error: int
    dlq: int
    duration_ms_p50: float | None
    duration_ms_p95: float | None


@dataclass(frozen=True, slots=True)
class VersionStats:
    version: int
    instances: int
    last_seen: datetime


@dataclass(frozen=True, slots=True)
class ChannelStats:
    """Counts of deduplicated message events since a time. Identifiers, codes, counts and durations only."""

    counts: dict[str, int]
    destinations: tuple[DestinationStats, ...]
    errors: tuple[tuple[str, int], ...]
    versions: tuple[VersionStats, ...]


@dataclass(frozen=True, slots=True)
class ContentIndexItem:
    """What content is stored for one message, without bytes."""

    stage: str
    destination: str
    content_type: str
    size_bytes: int
    is_claim: bool


@dataclass(frozen=True, slots=True)
class ContentView:
    """One stage's stored content. **PHI**: reading it is an auditable action; the caller audits first."""

    channel_version: int
    stage: str
    destination: str
    content_type: str
    size_bytes: int
    payload_base64: str
    claim_ref: str
    parent_id: uuid.UUID | None
    correlation_id: uuid.UUID | None
    attempt: int
    error_code: str
    archived_at: datetime


def _is_unknown_table(exc: BaseException) -> bool:
    code = getattr(exc, "ch_code", None)
    if isinstance(code, int):
        return code == _UNKNOWN_TABLE
    found = _CH_CODE.search(str(exc))
    return found is not None and int(found.group(1)) == _UNKNOWN_TABLE


class ArchiveQueries:
    """Queries over one `message_events` table with a `clickhouse-connect` client.

    The client is NOT thread-safe (one session): one `ArchiveQueries` per thread or request.
    """

    def __init__(
        self,
        client: Any,
        *,
        database: str | None = None,
        table: str = TABLE,
        limits: QueryLimits | None = None,
        content_table: str = CONTENT_TABLE,
    ) -> None:
        self._client = client
        self._settings = (limits or QueryLimits()).settings()
        if database is not None:
            _pattern(database, _IDENT, "database")
        _pattern(table, _IDENT, "table")
        self._table = f"`{database}`.{table}" if database else table
        _pattern(content_table, _IDENT, "content_table")
        self._content_table = f"`{database}`.{content_table}" if database else content_table
        self._settings_table = f"`{database}`.archive_settings" if database else "archive_settings"

    # -- timeline ---------------------------------------------------------------------------------

    def timeline(self, message_id: str | uuid.UUID, *, channel: str | None = None) -> tuple[EventView, ...]:
        """Every event of one message, oldest first, one row per `(stage, destination, attempt)`.

        `channel` makes the read a sorting-key prefix scan. A UUIDv7 id also prunes by time (`ts >= id time -
        5 min`). A timeline with neither (no channel and a non-v7 id) is refused: it would scan the whole table.
        """
        mid = _uuid_text(message_id, "message_id")
        params: dict[str, Any] = {"message_id": mid, "limit": MAX_TIMELINE_ROWS}
        where = ["kind = 'message'", "message_id = {message_id:UUID}"]
        if channel is not None:
            params["channel"] = _pattern(channel, _NAME, "channel")
            where.append("channel = {channel:String}")
        embedded = uuid7_ms(mid)
        if channel is None and embedded is None:
            # Without a channel the read is not a sorting-key prefix scan, and without a UUIDv7 time it is not
            # bounded by `ts` either: a full-table read per call.
            raise _bad("channel")
        if embedded is not None:
            params["since_ms"] = max(0, embedded - _TIMELINE_MARGIN_MS)
            where.append("ts >= fromUnixTimestamp64Milli({since_ms:Int64}, 'UTC')")
        sql = (
            f"SELECT {_VIEW_COLUMNS} FROM ("  # noqa: S608 - fixed fragments + validated identifier; values are bound
            f"SELECT {_VIEW_COLUMNS}, scope, min(ts) OVER (PARTITION BY {_KEY}) AS first_ts "
            f"FROM {self._table} WHERE {' AND '.join(where)} "
            f"ORDER BY {_KEY}, ts DESC LIMIT 1 BY {_KEY}) "
            # first_ts: a re-emit keeps its event in its original place instead of moving it to the end.
            "ORDER BY first_ts ASC, attempt ASC, stage ASC, destination ASC LIMIT {limit:UInt32}"
        )
        return self._run(sql, params)

    # -- reprocess facts ----------------------------------------------------------------------------

    def reprocess_facts(self, message_id: str | uuid.UUID, *, channel: str, raw_topic: str) -> ReprocessFacts:
        """What reprocess decides on, in two bounded aggregate queries (not the row-capped timeline)."""
        mid = _uuid_text(message_id, "message_id")
        params: dict[str, Any] = {"message_id": mid, "channel": _pattern(channel, _NAME, "channel")}
        where = ["kind = 'message'", "channel = {channel:String}", "message_id = {message_id:UUID}"]
        embedded = uuid7_ms(mid)
        if embedded is not None:
            params["since_ms"] = max(0, embedded - _TIMELINE_MARGIN_MS)
            where.append("ts >= fromUnixTimestamp64Milli({since_ms:Int64}, 'UTC')")
        clause = " AND ".join(where)
        flags = self._scalars(
            "SELECT count(), countIf(stage = 'dlq' AND destination != ''), countIf(parent_id IS NOT NULL) "  # noqa: S608
            f"FROM {self._table} WHERE {clause}",
            params,
        )
        coords = self._scalars(
            "SELECT `partition`, `offset` "  # noqa: S608 - fixed fragments + validated identifier; values are bound
            f"FROM {self._table} WHERE {clause} AND topic = {{topic:String}} AND `partition` IS NOT NULL "
            "AND `offset` IS NOT NULL ORDER BY ts ASC LIMIT 1",
            {**params, "topic": raw_topic},
        )
        return ReprocessFacts(
            found=bool(flags[0]),
            destination_dlq=bool(flags[1]),
            has_parent=bool(flags[2]),
            raw_partition=None if coords is None else int(coords[0]),
            raw_offset=None if coords is None else int(coords[1]),
        )

    def _scalars(self, sql: str, params: Mapping[str, Any]) -> Any:
        try:
            result = self._client.query(sql, parameters=dict(params), settings=dict(self._settings))
            rows = result.result_rows
        except Exception as exc:
            raise QueryError("QUERY_FAILED", f"archive query failed ({type(exc).__name__})") from None
        return rows[0] if rows else None

    # -- search -----------------------------------------------------------------------------------

    def search(self, flt: SearchFilter, *, limit: int = DEFAULT_PAGE, cursor: str | None = None) -> Page:
        """Newest first. Pass `Page.next_cursor` back as `cursor` for the next page."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_PAGE:
            raise _bad("limit")
        params: dict[str, Any] = {"channel": _pattern(flt.channel, _NAME, "channel"), "limit": limit + 1}
        inner = ["kind = 'message'", "channel = {channel:String}"]
        params["since_ms"] = _ms(flt.since, "since")
        inner.append("ts >= fromUnixTimestamp64Milli({since_ms:Int64}, 'UTC')")
        upper_ms: int | None = None
        if flt.until is not None:
            upper_ms = _ms(flt.until, "until")
            if upper_ms <= params["since_ms"]:
                raise _bad("until")
            params["until_ms"] = upper_ms
            inner.append("ts < fromUnixTimestamp64Milli({until_ms:Int64}, 'UTC')")
        if flt.stages:
            stages = list(flt.stages)
            if any(s not in STAGES for s in stages) or len(stages) > len(STAGES):
                raise _bad("stages")
            params["stages"] = stages
            inner.append("stage IN {stages:Array(String)}")
        if flt.destination is not None:
            params["destination"] = _pattern(flt.destination, _NAME, "destination")
            inner.append("destination = {destination:String}")

        outer: list[str] = []
        if flt.status is not None:
            if flt.status not in STATUSES:
                raise _bad("status")
            params["status"] = flt.status
            outer.append("status = {status:String}")
        if flt.error_code is not None:
            params["error_code"] = _pattern(flt.error_code, _ERROR_CODE, "error_code")
            outer.append("error_code = {error_code:String}")
        for i, (name, value) in enumerate(_validate_attrs(flt.attrs or {})):
            params[f"ak{i}"], params[f"av{i}"] = name, value
            outer.append(f"attrs[{{ak{i}:String}}] = {{av{i}:String}}")
        if cursor is not None:
            c = _Cursor.decode(cursor)
            params.update(c_ts=c.ts_ms, c_channel=c.channel, c_mid=c.message_id, c_stage=c.stage)
            params.update(c_dest=c.destination, c_attempt=c.attempt)
            # The cursor applies only AFTER the dedup: pruning the scan by time would hide a newer re-emitted copy
            # and let an older copy of the same event surface on a later page. Cost: a deep page re-sorts from `since`.
            outer.append(
                "(ts, channel, message_id, stage, destination, attempt) < "
                "(fromUnixTimestamp64Milli({c_ts:Int64}, 'UTC'), {c_channel:String}, {c_mid:UUID}, "
                "{c_stage:String}, {c_dest:String}, {c_attempt:UInt16})"
            )
        outer_sql = f" WHERE {' AND '.join(outer)}" if outer else ""
        sql = (
            f"SELECT {_VIEW_COLUMNS} FROM ("  # noqa: S608 - fixed fragments + validated identifier; values are bound
            f"SELECT {_VIEW_COLUMNS}, scope FROM {self._table} WHERE {' AND '.join(inner)} "
            f"ORDER BY {_KEY}, ts DESC LIMIT 1 BY {_KEY}){outer_sql} "
            "ORDER BY ts DESC, channel DESC, message_id DESC, stage DESC, destination DESC, attempt DESC "
            "LIMIT {limit:UInt32}"
        )
        rows = self._run(sql, params)
        items = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = items[-1]
            next_cursor = _Cursor(
                (last.ts - _EPOCH) // timedelta(milliseconds=1),
                last.channel,
                str(last.message_id),
                last.stage,
                last.destination,
                last.attempt,
            ).encode()
        return Page(items=items, next_cursor=next_cursor)

    # -- stats and content (W3-6) -----------------------------------------------------------------
    # Every statement is parameterised, runs with the limits above (`readonly=1`), and reads one channel.

    def _rows(self, sql: str, params: Mapping[str, Any]) -> list[tuple[Any, ...]]:
        try:
            result = self._client.query(sql, parameters=dict(params), settings=dict(self._settings))
            return list(result.result_rows)
        except Exception as exc:  # the driver's text can echo SQL and values: class name only
            if _is_unknown_table(exc):
                raise QueryError("CONTENT_TABLE_MISSING", "archive table is missing") from None
            raise QueryError("QUERY_FAILED", f"archive query failed ({type(exc).__name__})") from None

    def channel_stats(self, channel: str, since: datetime) -> ChannelStats:
        """Counts, per-destination outcomes, error codes and channel versions for `channel` since `since`."""
        _pattern(channel, _CHANNEL, "channel")
        params = {"channel": channel, "since_ms": _ms(since, "since")}
        dedup = (
            "(SELECT stage, destination, instance, channel_version, error_code, duration_ms, ts, "  # noqa: S608
            f"{_KEY} FROM {self._table} WHERE kind = 'message' AND channel = {{channel:String}} "
            "AND ts >= fromUnixTimestamp64Milli({since_ms:Int64}, 'UTC') "
            f"ORDER BY {_KEY}, ts DESC LIMIT 1 BY {_KEY})"
        )
        counts = dict.fromkeys(STATS_STAGES, 0)
        for stage, n in self._rows(f"SELECT stage, count() FROM {dedup} GROUP BY stage", params):  # noqa: S608
            if stage in counts:
                counts[stage] = int(n)

        def _ms_or_none(value: Any) -> float | None:
            return None if value is None or value != value else float(value)

        destinations = tuple(
            DestinationStats(d, int(sent), int(retrying), int(error), int(dlq), _ms_or_none(p50), _ms_or_none(p95))
            for d, sent, retrying, error, dlq, p50, p95 in self._rows(
                "SELECT destination, countIf(stage = 'sent'), countIf(stage = 'retrying'), countIf(stage = 'error'), "  # noqa: S608
                "countIf(stage = 'dlq'), quantileIf(0.5)(duration_ms, stage = 'sent'), "
                f"quantileIf(0.95)(duration_ms, stage = 'sent') FROM {dedup} WHERE destination != '' "
                "GROUP BY destination ORDER BY destination LIMIT 64",
                params,
            )
        )
        errors = tuple(
            (str(code), int(n))
            for code, n in self._rows(
                f"SELECT error_code, count() AS n FROM {dedup} WHERE error_code != '' "  # noqa: S608
                "GROUP BY error_code ORDER BY n DESC, error_code ASC LIMIT 20",
                params,
            )
        )
        versions = tuple(
            VersionStats(int(v), max(int(n), 1), last)
            for v, n, last in self._rows(
                "SELECT channel_version, uniqExactIf(instance, instance != ''), max(ts) "  # noqa: S608
                f"FROM {dedup} GROUP BY channel_version ORDER BY channel_version DESC LIMIT 64",
                params,
            )
        )
        return ChannelStats(counts, destinations, errors, versions)

    def content_index(self, channel: str, message_id: str | uuid.UUID) -> tuple[ContentIndexItem, ...]:
        """Which stages have stored content for a message, without bytes. A missing table is an empty index
        (content storage off), not an error."""
        _pattern(channel, _CHANNEL, "channel")
        params = {"channel": channel, "message_id": _uuid_text(message_id, "message_id")}
        sql = (
            "SELECT stage, destination, content_type, size_bytes, claim_ref != '' AS is_claim FROM ("  # noqa: S608
            f"SELECT stage, destination, content_type, size_bytes, claim_ref FROM {self._content_table} "
            "WHERE channel = {channel:String} AND message_id = {message_id:UUID} "
            "ORDER BY stage, destination, ts DESC LIMIT 1 BY stage, destination) "
            "ORDER BY stage, destination LIMIT 130"
        )
        try:
            rows = self._rows(sql, params)
        except QueryError as exc:
            if exc.code == "CONTENT_TABLE_MISSING":
                return ()
            raise
        return tuple(
            ContentIndexItem(str(st), str(d), str(ct), int(size), bool(claim)) for st, d, ct, size, claim in rows
        )

    def read_content(
        self, channel: str, message_id: str | uuid.UUID, stage: str, destination: str = ""
    ) -> ContentView | None:
        """The newest stored content for `(channel, message_id, stage, destination)`, or None. Raises
        `QueryError("CONTENT_TABLE_MISSING")` when the table does not exist. **PHI**: the caller audits the
        access before calling. A claim-checked row returns the locator and no bytes; a stored locator that fails
        the claim-check grammar is never returned (`QUERY_FAILED`)."""
        _pattern(channel, _CHANNEL, "channel")
        if stage not in CONTENT_STAGES:
            raise _bad("stage")
        _pattern(destination, _DEST, "destination")
        params = {
            "channel": channel,
            "message_id": _uuid_text(message_id, "message_id"),
            "stage": stage,
            "destination": destination,
        }
        rows = self._rows(
            "SELECT channel_version, stage, destination, content_type, size_bytes, "  # noqa: S608
            "base64Encode(payload) AS payload_b64, claim_ref, parent_id, correlation_id, attempt, error_code, ts "
            f"FROM {self._content_table} WHERE channel = {{channel:String}} AND message_id = {{message_id:UUID}} "
            "AND stage = {stage:String} AND destination = {destination:String} ORDER BY ts DESC LIMIT 1",
            params,
        )
        if not rows:
            return None
        version, st, dest, ctype, size, b64, claim, parent, corr, attempt, error_code, archived = rows[0]
        if claim and _LOCATOR.fullmatch(str(claim)) is None:
            raise QueryError("QUERY_FAILED", "stored claim-check locator is invalid")
        return ContentView(
            int(version), str(st), str(dest), str(ctype), int(size), "" if claim else str(b64), str(claim or ""),
            None if parent is None else uuid.UUID(str(parent)),
            None if corr is None else uuid.UUID(str(corr)),
            int(attempt), str(error_code), archived,
        )  # fmt: skip

    def content_storage_enabled(self) -> bool | None:
        """The flag the migrate step last wrote (`archive_settings`, row `content_storage`): True when content
        storage is on, False when off, None when no flag was ever written (migrate has not run, or predates it).
        The API answers 409 CONTENT_STORAGE_OFF for False and 503 for True with `CONTENT_TABLE_MISSING`."""
        try:
            rows = self._rows(
                f"SELECT count(), argMax(enabled, applied_at) FROM {self._settings_table} "  # noqa: S608
                "WHERE table_name = {t:String}",
                {"t": CONTENT_FLAG_ROW},
            )
        except QueryError as exc:
            if exc.code == "CONTENT_TABLE_MISSING":  # archive_settings absent: nothing was ever migrated
                return None
            raise
        if not rows or int(rows[0][0]) == 0 or rows[0][1] is None:
            return None
        return bool(rows[0][1])

    # -- plumbing ---------------------------------------------------------------------------------

    def _run(self, sql: str, params: Mapping[str, Any]) -> tuple[EventView, ...]:
        try:
            result = self._client.query(sql, parameters=dict(params), settings=dict(self._settings))
            names, rows = result.column_names, result.result_rows
        except Exception as exc:  # the driver's text can echo SQL and parameter values: class name only
            raise QueryError("QUERY_FAILED", f"archive query failed ({type(exc).__name__})") from None
        if not rows:  # an empty result may carry no column metadata
            return ()
        if tuple(names) != _VIEW_NAMES:
            raise QueryError("QUERY_FAILED", "archive query returned unexpected columns")
        return tuple(_row(names, r) for r in rows)
