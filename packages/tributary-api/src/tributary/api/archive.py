"""The archive read endpoints (W3-6): search, message detail, content, stats.

Search and the timeline delegate to `tributary.archiver.query.ArchiveQueries` (parameterised SQL, dedup at read,
keyset cursors). The library takes a `clickhouse-connect`-shaped client; `chclient.ArchiveClient` is that shape
over the API's own HTTP transport (the driver would pull `certifi` into the API, which the licence gate refuses).

The library has no content read and no stats, so those two are read here with the same discipline: server-side
typed parameters only, the database name validated and backticked, the library's `QueryLimits` (including
`readonly=1`) on every statement, and answers that carry identifiers, codes, counts and sizes. Content is the one
operation that returns bytes, and it is audited first (`message.content.view`, fail closed). **Claim-checked
payloads return the locator only; the API never fetches the object** (openapi/v0 README 3.12), so it holds no
bucket credential and needs no allow-list.

Nothing here logs a payload, a key, a header value or a query value. Errors reach the caller as closed problem
codes (`archiver_problem`), never as library or driver text.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from tributary.archiver.content import CONTENT_TABLE
from tributary.archiver.errors import (
    MessageNotFound,
    PayloadUnavailable,
    PublishError,
    QueryError,
    ReplayRefused,
    ReprocessRefused,
)
from tributary.archiver.query import ArchiveQueries, EventView, Page, QueryLimits, SearchFilter
from tributary.archiver.schema import TABLE, qualified
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import TopicDefaults
from tributary.sdk.context import Logger

from .audit import Actor, AuditHook
from .chclient import UNKNOWN_TABLE, ArchiveClient, ArchiveClientError
from .pools import Pools, submit_gated
from .problems import ApiProblem
from .service import ControlPlane

_DB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_LOCATOR = re.compile(r"^(s3|gs|az|https)://[A-Za-z0-9._-]{1,253}(:[0-9]{2,5})?(/[A-Za-z0-9._-]{1,128}){1,16}$")
_WINDOWS: Final = {"5m": 5, "15m": 15, "1h": 60, "6h": 360, "24h": 1440}
_DEST_STATES: Final = frozenset({"routed", "pending", "sent", "retrying", "error", "dlq"})
_COUNT_STAGES: Final = ("received", "filtered", "routed", "sent", "retrying", "error", "dlq")
_KEY: Final = "channel, scope, message_id, stage, destination, attempt"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def parse_timestamp(text: str) -> datetime:
    """`YYYY-MM-DDTHH:MM:SS.mmmZ` (the schema's pattern has been checked) to an aware UTC datetime."""
    return datetime.strptime(text[:-1], TIMESTAMP_FORMAT).replace(tzinfo=UTC)


def stamp(value: datetime) -> str:
    v = value.astimezone(UTC)
    return v.strftime("%Y-%m-%dT%H:%M:%S.") + f"{v.microsecond // 1000:03d}Z"


# -- library errors -> the contract's problems (openapi/v0 README 4.1) -------------------------------------


def archiver_problem(exc: BaseException) -> ApiProblem:
    """The closed mapping. The exception's text is never forwarded."""
    republished = getattr(exc, "republished", None)
    if isinstance(exc, ApiProblem):
        return exc
    if isinstance(exc, QueryError):
        if exc.code == "QUERY_INVALID":
            return ApiProblem("REQUEST_INVALID", status=422)
        if exc.code == "CURSOR_INVALID":
            return ApiProblem("REQUEST_INVALID")
        return ApiProblem("ARCHIVE_UNAVAILABLE")
    if isinstance(exc, ArchiveClientError):
        return ApiProblem("ARCHIVE_UNAVAILABLE")
    if isinstance(exc, MessageNotFound):
        return ApiProblem("MESSAGE_NOT_FOUND")
    code = getattr(exc, "code", None)
    if isinstance(exc, ReprocessRefused | ReplayRefused | PayloadUnavailable | PublishError) and isinstance(code, str):
        if code == "INVALID_REQUEST":
            return ApiProblem("REQUEST_INVALID", status=422)
        if code in _KNOWN_LIBRARY_CODES:
            return ApiProblem(code, republished=republished if isinstance(republished, int) else None)
    return ApiProblem("INTERNAL")


_KNOWN_LIBRARY_CODES: Final = frozenset(
    {
        "ORDERING_WOULD_BREAK", "DESTINATION_SCOPED", "CHILD_MESSAGE", "NO_PAYLOAD_COORDINATES", "RETENTION_LAPSED",
        "OFFSET_NOT_FOUND", "RECORD_MISMATCH", "RECORD_UNDECODABLE", "ENVELOPE_INVALID", "SOURCE_ERROR",
        "SOURCE_TIMEOUT", "PRODUCE_FAILED", "PRODUCE_TIMEOUT", "PRODUCE_REFUSED",
    }
)  # fmt: skip


# -- views -> contract bodies ---------------------------------------------------------------------------


def event_json(v: EventView) -> dict[str, Any]:
    """`MessageEvent`: required members always, the rest only when the stored value says something."""
    out: dict[str, Any] = {
        "ts": stamp(v.ts),
        "kind": "message",
        "channel": v.channel,
        "channel_version": v.channel_version,
        "stage": v.stage,
        "status": v.status,
        "message_id": str(v.message_id),
        "attempt": v.attempt,
        "size_bytes": v.size_bytes,
    }
    optional: dict[str, Any] = {
        "instance": v.instance,
        "parent_id": None if v.parent_id is None else str(v.parent_id),
        "correlation_id": None if v.correlation_id is None else str(v.correlation_id),
        "destination": v.destination,
        "content_type": v.content_type,
        "claim_check": v.claim_check if _LOCATOR.fullmatch(v.claim_check or "") else "",
        "topic": v.topic,
        "partition": v.partition,
        "offset": v.offset,
        "error_code": v.error_code,
        "error_message": v.error_message,
        "duration_ms": v.duration_ms,
        "traceparent": v.traceparent,
        "attrs": dict(v.attrs) or None,
    }
    out.update({k: val for k, val in optional.items() if val not in (None, "")})
    return out


def destination_statuses(events: tuple[EventView, ...]) -> list[dict[str, Any]]:
    """One entry per destination: the stage of its newest event (among the stages the contract lists), the highest
    attempt, when it last moved and the error code of that newest event."""
    newest: dict[str, EventView] = {}
    attempts: dict[str, int] = {}
    for e in events:
        if not e.destination or e.stage not in _DEST_STATES:
            continue
        attempts[e.destination] = max(attempts.get(e.destination, 0), e.attempt)
        if e.destination not in newest or e.ts >= newest[e.destination].ts:
            newest[e.destination] = e
    out = []
    for name in sorted(newest)[:64]:
        e = newest[name]
        item: dict[str, Any] = {
            "destination": name,
            "state": e.stage,
            "attempt": attempts[name],
            "last_ts": stamp(e.ts),
        }
        if e.error_code:
            item["error_code"] = e.error_code
        out.append(item)
    return out


@dataclass(frozen=True, slots=True)
class ArchiveBackend:
    """What the archive endpoints need beyond the control plane: the ClickHouse client and each environment's
    database."""

    client: ArchiveClient
    targets: Mapping[str, str]
    #: Separate clients (each with its own cap on stuck connections) for content views and for event streams.
    content_client: ArchiveClient | None = None
    stream_client: ArchiveClient | None = None
    #: Kafka for reprocess and replay (the `reprocessor` principal's link). Without it those two answer 503.
    kafka: KafkaSettings | None = None
    defaults: TopicDefaults | None = None
    stream_poll_s: float = 2.0
    stream_heartbeat_s: float = 15.0
    max_streams: int = 64  # open event streams per API replica, all subjects
    max_replays: int = 4  # replays running at once in this process
    publisher_factory: Any = None  # tests inject one; the default is the library's `make_publisher`


class ArchiveReader:
    def __init__(
        self,
        client: ArchiveClient,
        targets: Mapping[str, str],
        control: ControlPlane,
        audit: AuditHook,
        log: Logger,
        pools: Pools,
        *,
        content_client: ArchiveClient | None = None,
        stream_client: ArchiveClient | None = None,
        limits: QueryLimits | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client
        self._targets = dict(targets)
        self._control = control
        self._audit = audit
        self._log = log
        self._pools = pools
        self._content_client = content_client or client
        self._stream_client = stream_client or client
        self._limits = limits or QueryLimits()
        self._clock = clock

    # -- plumbing ------------------------------------------------------------------------------------

    def database(self, env: str) -> str:
        """The environment's archive database. 503 `ARCHIVE_UNAVAILABLE` if none is configured for it."""
        db = self._targets.get(env)
        if db is None or not _DB_NAME.fullmatch(db):
            raise ApiProblem("ARCHIVE_UNAVAILABLE")
        return db

    async def require(self, env: str, channel: str) -> None:
        await self._control.require_channel(env, channel)

    def queries(self, env: str, *, stream: bool = False) -> ArchiveQueries:
        """The library's queries on this environment's database. Event streams use their own client, so their
        stuck connections count against their own cap and not the reads'."""
        client = self._stream_client if stream else self._client
        return ArchiveQueries(client, database=self.database(env), limits=self._limits)

    async def _blocking[T](self, work: Callable[[], T]) -> T:
        """Run blocking archive work off the event loop on the reads pool, admitted through its gate: a full gate
        is 503 `ARCHIVE_UNAVAILABLE` at once. Library and driver failures become closed problems."""
        leave = self._pools.read_gate.enter()
        return await self._run_gated(self._pools.reads, work, leave)

    async def _run_gated[T](self, pool: Executor, work: Callable[[], T], leave: Callable[[], None]) -> T:
        """Run on `pool` with the gate slot tied to the job (`submit_gated`): a cancelled request that was still
        queued gives its slot back at once; one that was running keeps it until its thread ends."""
        try:
            return await submit_gated(pool, leave, work)
        except ApiProblem:
            raise
        except Exception as exc:
            raise archiver_problem(exc) from None

    # -- search --------------------------------------------------------------------------------------

    async def search(self, env: str, channel: str, request: Any) -> dict[str, Any]:
        await self._control.require_channel(env, channel)
        queries = self.queries(env)
        flt = SearchFilter(
            channel=channel,
            since=parse_timestamp(request.since),
            until=None if request.until is None else parse_timestamp(request.until),
            stages=tuple(request.stages or ()),
            status=request.status,
            destination=request.destination,
            error_code=request.error_code,
            attrs=request.attrs,
        )
        page: Page = await self._blocking(lambda: queries.search(flt, limit=request.limit, cursor=request.cursor))
        return {"items": [event_json(v) for v in page.items], "next_cursor": page.next_cursor}

    # -- detail --------------------------------------------------------------------------------------

    async def detail(self, env: str, channel: str, message_id: str) -> dict[str, Any]:
        await self._control.require_channel(env, channel)
        queries, database = self.queries(env), self.database(env)

        def work() -> dict[str, Any]:
            events = queries.timeline(message_id, channel=channel)
            if not events:
                raise MessageNotFound(message_id)
            return {"events": events, "content": self._content_index(database, channel, message_id)}

        got = await self._blocking(work)
        events: tuple[EventView, ...] = got["events"]
        body: dict[str, Any] = {
            "environment": env,
            "channel": channel,
            "message_id": message_id,
            "events": [event_json(e) for e in events[:1000]],
            "truncated": len(events) > 1000,
            "destinations": destination_statuses(events),
            "content": got["content"],
        }
        for e in events:  # the lineage ids are on any of the message's events
            if e.parent_id is not None and "parent_id" not in body:
                body["parent_id"] = str(e.parent_id)
            if e.correlation_id is not None and "correlation_id" not in body:
                body["correlation_id"] = str(e.correlation_id)
        return body

    def _content_index(self, database: str, channel: str, message_id: str) -> list[dict[str, Any]]:
        """What is stored, without bytes. Content storage off (no table) is an empty index, not an error."""
        table = qualified(CONTENT_TABLE, database)
        sql = (
            "SELECT stage, destination, content_type, size_bytes, claim_ref != '' AS is_claim FROM ("  # noqa: S608
            f"SELECT stage, destination, content_type, size_bytes, claim_ref FROM {table} "
            "WHERE channel = {channel:String} AND message_id = {message_id:UUID} "
            "ORDER BY stage, destination, ts DESC LIMIT 1 BY stage, destination) "
            "ORDER BY stage, destination LIMIT 130"
        )
        try:
            rows = self._client.query(
                sql, {"channel": channel, "message_id": message_id}, self._limits.settings()
            ).result_rows
        except ArchiveClientError as exc:
            if exc.ch_code == UNKNOWN_TABLE:
                return []
            raise
        out = []
        for stage, destination, content_type, size_bytes, is_claim in rows:
            item: dict[str, Any] = {
                "stage": stage,
                "content_type": content_type,
                "size_bytes": int(size_bytes),
                "storage": "claim_check" if is_claim else "inline",
            }
            if destination:
                item["destination"] = destination
            out.append(item)
        return out

    # -- content (PHI) -------------------------------------------------------------------------------

    async def content(
        self, env: str, channel: str, message_id: str, stage: str, destination: str | None, actor: Actor
    ) -> dict[str, Any]:
        """One stage's stored content. **Audited before the read, fail closed** (`message.content.view`): if the
        record is not acknowledged this raises 503 `AUDIT_UNAVAILABLE` and nothing has been read. A read that then
        fails is still on the record (audit/v0 README 3 rule 2). Claim-checked payloads return the locator only."""
        if stage not in ("received", "routed", "dlq"):
            raise ApiProblem("REQUEST_INVALID")  # a malformed value: 400
        if (stage == "received" and destination is not None) or (stage == "routed" and destination is None):
            raise ApiProblem("REQUEST_INVALID", status=422)  # well formed, but not a combination that exists
        await self._control.require_channel(env, channel)
        database = self.database(env)  # configuration problems surface before an audit record is spent
        leave = self._pools.content_gate.enter()  # a full gate refuses here, BEFORE any audit record is written
        try:
            await self._audit.requested(
                action="message.content.view",
                environment=env,
                actor=actor,
                channel=channel,
                message_id=message_id,
                stage=stage,
                destination=destination,
            )
        except BaseException:
            leave()
            raise
        row = await self._run_gated(
            self._pools.content,
            lambda: self._content_row(database, channel, message_id, stage, destination or ""),
            leave,
        )
        if row is None:
            raise ApiProblem("CONTENT_NOT_FOUND")
        return row

    def _content_row(
        self, database: str, channel: str, message_id: str, stage: str, destination: str
    ) -> dict[str, Any] | None:
        table = qualified(CONTENT_TABLE, database)
        sql = (
            "SELECT channel_version, stage, destination, content_type, size_bytes, base64Encode(payload) AS payload_b64, "  # noqa: S608
            "claim_ref, parent_id, correlation_id, attempt, error_code, ts "
            f"FROM {table} WHERE channel = {{channel:String}} AND message_id = {{message_id:UUID}} "
            "AND stage = {stage:String} AND destination = {destination:String} ORDER BY ts DESC LIMIT 1"
        )
        try:
            rows = self._content_client.query(
                sql,
                {"channel": channel, "message_id": message_id, "stage": stage, "destination": destination},
                self._limits.settings(),
            ).result_rows
        except ArchiveClientError as exc:
            if exc.ch_code == UNKNOWN_TABLE:
                raise ApiProblem("CONTENT_STORAGE_OFF") from None
            raise
        if not rows:
            return None
        (version, st, dest, ctype, size, b64, claim, parent, corr, attempt, error_code, archived) = rows[0]
        body: dict[str, Any] = {
            "channel": channel,
            "message_id": message_id,
            "stage": st,
            "content_type": ctype,
            "size_bytes": int(size),
            "archived_at": stamp(archived),
        }
        if dest:
            body["destination"] = dest
        if claim:
            if not _LOCATOR.fullmatch(claim):
                raise ArchiveClientError  # a stored locator the grammar refuses is never shown
            body["storage"] = "claim_check"
            body["claim_check"] = claim
        else:
            body["storage"] = "inline"
            body["payload_base64"] = b64
        headers = {
            "tb-channel": channel,
            "tb-channel-version": str(version),
            "tb-msg-id": message_id,
            "tb-attempt": str(attempt),
            "tb-content-type": ctype,
            "tb-parent-id": None if parent is None else str(parent),
            "tb-correlation-id": None if corr is None else str(corr),
            "tb-error-code": error_code,
        }
        body["headers"] = {k: v for k, v in headers.items() if v}
        return body

    # -- stats ---------------------------------------------------------------------------------------

    async def stats(self, env: str, channel: str, window: str) -> dict[str, Any]:
        if window not in _WINDOWS:
            raise ApiProblem("REQUEST_INVALID")
        await self._control.require_channel(env, channel)
        database = self.database(env)
        now = self._clock().replace(microsecond=0)
        since = now - timedelta(minutes=_WINDOWS[window])
        body = await self._blocking(lambda: self._stats(database, channel, since))
        return {
            "environment": env,
            "channel": channel,
            "window": window,
            "since": stamp(since),
            "until": stamp(now),
            "generated_at": stamp(self._clock()),
            **body,
        }

    def _stats(self, database: str, channel: str, since: datetime) -> dict[str, Any]:
        table = qualified(TABLE, database)
        params = {"channel": channel, "since_ms": int(since.timestamp() * 1000)}
        dedup = (
            "(SELECT stage, destination, instance, channel_version, error_code, duration_ms, ts, "  # noqa: S608
            f"{_KEY} FROM {table} WHERE kind = 'message' AND channel = {{channel:String}} "
            "AND ts >= fromUnixTimestamp64Milli({since_ms:Int64}, 'UTC') "
            f"ORDER BY {_KEY}, ts DESC LIMIT 1 BY {_KEY})"
        )
        settings = self._limits.settings()

        def run(sql: str) -> list[tuple[Any, ...]]:
            return self._client.query(sql, params, settings).result_rows

        counts = dict.fromkeys(_COUNT_STAGES, 0)
        for stage, n in run(f"SELECT stage, count() FROM {dedup} GROUP BY stage"):  # noqa: S608
            if stage in counts:
                counts[stage] = int(n)
        destinations = [
            {
                "destination": d,
                "sent": int(sent),
                "retrying": int(retrying),
                "error": int(error),
                "dlq": int(dlq),
                "duration_ms_p50": None if p50 is None or p50 != p50 else float(p50),
                "duration_ms_p95": None if p95 is None or p95 != p95 else float(p95),
            }
            for d, sent, retrying, error, dlq, p50, p95 in run(
                "SELECT destination, countIf(stage = 'sent'), countIf(stage = 'retrying'), countIf(stage = 'error'), "  # noqa: S608
                "countIf(stage = 'dlq'), quantileIf(0.5)(duration_ms, stage = 'sent'), "
                f"quantileIf(0.95)(duration_ms, stage = 'sent') FROM {dedup} WHERE destination != '' "
                "GROUP BY destination ORDER BY destination LIMIT 64"
            )
        ]
        errors = [
            {"error_code": code, "count": int(n)}
            for code, n in run(
                f"SELECT error_code, count() AS n FROM {dedup} WHERE error_code != '' "  # noqa: S608
                "GROUP BY error_code ORDER BY n DESC, error_code ASC LIMIT 20"
            )
        ]
        versions = [
            {"version": int(v), "instances": max(int(n), 1), "last_seen": stamp(last)}
            for v, n, last in run(
                "SELECT channel_version, uniqExactIf(instance, instance != ''), max(ts) "  # noqa: S608
                f"FROM {dedup} GROUP BY channel_version ORDER BY channel_version DESC LIMIT 64"
            )
        ]
        return {"counts": counts, "destinations": destinations, "errors": errors, "versions": versions}
