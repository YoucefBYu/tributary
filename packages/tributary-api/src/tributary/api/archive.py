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

from tributary.archiver.errors import (
    MessageNotFound,
    PayloadUnavailable,
    PublishError,
    QueryError,
    ReplayRefused,
    ReprocessRefused,
)
from tributary.archiver.query import (
    ArchiveQueries,
    ChannelStats,
    ContentIndexItem,
    ContentView,
    EventView,
    Page,
    QueryLimits,
    SearchFilter,
)
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import TopicDefaults
from tributary.sdk.context import Logger

from .audit import Actor, AuditHook
from .chclient import ArchiveClient, ArchiveClientError
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


def content_index_json(items: tuple[ContentIndexItem, ...]) -> list[dict[str, Any]]:
    """`ContentRef`s: what is stored, without bytes."""
    out = []
    for item in items:
        ref: dict[str, Any] = {
            "stage": item.stage,
            "content_type": item.content_type,
            "size_bytes": item.size_bytes,
            "storage": "claim_check" if item.is_claim else "inline",
        }
        if item.destination:
            ref["destination"] = item.destination
        out.append(ref)
    return out


def read_content(
    queries: ArchiveQueries, channel: str, message_id: str, stage: str, destination: str
) -> ContentView | None:
    """The stored content, honouring the archiver's content-storage flag (`archive_settings`, written by migrate).

    * flag false: `409 CONTENT_STORAGE_OFF`;
    * flag unreadable (the archive is down): 503 `ARCHIVE_UNAVAILABLE`, the library's `QUERY_FAILED`;
    * flag true but the table missing: 503 `ARCHIVE_UNAVAILABLE` (enabled and not there is a fault, not "off");
    * no flag ever written (a migrate step that predates it): a missing table reads as off, the interim rule.
    """
    enabled = queries.content_storage_enabled()
    if enabled is False:
        raise ApiProblem("CONTENT_STORAGE_OFF")
    try:
        return queries.read_content(channel, message_id, stage, destination)
    except QueryError as exc:
        if exc.code != "CONTENT_TABLE_MISSING":
            raise
        raise ApiProblem("ARCHIVE_UNAVAILABLE" if enabled else "CONTENT_STORAGE_OFF") from None


def content_json(view: ContentView, channel: str, message_id: str) -> dict[str, Any]:
    """`MessageContent`. A claim-checked row carries the locator and no bytes; headers come from the stored columns."""
    body: dict[str, Any] = {
        "channel": channel,
        "message_id": message_id,
        "stage": view.stage,
        "content_type": view.content_type,
        "size_bytes": view.size_bytes,
        "archived_at": stamp(view.archived_at),
    }
    if view.destination:
        body["destination"] = view.destination
    if view.claim_ref:
        body["storage"] = "claim_check"
        body["claim_check"] = view.claim_ref
    else:
        body["storage"] = "inline"
        body["payload_base64"] = view.payload_base64
    headers = {
        "tb-channel": channel,
        "tb-channel-version": str(view.channel_version),
        "tb-msg-id": message_id,
        "tb-attempt": str(view.attempt),
        "tb-content-type": view.content_type,
        "tb-parent-id": None if view.parent_id is None else str(view.parent_id),
        "tb-correlation-id": None if view.correlation_id is None else str(view.correlation_id),
        "tb-error-code": view.error_code,
    }
    body["headers"] = {k: v for k, v in headers.items() if v}
    return body


def stats_json(stats: ChannelStats) -> dict[str, Any]:
    return {
        "counts": dict(stats.counts),
        "destinations": [
            {
                "destination": d.destination,
                "sent": d.sent,
                "retrying": d.retrying,
                "error": d.error,
                "dlq": d.dlq,
                "duration_ms_p50": d.duration_ms_p50,
                "duration_ms_p95": d.duration_ms_p95,
            }
            for d in stats.destinations
        ],
        "errors": [{"error_code": code, "count": n} for code, n in stats.errors],
        "versions": [
            {"version": v.version, "instances": v.instances, "last_seen": stamp(v.last_seen)} for v in stats.versions
        ],
    }


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

    def content_queries(self, env: str) -> ArchiveQueries:
        """The library's queries on the content client (its own cap on stuck connections)."""
        return ArchiveQueries(self._content_client, database=self.database(env), limits=self._limits)

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
        queries, content_queries = self.queries(env), self.content_queries(env)

        def work() -> dict[str, Any]:
            events = queries.timeline(message_id, channel=channel)
            if not events:
                raise MessageNotFound(message_id)
            # Content storage off (no table) is an empty index, not an error: the library says so itself.
            return {"events": events, "content": content_index_json(content_queries.content_index(channel, message_id))}

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
        queries = self.content_queries(env)  # configuration problems surface before an audit record is spent
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
        view = await self._run_gated(
            self._pools.content, lambda: read_content(queries, channel, message_id, stage, destination or ""), leave
        )
        if view is None:
            raise ApiProblem("CONTENT_NOT_FOUND")
        return content_json(view, channel, message_id)

    # -- stats ---------------------------------------------------------------------------------------

    async def stats(self, env: str, channel: str, window: str) -> dict[str, Any]:
        if window not in _WINDOWS:
            raise ApiProblem("REQUEST_INVALID")
        await self._control.require_channel(env, channel)
        queries = self.queries(env)
        now = self._clock().replace(microsecond=0)
        since = now - timedelta(minutes=_WINDOWS[window])
        stats: ChannelStats = await self._blocking(lambda: queries.channel_stats(channel, since))
        return {
            "environment": env,
            "channel": channel,
            "window": window,
            "since": stamp(since),
            "until": stamp(now),
            "generated_at": stamp(self._clock()),
            **stats_json(stats),
        }
