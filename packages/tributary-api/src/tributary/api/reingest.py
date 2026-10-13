"""Reprocess and replay (W3-6) on the archiver library's `ReprocessService` and `ReplayService`.

Both services are blocking and talk to Kafka, so they run in worker threads. Their `AuditHook` is synchronous and
fails closed by raising; `LibraryAudit` bridges it to the API's async `AuditHook` from the worker thread, so the
`requested` record is produced, acknowledged and checked before the service reads or publishes anything, and a
refusal comes back as 503 `AUDIT_UNAVAILABLE` (or, for a replay, `failure_code: AUDIT_UNAVAILABLE`).

* **Reprocess** is synchronous (`200` after the broker acknowledged). One at a time per process: the library's
  `RawPublisher` keeps a cumulative delivery counter that concurrent calls would confuse.
* **Replay** runs in the background (`202`), one producer per replay, with its state in PostgreSQL
  (`replays.ReplayStore`). `replay_id` is the idempotency key. A run this process loses reads as `INTERRUPTED`
  once its deadline passes.
* **Ordering (invariant 5).** The deployed document's `spec.ordering` supplies the library's traits. A per-key
  channel without `allow_reorder` is refused (`ORDERING_WOULD_BREAK`, 409, before any audit record). The API checks
  it itself for a replay, so the refusal comes before `202`.

Nothing here logs a payload, a key or a header value; the library logs ids and counts only.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Protocol

from tributary.archiver.audit import AuditEvent
from tributary.archiver.errors import ArchiverError
from tributary.archiver.ordering import ORDERING_WOULD_BREAK, ChannelTraits
from tributary.archiver.payload_source import KafkaRawPayloadSource
from tributary.archiver.replay import ReplayService
from tributary.archiver.reprocess import ReprocessService
from tributary.archiver.republish import RawPublisher, make_publisher
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import TopicDefaults
from tributary.runtime.logs import JsonLogger
from tributary.sdk.context import Logger

from .archive import ArchiveReader, archiver_problem, parse_timestamp
from .audit import Actor, AuditHook
from .pools import Pools
from .problems import ApiProblem
from .replays import ReplayStore
from .service import ControlPlane

#: `ReplayFailureCode` members the library can produce after `requested`, by its own code.
_REPLAY_CODES = frozenset(
    {
        "TOPIC_NOT_FOUND", "RANGE_TOO_LARGE", "SOURCE_ERROR", "SOURCE_TIMEOUT", "DEADLINE_EXCEEDED",
        "PRODUCE_FAILED", "PRODUCE_TIMEOUT", "PRODUCE_REFUSED", "ENVELOPE_INVALID",
    }
)  # fmt: skip
MAX_CONCURRENT_REPLAYS = 4
AUDIT_WAIT_S = 60.0
CLOSE_TIMEOUT_S = 30.0
#: The longest a replay or reprocess waits on Kafka for one read. Below `CLOSE_TIMEOUT_S` with room to spare, so a
#: replay stopped at shutdown ends within the close wait instead of outliving it.
SOURCE_TIMEOUT_S = 10.0


class PublisherFactory(Protocol):
    def __call__(self, settings: KafkaSettings, defaults: TopicDefaults, *, client_id: str) -> RawPublisher: ...


class LibraryAudit:
    """The library's synchronous `AuditHook`, backed by the API's async one. Called from a worker thread."""

    def __init__(self, loop: asyncio.AbstractEventLoop, hook: AuditHook, environment: str, actor: Actor) -> None:
        self._loop, self._hook, self._env, self._actor = loop, hook, environment, actor

    def __call__(self, event: AuditEvent) -> None:
        fields: dict[str, Any] = {
            "channel": event.channel,
            "message_id": event.message_id,
            "operation_id": event.request_id,
            "target_topic": event.target_topic,
            "range_start": event.range_start,
            "range_end": event.range_end,
        }
        coro: Coroutine[Any, Any, Any]
        if event.phase == "requested":
            fields["allow_reorder"] = event.allow_reorder  # on every requested record, true or false
            coro = self._hook.requested(
                action=event.action, environment=self._env, actor=self._actor, **fields
            )  # raises ApiProblem 503 AUDIT_UNAVAILABLE: the library fails closed on it
        else:
            fields["count"] = event.count
            fields["error_code"] = event.error_code
            coro = self._hook.followup(
                action=event.action, phase=event.phase, environment=self._env, actor=self._actor, **fields
            )
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            future.result(timeout=AUDIT_WAIT_S)
        except TimeoutError:
            future.cancel()  # the record may or may not land; the action must not run (fail closed)
            raise ApiProblem("AUDIT_UNAVAILABLE") from None


def replay_failure_code(exc: BaseException) -> str:
    if isinstance(exc, ApiProblem):
        return "AUDIT_UNAVAILABLE" if exc.code == "AUDIT_UNAVAILABLE" else "INTERNAL"
    code = getattr(exc, "code", None)
    return code if isinstance(exc, ArchiverError) and code in _REPLAY_CODES else "INTERNAL"


class Reingest:
    def __init__(
        self,
        reader: ArchiveReader,
        control: ControlPlane,
        audit: AuditHook,
        store: ReplayStore,
        kafka: KafkaSettings,
        defaults: TopicDefaults,
        log: Logger,
        *,
        pools: Pools,
        publisher_factory: PublisherFactory = make_publisher,
        max_replays: int = MAX_CONCURRENT_REPLAYS,
    ) -> None:
        self._reader, self._control, self._audit, self._store = reader, control, audit, store
        self._kafka, self._defaults, self._log = kafka, defaults, log
        self._factory = publisher_factory
        self._lock = asyncio.Lock()
        self._shared: RawPublisher | None = None
        self._pools = pools
        self._running: dict[asyncio.Task[None], tuple[threading.Event, str]] = {}
        self._max_replays = max_replays
        self._active = 0  # replays holding a slot; touched on the event loop only
        self._closing = False
        self._library_log = JsonLogger("api")  # one for the process, not one per call

    # -- reprocess -----------------------------------------------------------------------------------

    async def reprocess(self, env: str, channel: str, message_id: str, request: Any, actor: Actor) -> dict[str, Any]:
        """Order of checks (openapi/v0 README 3.13): role, channel exists (404), channel deployed (409), then the
        library's refusals, the audit `requested` record (fail closed), the fetch and the publish."""
        ordering = await self._control.deployed_ordering(env, channel)
        if ordering == "per-key" and not request.allow_reorder:
            raise ApiProblem(ORDERING_WOULD_BREAK)
        queries = self._reader.queries(env)
        loop = asyncio.get_running_loop()
        async with self._lock:
            if self._shared is None:
                self._shared = self._factory(self._kafka, self._defaults, client_id="tb-api-reprocess")
            service = ReprocessService(
                queries,
                KafkaRawPayloadSource(self._kafka, self._defaults, env=env, timeout_s=SOURCE_TIMEOUT_S),
                self._shared,
                env=env,
                audit=LibraryAudit(loop, self._audit, env, actor),
                log=self._library_log,
                traits=lambda name: ChannelTraits(ordering) if name == channel else None,  # type: ignore[arg-type]
            )
            try:
                result = await loop.run_in_executor(
                    self._pools.reprocess,
                    lambda: service.reprocess(
                        message_id,
                        channel=channel,
                        request_id=request.operation_id,
                        actor=actor.subject,
                        allow_reorder=request.allow_reorder,
                    ),
                )
            except Exception as exc:
                raise archiver_problem(exc) from None
        return {
            "operation_id": request.operation_id,
            "channel": result.channel,
            "original_message_id": result.original_message_id,
            "new_message_id": result.new_message_id,
            "topic": result.topic,
            "partition": result.partition,
            "offset": result.offset,
        }

    # -- replay --------------------------------------------------------------------------------------

    async def start_replay(self, env: str, channel: str, request: Any, actor: Actor) -> tuple[dict[str, Any], int]:
        """`(Replay, status)`: 202 for a new or re-run replay, 200 for an identical one already running or done.
        Synchronous refusals come before `202` and before any audit record."""
        start, end = parse_timestamp(request.start), parse_timestamp(request.end)
        if end <= start:
            raise ApiProblem("REQUEST_INVALID", status=422)
        ordering = await self._control.deployed_ordering(env, channel)
        self._reader.database(env)
        if ordering == "per-key" and not request.allow_reorder:
            raise ApiProblem(ORDERING_WOULD_BREAK)
        if self._closing or self._active >= self._max_replays:
            raise ApiProblem("UNAVAILABLE")
        self._active += 1  # the slot is taken before anything is stored, and given back on every path that ends here
        try:
            record, started = await self._store.begin(env, channel, request, start, end, actor.subject)
        except BaseException:
            self._active -= 1
            raise
        if not started:
            self._active -= 1
            return record, 200
        stop = threading.Event()
        task = asyncio.create_task(self._run_replay(env, channel, request, start, end, actor, ordering, stop))
        self._running[task] = (stop, request.replay_id)
        task.add_done_callback(lambda t: self._running.pop(t, None))
        return record, 202

    async def _run_replay(
        self,
        env: str,
        channel: str,
        request: Any,
        start: datetime,
        end: datetime,
        actor: Actor,
        ordering: str,
        stop: threading.Event,
    ) -> None:
        publisher: RawPublisher | None = None
        try:
            publisher = self._factory(self._kafka, self._defaults, client_id=f"tb-api-replay-{request.replay_id[-8:]}")
            service = ReplayService(
                self._kafka,
                self._defaults,
                publisher,
                env=env,
                audit=LibraryAudit(asyncio.get_running_loop(), self._audit, env, actor),
                log=self._library_log,
                traits=lambda name: ChannelTraits(ordering) if name == channel else None,  # type: ignore[arg-type]
                idle_timeout_s=SOURCE_TIMEOUT_S,
                # The library checks this clock against its deadline on every loop: once `stop` is set time jumps
                # past any deadline, so the scan ends at its next turn instead of publishing into a closing process.
                clock=lambda: time.monotonic() + (1e9 if stop.is_set() else 0.0),
            )
            result = await asyncio.get_running_loop().run_in_executor(
                self._pools.replays,
                lambda: service.replay(
                    channel,
                    start=start,
                    end=end,
                    replay_id=request.replay_id,
                    actor=actor.subject,
                    max_records=request.max_records,
                    deadline_s=float(request.deadline_s),
                    allow_reorder=request.allow_reorder,
                ),
            )
            await self._store.finish_completed(request.replay_id, result)
        except asyncio.CancelledError:
            raise  # only if the loop itself is torn down: the record stays `running` and reads as INTERRUPTED
        except Exception as exc:
            republished = int(getattr(exc, "republished", 0) or 0)
            code = "INTERRUPTED" if stop.is_set() else replay_failure_code(exc)
            self._log.warning("replay_failed", failure_code=code, republished=republished)
            with contextlib.suppress(Exception):  # the database being down must not hide the cause in the log
                await self._store.finish_failed(request.replay_id, code, republished)
        finally:
            self._active -= 1
            if publisher is not None:
                with contextlib.suppress(Exception):
                    publisher.close()

    async def close(self, timeout_s: float = CLOSE_TIMEOUT_S) -> None:
        """Stop the replays and wait for their threads to end BEFORE the publisher and the audit sink close, so
        nothing publishes or audits into a closed sink. A replay stopped here is recorded `INTERRUPTED` with the
        count it had acknowledged."""
        self._closing = True
        for stop, _ in self._running.values():
            stop.set()
        tasks = dict(self._running)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout_s)
            if pending:  # a thread that outlived the wait: its ids, so an operator can find the rows
                self._log.warning(
                    "replay_close_timeout", replay_ids=sorted(tasks[t][1] for t in pending), waited_s=timeout_s
                )
        if self._shared is not None:
            with contextlib.suppress(Exception):
                self._shared.close()
