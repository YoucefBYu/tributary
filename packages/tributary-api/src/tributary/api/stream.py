"""Live message events as Server-Sent Events, tailed from the archive (`openapi/v0` README 3.16).

* **Source: the archive, polled** (the library's `search`, with its read-time dedup), every `poll_s` (2 s). The API
  does not consume `_tb.<env>.events`. With the archive down the stream carries heartbeats only.
* **Events:** `message` (data `MessageEvent`, `id` an opaque cursor), `heartbeat` every 15 s, `reset`
  (`cursor_expired`, `cursor_invalid`, `backlog_too_dense`: the stream continues live) and `end` (`token_expiring`, `max_duration`,
  `server_shutdown`: the server closes). Delivery is at-least-once; clients deduplicate on
  `(message_id, stage, destination, attempt)`.
* **Resume:** `Last-Event-ID` within 15 minutes of the event it names; older is `reset` and live.
* **Bounds:** at most 4 open streams per subject and a configurable total per replica (429 `TOO_MANY_STREAMS`),
  60 minutes at most, never past the token's `exp` (checked after every poll and before every event). A poll
  reads each channel with at most 5 pages: up to 16 x 5 = 80 statements per interval per stream, on a pool of its
  own, so a slow archive cannot starve the audit produce on the default executor.
* **No silent gaps under backlog.** If a channel has more new events than a poll can page through, the poll
  narrows its time window (halving, oldest first) until it fits and only advances past what it read. If even the
  smallest window (about a tenth of a second) holds more than 1000 events, the stream sends `reset`
  (`backlog_too_dense`) and continues live: the gap is announced, never skipped quietly.
* **Metadata only.** The stream carries what search returns: no payload, no header values. Nothing about a stream
  is logged beyond counts (the subject is not logged).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable, MutableMapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send
from tributary.archiver.query import EventView, SearchFilter
from tributary.sdk.context import Logger

from .archive import ArchiveReader, event_json, stamp
from .pools import Pools, submit_gated
from .problems import ApiProblem

MAX_STREAMS_PER_SUBJECT: Final = 4
MAX_STREAMS_TOTAL: Final = 64
MAX_DURATION_S: Final = 3600.0
RESUME_WINDOW: Final = timedelta(minutes=15)
POLL_MARGIN: Final = timedelta(seconds=30)  # the archive lags the data plane: look back a little each poll
MAX_PAGES_PER_POLL: Final = 5
MAX_HALVINGS: Final = 10
SEEN_LIMIT: Final = 50_000
POLL_FAILURE_LOG_EVERY_S: Final = 60.0
POLL_BUDGET_S: Final = 20.0  # one poll's whole run, all channels and pages: a poll cannot hold a pool thread longer

EventKey = tuple[str, str, str, int]


def event_id(v: EventView) -> str:
    """The opaque cursor of one event: the library's cursor tuple, URL-safe base64 without padding."""
    ts_ms = int(v.ts.timestamp() * 1000)
    raw = json.dumps([ts_ms, v.channel, str(v.message_id), v.stage, v.destination, v.attempt], separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_event_id(token: str) -> tuple[int, EventKey, str] | None:
    """`(ts_ms, key, channel)` of a well-formed cursor, else `None`."""
    try:
        if not token or len(token) > 512:
            return None
        data = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
        ts_ms, channel, message_id, stage, destination, attempt = data
        if (
            not isinstance(ts_ms, int)
            or isinstance(ts_ms, bool)
            or not all(isinstance(x, str) for x in (channel, message_id, stage, destination))
            or not isinstance(attempt, int)
            or isinstance(attempt, bool)
        ):
            return None
        return ts_ms, (message_id, stage, destination, attempt), channel
    except (ValueError, TypeError, binascii.Error):
        return None


def sse(event: str, data: dict[str, Any], *, event_id_: str | None = None) -> bytes:
    lines = [f"event: {event}"]
    if event_id_ is not None:
        lines.append(f"id: {event_id_}")
    lines.append("data: " + json.dumps(data, separators=(",", ":")))
    return ("\n".join(lines) + "\n\n").encode()


def _key(v: EventView) -> EventKey:
    return str(v.message_id), v.stage, v.destination, v.attempt


class StreamHub:
    """Open streams, per subject and in total. Counts are for this process: with several API replicas the caps
    apply per replica, which bounds the load each replica carries."""

    def __init__(self, per_subject: int = MAX_STREAMS_PER_SUBJECT, total: int = MAX_STREAMS_TOTAL) -> None:
        self._per_subject, self._total = per_subject, total
        self._open: Counter[str] = Counter()
        self.closing = asyncio.Event()

    def acquire(self, subject: str) -> Callable[[], None]:
        if self._open[subject] >= self._per_subject or self.open_streams() >= self._total:
            raise ApiProblem("TOO_MANY_STREAMS")
        self._open[subject] += 1
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return  # idempotent: the response and a failure path may both call it
            released = True
            self._open[subject] -= 1
            if self._open[subject] <= 0:
                del self._open[subject]

        return release

    def open_streams(self) -> int:
        return sum(self._open.values())


class SlotStreamingResponse(StreamingResponse):
    """A streaming response that gives its stream slot back however it ends: finished, client gone, or an error
    before the first byte. The slot is taken before the response exists (so a refusal is a normal 429), which is
    why releasing it cannot be left to the event generator: a generator that never starts never runs its `finally`."""

    def __init__(self, content: AsyncIterator[bytes], release: Callable[[], None], **kwargs: Any) -> None:
        super().__init__(content, **kwargs)
        self._release = release

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._release()
            aclose = getattr(self.body_iterator, "aclose", None)
            if aclose is not None:
                with contextlib.suppress(Exception):
                    await aclose()


class EventStreamer:
    def __init__(
        self,
        reader: ArchiveReader,
        hub: StreamHub,
        log: Logger,
        pools: Pools,
        *,
        poll_s: float = 2.0,
        heartbeat_s: float = 15.0,
        max_duration_s: float = MAX_DURATION_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._reader, self._hub, self._log, self._pools = reader, hub, log, pools
        self._poll_s, self._heartbeat_s, self._max_s, self._clock = poll_s, heartbeat_s, max_duration_s, clock
        self._last_failure_log = -1e18

    async def prepare(
        self, env: str, channels: list[str], subject: str, expires_at: float, last_event_id: str | None
    ) -> tuple[AsyncIterator[bytes], Callable[[], None]]:
        """Everything that can be refused is refused here, before the response starts: unknown channel (404),
        no archive for the environment (503), too many streams (429). Returns the event generator and the
        callback that gives the slot back (the response owns it)."""
        if len(set(channels)) != len(channels) or not 1 <= len(channels) <= 16:
            raise ApiProblem("REQUEST_INVALID")
        for name in channels:
            await self._reader.require(env, name)
        queries = self._reader.queries(env, stream=True)  # 503 when the environment has no archive
        release = self._hub.acquire(subject)
        return self._events(queries, channels, expires_at, last_event_id), release

    def _log_poll_failure(self, exc: BaseException) -> None:
        now = self._clock()
        if now - self._last_failure_log >= POLL_FAILURE_LOG_EVERY_S:  # an archive that is down is not a log flood
            self._last_failure_log = now
            self._log.warning("event_stream_poll_failed", error_type=type(exc).__name__)

    async def _events(
        self, queries: Any, channels: list[str], expires_at: float, last_event_id: str | None
    ) -> AsyncIterator[bytes]:
        started = self._clock()
        deadline = min(started + self._max_s, expires_at if expires_at > 0 else started + self._max_s)
        ending = "token_expiring" if expires_at > 0 and expires_at <= started + self._max_s else "max_duration"
        origin = datetime.fromtimestamp(started, UTC)  # nothing older than this is emitted
        skip: EventKey | None = None
        if last_event_id is not None:
            decoded = decode_event_id(last_event_id)
            if decoded is None:
                yield sse("reset", {"reason": "cursor_invalid"})
            else:
                when = datetime.fromtimestamp(decoded[0] / 1000, UTC)
                if origin - when > RESUME_WINDOW:
                    yield sse("reset", {"reason": "cursor_expired"})
                else:
                    origin, skip = when, decoded[1]
        watermark = origin  # everything before this has been read; each poll looks back from here
        seen: dict[EventKey, datetime] = {}
        closing = asyncio.ensure_future(self._hub.closing.wait())
        poll: asyncio.Future[tuple[list[EventView], datetime | None, bool]] | None = None
        last_beat, next_poll = started, started
        try:
            while True:
                now = self._clock()
                if self._hub.closing.is_set():
                    yield sse("end", {"reason": "server_shutdown"})
                    return
                if now >= deadline:
                    yield sse("end", {"reason": ending})
                    return
                if poll is None and now >= next_poll:
                    leave = self._pools.stream_gate.try_enter()
                    if leave is None:  # the pool is saturated (an archive that answers slowly): skip this round
                        self._log_poll_failure(TimeoutError())
                        next_poll = now + self._poll_s
                    else:
                        poll = submit_gated(
                            self._pools.streams, leave, self._poll, queries, channels, origin, watermark, seen
                        )
                due = [deadline - now, last_beat + self._heartbeat_s - now]
                due.append(next_poll - now if poll is None else self._poll_s * 10)
                waiting: set[asyncio.Future[Any]] = {closing} if poll is None else {closing, poll}
                await asyncio.wait(waiting, timeout=max(0.0, min(due)), return_when=asyncio.FIRST_COMPLETED)
                if poll is not None and poll.done():
                    fresh: list[EventView] = []
                    covered: datetime | None = None
                    lost = False
                    try:
                        fresh, covered, lost = poll.result()
                    except Exception as exc:  # the archive is down or slow: heartbeats only until it answers
                        self._log_poll_failure(exc)
                    poll, next_poll = None, self._clock() + self._poll_s
                    if lost:
                        # Too many events in too small a window to read in order: say so instead of skipping
                        # them silently. The position is lost; the stream continues live (README 3.16 `reset`).
                        yield sse("reset", {"reason": "backlog_too_dense"})
                        origin = watermark = datetime.fromtimestamp(self._clock(), UTC)
                        seen.clear()
                        continue
                    newest = watermark
                    for view in fresh:
                        if self._clock() >= deadline:  # the token may expire while a long poll was running
                            yield sse("end", {"reason": ending})
                            return
                        newest = max(newest, view.ts)
                        if skip is not None and _key(view) == skip:
                            skip = None
                            continue
                        yield sse("message", event_json(view), event_id_=event_id(view))
                        last_beat = self._clock()
                    watermark = max(watermark, covered if covered is not None else newest)
                now = self._clock()
                if now - last_beat >= self._heartbeat_s:
                    yield sse("heartbeat", {"ts": stamp(datetime.fromtimestamp(now, UTC))})
                    last_beat = now
        finally:
            closing.cancel()
            if poll is not None:
                poll.cancel()  # the thread ends with its own query's deadline; its result is not used

    def _read(
        self, queries: Any, channel: str, since: datetime, until: datetime | None, budget_end: float
    ) -> tuple[list[EventView], bool]:
        """Up to `MAX_PAGES_PER_POLL` pages of one channel's window, newest first. `complete` is False if there
        were more pages than that. Stops with `TimeoutError` once the poll's budget is spent."""
        items: list[EventView] = []
        cursor: str | None = None
        for _ in range(MAX_PAGES_PER_POLL):
            if time.monotonic() > budget_end:
                raise TimeoutError
            page = queries.search(SearchFilter(channel=channel, since=since, until=until), limit=200, cursor=cursor)
            items.extend(page.items)
            cursor = page.next_cursor
            if cursor is None:
                return items, True
        return items, False

    def _poll(
        self,
        queries: Any,
        channels: list[str],
        origin: datetime,
        watermark: datetime,
        seen: MutableMapping[EventKey, datetime],
    ) -> tuple[list[EventView], datetime | None, bool]:
        """New events since the last poll, oldest first, and how far the poll read (`None`: up to now).

        Each poll looks back `POLL_MARGIN` from the watermark (rows reach the archive late) and relies on `seen` to
        emit each event once; nothing older than `origin` is emitted. A channel whose window holds more events
        than `MAX_PAGES_PER_POLL` pages (a long backlog after a resume) is read in a narrower window, halving until
        it fits, so the oldest events are emitted first and the watermark only advances to the end of what was read.
        """
        since = max(origin, watermark - POLL_MARGIN)
        budget_end = time.monotonic() + POLL_BUDGET_S
        found: list[EventView] = []
        covered: datetime | None = None
        for channel in channels:
            until: datetime | None = None
            items, complete = self._read(queries, channel, since, until, budget_end)
            for _ in range(MAX_HALVINGS):
                if complete:
                    break
                horizon = until or (datetime.now(UTC) + timedelta(seconds=1))
                until = since + (horizon - since) / 2
                items, complete = self._read(queries, channel, since, until, budget_end)
            if not complete:
                return [], None, True  # still more than a poll can page through in the smallest window
            if until is not None:
                covered = until if covered is None else min(covered, until)
            for view in items:
                key = _key(view)
                if key in seen or view.ts < origin:
                    continue
                seen[key] = view.ts
                found.append(view)
        if len(seen) > SEEN_LIMIT:
            for k in sorted(seen, key=lambda k: seen[k])[: len(seen) - SEEN_LIMIT // 2]:
                del seen[k]
        found.sort(key=lambda v: (v.ts, v.channel, str(v.message_id), v.stage, v.destination, v.attempt))
        return found, covered, False
