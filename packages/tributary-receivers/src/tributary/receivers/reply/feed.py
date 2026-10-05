"""The receiver's reply consumer: one per process, in the `replier` group, on a thread of its own.

Channel-spec README 3.2.6, ADR-003 D10, `contracts/topics/v0` `clients.reply_consumer`.

**Targeted routing.** Every receiver process of a channel joins `tb.<env>.<channel>.replier` with a
`group.instance.id` that is unique **per process**; Kafka's assignment is the lease on a reply partition and
there is no coordinator (invariant 3). A process names a partition in `tb-reply-partition` only while it holds
it **and** has an established fetch position on it, so a reply written between assignment and an
`auto.offset.reset=latest` reset can never be skipped.

**Establishing the position.** `position()` stays invalid until a record is consumed, even for `OFFSET_END`
(measured), so it cannot be the signal. Instead the rebalance callback reads the partition's high watermark
and assigns **at that explicit offset**: every reply the worker produces after the process advertises the
partition has an offset at or above it. If the watermark cannot be read the partition is assigned at the end
but not advertised, and the thread retries (watermark, then `seek`) until it can. Nothing is ever committed.

**Revoke and loss** (`on_revoke`, `on_lost`): the partitions leave the advertised set at once and each sink
(a server's pending map) answers its entries on them `AE` / `REPLY_ROUTE_LOST`, once their `raw` callback is in
(`SyncRequest`).

**An unreadable reply record is dropped, never fatal** (envelope README 2.3.3 rule 1): a record without a
well-formed `tb-correlation-id` and `tb-msg-id`, or with undecodable headers, is counted and logged with
topic, partition and offset only. A receiver cannot dead-letter and a reply is not a message; the pending
request times out.

**Bounded hand-off.** The thread never writes to a socket and only reaches the event loop through
`call_soon_threadsafe`, so a slow or dead sender cannot stall it. At most `slots` records are in flight to the
loop: a loop that falls behind pauses the poll rather than growing a queue.

**Ready** means at least one advertised partition and a connected consumer. Not ready: every sync request is
refused `AR` / `REPLY_UNAVAILABLE` before it is published (`PendingMap.open`).

State (`_held`, `_connected`, the sinks) is touched only on the event loop thread. The consumer thread posts.

PHI: a reply carries an engine-minted correlation id, an enum and a code, and under `body: step` a response
payload. None of it is logged; only topic, partition and offset are.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import TopicDefaults
from tributary.runtime.sync_answer import Resolution
from tributary.sdk import headers as h
from tributary.sdk.context import Logger

from ..metrics import Metrics

#: Kafka error names for "the consumer cannot reach any broker".
_DOWN = frozenset({"_ALL_BROKERS_DOWN", "_TRANSPORT", "_RESOLVE"})
#: librdkafka's name for a consumer that can never continue (measured: a second member with the same
#: `group.instance.id` makes the first one's `poll` return `_FATAL`, with no revoke or lost callback).
_FATAL_NAME = "_FATAL"
_MAX_ENUM_CHARS = 64
_LOG_FIRST = 5
_LOG_EVERY = 1000
_RETRY_S = 1.0


@dataclass(frozen=True, slots=True)
class ReplyRecord:
    """A reply record, parsed. `status` and `code` are the header values as sent (`None` when absent);
    nothing here has been trusted yet."""

    partition: int
    correlation_id: str
    msg_id: str
    status: str | None
    code: str | None
    body: bytes


class ReplySink(Protocol):
    """A server's pending map, as the feed sees it."""

    @property
    def retired(self) -> bool: ...

    def __len__(self) -> int: ...

    def deliver_reply(self, record: ReplyRecord) -> tuple[bool, Resolution | None]: ...

    def partitions_revoked(self, partitions: frozenset[int]) -> list[Resolution]: ...


class ConsumerLike(Protocol):
    """The slice of `confluent_kafka.Consumer` the feed uses (a fake implements it in the unit tests)."""

    def subscribe(self, topics: list[str], on_assign: Any = ..., on_revoke: Any = ..., on_lost: Any = ...) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def incremental_assign(self, partitions: list[Any]) -> None: ...

    def incremental_unassign(self, partitions: list[Any]) -> None: ...

    def get_watermark_offsets(self, partition: Any, timeout: float, cached: bool) -> tuple[int, int]: ...

    def seek(self, partition: Any) -> None: ...

    def list_topics(self, topic: str | None, timeout: float) -> Any: ...

    def close(self) -> None: ...


def parse_reply_record(msg: Any) -> ReplyRecord | None:
    """The reply in a consumed record, or `None` when it cannot be read. Total: never raises, and never
    quotes a value."""
    try:
        headers: dict[str, str] = {}
        for name, value in msg.headers() or []:
            if isinstance(name, str) and name not in headers and isinstance(value, bytes):
                headers[name] = value.decode("utf-8")
        correlation_id = headers.get(h.TB_CORRELATION_ID)
        msg_id = headers.get(h.TB_MSG_ID)
        if correlation_id is None or not h.is_well_formed(h.TB_CORRELATION_ID, correlation_id):
            return None
        if msg_id is None or not h.is_well_formed(h.TB_MSG_ID, msg_id):
            return None
        status = headers.get(h.TB_REPLY_STATUS)
        code = headers.get(h.TB_ERROR_CODE)
        value = msg.value()
        return ReplyRecord(
            partition=int(msg.partition()),
            correlation_id=correlation_id,
            msg_id=msg_id,
            status=status[:_MAX_ENUM_CHARS] if status is not None else None,
            code=code[:_MAX_ENUM_CHARS] if code is not None else None,
            body=bytes(value) if value is not None else b"",
        )
    except Exception:  # headers() or value() on a malformed record; a drop, never a crash
        return None


class ReplyFeed:
    def __init__(
        self,
        consumer: ConsumerLike,
        topic: str,
        *,
        log: Logger,
        metrics: Metrics,
        slots: int = 1024,
        poll_timeout_s: float = 0.05,
        watermark_timeout_s: float = 5.0,
        post: Callable[..., None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._log = log
        self._metrics = metrics
        self._slots = threading.Semaphore(slots)
        self._poll_timeout_s = poll_timeout_s
        self._watermark_timeout_s = watermark_timeout_s
        self._custom_post = post
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._closed = False
        # event loop thread only
        self._held: list[int] = []
        self._connected = True
        self._fatal = False
        #: Called on the event loop thread when the consumer is fatally broken (fenced by a duplicate
        #: `group.instance.id`, say). The runner makes the process exit so a supervisor restarts it.
        self.fatal_handler: Callable[[BaseException], None] | None = None
        self._rr = 0
        self._sinks: list[ReplySink] = []
        self._dropped = 0
        # consumer thread only
        self._unpositioned: set[int] = set()
        self._retry_at = 0.0
        self._down_probe_at = 0.0

    # -- event loop side --------------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        return self._connected and bool(self._held)

    @property
    def held(self) -> tuple[int, ...]:
        return tuple(self._held)

    def choose_partition(self) -> int | None:
        """A held, positioned partition (round-robin across them), or `None`: not ready."""
        if not self._connected or not self._held:
            return None
        partition = self._held[self._rr % len(self._held)]
        self._rr += 1
        return partition

    def attach(self, sink: ReplySink) -> None:
        self._sinks = [s for s in self._sinks if not (s.retired and len(s) == 0)]
        self._sinks.append(sink)

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._consumer.subscribe(
            [self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_revoke
        )
        self._thread = threading.Thread(target=self._run, name="reply-feed", daemon=True)
        self._thread.start()

    def close(self, timeout_s: float = 5.0) -> None:
        """Stop the thread, leave the group, drop every advertised partition. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout_s)
        with contextlib.suppress(Exception):
            self._consumer.close()
        self._held = []
        self._set_ready_gauge()

    def _post(self, fn: Callable[..., None], *args: Any) -> bool:
        if self._custom_post is not None:
            self._custom_post(fn, *args)
            return True
        loop = self._loop
        if loop is None:
            return False
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:  # the loop is closed: the process is stopping
            self._stop.set()
            return False
        return True

    def _set_ready_gauge(self) -> None:
        self._metrics.reply_ready = 1 if self.ready else 0

    def _on_positioned(self, partitions: frozenset[int]) -> None:
        for partition in sorted(partitions):
            if partition not in self._held:
                self._held.append(partition)
        self._set_ready_gauge()
        self._log.info("reply_partitions_held", count=len(self._held))

    def _on_revoked_event(self, partitions: frozenset[int]) -> None:
        """Out of the advertised set first, then the sinks answer their entries on them."""
        self._held = [p for p in self._held if p not in partitions]
        self._set_ready_gauge()
        self._log.warning("reply_partitions_revoked", revoked=len(partitions), held=len(self._held))
        for sink in list(self._sinks):
            for resolution in sink.partitions_revoked(partitions):
                if resolution.answer == "AE":
                    self._metrics.reply_route_lost += 1
        self._sinks = [s for s in self._sinks if not (s.retired and len(s) == 0)]

    def _on_fatal(self) -> None:
        """The consumer cannot continue: stop advertising at once, answer entries on its partitions, say so."""
        if self._fatal:
            return
        self._fatal = True
        self._log.error("reply_consumer_fatal")
        self._on_revoked_event(frozenset(self._held))
        self._connected = False
        self._set_ready_gauge()
        if self.fatal_handler is not None:
            self.fatal_handler(RuntimeError("reply consumer fatal"))

    def _on_connection(self, up: bool) -> None:
        if self._fatal:
            return  # never ready again: the process restarts
        if up != self._connected:
            self._connected = up
            self._set_ready_gauge()
            (self._log.info if up else self._log.warning)("reply_consumer_connected" if up else "reply_consumer_down")

    def _on_record(self, record: ReplyRecord) -> None:
        try:
            for sink in list(self._sinks):
                matched, _ = sink.deliver_reply(record)
                if matched:
                    return
            self._metrics.replies_unmatched += 1
        finally:
            self._slots.release()

    def _count_unreadable(self) -> None:
        self._metrics.replies_unreadable += 1

    # -- consumer thread side ---------------------------------------------------------------------

    def _on_assign(self, consumer: Any, partitions: Sequence[Any]) -> None:
        """Assign at an explicit offset (the high watermark) so the fetch position exists before the process
        advertises the partition. See the module docstring."""
        from confluent_kafka import OFFSET_END, TopicPartition

        positioned: list[Any] = []
        fallback: list[Any] = []
        for tp in partitions:
            try:
                _low, high = consumer.get_watermark_offsets(tp, timeout=self._watermark_timeout_s, cached=False)
                positioned.append(TopicPartition(tp.topic, tp.partition, high))
            except Exception:
                fallback.append(TopicPartition(tp.topic, tp.partition, OFFSET_END))
        consumer.incremental_assign(positioned + fallback)
        self._unpositioned.update(tp.partition for tp in fallback)
        if fallback:
            self._retry_at = time.monotonic() + _RETRY_S
        if positioned:
            self._post(self._on_positioned, frozenset(tp.partition for tp in positioned))

    def _on_revoke(self, consumer: Any, partitions: Sequence[Any]) -> None:
        with contextlib.suppress(Exception):  # a partition already gone from the assignment
            consumer.incremental_unassign(list(partitions))
        gone = frozenset(tp.partition for tp in partitions)
        self._unpositioned -= gone
        self._post(self._on_revoked_event, gone)

    def _retry_unpositioned(self) -> None:
        if not self._unpositioned or time.monotonic() < self._retry_at:
            return
        self._retry_at = time.monotonic() + _RETRY_S
        done = {p for p in sorted(self._unpositioned) if self._seek_to_end(p)}
        if done:
            self._unpositioned -= done
            self._post(self._on_positioned, frozenset(done))

    def _seek_to_end(self, partition: int) -> bool:
        """Read the high watermark and seek there: the fetch position the advertise rule needs."""
        from confluent_kafka import TopicPartition

        try:
            tp = TopicPartition(self._topic, partition)
            _low, high = self._consumer.get_watermark_offsets(tp, timeout=self._watermark_timeout_s, cached=False)
            self._consumer.seek(TopicPartition(self._topic, partition, high))
        except Exception:
            return False
        return True

    def _probe_connection(self) -> None:
        """After a down report: ask for metadata until it answers, then report up."""
        now = time.monotonic()
        if now < self._down_probe_at:
            return
        self._down_probe_at = now + _RETRY_S
        try:
            self._consumer.list_topics(self._topic, timeout=1.0)
        except Exception:
            return
        self._down_probe_at = 0.0
        self._post(self._on_connection, True)

    def tick(self) -> None:
        """One bounded poll. Public so a unit test can drive the feed without a thread."""
        self._retry_unpositioned()
        if self._down_probe_at:
            self._probe_connection()
        if not self._slots.acquire(blocking=False):
            time.sleep(self._poll_timeout_s)  # the loop is behind: do not read more replies yet
            return
        handed_off = False
        try:
            msg = self._consumer.poll(self._poll_timeout_s)
            if msg is None:
                return
            error = msg.error()
            if error is not None:
                self.on_kafka_error(error)
                return
            record = parse_reply_record(msg)
            if record is None:
                self._drop_unreadable(msg)
                return
            handed_off = self._post(self._on_record, record)
        finally:
            if not handed_off:
                self._slots.release()

    def on_kafka_error(self, error: Any) -> None:
        name = ""
        fatal = False
        with contextlib.suppress(Exception):
            name = str(error.name())
            fatal = bool(error.fatal())
        if fatal or name == _FATAL_NAME:
            self._down_probe_at = 0.0
            self._post(self._on_fatal)
        elif name in _DOWN:
            self._down_probe_at = time.monotonic() + _RETRY_S
            self._post(self._on_connection, False)

    def _drop_unreadable(self, msg: Any) -> None:
        self._post(self._count_unreadable)
        self._dropped += 1
        if self._dropped <= _LOG_FIRST or self._dropped % _LOG_EVERY == 0:
            topic = partition = offset = None
            with contextlib.suppress(Exception):
                topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
            self._log.warning(
                "reply_record_dropped", reason="unreadable", topic=topic, partition=partition, offset=offset
            )

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # the thread must outlive any one bad poll
                self._log.error("reply_consumer_error", error=type(exc).__name__)
                self._post(self._on_connection, False)
                self._down_probe_at = time.monotonic() + _RETRY_S
                self._stop.wait(_RETRY_S)


def open_reply_feed(
    kafka: KafkaSettings,
    defaults: TopicDefaults,
    *,
    env: str,
    channel: str,
    instance: str | None,
    pid: int,
    log: Logger,
    metrics: Metrics,
) -> ReplyFeed:
    """The production feed: a `confluent_kafka.Consumer` in group `tb.<env>.<channel>.replier` with static
    membership and cooperative-sticky assignment, never committing, `read_uncommitted` (the reply producer is
    not transactional: sync reply is refused on transactional channels).

    `group.instance.id` is `<instance>-p<pid>`: unique per **process**, since a duplicate fences a live process
    out of the group, and stable across a container's restarts (pid 1), so a planned restart re-takes its
    partitions without a rebalance. Session and heartbeat timeouts come from the contract's consumer profile
    (`clients.reply_consumer` says: the same measured values as the worker's)."""
    from confluent_kafka import Consumer
    from tributary.runtime.topics import reply_topic

    from ..errors import ConfigError

    if not instance:
        # The default `mllp-0-p1` would be the same in every container, and replicas sharing a
        # `group.instance.id` fence one another out of the group.
        raise ConfigError("source.reply needs an explicit --instance, unique per replica; none was given")
    feed_box: list[ReplyFeed] = []

    def error_cb(error: Any) -> None:
        if feed_box:
            feed_box[0].on_kafka_error(error)

    config = consumer_config(
        kafka,
        defaults,
        group_id=f"tb.{env}.{channel}.replier",
        group_instance_id=f"{instance}-p{pid}",
        auto_offset_reset="latest",
    )
    config.update(
        {
            "isolation.level": "read_uncommitted",
            "client.id": f"mllp-replier-{channel}",
            "error_cb": error_cb,
            "socket.keepalive.enable": True,
        }
    )
    feed = ReplyFeed(Consumer(config), reply_topic(env, channel), log=log, metrics=metrics)
    feed_box.append(feed)
    return feed


__all__ = [
    "ConsumerLike",
    "ReplyFeed",
    "ReplyRecord",
    "ReplySink",
    "open_reply_feed",
    "parse_reply_record",
]
