"""The channel-spec cache: `_tb.<env>.channels`, read as a compacted table.

Invariant 3 in one file. A worker's only source of truth for a channel is this topic, so it starts by
reading the topic from the beginning, keeps the latest record per key, and then keeps polling for
changes — which is also how hot reload works. **PostgreSQL, the API and ClickHouse can all be down and
this still works**, because nothing here calls any of them.

Two details that are easy to get wrong:

* **Catch-up ends at partition EOF, not at the high watermark.** The watermark counts transaction
  control records that are never handed to the application, so a "read until offset == watermark" loop
  can simply never terminate. `enable.partition.eof` is the supported signal.
* **A bad record does not take the channel down.** A spec that fails to parse — including one with no
  `metadata.version` (channel-spec contract 6 rule 1) — is logged with the channel name and offset, and
  the previously applied spec keeps running.
* **A broker outage is invisible to `poll()`.** Measured (confluent-kafka 2.15.1, Kafka 4.0.0, a 45 s
  `docker stop`): with no `error_cb`, `Consumer.poll` returns no error event at all while every broker
  is down, and nothing at all when they come back; an idle compacted partition does not even re-emit
  EOF. So the cache proves readability itself, with a rate-limited metadata probe of this topic
  (`list_topics`, every `probe_interval_s`, bounded by `probe_timeout_s`), and treats a delivered record,
  an EOF or a passing probe as recovery. While it cannot prove it, `poll_once` raises
  `ChannelsUnavailableError` after doing its work, so a caller can say it is serving possibly-stale
  specs; the specs themselves are never dropped (invariant 3). `LiveSpecSource.current` swallows it and
  answers from the cache; the transition is logged here once each way.
* **The probe never runs on the caller's thread.** Measured (2026-10-05, confluent-kafka 2.15.1, Kafka
  4.0.0): `list_topics` on this consumer took 470-490 ms on a healthy loopback broker, because the broker
  serves one request at a time per connection and the MetadataRequest queues behind the consumer's own
  Fetch long-poll on the idle channels topic (`fetch.wait.max.ms`, 500 ms by default). Run inline, that
  stalled the worker's consumer thread and the MLLP receiver's event loop once per `probe_interval_s`:
  the sync-reply round trip's p99 of 440-500 ms. So `poll_once` starts the probe on a background thread
  (at most one in flight) and applies its result, and logs it, on the caller's thread at a later call.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import orjson
from tributary.sdk.context import Logger

from .consumer import PARTITION_EOF
from .errors import ChannelSpecError, ChannelsUnavailableError
from .spec import ChannelSpec

# Re-exported next to `open_spec_source`: the group id every caller of the reader must configure (set
# 1.4, `consumer_groups.spec_reader`). Defined in `topics`, with the other group names.
from .topics import spec_reader_group as spec_reader_group


class ChannelsConsumerLike(Protocol):
    """The slice of `confluent_kafka.Consumer` the cache uses."""

    def assign(self, partitions: list[Any]) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...

    def close(self) -> None: ...


#: How often a healthy cache proves the topic is readable, and how long one proof may take.
#: The interval is JUDGEMENT: one metadata request per 5 s costs nothing when healthy. The timeout must
#: exceed the consumer's `fetch.wait.max.ms` (librdkafka default 500 ms), because the probe's request
#: waits behind that long-poll on the same connection (measured 470-490 ms on a healthy broker); at the
#: former 0.5 s a healthy cluster could fail the probe. It blocks nobody: the probe runs off-thread.
PROBE_INTERVAL_S = 5.0
PROBE_TIMEOUT_S = 2.0

#: Runs one probe. The default is a daemon thread; tests pass an inline or a deferred runner.
ProbeSpawner = Callable[[Callable[[], None]], None]


def _spawn_daemon(target: Callable[[], None]) -> None:
    threading.Thread(target=target, name="tb-channels-probe", daemon=True).start()


@dataclass(frozen=True, slots=True)
class ChannelsOutage:
    """Why and since when the cache cannot prove its specs are current. `since` is `clock()` time."""

    error_code: str
    since: float


class ChannelCache:
    """An in-memory view of the deployed channel specs."""

    def __init__(
        self,
        consumer: ChannelsConsumerLike,
        topic: str,
        *,
        log: Logger,
        probe_interval_s: float = PROBE_INTERVAL_S,
        probe_timeout_s: float = PROBE_TIMEOUT_S,
        clock: Callable[[], float] = time.monotonic,
        spawn: ProbeSpawner | None = None,
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._log = log
        self._specs: dict[str, ChannelSpec] = {}
        self._eof: set[int] = set()
        self._partitions: set[int] = set()
        self._probe_interval_s = probe_interval_s
        self._probe_timeout_s = probe_timeout_s
        self._clock = clock
        self._probed_at: float | None = None
        self._error_event_at: float | None = None
        self._outage: ChannelsOutage | None = None
        self._spawn = spawn if spawn is not None else _spawn_daemon
        #: Guards the two fields below, which the probe thread writes and the caller's thread reads.
        self._probe_lock = threading.Lock()
        self._probe_in_flight = False
        #: A finished probe's answer (`(problem,)`), not yet applied on the caller's thread.
        self._probe_result: tuple[str | None] | None = None
        self._probe_idle = threading.Event()
        self._probe_idle.set()
        self.applied = 0
        self.rejected = 0
        self.removed = 0
        #: Outages entered since start: one per streak, however long it lasts.
        self.outages = 0

    # -- assignment ------------------------------------------------------------------------------

    def start(self, *, timeout_s: float = 10.0) -> None:
        """Assign every partition from the beginning.

        `assign`, not `subscribe`: this is a table read, not a shared workload. Every worker needs every
        spec, so consumer-group partition sharing would give each worker a fraction of the channels.
        """
        from confluent_kafka import OFFSET_BEGINNING, TopicPartition

        metadata = self._consumer.list_topics(self._topic, timeout=timeout_s)
        topic_metadata = metadata.topics.get(self._topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise ChannelSpecError(f"channels topic {self._topic} is not available")
        self._partitions = set(topic_metadata.partitions)
        self._probed_at = self._clock()  # that was a successful probe
        self._consumer.assign(
            [TopicPartition(self._topic, partition, OFFSET_BEGINNING) for partition in sorted(self._partitions)]
        )

    def close(self) -> None:
        # A probe thread may be inside `list_topics` on this consumer; closing the handle under it is a
        # use-after-free in librdkafka. `list_topics` honours its timeout, so this wait is bounded.
        self._probe_idle.wait(self._probe_timeout_s + 1.0)
        self._consumer.close()

    # -- reading ---------------------------------------------------------------------------------

    def catch_up(self, *, timeout_s: float = 30.0) -> None:
        """Read to the end of every partition, or raise if the topic cannot be read in time.

        Uses the record poll only, not the readability check: startup keeps its own rule (refuse after
        `timeout_s`) instead of failing on the first error event.
        """
        deadline = time.monotonic() + timeout_s
        while self._eof != self._partitions:
            if time.monotonic() > deadline:
                raise ChannelSpecError(
                    f"timed out reading {self._topic}: a worker must not start without its channel specs"
                )
            self._poll(timeout_s=0.5)

    def poll_once(self, *, timeout_s: float = 0.2) -> bool:
        """Apply at most one record. Returns True when something changed.

        Raises `ChannelsUnavailableError` when the topic cannot currently be shown to be readable: an
        error event from the consumer, or a failed probe (module docstring). The record, if one arrived,
        is applied first, and the cache keeps serving every applied spec either way; the exception
        only tells the caller that what it serves may be stale. It is raised on every call for as long
        as the outage lasts (a level, not an edge), so a caller can log a streak once and its end once.
        """
        changed = self._poll(timeout_s=timeout_s)
        self._check_readable()
        return changed

    @property
    def available(self) -> bool:
        """False while the cache cannot prove `_tb.<env>.channels` is readable. Specs are served anyway."""
        return self._outage is None

    @property
    def outage(self) -> ChannelsOutage | None:
        return self._outage

    def _poll(self, *, timeout_s: float) -> bool:
        raw = self._consumer.poll(timeout_s)
        if raw is None:
            return False
        error = raw.error()
        if error is not None:
            if error.code() == PARTITION_EOF:
                self._eof.add(raw.partition())
                self._recovered()  # a fetch reached the end of a partition: the topic is readable
                return False
            code = _error_name(error)
            self._log.warning("channels_consumer_error", kafka_error=code)
            self._error_event_at = self._clock()
            self._unavailable(code)
            return False
        self._recovered()  # a fetch delivered a record
        return self._apply(raw)

    def _check_readable(self) -> None:
        """Start a probe when one is due, apply any finished one, and raise while in an outage.

        Never waits for the probe (module docstring): a due probe starts on `spawn`, and its result is
        applied by whichever call comes after it finished, on the caller's thread, so logging and the
        outage state stay single-threaded. With an inline `spawn` that is the same call.
        """
        now = self._clock()
        if self._probed_at is None or now - self._probed_at >= self._probe_interval_s:
            with self._probe_lock:
                due = not self._probe_in_flight
                if due:
                    self._probe_in_flight = True
                    self._probe_idle.clear()
            if due:
                self._probed_at = now
                self._start_probe()
        self._apply_probe_result(now)
        outage = self._outage
        if outage is not None:
            raise ChannelsUnavailableError(
                self._topic, error_code=outage.error_code, unavailable_for_s=max(0.0, now - outage.since)
            )

    def _start_probe(self) -> None:
        try:
            self._spawn(self._run_probe)
        except Exception as exc:  # e.g. RuntimeError: can't start new thread. Unproven, so an outage.
            self._finish_probe(type(exc).__name__)

    def _run_probe(self) -> None:
        """The probe thread's body. Touches only the consumer's metadata call and the guarded fields."""
        problem: str | None = "PROBE_FAILED"
        try:
            problem = self._probe()
        finally:
            self._finish_probe(problem)

    def _finish_probe(self, problem: str | None) -> None:
        with self._probe_lock:
            self._probe_result = (problem,)
            self._probe_in_flight = False
        self._probe_idle.set()

    def _apply_probe_result(self, now: float) -> None:
        with self._probe_lock:
            result, self._probe_result = self._probe_result, None
        if result is None:
            return
        problem = result[0]
        if problem is not None:
            self._unavailable(problem)
        elif self._error_event_at is None or now - self._error_event_at >= self._probe_interval_s:
            # A passing probe does not overrule a fetch error seen within the last interval: metadata
            # can be readable while fetching is not (an ACL on READ but not DESCRIBE, say).
            self._recovered()

    def _probe(self) -> str | None:
        """One metadata request for this topic. `None` when every assigned partition has a leader."""
        try:
            metadata = self._consumer.list_topics(self._topic, timeout=self._probe_timeout_s)
        except Exception as exc:  # KafkaException(_TRANSPORT / _TIMED_OUT), or anything a client raises
            return _exception_code(exc)
        topic = metadata.topics.get(self._topic)
        if topic is None:
            return "UNKNOWN_TOPIC_OR_PART"
        if topic.error is not None:
            return _error_name(topic.error)
        for partition in sorted(self._partitions):
            if partition not in topic.partitions:
                return "_UNKNOWN_PARTITION"
            detail = topic.partitions[partition]
            error = getattr(detail, "error", None)
            if error is not None:
                return _error_name(error)
            if getattr(detail, "leader", 0) < 0:
                return "LEADER_NOT_AVAILABLE"
        return None

    def _unavailable(self, error_code: str) -> None:
        if self._outage is not None:
            return
        self._outage = ChannelsOutage(error_code=error_code, since=self._clock())
        self.outages += 1
        self._log.warning(
            "channels_unavailable",
            topic=self._topic,
            kafka_error=error_code,
            serving_last_applied=True,
            channels=len(self._specs),
        )

    def _recovered(self) -> None:
        outage = self._outage
        if outage is None:
            return
        self._outage = None
        self._log.info(
            "channels_recovered",
            topic=self._topic,
            kafka_error=outage.error_code,
            outage_s=round(max(0.0, self._clock() - outage.since), 1),
        )

    def _apply(self, raw: Any) -> bool:
        key = raw.key()
        name = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        value = raw.value()

        if value is None:
            # A tombstone means undeployed: stop the channel and release its partitions.
            if self._specs.pop(name, None) is not None:
                self.removed += 1
                self._log.info("channel_undeployed", channel=name, offset=raw.offset())
                return True
            return False

        try:
            spec = ChannelSpec.from_document(orjson.loads(value))
        except (orjson.JSONDecodeError, ChannelSpecError) as exc:
            self.rejected += 1
            self._log.error(
                "channel_spec_rejected",
                channel=name,
                offset=raw.offset(),
                error_type=type(exc).__name__,
                reason=str(exc)[:200].replace("\n", " "),
            )
            return False

        if spec.name != name:
            self.rejected += 1
            self._log.error("channel_spec_key_mismatch", channel=name, offset=raw.offset())
            return False

        previous = self._specs.get(name)
        self._specs[name] = spec
        self.applied += 1
        if previous is None or previous.version != spec.version:
            self._log.info(
                "channel_spec_applied",
                channel=name,
                channel_version=spec.version,
                previous_version=previous.version if previous else None,
                offset=raw.offset(),
            )
        return True

    # -- access ----------------------------------------------------------------------------------

    def get(self, name: str) -> ChannelSpec | None:
        return self._specs.get(name)

    def require(self, name: str) -> ChannelSpec:
        spec = self._specs.get(name)
        if spec is None:
            raise ChannelSpecError(f"channel {name} is not deployed on {self._topic}")
        return spec

    def names(self) -> list[str]:
        return sorted(self._specs)


class LiveSpecSource:
    """One channel's spec, refreshed from `_tb.<env>.channels` without blocking.

    This is the hot-reload path, and it is deliberately the *only* one: every component that needs a
    spec — the pipeline worker, a loader, later the replier — calls `current()` once per message, so a
    republished `metadata.version` takes effect on the next record and nobody has to restart. A
    component that read its spec once at startup would drift from the deployed channel and need a
    rolling restart to pick up a change, which is the failure invariant 3 is written against.

    `current()` does not block: `poll_once(0.0)` is a non-blocking librdkafka call, the cache's
    rate-limited readability probe runs on its own thread (`ChannelCache`), and a spec that fails to parse leaves the previously applied one running
    (channel-spec contract 6 rule 1). A broker outage does not fail `current()` either (invariant 3): it
    answers from the cache, and `available` says whether that answer can be shown to be current.
    """

    __slots__ = ("_cache", "_channel")

    def __init__(self, cache: ChannelCache, channel: str) -> None:
        self._cache = cache
        self._channel = channel

    @property
    def cache(self) -> ChannelCache:
        return self._cache

    @property
    def available(self) -> bool:
        """False while `_tb.<env>.channels` cannot be shown to be readable; `current()` may be stale."""
        return self._cache.available

    def current(self) -> ChannelSpec:
        # Invariant 3: an outage serves the last applied spec; the cache has logged the transition.
        with contextlib.suppress(ChannelsUnavailableError):
            self._cache.poll_once(timeout_s=0.0)
        return self._cache.require(self._channel)

    def close(self) -> None:
        self._cache.close()


def open_spec_source(
    *,
    channel: str,
    consumer: ChannelsConsumerLike,
    topic: str,
    log: Logger,
    catch_up_timeout_s: float = 30.0,
    probe_interval_s: float = PROBE_INTERVAL_S,
    probe_timeout_s: float = PROBE_TIMEOUT_S,
    spawn: ProbeSpawner | None = None,
) -> LiveSpecSource:
    """Build a caught-up `LiveSpecSource`, or raise rather than start without the spec.

    Raising is the point: a worker or loader that starts with no spec has nothing to apply, and
    guessing (an empty pipeline, a default retry policy) would be worse than not starting. Once started,
    an outage never raises out of `current()`; see `ChannelCache.poll_once` for how it is reported.
    """
    cache = ChannelCache(
        consumer, topic, log=log, probe_interval_s=probe_interval_s, probe_timeout_s=probe_timeout_s, spawn=spawn
    )
    cache.start()
    cache.catch_up(timeout_s=catch_up_timeout_s)
    source = LiveSpecSource(cache, channel)
    source.current()  # fails now, loudly, if this channel is not deployed
    return source


def _error_name(error: Any) -> str:
    """A librdkafka error's symbolic name (`_ALL_BROKERS_DOWN`), else its numeric code. Never its text."""
    name = getattr(error, "name", None)
    if callable(name):
        value = name()
        if isinstance(value, str) and value:
            return value
    return str(error.code())


def _exception_code(exc: Exception) -> str:
    """The error name inside a `KafkaException`, or the exception's type name. Never its message."""
    inner = exc.args[0] if exc.args else None
    if inner is not None and callable(getattr(inner, "code", None)):
        return _error_name(inner)
    return type(exc).__name__
