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
"""

from __future__ import annotations

from typing import Any, Protocol

import orjson
from tributary.sdk.context import Logger

from .consumer import PARTITION_EOF
from .errors import ChannelSpecError
from .spec import ChannelSpec


class ChannelsConsumerLike(Protocol):
    """The slice of `confluent_kafka.Consumer` the cache uses."""

    def assign(self, partitions: list[Any]) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any: ...

    def close(self) -> None: ...


class ChannelCache:
    """An in-memory view of the deployed channel specs."""

    def __init__(self, consumer: ChannelsConsumerLike, topic: str, *, log: Logger) -> None:
        self._consumer = consumer
        self._topic = topic
        self._log = log
        self._specs: dict[str, ChannelSpec] = {}
        self._eof: set[int] = set()
        self._partitions: set[int] = set()
        self.applied = 0
        self.rejected = 0
        self.removed = 0

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
        self._consumer.assign(
            [TopicPartition(self._topic, partition, OFFSET_BEGINNING) for partition in sorted(self._partitions)]
        )

    def close(self) -> None:
        self._consumer.close()

    # -- reading ---------------------------------------------------------------------------------

    def catch_up(self, *, timeout_s: float = 30.0) -> None:
        """Read to the end of every partition, or raise if the topic cannot be read in time."""
        import time

        deadline = time.monotonic() + timeout_s
        while self._eof != self._partitions:
            if time.monotonic() > deadline:
                raise ChannelSpecError(
                    f"timed out reading {self._topic}: a worker must not start without its channel specs"
                )
            self.poll_once(timeout_s=0.5)

    def poll_once(self, *, timeout_s: float = 0.2) -> bool:
        """Apply at most one record. Returns True when something changed."""
        raw = self._consumer.poll(timeout_s)
        if raw is None:
            return False
        error = raw.error()
        if error is not None:
            if error.code() == PARTITION_EOF:
                self._eof.add(raw.partition())
                return False
            self._log.warning("channels_consumer_error", error_code=str(error.code()))
            return False
        return self._apply(raw)

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

    `current()` does not block: `poll_once(0.0)` costs a non-blocking librdkafka call, and a spec that
    fails to parse leaves the previously applied one running (channel-spec contract 6 rule 1).
    """

    __slots__ = ("_cache", "_channel")

    def __init__(self, cache: ChannelCache, channel: str) -> None:
        self._cache = cache
        self._channel = channel

    @property
    def cache(self) -> ChannelCache:
        return self._cache

    def current(self) -> ChannelSpec:
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
) -> LiveSpecSource:
    """Build a caught-up `LiveSpecSource`, or raise rather than start without the spec.

    Raising is the point: a worker or loader that starts with no spec has nothing to apply, and
    guessing (an empty pipeline, a default retry policy) would be worse than not starting.
    """
    cache = ChannelCache(consumer, topic, log=log)
    cache.start()
    cache.catch_up(timeout_s=catch_up_timeout_s)
    source = LiveSpecSource(cache, channel)
    source.current()  # fails now, loudly, if this channel is not deployed
    return source
