"""Publishing to `<env>.<channel>.raw`: builds a schema-conformant envelope and commits it before
this package's caller is allowed to ACK the sender (invariant 1).

Uses `tributary.sdk.Message` and `tributary.sdk.ids`/`headers` for everything envelope-shaped, so the
headers this receiver emits are exactly the ones `contracts/envelope/v0` and the SDK agree on — gate
requirement 10 exists because Phase 0 spike code invented its own id encoding
(`spikes/S2-mllp-kafka/server.py`'s hand-rolled UUIDv7, since fixed but never checked against the
schema) and requirement 10 says "validated against `contracts/envelope/v0`", not "matches the spec by
inspection." `RawPublisher.publish` calls `tributary.sdk.projection.validate_for_produce` before every
produce — the checks a JSON Schema cannot make (byte budgets, per-class key identity, the `tb-x-*`
ban) enforced at the point a record could otherwise reach a topic — and
`tests/mllp/test_kafka_publisher.py` runs the real schema over `record_projection`'s output.

The producer's durability settings (`acks`, `enable.idempotence`, `delivery.timeout.ms`) come from
`tributary.runtime.config.load_topic_defaults` / `tributary.runtime.clients.producer_config`, not
from a literal in this file. `contracts/topics/v0/topic-defaults.json`'s `must_not_hardcode` list
names exactly these three settings as NORMATIVE: a topic-created-without-`min.insync.replicas`-style
mistake, but for the producer side of invariant 1 — a literal `acks="all"` here reads correctly
today and stops meaning anything the day the contract changes and nothing here does (devops-engineer
code review, W1-5 second round). `tributary-runtime` is a dependency for this module alone, not
because this package is now runtime-driven (see `mllp/__init__.py`: there is still no pipeline here).
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from functools import lru_cache
from typing import Literal, Protocol

from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import TopicDefaults, load_topic_defaults
from tributary.runtime.errors import KafkaSecurityError
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.errors import EnvelopeError
from tributary.sdk.projection import Ordering, validate_for_produce

from ..errors import PermanentPublishError

_ENV = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_CHANNEL = re.compile(r"^[a-z][a-z0-9-]{0,63}$")

CONTENT_TYPE = "application/hl7-v2+er7"

PublishResult = Literal["committed", "failed", "timeout"]


class _Publishes(Protocol):
    async def publish(self, message: Message, *, timeout_s: float) -> bool: ...


async def publish_outcome(publisher: _Publishes, message: Message, *, timeout_s: float) -> PublishResult:
    """`publisher.publish_result` when it has one (`RawPublisher`), else the plain bool mapped to
    `committed` / `failed` (a test double that cannot tell a timeout)."""
    result = getattr(publisher, "publish_result", None)
    if result is not None:
        return await result(message, timeout_s=timeout_s)  # type: ignore[no-any-return]
    return "committed" if await publisher.publish(message, timeout_s=timeout_s) else "failed"


#: Readiness (`contracts/observability/v0` 2.3, R1): while the producer reports every broker down, one
#: metadata probe at most this often, each bounded by `PROBE_TIMEOUT_S`, tells it that it has recovered.
PROBE_INTERVAL_S = 5.0
PROBE_TIMEOUT_S = 2.0
#: The warm-up round trip (2.4 W1): long enough for the first connection, short enough to retry.
WARM_TIMEOUT_S = 5.0
_ALL_DOWN = "_ALL_BROKERS_DOWN"
_FATAL = "_FATAL"


def topic_name(env: str, channel: str) -> str:
    """`<env>.<channel>.raw` (`contracts/topics/v0` section 1). Checked here, not just trusted, so a
    bad `--env` fails fast at startup instead of producing to a topic nothing else expects."""
    if not _ENV.match(env):
        raise ValueError(f"env {env!r} does not match {_ENV.pattern}")
    if not _CHANNEL.match(channel):
        raise ValueError(f"channel {channel!r} does not match {_CHANNEL.pattern}")
    return f"{env}.{channel}.raw"


def build_raw_envelope(
    *,
    channel: str,
    channel_version: int,
    source: str,
    payload: bytes,
    key: bytes | None,
    reply_headers: Mapping[str, str] | None = None,
) -> Message:
    """The `class: raw` envelope for one received message: a fresh `tb-msg-id`, no `tb-attempt`
    beyond `0` (this is the first hop), a fresh `traceparent` (this receiver starts the trace).

    `reply_headers` is synchronous reply mode's `tb-correlation-id`, `tb-reply-partition` and
    `tb-reply-deadline` (`reply/pending.py`): engine-minted ids, an integer and a timestamp. The sender's own
    control id is never a header."""
    return Message(
        headers={
            **(reply_headers or {}),
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: channel,
            h.TB_CHANNEL_VERSION: str(channel_version),
            h.TB_SOURCE: source,
            h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
            h.TB_CONTENT_TYPE: CONTENT_TYPE,
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
        },
        payload=payload,
        key=key,
    )


_SSL_CLIENT_CERT_KEYS = ("ssl.certificate.location", "ssl.key.location")


@lru_cache(maxsize=1)
def _cached_topic_defaults() -> TopicDefaults:
    """Read `contracts/topics/v0/topic-defaults.json` once per process. `load_topic_defaults`
    already refuses to start (`ConfigError`) rather than invent a number when the file cannot be
    found; see its docstring for the search order (`TB_CONTRACTS_DIR`, then a `contracts/` directory
    above the installed package — a container image must ship one or set the other)."""
    return load_topic_defaults()


class RawPublisher:
    """Awaitable produce: resolves `True` only once the delivery report for that exact record says
    success. `enable.idempotence` plus `acks=all` is invariant 1's broker-side half; the awaited
    future is the process-side half — nothing calls this "committed" before the callback fires.
    Both of those, and `delivery.timeout.ms`, come from the topics contract's `receiver_producer`
    client profile (module docstring), never from a parameter this class exposes — there is no
    legal way to construct a `RawPublisher` that ACKs on anything less than `acks=all`.

    One producer, and one background thread polling it, is shared by every connection a server
    handles; `confluent_kafka.Producer.produce` is not itself a coroutine; polling on the event loop
    would starve delivery-report callbacks under load, hence the dedicated thread (same design as the
    Phase 0 S2 spike, `spikes/S2-mllp-kafka/server.py`'s `KafkaPublisher`, reimplemented here rather
    than imported per the Phase 0 ground rule).
    """

    def __init__(
        self,
        kafka: KafkaSettings,
        topic: str,
        *,
        linger_ms: int | None = None,
        queue_max_messages: int = 100_000,
        topic_defaults: TopicDefaults | None = None,
        ordering: Ordering = "none",
        client_id_prefix: str = "mllp-receiver",
    ) -> None:
        from confluent_kafka import Producer  # lazy: unit tests need no broker client

        # `parse_engine_kafka` requires a client certificate for SSL, but an embedded caller can build
        # server-auth-only `KafkaSettings` directly. The receiver principal authenticates by mTLS
        # (contracts/engine-kafka/v0), so refuse here rather than connect anonymously-by-cert.
        if kafka.protocol == "SSL" and not all(k in kafka.security for k in _SSL_CLIENT_CERT_KEYS):
            raise KafkaSecurityError("security.protocol SSL requires ssl.certificate.location and ssl.key.location")

        self.topic = topic
        self._ordering: Ordering = ordering
        defaults = topic_defaults if topic_defaults is not None else _cached_topic_defaults()
        config = producer_config(kafka, defaults, client_id=f"{client_id_prefix}-{topic}")
        if linger_ms is not None:
            config["linger.ms"] = linger_ms  # not in the contract's must_not_hardcode list
        config["queue.buffering.max.messages"] = queue_max_messages
        config["socket.keepalive.enable"] = True
        config["error_cb"] = self._on_error
        self._producer = Producer(config)
        self._warmed = False
        self._all_brokers_down = False
        self._fatal = False
        self._probe_lock = threading.Lock()
        self._probe_in_flight = False
        self._probe_thread: threading.Thread | None = None
        self._next_probe_at = 0.0
        #: Bumped on every `_ALL_BROKERS_DOWN`. A probe or warm-up that began before the latest one proves nothing
        #: about it, so it may not clear the flag.
        self._down_generation = 0
        #: Called once, on the producer's poll thread, when librdkafka reports a fatal error. The runner hops
        #: to its event loop and exits non-zero (observability/v0 R2).
        self.fatal_handler: Callable[[], None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._closed = False
        self._poller = threading.Thread(target=self._poll, daemon=True, name="raw-publisher-poll")

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._poller.start()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.005)
            if self._all_brokers_down:
                self._maybe_probe()

    # -- readiness (state only; nothing here is called on a request path that waits for Kafka) ------

    @property
    def warmed(self) -> bool:
        return self._warmed

    def readiness_reason(self) -> str | None:
        """`None` when ready, else a closed reason from `readiness.schema.json` (`raw_producer` check)."""
        if self._fatal:
            return "fatal"
        if not self._warmed:
            return "not_warmed"
        if self._all_brokers_down:
            return "all_brokers_down"
        return None

    def _on_error(self, error: object) -> None:
        """librdkafka `error_cb`, on the poll thread. Only the symbolic name is read, never the text."""
        name = ""
        fatal = False
        with contextlib.suppress(Exception):
            name = str(error.name())  # type: ignore[attr-defined]
            fatal = bool(error.fatal())  # type: ignore[attr-defined]
        if fatal or name == _FATAL:
            if not self._fatal:
                self._fatal = True
                handler = self.fatal_handler
                if handler is not None:
                    with contextlib.suppress(Exception):
                        handler()
        elif name == _ALL_DOWN:
            self._down_generation += 1
            self._all_brokers_down = True

    def _metadata_names_every_leader(self, timeout_s: float) -> bool:
        """One metadata round trip for this topic: it exists and every partition has a leader."""
        try:
            metadata = self._producer.list_topics(self.topic, timeout=timeout_s)
        except Exception:
            return False
        topic = metadata.topics.get(self.topic)
        if topic is None or topic.error is not None or not topic.partitions:
            return False
        for detail in topic.partitions.values():
            if getattr(detail, "error", None) is not None or getattr(detail, "leader", -1) < 0:
                return False
        return True

    def warm_up(self, timeout_s: float = WARM_TIMEOUT_S) -> bool:
        """Blocking (run it off the event loop): the warm-up of observability/v0 2.4. True once the reply names
        a leader for every partition of `<env>.<channel>.raw`; `warmed` stays False otherwise."""
        generation = self._down_generation
        if self._metadata_names_every_leader(timeout_s):
            self._warmed = True
            if generation == self._down_generation:
                self._all_brokers_down = False
            return True
        return False

    def _maybe_probe(self) -> None:
        now = time.monotonic()
        with self._probe_lock:
            if self._probe_in_flight or now < self._next_probe_at:
                return
            self._probe_in_flight = True
            self._next_probe_at = now + PROBE_INTERVAL_S
        thread = threading.Thread(target=self._probe, name="raw-publisher-probe", daemon=True)
        thread.start()
        self._probe_thread = thread  # after start: `close` joins it only once it is running

    def _probe(self) -> None:
        generation = self._down_generation
        try:
            if self._closed:
                return  # the producer is closing: no metadata call on it
            if self._metadata_names_every_leader(PROBE_TIMEOUT_S) and generation == self._down_generation:
                self._all_brokers_down = False
        finally:
            with self._probe_lock:
                self._probe_in_flight = False

    def close(self, timeout_s: float = 5.0) -> None:
        """Flush what is in flight, then refuse every later `publish` (it returns `False`: not committed, so
        the caller NACKs). Set before the flush, so nothing new can slip in behind it. Idempotent."""
        self._closed = True
        self._producer.flush(timeout_s)
        self._stop.set()  # the poller keeps running through the flush: it delivers the reports
        probe = self._probe_thread
        if probe is not None:
            probe.join(PROBE_TIMEOUT_S + 0.5)  # bounded: `list_topics` honours its timeout

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        """Raises `PermanentPublishError` for a record that breaks the envelope contract (retrying cannot
        help). Otherwise `True` once committed, `False` on any failure, a full local queue, or `timeout_s`
        elapsing first — every one of those is "not committed," so every one of them is a NACK,
        never a dropped message that was silently ACKed (invariant 1)."""
        return await self.publish_result(message, timeout_s=timeout_s) == "committed"

    async def publish_result(self, message: Message, *, timeout_s: float) -> PublishResult:
        """`publish`, telling a delivery **timeout** apart from a failure. After a timeout librdkafka may still
        complete the write, so a resend can duplicate (it cannot lose data): synchronous reply mode answers it
        `PRODUCE_TIMEOUT`, distinct from `PRODUCE_FAILED` (channel-spec README 3.2.6)."""
        loop = self._loop
        if loop is None:
            raise RuntimeError("RawPublisher.start() must run before publish() is awaited")
        if self._closed:
            return "failed"  # a closed producer commits nothing: never ACK
        try:
            # `ordering` is the channel's, fixed at construction: `none` (the MLLP receiver's only mode)
            # takes a null key, `per-key` refuses a record whose key did not resolve (invariant 5).
            validate_for_produce(message, topic_class="raw", ordering=self._ordering)
        except EnvelopeError:
            # Never produce a record that would fail the envelope contract. Not `False`: that means "try again".
            raise PermanentPublishError("record breaks the envelope contract") from None
        future: asyncio.Future[bool] = loop.create_future()

        def on_delivery(err: object, _msg: object) -> None:
            if err is None:
                self._all_brokers_down = False  # a delivery report proves the brokers answer (R1)

            def set_result() -> None:
                if not future.done():
                    future.set_result(err is None)

            loop.call_soon_threadsafe(set_result)

        kafka_headers: list[tuple[str, str | bytes | None]] = [
            (name, value.encode("utf-8")) for name, value in message.headers.items()
        ]
        try:
            self._producer.produce(
                self.topic, value=message.payload, key=message.key, headers=kafka_headers, on_delivery=on_delivery
            )
        except BufferError:
            return "failed"  # local queue full: refuse rather than buffer unboundedly
        try:
            return "committed" if await asyncio.wait_for(future, timeout_s) else "failed"
        except TimeoutError:
            return "timeout"
