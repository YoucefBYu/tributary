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
import re
import threading
from datetime import UTC, datetime
from functools import lru_cache

from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import TopicDefaults, load_topic_defaults
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.errors import EnvelopeError
from tributary.sdk.projection import Ordering, validate_for_produce

from ..errors import PermanentPublishError

_ENV = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_CHANNEL = re.compile(r"^[a-z][a-z0-9-]{0,63}$")

CONTENT_TYPE = "application/hl7-v2+er7"


def topic_name(env: str, channel: str) -> str:
    """`<env>.<channel>.raw` (`contracts/topics/v0` section 1). Checked here, not just trusted, so a
    bad `--env` fails fast at startup instead of producing to a topic nothing else expects."""
    if not _ENV.match(env):
        raise ValueError(f"env {env!r} does not match {_ENV.pattern}")
    if not _CHANNEL.match(channel):
        raise ValueError(f"channel {channel!r} does not match {_CHANNEL.pattern}")
    return f"{env}.{channel}.raw"


def build_raw_envelope(
    *, channel: str, channel_version: int, source: str, payload: bytes, key: bytes | None
) -> Message:
    """The `class: raw` envelope for one received message: a fresh `tb-msg-id`, no `tb-attempt`
    beyond `0` (this is the first hop), a fresh `traceparent` (this receiver starts the trace)."""
    return Message(
        headers={
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
        bootstrap_servers: str,
        topic: str,
        *,
        linger_ms: int | None = None,
        queue_max_messages: int = 100_000,
        security: dict[str, object] | None = None,
        topic_defaults: TopicDefaults | None = None,
        ordering: Ordering = "none",
        client_id_prefix: str = "mllp-receiver",
    ) -> None:
        from confluent_kafka import Producer  # lazy: unit tests need no broker client

        from ..engine_kafka import require_secure_link

        require_secure_link(bootstrap_servers, security)  # before anything is built or connected

        self.topic = topic
        self._ordering: Ordering = ordering
        defaults = topic_defaults if topic_defaults is not None else _cached_topic_defaults()
        settings = KafkaSettings(bootstrap_servers=bootstrap_servers, security=security or {})
        config = producer_config(settings, defaults, client_id=f"{client_id_prefix}-{topic}")
        if linger_ms is not None:
            config["linger.ms"] = linger_ms  # not in the contract's must_not_hardcode list
        config["queue.buffering.max.messages"] = queue_max_messages
        config["socket.keepalive.enable"] = True
        self._producer = Producer(config)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll, daemon=True, name="raw-publisher-poll")

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._poller.start()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.005)

    def close(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._producer.flush(timeout_s)

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        """Raises `PermanentPublishError` for a record that breaks the envelope contract (retrying cannot
        help). Otherwise `True` once committed, `False` on any failure, a full local queue, or `timeout_s`
        elapsing first — every one of those is "not committed," so every one of them is a NACK,
        never a dropped message that was silently ACKed (invariant 1)."""
        loop = self._loop
        if loop is None:
            raise RuntimeError("RawPublisher.start() must run before publish() is awaited")
        try:
            # `ordering` is the channel's, fixed at construction: `none` (the MLLP receiver's only mode)
            # takes a null key, `per-key` refuses a record whose key did not resolve (invariant 5).
            validate_for_produce(message, topic_class="raw", ordering=self._ordering)
        except EnvelopeError:
            # Never produce a record that would fail the envelope contract. Not `False`: that means "try again".
            raise PermanentPublishError("record breaks the envelope contract") from None
        future: asyncio.Future[bool] = loop.create_future()

        def on_delivery(err: object, _msg: object) -> None:
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
            return False  # local queue full: refuse rather than buffer unboundedly
        try:
            return await asyncio.wait_for(future, timeout_s)
        except TimeoutError:
            return False
