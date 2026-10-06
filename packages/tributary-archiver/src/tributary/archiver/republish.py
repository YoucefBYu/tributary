"""Re-publishing records into a `raw` topic: the id rule, the header rule and the producer.

**Identity (envelope README 6 rule 5).** A re-injected record gets a NEW `tb-msg-id` and `tb-parent-id` set to
the original, so a destination that is idempotent on `tb-msg-id` does not drop a deliberate reprocess.

**Idempotency key.** The new id is NOT random. It is a UUIDv7-shaped function of `(original tb-msg-id,
request id)`: the timestamp is the request id's, the remaining bits are the first 80 bits of
`SHA-256("tributary.reprocess.v0" 0x00 || original id bytes || request id bytes)`. The caller (the API) mints
one UUIDv7 per operator action and reuses it on a retry. Re-running an action whose first attempt had an unknown
outcome therefore produces records with the SAME `tb-msg-id`s, and every loader's `tb-msg-id` idempotency
(invariant 4) absorbs the duplicate delivery. A different request id is a different, deliberate reprocess. The
`raw` topic itself may hold both copies (publishing is at-least-once). Nothing here is secret or keyed: it is
an id derivation, not cryptography (same shape as split-child ids, but a distinct domain so a reprocess id can
never equal a split child's).

**Ordering.** See `ordering`: re-injection into a live per-key channel is refused unless `allow_reorder=True`.

**Headers.** Everything is copied unchanged (including `tb-received-at`, `tb-channel-version`, `tb-source`,
`tb-usr-*`, the claim-check) except (`tb-parent-id` is OVERWRITTEN with the original's `tb-msg-id`, even when the
original already had a parent, e.g. a replay of a reprocessed record: the tree grows one level, it does not keep the
older link): `tb-msg-id` and `tb-parent-id` (above); `tb-attempt` reset to `0`;
`traceparent` gets a new span id on the same trace; and the headers a `raw` record must not carry or must not
re-arm are dropped: reply routing and status (envelope 6 rule 7, a re-injected record never answers on a
partition nobody watches), `tb-destination`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`. The record key
and payload are copied byte for byte (the key is usually PHI: it is never logged or put in an exception).

**Producer.** `producer_config` (`acks=all`, idempotent producer, the invariant 1 profile) from the topics
contract. `flush` returns only when every delivery callback reported success; otherwise `PublishError`. The
idempotent producer prevents duplicates inside one producer session on retry; it does not span processes, which
is what the id rule above is for.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

from confluent_kafka import Producer
from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import TopicDefaults
from tributary.runtime.records import to_kafka_headers
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import EnvelopeError
from tributary.sdk.ids import child_traceparent, new_traceparent
from tributary.sdk.projection import Ordering, validate_for_produce

from .errors import PublishError

_DOMAIN: Final = b"tributary.reprocess.v0\x00"
_RAND_B_BITS: Final = 62
#: Headers a re-injected `raw` record drops (see module doc).
DROPPED_HEADERS: Final = (
    h.TB_REPLY_PARTITION,
    h.TB_REPLY_DEADLINE,
    h.TB_REPLY_STATUS,
    h.TB_DESTINATION,
    h.TB_STAGE,
    h.TB_ERROR_CODE,
    h.TB_RETRY_NOT_BEFORE,
)


def request_id_is_valid(request_id: str) -> bool:
    return h.is_well_formed(h.TB_MSG_ID, request_id)  # canonical lowercase UUIDv7


def reprocess_id(original_id: str, request_id: str) -> str:
    """The deterministic new `tb-msg-id` (module doc). `request_id` must be a canonical UUIDv7."""
    if not request_id_is_valid(request_id):
        raise ValueError("request id must be a canonical lowercase UUIDv7")
    original = uuid.UUID(original_id)
    request = uuid.UUID(request_id)
    digest = int.from_bytes(hashlib.sha256(_DOMAIN + original.bytes + request.bytes).digest()[:10], "big")
    value = (request.int >> 80) << 80
    value |= 0x7 << 76
    value |= ((digest >> 68) & 0xFFF) << 64
    value |= 0b10 << 62
    value |= digest & ((1 << _RAND_B_BITS) - 1)
    return str(uuid.UUID(int=value))


def reinjected(original: Message, *, new_id: str, parent_id: str, channel: str | None = None) -> Message:
    """The record to publish to `raw`: see the module doc for exactly what changes."""
    result = original
    for name in DROPPED_HEADERS:
        result = result.without_header(name)
    result = result.with_header(h.TB_MSG_ID, new_id).with_header(h.TB_PARENT_ID, parent_id)
    result = result.with_header(h.TB_ATTEMPT, "0")
    previous = original.traceparent
    result = result.with_header(h.TRACEPARENT, child_traceparent(previous) if previous else new_traceparent())
    if channel is not None:
        result = result.with_header(h.TB_CHANNEL, channel)
    return result


@dataclass(frozen=True, slots=True)
class Delivery:
    partition: int
    offset: int


class RawPublisher:
    """Produces envelope-validated records to `raw` and reports each broker acknowledgement."""

    def __init__(self, producer: Any) -> None:
        self._producer = producer
        self._deliveries: list[Delivery] = []
        self._failures = 0
        self.delivered = 0  # broker-acknowledged records, cumulative

    def publish(self, topic: str, message: Message) -> None:
        # A null key is faithful to the original record (it was accepted on a channel with `ordering: none`).
        ordering: Ordering = "per-key" if message.key is not None else "none"
        try:
            validate_for_produce(message, topic_class="raw", ordering=ordering)
        except EnvelopeError:
            raise PublishError("ENVELOPE_INVALID") from None
        for _ in range(600):  # a full local queue is back-pressure: wait for deliveries, never drop
            try:
                self._producer.produce(
                    topic,
                    value=message.payload,
                    key=message.key,
                    headers=to_kafka_headers(message),
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError:
                self._producer.poll(0.1)
            except Exception:
                raise PublishError("PRODUCE_REFUSED") from None
        raise PublishError("PRODUCE_TIMEOUT")

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._failures += 1  # the error text is a librdkafka string; only the count is kept
        else:
            self.delivered += 1
            self._deliveries.append(Delivery(msg.partition(), msg.offset()))

    def flush(self, timeout_s: float = 60.0) -> Sequence[Delivery]:
        """Deliveries since the last flush, in acknowledgement order. Raises unless every record landed."""
        remaining = self._producer.flush(timeout_s)
        if remaining > 0:
            raise PublishError("PRODUCE_TIMEOUT")
        failures, self._failures = self._failures, 0
        done, self._deliveries = self._deliveries, []
        if failures:
            raise PublishError("PRODUCE_FAILED")
        return done

    def close(self) -> None:
        self._producer.flush(5.0)


def make_publisher(settings: KafkaSettings, defaults: TopicDefaults, *, client_id: str) -> RawPublisher:
    """A `RawPublisher` on a durable producer: `acks=all`, idempotence on (the contract's profile)."""
    return RawPublisher(Producer(producer_config(settings, defaults, client_id=client_id)))
