"""Where a reprocess gets the original payload: the `PayloadSource` interface and its Kafka implementation.

Content storage (W3-2b, claim-check and `message_content`) is not built; it waits on ADR-004. Reprocess is
written against `PayloadSource` so a content-store implementation can be added without touching it.

`KafkaRawPayloadSource` reads the one record at `(topic, partition, offset)` from the channel's `raw` topic,
using `assign` (no consumer group membership, no commits, no rebalance). It needs READ on the `raw` topic only.
The record is trusted only if its `tb-msg-id` equals the id the caller expects: an offset the archive recorded
can point at a different record if the topic was recreated or the log was truncated, and re-publishing the
wrong patient's message would be worse than refusing.

**What is lost when retention lapses.** `raw` retention is days (ADR-001 6.3, 7 days). Once the offset is below
the log start the payload is gone from Kafka and, until content storage exists, from everywhere: the event
timeline stays, reprocess answers `PayloadUnavailable(RETENTION_LAPSED)`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Final, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import TopicDefaults
from tributary.runtime.topics import ENV as _ENV_PATTERN
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError

from .errors import PayloadUnavailable


def reprocessor_group(env: str) -> str:
    """`tb.<env>.reprocessor`: the literal group id reprocess and replay hand to librdkafka.

    librdkafka refuses a consumer with no `group.id`, even when it only uses `assign`. The group is NEVER joined
    (no `subscribe`), never committed to and never used for lag: `assign` does not contact the group coordinator,
    so no GROUP ACL is needed (proved on a deny-by-default broker in the integration tests). It is deliberately not
    the events archiver's group (`tb.<env>.archiver`): the archiver reads `_tb.<env>.events`, this reads `raw`
    (ADR-004 role separation).
    """
    if not isinstance(env, str) or _ENV_PATTERN.match(env) is None:
        raise ValueError("env is not a valid environment name")
    return f"tb.{env}.reprocessor"


@dataclass(frozen=True, slots=True)
class PayloadRef:
    """Where the archive saw the message on `raw`. `message_id` is what the record must carry."""

    topic: str
    partition: int
    offset: int
    message_id: str


class PayloadSource(Protocol):
    def fetch(self, ref: PayloadRef) -> Message:
        """The original record as a `Message` (headers, key, payload), or `PayloadUnavailable`."""
        ...


_POLL_S: Final = 1.0


class KafkaRawPayloadSource:
    def __init__(self, settings: KafkaSettings, defaults: TopicDefaults, *, env: str, timeout_s: float = 30.0) -> None:
        self._config = consumer_config(settings, defaults, group_id=reprocessor_group(env), enable_partition_eof=True)
        self._config["enable.auto.commit"] = False
        self._timeout_s = timeout_s

    def fetch(self, ref: PayloadRef) -> Message:
        consumer = Consumer(self._config)
        try:
            return self._fetch(consumer, ref)
        finally:
            consumer.close()

    def _fetch(self, consumer: Any, ref: PayloadRef) -> Message:
        try:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(ref.topic, ref.partition), timeout=self._timeout_s
            )
        except Exception:
            raise PayloadUnavailable("SOURCE_ERROR") from None
        if ref.offset < low:
            raise PayloadUnavailable("RETENTION_LAPSED")
        if ref.offset >= high:
            raise PayloadUnavailable("OFFSET_NOT_FOUND")
        try:
            consumer.assign([TopicPartition(ref.topic, ref.partition, ref.offset)])
        except KafkaException:
            raise PayloadUnavailable("SOURCE_ERROR") from None
        deadline = time.monotonic() + self._timeout_s
        while time.monotonic() < deadline:
            try:
                record = consumer.poll(_POLL_S)
            except KafkaException:
                raise PayloadUnavailable("SOURCE_ERROR") from None
            if record is None:
                continue
            error = record.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    raise PayloadUnavailable("OFFSET_NOT_FOUND")
                raise PayloadUnavailable("SOURCE_ERROR")
            if record.offset() != ref.offset:
                # Compaction or a control record at the offset: the next visible record is not ours.
                raise PayloadUnavailable("RECORD_MISMATCH")
            try:
                message = Message(
                    headers=h.decode_headers(record.headers() or []), payload=record.value(), key=record.key()
                )
            except HeaderError:
                raise PayloadUnavailable("RECORD_UNDECODABLE") from None
            if message.get(h.TB_MSG_ID) != ref.message_id:
                raise PayloadUnavailable("RECORD_MISMATCH")
            return message
        raise PayloadUnavailable("SOURCE_TIMEOUT")
