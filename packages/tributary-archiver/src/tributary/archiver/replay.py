"""Replay a channel's `raw` records for a time range, back into the SAME channel's `raw`.

**Range.** `[start, end)` on the Kafka record timestamp (producer CreateTime unless the topic is
LogAppendTime). Boundaries are `offsetsForTimes` per partition: the first offset whose timestamp is `>=` the
time (`-1`, nothing at or after it, means the high watermark). The end offsets are taken ONCE, before anything
is published, so replaying into the same `raw` topic cannot chase its own output. A record inside the offset
span whose own timestamp is outside `[start, end)` (a producer with a skewed clock) is not republished, so the
republished set is exactly the records whose timestamp is in range AND that lie in the offset span. Records
with a timestamp in range but beyond the end offset are not found: the span is what `offsetsForTimes` says.

**Control records.** Transaction markers occupy offsets but are never returned to a consumer. They are stamped
with the broker's wall clock, so `offsetsForTimes` can land on a marker; on a topic whose records carry a
backdated CreateTime that moves a boundary (the per-record timestamp filter below still bounds what is published). The scan ends a
partition on the first record at or after its end offset, on `PARTITION_EOF`, or after reading the offset
before it, never by waiting for an exact offset, so a marker at the range boundary neither hangs the scan nor
is republished. Aborted transactional records are invisible (`read_committed`). A record that does not
decode as a Tributary message (no valid UUIDv7 `tb-msg-id`, or another channel's `tb-channel`) is skipped and
counted, never republished.

**Identity, headers, durability.** As `republish` (including: `tb-parent-id` is overwritten with the original's id): each record gets a new deterministic `tb-msg-id`
(`reprocess_id(original, replay_id)`) and `tb-parent-id` = original. Re-running a replay with the same
`replay_id` produces the same ids, so loader idempotency absorbs a retried replay. Memory is bounded: records stream through the producer; `max_records` bounds the span (upper bound, it counts
control records and records of other timestamps) and a larger range is refused before anything is published.

**No other target.** The target is always the source channel's own `raw`. A replay into another channel could move
PHI to a channel with weaker access control, so the option does not exist; "shadow" in the contracts is a second
consumer group on the same topic, not a re-publish.

**Ordering (invariant 5).** Replay appends old records to a live `raw` topic. Into an `ordering: per-key` channel
that puts them after newer records of the same key, so it is refused with `ORDERING_WOULD_BREAK` unless
`allow_reorder=True`, which is recorded in the audit `requested` event. Unknown channels are refused. Full rule:
`ordering`. The check runs before the audit event and before any read.

**Limits.** `max_records` is capped at `HARD_MAX_RECORDS` (a larger request is `INVALID_REQUEST`, not clamped), the
whole replay has a `deadline_s` (default `DEFAULT_DEADLINE_S`, at most `HARD_DEADLINE_S`; `DEADLINE_EXCEEDED`), and
a stalled read is `SOURCE_TIMEOUT`. Kafka client exceptions are wrapped in `SOURCE_ERROR`.

**Consumer.** `assign` only, group id `tb.<env>.reprocessor` (never joined, never committed): see
`payload_source.reprocessor_group`.

**Failure.** After `requested`, any failure stops the scan, flushes what was queued (best effort),
logs `channel_replay_failed` with the acknowledged count, emits an audit `failed` event with that count and puts the
count on the raised error (`error.republished`). Records already acknowledged stay on the topic (at-least-once);
re-running with the same `replay_id` regenerates the same ids.

**Audit.** `audit(requested)` fails closed before the first record is read.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, NoReturn

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import TopicDefaults
from tributary.runtime.errors import TopicNameError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import raw_topic
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError

from .audit import ACTOR, AuditEvent, AuditHook, Phase
from .errors import ArchiverError, PublishError, ReplayRefused
from .ordering import ORDERING_WOULD_BREAK, TraitsLookup, order_safe
from .payload_source import reprocessor_group
from .republish import RawPublisher, reinjected, reprocess_id, request_id_is_valid

DEFAULT_MAX_RECORDS: Final = 100_000
HARD_MAX_RECORDS: Final = 1_000_000
DEFAULT_DEADLINE_S: Final = 900.0
HARD_DEADLINE_S: Final = 3600.0
_FLUSH_EVERY: Final = 5_000
_TIMESTAMP_NOT_AVAILABLE: Final = 0  # confluent_kafka.TIMESTAMP_NOT_AVAILABLE


@dataclass(frozen=True, slots=True)
class ReplayResult:
    channel: str
    target_topic: str
    partitions_scanned: int
    #: Upper bound of the offset span read (includes control records and out-of-range timestamps).
    span: int
    republished: int
    skipped_undecodable: int
    skipped_out_of_range: int
    #: Records with no timestamp at all: not republished, since "in range" cannot be established.
    skipped_no_timestamp: int = 0


@dataclass(slots=True)
class _Counts:
    undecodable: int = 0
    out_of_range: int = 0
    no_timestamp: int = 0


def _ms(value: datetime) -> int:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ReplayRefused("INVALID_REQUEST")
    return int(value.astimezone(UTC).timestamp() * 1000)


class ReplayService:
    def __init__(
        self,
        settings: KafkaSettings,
        defaults: TopicDefaults,
        publisher: RawPublisher,
        *,
        env: str,
        audit: AuditHook,
        log: JsonLogger,
        traits: TraitsLookup,
        idle_timeout_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
    ) -> None:
        self._config = consumer_config(settings, defaults, group_id=reprocessor_group(env), enable_partition_eof=True)
        self._config["enable.auto.commit"] = False
        self._publisher = publisher
        self._env = env
        self._audit = audit
        self._log = log
        self._traits = traits
        self._idle_timeout_s = idle_timeout_s
        self._clock = clock
        self._consumer_factory = consumer_factory

    def replay(
        self,
        channel: str,
        *,
        start: datetime,
        end: datetime,
        replay_id: str,
        actor: str,
        max_records: int = DEFAULT_MAX_RECORDS,
        deadline_s: float = DEFAULT_DEADLINE_S,
        allow_reorder: bool = False,
    ) -> ReplayResult:
        start_ms, end_ms = _ms(start), _ms(end)
        if (
            start_ms >= end_ms
            or not request_id_is_valid(replay_id)
            or ACTOR.fullmatch(actor) is None
            or isinstance(max_records, bool)
            or not 1 <= max_records <= HARD_MAX_RECORDS
            or isinstance(deadline_s, bool)
            or not 0 < deadline_s <= HARD_DEADLINE_S
        ):
            raise ReplayRefused("INVALID_REQUEST")
        try:
            topic = raw_topic(self._env, channel)
        except TopicNameError:
            raise ReplayRefused("INVALID_REQUEST") from None
        if not order_safe(self._traits, channel=channel, allow_reorder=allow_reorder):
            raise ReplayRefused(ORDERING_WOULD_BREAK)

        def event(phase: Phase, count: int | None = None) -> AuditEvent:
            return AuditEvent(
                "channel.replay",
                phase,
                actor,
                channel,
                None,
                request_id=replay_id,
                count=count,
                allow_reorder=allow_reorder,
                target_topic=topic,
                range_start=start,
                range_end=end,
            )

        self._audit(event("requested"))  # fails closed
        start_delivered = self._publisher.delivered
        deadline = self._clock() + deadline_s
        counts = _Counts()
        plan: dict[int, tuple[int, int]] = {}
        span = 0
        try:
            consumer = self._consumer_factory(self._config)
        except KafkaException:
            self._failed(ReplayRefused("SOURCE_ERROR"), channel, start_delivered, event)
        try:
            plan = self._plan(consumer, topic, start_ms, end_ms)
            span = sum(stop - first for first, stop in plan.values())
            if span > max_records:
                raise ReplayRefused("RANGE_TOO_LARGE")
            self._scan(consumer, topic, plan, start_ms, end_ms, replay_id, channel, counts, deadline)
        except Exception as exc:
            self._failed(exc, channel, start_delivered, event)
        finally:
            consumer.close()
        republished = self._publisher.delivered - start_delivered
        result = ReplayResult(
            channel, topic, len(plan), span, republished, counts.undecodable, counts.out_of_range, counts.no_timestamp
        )
        self._log.info(
            "channel_replayed",
            channel=channel,
            partitions=len(plan),
            span=span,
            republished=republished,
            skipped=counts.undecodable + counts.out_of_range + counts.no_timestamp,
        )
        self._audit_best_effort(event("completed", republished))
        return result

    def _failed(self, exc: Exception, channel: str, start_delivered: int, event: Callable[..., AuditEvent]) -> NoReturn:
        """Flush what was queued, log and audit the partial count, put it on the error, and re-raise it."""
        with suppress(PublishError):  # best effort: acknowledge what was queued before reporting
            self._publisher.flush()
        done = self._publisher.delivered - start_delivered
        if isinstance(exc, ArchiverError):
            exc.republished = done
        self._log.warning(
            "channel_replay_failed",
            channel=channel,
            error_code=getattr(exc, "code", type(exc).__name__),
            republished=done,
        )
        self._audit_best_effort(event("failed", done))
        raise exc

    def _audit_best_effort(self, event: AuditEvent) -> None:
        try:
            self._audit(event)
        except Exception as exc:  # after the work: class name only, never turns a delivery into an error
            self._log.warning("audit_followup_failed", error_type=type(exc).__name__)

    # -- planning ---------------------------------------------------------------------------------

    def _plan(self, consumer: Any, topic: str, start_ms: int, end_ms: int) -> dict[int, tuple[int, int]]:
        try:
            metadata = consumer.list_topics(topic, timeout=30)
            entry = metadata.topics.get(topic)
            if entry is None or entry.error is not None:
                raise ReplayRefused("TOPIC_NOT_FOUND")
            partitions = sorted(entry.partitions)
            starts = consumer.offsets_for_times([TopicPartition(topic, p, start_ms) for p in partitions], timeout=30)
            ends = consumer.offsets_for_times([TopicPartition(topic, p, end_ms) for p in partitions], timeout=30)
            plan: dict[int, tuple[int, int]] = {}
            for first, stop in zip(starts, ends, strict=True):
                _, high = consumer.get_watermark_offsets(TopicPartition(topic, first.partition), timeout=30)
                first_offset = high if first.offset < 0 else first.offset
                stop_offset = high if stop.offset < 0 else stop.offset
                if first_offset < stop_offset:
                    plan[first.partition] = (first_offset, stop_offset)
            return plan
        except ReplayRefused:
            raise
        except Exception:
            raise ReplayRefused("SOURCE_ERROR") from None

    # -- scan and publish ---------------------------------------------------------------------------

    def _scan(
        self,
        consumer: Any,
        topic: str,
        plan: dict[int, tuple[int, int]],
        start_ms: int,
        end_ms: int,
        replay_id: str,
        channel: str,
        counts: _Counts,
        deadline: float,
    ) -> None:
        if not plan:
            return
        try:
            consumer.assign([TopicPartition(topic, p, first) for p, (first, _) in plan.items()])
        except KafkaException:
            raise ReplayRefused("SOURCE_ERROR") from None
        pending = set(plan)
        pending_flush = 0
        idle_since = self._clock()

        def finish(partition: int) -> None:
            if partition in pending:
                pending.discard(partition)
                try:
                    consumer.pause([TopicPartition(topic, partition)])
                except KafkaException:
                    raise ReplayRefused("SOURCE_ERROR") from None

        while pending:
            if self._clock() > deadline:
                raise ReplayRefused("DEADLINE_EXCEEDED")
            try:
                record = consumer.poll(1.0)
            except KafkaException:
                raise ReplayRefused("SOURCE_ERROR") from None
            if record is None:
                if self._clock() - idle_since > self._idle_timeout_s:
                    raise ReplayRefused("SOURCE_TIMEOUT")
                continue
            idle_since = self._clock()
            error = record.error()
            partition = record.partition()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    finish(partition)
                    continue
                raise ReplayRefused("SOURCE_ERROR")
            if partition not in pending:
                continue  # a straggler fetched before the pause took effect
            offset = record.offset()
            stop = plan[partition][1]
            if offset >= stop:
                finish(partition)
                continue
            if offset == stop - 1:
                finish(partition)
            stamp_type, stamp = record.timestamp()
            if stamp_type == _TIMESTAMP_NOT_AVAILABLE:
                counts.no_timestamp += 1
                continue
            if not start_ms <= stamp < end_ms:
                counts.out_of_range += 1
                continue
            message = _decode(record)
            if message is None or message.get(h.TB_CHANNEL) != channel:
                counts.undecodable += 1
                continue
            original_id = message.require(h.TB_MSG_ID)
            self._publisher.publish(
                topic, reinjected(message, new_id=reprocess_id(original_id, replay_id), parent_id=original_id)
            )
            pending_flush += 1
            if pending_flush >= _FLUSH_EVERY:
                self._publisher.flush()
                pending_flush = 0
        self._publisher.flush()


def _decode(record: Any) -> Message | None:
    try:
        message = Message(headers=h.decode_headers(record.headers() or []), payload=record.value(), key=record.key())
    except HeaderError:
        return None
    return message if h.is_well_formed(h.TB_MSG_ID, message.get(h.TB_MSG_ID) or "") else None
