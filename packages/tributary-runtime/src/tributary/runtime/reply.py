"""The worker side of synchronous reply mode (set 1.2, W2-8): route, status, and one reply record.

Normative sources: channel-spec README 3.2.6 ("Worker rules"), envelope README 3 and 2.3.3, ADR-003 D9
(amended) and D10. The receiver side — pending map, `replier` consumer, MLLP answers — is not here.

**Whether to reply is decided by the consumed record, not the spec.** The worker replies if and only if
the `raw` record carries `tb-reply-partition`, so a hot reload between sync and async strands nothing.
Three exceptions produce no reply and count it: a worker whose applied spec is transactional or has a
splitter (`replies_skipped`), a reply past `tb-reply-deadline` plus 5 s of skew (`replies_expired`;
the message is still processed; a `replied` event with `status: dropped`, code `REPLY_EXPIRED` (set
1.4: a registered engine-initiated skip) and a fixed `error_message`), and a malformed or out-of-range routing header, which
disables the reply and **not** the message (envelope 2.3.3: a `replied` event with
`REPLY_ROUTE_INVALID`). In every one of them the sender gets `AE` / `REPLY_TIMEOUT` from its receiver,
which is the truthful answer: the message was committed.

**Out of range means proven out of range.** The `reply` partition count comes from cluster metadata,
fetched off the consumer thread (`ReplyPartitionCount`), so a slow or failing metadata request never
stalls a partition. `REPLY_ROUTE_INVALID` is only given against a count fetched within `refresh_s`.
When the count is unknown (the first sync record, a failed fetch) or a header names a partition beyond
a stale count (an operator may have added partitions), the route is *unverified*, not invalid: the
reply is attempted, and a partition that does not exist fails its produce as `PRODUCE_FAILED`. A
transient metadata failure is therefore never reported as a bad header.

**When.** After every `out` record's delivery callback, and after the `dlq` callback when the message
was dead-lettered — `AA` means committed *and routed* (D10). The worker caller guarantees the order;
this module only produces.

**Fail-fast (ADR-003 D9 rule 3).** A reply is produced once and never retried, never pauses a
partition and never holds back an offset commit: losing a reply ends as `AE`, which is safe, while
retrying it would stall every other sender's request on the partition. Its delivery is observed through
its own callback, never through `RecordProducer`'s failure list, so a lost reply cannot turn into a
blocking retry that duplicates the message's `out` records.

**Status (channel-spec 3.2.6).** `accepted` only if every destination was produced or deliberately
dropped, and for a whole-message `Drop`. Otherwise the status and code of the **first failure in spec
order** — pipeline steps, then destinations in list order — not the first in time, so the answer is
deterministic. A `Reject` is `rejected`; anything else is `failed`.

**PHI (invariant 6).** A reply record carries the engine-minted correlation id as its key, an integer
partition, an enum and a code. It never carries the `raw` record's key (often a patient identifier),
`tb-usr-*` headers, or anything from the payload unless a step supplied a body under `body: step`, and
that body is payload: never logged. Log lines carry `tb-msg-id`, codes and coordinates only.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_MISSING, REPLY_EXPIRED, REPLY_ROUTE_INVALID, TB_OUTPUT_ENVELOPE_INVALID
from tributary.sdk.context import Logger
from tributary.sdk.errors import EnvelopeError, HeaderError
from tributary.sdk.ids import child_traceparent, new_traceparent
from tributary.sdk.projection import validate_for_produce
from tributary.sdk.reply import ReplyBody

from .events import EventEmitter, MessageEvent
from .producer import KafkaProducerLike
from .records import ConsumedRecord, to_kafka_headers

ReplyStatus = Literal["accepted", "rejected", "failed"]

#: A reply past `tb-reply-deadline` plus this allowance is skipped (channel-spec 3.2.6). The receiver
#: sets the deadline no earlier than its real one, so the allowance only absorbs clock skew.
SKEW_ALLOWANCE = timedelta(seconds=5)

#: Envelope 3: a reply without a step body is zero bytes of this type; the status header is the answer.
EMPTY_REPLY_CONTENT_TYPE = "application/octet-stream"

#: The `error_message` of the `replied` / `dropped` event for a reply past its deadline. Static text.
EXPIRED_MESSAGE = "reply past tb-reply-deadline plus the skew allowance; not produced, message processed"

#: Reply produce failures reuse the registered `PRODUCE_FAILED` with stage `replied` (proposal 6).
PRODUCE_FAILED = "PRODUCE_FAILED"

#: Reply-stage codes: the reply itself failed, so its `replied` event is `status: error`. For a pipeline
#: failure the reply stage worked and its event is `ok`, carrying the failure's code.
_REPLY_STAGE_FAILURES = frozenset({REPLY_BODY_MISSING, REPLY_ROUTE_INVALID, PRODUCE_FAILED, TB_OUTPUT_ENVELOPE_INVALID})

#: Rank of a pipeline-level failure in "first failure in spec order"; destination i ranks i + 1.
PIPELINE_RANK = 0


def destination_rank(index: int) -> int:
    """Spec-order rank of the destination at `index` in `spec.destinations`."""
    return index + 1


# -- routing ---------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplyRoute:
    """Where and until when to answer one `raw` record."""

    partition: int
    deadline: datetime
    correlation_id: str


@dataclass(frozen=True, slots=True)
class RouteInvalid:
    """A routing header is malformed or out of range. `reason` is a fixed token, never a header value."""

    reason: str


def parse_route(message: Message, *, partition_count: int | None) -> ReplyRoute | RouteInvalid | None:
    """The reply route of a consumed `raw` record: `None` for an async record, else a route or a refusal.

    Total: never raises. `partition_count` is a **verified** partition count of the channel's `reply`
    topic, or `None` when none is available now; then the range check is skipped (the route is
    unverified, see the module doc) rather than failed, because an unknown count says nothing about the
    header.
    """
    partition_text = message.get(h.TB_REPLY_PARTITION)
    deadline_text = message.get(h.TB_REPLY_DEADLINE)
    if partition_text is None and deadline_text is None:
        return None
    if partition_text is None:
        return RouteInvalid("deadline-without-partition")
    if deadline_text is None:
        return RouteInvalid("partition-without-deadline")
    try:
        partition = h.parse_int(h.TB_REPLY_PARTITION, partition_text)  # 0-63, envelope 3
    except HeaderError:
        return RouteInvalid("partition-malformed")
    if partition_count is not None and partition >= partition_count:
        return RouteInvalid("partition-out-of-range")
    try:
        deadline = h.parse_timestamp(h.TB_REPLY_DEADLINE, deadline_text)
    except HeaderError:
        return RouteInvalid("deadline-malformed")
    correlation_id = message.get(h.TB_CORRELATION_ID)
    if correlation_id is None or not h.is_well_formed(h.TB_CORRELATION_ID, correlation_id):
        return RouteInvalid("correlation-id-invalid")
    return ReplyRoute(partition=partition, deadline=deadline, correlation_id=correlation_id)


def _spawn_daemon(target: Callable[[], None]) -> None:
    threading.Thread(target=target, name="tb-reply-metadata", daemon=True).start()


class ReplyPartitionCount:
    """The partition count of one `reply` topic, from cluster metadata, **never fetched on the caller's
    thread** (the consumer thread: a 5 s `list_topics` there stalled every partition it held).

    `__call__(wanted)` answers from the cache at once. It returns a count to check `wanted` against, or
    `None` when the route cannot be verified now: the count is unknown, or `wanted` is at or beyond a
    count fetched more than `refresh_s` ago. A partition below any known count is always valid (Kafka
    cannot remove partitions). Only a count fetched within `refresh_s` can prove a header out of range.

    Whenever the count is unknown or older than `refresh_s`, a background fetch is started: at most one
    in flight and at most one started per `refresh_s`, so a flood of bad headers cannot turn into a
    metadata request per record, and an operator who adds reply partitions is picked up without a
    restart. Async records never get here (`ReplyPublisher.route`), so an async-only worker never asks
    for a `reply` topic's metadata. `spawn` runs the fetch; the default is a daemon thread. `prime()` is
    the one synchronous fetch, for start-up.
    """

    __slots__ = (
        "_attempted_at",
        "_clock",
        "_count",
        "_fetched_at",
        "_in_flight",
        "_list_topics",
        "_lock",
        "_refresh_s",
        "_spawn",
        "_topic",
    )

    def __init__(
        self,
        list_topics: Callable[..., Any],
        topic: str,
        *,
        refresh_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        spawn: Callable[[Callable[[], None]], None] = _spawn_daemon,
    ) -> None:
        self._list_topics = list_topics
        self._topic = topic
        self._refresh_s = refresh_s
        self._clock = clock
        self._spawn = spawn
        self._lock = threading.Lock()
        self._count: int | None = None
        self._fetched_at: float | None = None
        self._attempted_at: float | None = None
        self._in_flight = False

    def __call__(self, wanted: int | None = None) -> int | None:
        now = self._clock()
        with self._lock:
            count, fetched_at = self._count, self._fetched_at
        fresh = count is not None and fetched_at is not None and now - fetched_at < self._refresh_s
        if not fresh:
            self._start_fetch(now)
        if count is not None and (fresh or wanted is None or wanted < count):
            return count
        return None  # unknown, or beyond a stale count: unverified, not invalid

    def prime(self) -> int | None:
        """Fetch once **on the caller's thread**, bounded by the 5 s metadata timeout. For start-up only,
        before any record is consumed (the worker calls it when the applied spec is sync), so the first
        records' out-of-range headers are already provable. A failure leaves the count unknown."""
        with self._lock:
            if self._in_flight:
                return self._count
            self._in_flight = True
            self._attempted_at = self._clock()
        self._fetch_and_store()
        with self._lock:
            return self._count

    def _start_fetch(self, now: float) -> None:
        with self._lock:
            if self._in_flight or (self._attempted_at is not None and now - self._attempted_at < self._refresh_s):
                return
            self._in_flight = True
            self._attempted_at = now
        try:
            self._spawn(self._fetch_and_store)
        except Exception:  # no thread: stay unverified and try again after refresh_s
            with self._lock:
                self._in_flight = False

    def _fetch_and_store(self) -> None:
        count: int | None = None
        try:
            count = self._fetch()
        finally:
            with self._lock:
                if count is not None:
                    self._count = count
                    self._fetched_at = self._clock()
                self._in_flight = False

    def _fetch(self) -> int | None:
        try:
            metadata = self._list_topics(topic=self._topic, timeout=5.0)
            topic = metadata.topics.get(self._topic)
        except Exception:  # metadata is best effort: unknown leaves routes unverified, never invalid
            return None
        if topic is None or getattr(topic, "error", None) is not None or not topic.partitions:
            return None
        return len(topic.partitions)


# -- status ----------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplyOutcome:
    """What the reply says: a status and, unless accepted, the code (envelope 3 `tb-error-code` rule)."""

    status: ReplyStatus
    error_code: str | None = None

    def __post_init__(self) -> None:
        if (self.status == "accepted") != (self.error_code is None):
            raise ValueError("an accepted reply carries no code; a rejected or failed one needs one")

    @classmethod
    def failure(cls, error_code: str, *, rejected: bool) -> ReplyOutcome:
        return cls("rejected" if rejected else "failed", error_code)


ACCEPTED = ReplyOutcome("accepted")


@dataclass(slots=True)
class FailureLedger:
    """ "First failure in spec order": pipeline steps (rank 0), then destinations in list order.

    Ranks, not arrival order, so the answer does not depend on which branch happened to run first.
    """

    _first: tuple[int, ReplyOutcome] | None = None

    def record(self, rank: int, outcome: ReplyOutcome) -> None:
        if outcome.status == "accepted":
            return
        if self._first is None or rank < self._first[0]:
            self._first = (rank, outcome)

    def outcome(self) -> ReplyOutcome:
        return ACCEPTED if self._first is None else self._first[1]


# -- the record ------------------------------------------------------------------------------------


def reply_message(
    request: Message,
    route: ReplyRoute,
    outcome: ReplyOutcome,
    *,
    channel: str,
    channel_version: int,
    body: ReplyBody | None,
) -> Message:
    """The `reply` record for one request: keyed by `tb-correlation-id`, never by the request's key.

    Built from registered headers only. `tb-usr-*` and the routing headers are not copied (envelope 6
    rule 7), and `tb-received-at` is copied unchanged (rule 1).
    """
    headers = {
        h.TB_MSG_ID: request.require(h.TB_MSG_ID),
        h.TB_CHANNEL: channel,
        h.TB_CHANNEL_VERSION: str(channel_version),
        h.TB_RECEIVED_AT: request.require(h.TB_RECEIVED_AT),
        h.TB_CONTENT_TYPE: body.content_type if body is not None else EMPTY_REPLY_CONTENT_TYPE,
        h.TB_CORRELATION_ID: route.correlation_id,
        h.TB_REPLY_STATUS: outcome.status,
        h.TB_STAGE: "replied",
        h.TRACEPARENT: child_traceparent(request.traceparent or new_traceparent()),
    }
    if outcome.error_code is not None:
        headers[h.TB_ERROR_CODE] = outcome.error_code
    return Message(
        headers=headers,
        payload=body.body if body is not None else b"",
        key=route.correlation_id.encode("utf-8"),
    )


# -- the publisher ---------------------------------------------------------------------------------


@dataclass(slots=True)
class ReplyStats:
    """Counters for the worker's shutdown line. Metrics export waits on `contracts/observability/v0`."""

    produced: int = 0
    delivered: int = 0
    failed: int = 0
    expired: int = 0
    skipped: int = 0
    route_invalid: int = 0
    body_missing: int = 0
    body_discarded: int = 0
    by_status: dict[str, int] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(UTC)


class ReplyPublisher:
    """Produces reply records to `<env>.<channel>.reply`, fail-fast, and reports them as `replied` events.

    `producer` is the worker's existing idempotent `acks=all` producer (a weaker `acks` needs a
    measurement and a ruling first, channel-spec 3.2.6). Delivery callbacks are served whenever that
    producer is polled or flushed — by the next record's produce or flush, or by `poll()` / `close()` —
    so a `replied` event can lag an idle worker's last reply. The reply itself does not wait for that:
    librdkafka sends it in the background.
    """

    __slots__ = ("_emitter", "_instance", "_log", "_now", "_partition_count", "_producer", "_topic", "stats")

    def __init__(
        self,
        producer: KafkaProducerLike,
        topic: str,
        *,
        emitter: EventEmitter,
        log: Logger,
        instance: str,
        partition_count: Callable[[int | None], int | None],
        now: Callable[[], datetime] = utc_now,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._emitter = emitter
        self._log = log
        self._instance = instance
        self._partition_count = partition_count
        self._now = now
        self.stats = ReplyStats()

    @property
    def topic(self) -> str:
        return self._topic

    def route(self, message: Message) -> ReplyRoute | RouteInvalid | None:
        """The record's route, checked against the real `reply` partition count. Never raises."""
        if not message.has(h.TB_REPLY_PARTITION) and not message.has(h.TB_REPLY_DEADLINE):
            return None  # an async record costs no metadata lookup
        wanted: int | None
        try:
            wanted = h.parse_int(h.TB_REPLY_PARTITION, message.get(h.TB_REPLY_PARTITION))
        except HeaderError:
            wanted = None
        return parse_route(message, partition_count=self._partition_count(wanted))

    def publish(
        self,
        record: ConsumedRecord,
        route: ReplyRoute,
        outcome: ReplyOutcome,
        *,
        channel: str,
        channel_version: int,
        body: ReplyBody | None,
    ) -> bool:
        """Produce one reply. Never raises and never retries. True when it was handed to the producer."""
        if self._now() > route.deadline + SKEW_ALLOWANCE:
            # The message was processed; only the answer is skipped. Nobody is waiting for it, and after
            # an outage a backlog would otherwise flood `reply` (S3: 1,026 late replies in one restart).
            # The skip is deliberate, so its event stays `dropped` (not an error-rate event), carrying the
            # registered engine code `REPLY_EXPIRED` so it is queryable by a stable key (set 1.4, envelope
            # 2.3.2). The sender's `REPLY_TIMEOUT` is the receiver's code, never this one.
            self.stats.expired += 1
            self._emit(
                record,
                channel,
                channel_version,
                status="dropped",
                error_code=REPLY_EXPIRED,
                error_message=EXPIRED_MESSAGE,
                correlation_id=route.correlation_id,
            )
            return False
        try:
            message = reply_message(
                record.message, route, outcome, channel=channel, channel_version=channel_version, body=body
            )
            validate_for_produce(message, topic_class="reply", ordering="none")
        except (EnvelopeError, HeaderError):
            self._failed(
                record, route, channel, channel_version, TB_OUTPUT_ENVELOPE_INVALID, "reply record breaks the envelope"
            )
            return False
        try:
            self._producer.produce(
                self._topic,
                value=message.payload,
                key=message.key,
                partition=route.partition,
                headers=to_kafka_headers(message),
                on_delivery=self._on_delivery(record, route, outcome, message, channel, channel_version),
            )
        except BufferError:
            self._failed(
                record, route, channel, channel_version, PRODUCE_FAILED, "reply not queued: producer queue full"
            )
            return False
        except Exception:  # the client refused it: the reply is lost, the message is not
            self._failed(record, route, channel, channel_version, PRODUCE_FAILED, "reply refused by the Kafka client")
            return False
        self.stats.produced += 1
        self.stats.by_status[outcome.status] = self.stats.by_status.get(outcome.status, 0) + 1
        if outcome.error_code == REPLY_BODY_MISSING:
            self.stats.body_missing += 1
        self._producer.poll(0)
        return True

    def route_invalid(
        self, record: ConsumedRecord, invalid: RouteInvalid, *, channel: str, channel_version: int
    ) -> None:
        """Envelope 2.3.3: the message is processed normally; no reply; a `replied` event says why."""
        self.stats.route_invalid += 1
        self._log.warning(
            "reply_route_invalid",
            message_id=record.msg_id,
            reason=invalid.reason,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        self._emit(
            record,
            channel,
            channel_version,
            status="error",
            error_code=REPLY_ROUTE_INVALID,
            error_message="reply routing header malformed or out of range; no reply produced",
            correlation_id=record.message.get(h.TB_CORRELATION_ID),
        )

    def skipped(self) -> None:
        """A record asked for a reply that this worker's applied spec cannot give (transactional, split)."""
        self.stats.skipped += 1

    def body_discarded(self) -> None:
        """A `ctx.reply` body that will not be used (async record, `body: ack`, drop or failure)."""
        self.stats.body_discarded += 1

    def poll(self) -> None:
        """Serve pending delivery callbacks without waiting."""
        self._producer.poll(0)

    def close(self, timeout_s: float = 2.0) -> None:
        """Serve the last callbacks at shutdown, briefly. An unconfirmed reply at exit is a lost reply."""
        self._producer.flush(timeout_s)
        self._log.info(
            "reply_stats",
            topic=self._topic,
            produced=self.stats.produced,
            delivered=self.stats.delivered,
            failed=self.stats.failed,
            expired=self.stats.expired,
            skipped=self.stats.skipped,
            route_invalid=self.stats.route_invalid,
            body_missing=self.stats.body_missing,
            body_discarded=self.stats.body_discarded,
        )

    # -- internals ---------------------------------------------------------------------------------

    def _on_delivery(
        self,
        record: ConsumedRecord,
        route: ReplyRoute,
        outcome: ReplyOutcome,
        message: Message,
        channel: str,
        channel_version: int,
    ) -> Callable[[Any, Any], None]:
        def delivered(err: Any, _msg: Any) -> None:
            if err is not None:
                self._failed(
                    record,
                    route,
                    channel,
                    channel_version,
                    PRODUCE_FAILED,
                    "reply delivery failed",
                    kafka_error=_error_name(err),
                )
                return
            self.stats.delivered += 1
            reply_failed = outcome.error_code in _REPLY_STAGE_FAILURES
            self._emit(
                record,
                channel,
                channel_version,
                status="error" if reply_failed else "ok",
                error_code=outcome.error_code,
                error_message="reply produced without the step body it required" if reply_failed else None,
                correlation_id=route.correlation_id,
                content_type=message.get(h.TB_CONTENT_TYPE),
                size_bytes=message.payload_size,
                traceparent=message.traceparent,
            )

        return delivered

    def _failed(
        self,
        record: ConsumedRecord,
        route: ReplyRoute,
        channel: str,
        channel_version: int,
        error_code: str,
        reason: str,
        *,
        kafka_error: str | None = None,
    ) -> None:
        self.stats.failed += 1
        self._log.warning(
            "reply_failed",
            message_id=record.msg_id,
            error_code=error_code,
            kafka_error=kafka_error,
            reply_partition=route.partition,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        self._emit(
            record,
            channel,
            channel_version,
            status="error",
            error_code=error_code,
            error_message=reason,
            correlation_id=route.correlation_id,
        )

    def _emit(
        self,
        record: ConsumedRecord,
        channel: str,
        channel_version: int,
        *,
        status: Literal["ok", "error", "dropped"],
        error_code: str | None,
        error_message: str | None,
        correlation_id: str | None,
        content_type: str | None = None,
        size_bytes: int | None = None,
        traceparent: str | None = None,
    ) -> None:
        message_id = record.msg_id
        if message_id is None:  # pragma: no cover - a record without an id never reaches the handler
            return
        self._emitter.emit(
            MessageEvent(
                stage="replied",
                status=status,
                channel=channel,
                channel_version=channel_version,
                instance=self._instance,
                message_id=message_id,
                attempt=record.message.attempt,
                content_type=content_type,
                size_bytes=size_bytes,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error_code=error_code,
                error_message=error_message,
                traceparent=traceparent or record.message.traceparent,
                parent_id=record.message.get(h.TB_PARENT_ID),
                correlation_id=correlation_id
                if correlation_id and h.is_well_formed(h.TB_CORRELATION_ID, correlation_id)
                else None,
            )
        )


def _error_name(err: Any) -> str:
    """librdkafka's error *name* (`_MSG_TIMED_OUT`), a fixed token. Never `str(err)`, which is free text."""
    name = getattr(err, "name", None)
    try:
        text = name() if callable(name) else None
    except Exception:  # pragma: no cover - a broken error object must not break the callback
        text = None
    return text if isinstance(text, str) and text.replace("_", "").isalnum() else "unknown"
