"""The pipeline worker: `<env>.<channel>.raw` -> steps in process -> `<env>.<channel>.out.<dest>`.

One Kafka hop in, one per destination out, and everything between them in memory (invariant 2). The
worker commits its input offset only after every `out.<dest>` record has a delivery callback, so a
crash mid-flight replays rather than drops (invariants 1 and 4).

`pipeline.transactional: true` (ADR-003 D6) makes each commit window one Kafka transaction: the
`out.<dest>` and DLQ records and the `raw` offsets commit atomically (`transactions.py`). The flag is
read at startup, because the producer's transactional identity is fixed at construction; a hot reload
that flips it is logged and takes effect at the next restart.

Hot reload without a control plane: the channel-spec cache is polled once per message, so a new
`metadata.version` published to `_tb.<env>.channels` takes effect on the next record. PostgreSQL and
the API can be down throughout (invariant 3).
"""

from __future__ import annotations

import os
import signal
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from tributary.sdk import Drop, Message
from tributary.sdk import headers as h
from tributary.sdk.context import Logger
from tributary.sdk.ids import child_traceparent, new_traceparent, uuid7
from tributary.sdk.projection import Ordering, TopicClass

from .channels import ChannelsConsumerLike, LiveSpecSource, open_spec_source
from .clients import KafkaSettings, consumer_config, events_producer_config, max_poll_interval_ms, producer_config
from .config import TopicDefaults, load_topic_defaults
from .consumer import ConsumerLoop, KafkaConsumerLike, LoopSettings, LoopStats
from .context import WorkerContext
from .errors import ChannelSpecError, DeliveryError, PartitionMismatchError, RuntimeConfigurationError, safe_token
from .events import EventEmitter, MessageEvent, NullEmitter
from .executor import ChannelPlan, StepFailure, StepStats, run_splitter
from .logs import JsonLogger
from .producer import KafkaEventEmitter, KafkaProducerLike, RecordProducer
from .records import RECORD_UNDECODABLE, ConsumedRecord, quarantine_blob
from .spec import ChannelSpec, RetryPolicy
from .steps import Node, Resolver, build_plan, load_step
from .topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic, raw_topic
from .transactions import (
    GroupMetadataSource,
    TransactionalCommitter,
    TransactionalProducerLike,
    transactional_id,
    transactional_producer_config,
)

#: Retry policy for the produce half of the pipeline worker. The channel spec carries a policy per
#: destination, which governs the loader; a failure to produce to `out.<dest>` is a Kafka-side failure
#: with no destination to ask. JUDGEMENT, unmeasured: halting preserves order and never drops, which is
#: the safe direction when the broker itself is the thing that is unwell.
PRODUCE_RETRY = RetryPolicy(policy="blocking", max_attempts=10, on_exhaust="halt-partition", max_delay_ms=30_000)


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    """Everything a worker needs that does not come from the channel spec."""

    env: str
    channel: str
    kafka: KafkaSettings
    instance_id: str
    #: The `topic-defaults.json` profile this deployment runs. Defaults to `lite`, which is the profile
    #: that requires the warning: a component that defaulted to `production` would stay silent on a
    #: single-broker cluster, which is the one case the warning exists for.
    profile: str = "lite"
    contracts_dir: str | None = None
    poll_timeout_s: float = 0.2
    flush_timeout_s: float = 30.0
    shadow: str | None = None

    @property
    def instance(self) -> str:
        """The `instance` field of every event: `<component>:<instance>@<host>` (events contract 2)."""
        import socket

        return f"worker:{self.instance_id}@{socket.gethostname()}"


def emit_profile_warning(log: Logger, defaults: TopicDefaults, *, profile: str) -> bool:
    """Emit the profile's mandatory startup warning. Returns True when one was emitted.

    `contracts/topics/v0` README 3.8: any component started against a `lite`-profile cluster MUST log
    this at WARN level, on by default, **with no flag to suppress it** — so there is no parameter here
    that turns it off, only the profile itself. RF=1 with `min.insync.replicas=1` cannot guarantee
    RPO=0 for an acknowledged message, which is the one promise invariant 1 makes, and `lite` is a
    human-approved exception to it (ADR-002 D14) rather than a quiet weakening.

    The text comes from the contract file, not from a literal here: rewording it would drift from the
    sentence a human actually approved.
    """
    warning = defaults.startup_warning(profile)
    if warning is None:
        return False
    log.warning(
        "lite_profile_active" if profile == "lite" else "profile_warning",
        profile=profile,
        production_use="barred" if defaults.barred_from_production(profile) else "permitted",
        detail=warning[:256],
    )
    return True


class KafkaDlqWriter:
    """Writes dead letters to `<env>.<channel>.dlq` and waits for their delivery.

    The wait is the point: the loop commits the input offset right after this returns, so a DLQ record
    still sitting in a producer buffer would be a lost message wearing the costume of a handled one.
    """

    __slots__ = ("_flush_timeout_s", "_log", "_producer", "_source", "_topic")

    def __init__(
        self,
        producer: RecordProducer,
        topic: str,
        *,
        log: Logger,
        source: str,
        flush_timeout_s: float = 30.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._log = log
        self._source = source
        self._flush_timeout_s = flush_timeout_s

    def dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str) -> None:
        message = dlq_message(record.message, error_code=error_code, stage=stage)
        self._producer.produce(self._topic, message, topic_class="dlq", ordering="none")
        self._producer.flush_pending(self._flush_timeout_s)
        self._log.warning(
            "dead_lettered",
            message_id=record.msg_id,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            error_code=error_code,
            stage=stage,
        )

    def quarantine(self, raw: Any, *, channel: str, channel_version: int) -> str:
        """Wrap a record that could not be decoded and write it to the DLQ. Returns the minted id.

        Envelope contract 5.2: the quarantined record is a **new** record the engine constructs, not
        the original moved — a `dlq` record needs the whole core header set, which is exactly the bytes
        that could not be read. The original bytes go in the payload as an opaque blob.

        **Nothing from the record reaches a log line here or in the caller** — not a value, not a header
        *name*, not a `repr`. Under mis-framing a header name is payload, so an HL7 segment or a patient
        name can be sitting in a name slot; the contract calls logging any of it a Critical finding.
        """
        wrapper = quarantine_message(
            blob=quarantine_blob(key=raw.key(), value=raw.value(), headers=raw.headers() or []),
            channel=channel,
            channel_version=channel_version,
            source=self._source,
        )
        self._producer.produce(self._topic, wrapper, topic_class="dlq", ordering="none")
        self._producer.flush_pending(self._flush_timeout_s)
        return wrapper.require(h.TB_MSG_ID)


def quarantine_message(
    *,
    blob: bytes,
    channel: str,
    channel_version: int,
    source: str,
) -> Message:
    """The quarantine wrapper for an undecodable record (envelope contract 5.2).

    Every header comes from what the engine knows — the subscription, the applied spec, this component —
    and never from the record. `tb-msg-id` is a **fresh** UUIDv7: a value recovered from the broken
    record is untrusted input, and a forged id would collide with a real message in the browser and in
    every loader's idempotency check.

    `tb-received-at` is the quarantine time, a deliberate deviation from "set once at receive" because
    the original receive time is unknowable — so end-to-end latency for this record is meaningless and
    must not be computed.

    `tb-stage` is always `quarantined`, never the stage that read the record: the record never reached
    `queued` or `pending`, so claiming it did would be false rather than merely inconsistent. Where it
    was read from is carried precisely by `tb-source` and the event's topic/partition/offset, which is
    strictly more information than a stage name.
    """
    msg_id = uuid7()
    return Message(
        headers={
            h.TB_MSG_ID: msg_id,
            h.TB_CHANNEL: channel,
            h.TB_CHANNEL_VERSION: str(channel_version),
            h.TB_SOURCE: source,
            h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
            h.TB_CONTENT_TYPE: "application/octet-stream",
            h.TB_ATTEMPT: "0",
            h.TB_STAGE: "quarantined",
            h.TB_ERROR_CODE: RECORD_UNDECODABLE,
            h.TRACEPARENT: new_traceparent(),
        },
        payload=blob,
        key=msg_id.encode("utf-8"),
    )


def dlq_message(message: Message, *, error_code: str, stage: str) -> Message:
    """The DLQ projection of a failed message: keyed by `tb-msg-id`, carrying the failing stage and code.

    The original payload is kept intact so the record can be reprocessed after a code fix (ADR-003 D4),
    and the record carries a **code, never exception text** — parser messages quote the offending field
    value, which is PHI.
    """
    msg_id = message.require(h.TB_MSG_ID)
    return (
        message.without_experimental()
        .with_header(h.TB_STAGE, stage)
        .with_header(h.TB_ERROR_CODE, error_code)
        .without_header(h.TB_RETRY_NOT_BEFORE)
        .with_key(msg_id.encode("utf-8"))
    )


def out_message(message: Message, destination: str, *, usr: dict[str, str]) -> Message:
    """The `out.<dest>` projection: destination named, attempt reset, a new span, same ordering key.

    `tb-attempt` resets to 0 because the record is entering a new stage (envelope contract 6 rule 4),
    and the key is untouched: `out` has the same partition count as `raw`, so keeping the key is what
    keeps one patient's messages on one partition (invariant 5).
    """
    result = (
        # tb-x-* is tolerated inbound and never handed on: the engine must not produce one
        # (envelope contract 2.1 against 1.2).
        message.without_experimental()
        .with_header(h.TB_DESTINATION, destination)
        .with_header(h.TB_STAGE, "routed")
        .with_attempt(0)
        .with_header(h.TRACEPARENT, child_traceparent(message.traceparent or new_traceparent()))
    )
    for name, value in usr.items():
        result = result.with_usr(name, value)
    return result


@dataclass(slots=True)
class _Routed:
    """One record produced to `out.<dest>`, remembered until the flush makes it true."""

    subject: Message
    destination: str
    attrs: dict[str, str]


@dataclass(slots=True)
class _Pending:
    """Events that describe what a flush made durable. Emitted only after the flush succeeds."""

    routed: list[_Routed] = field(default_factory=list)
    dead: list[tuple[Message, str | None, str]] = field(default_factory=list)


class PipelineHandler:
    """Runs the channel's executor tree and produces the result to every destination.

    The tree (README 3.3.2 and 3.5): `head` steps on the consumed message; an optional splitter; `tail`
    steps per child; then per destination, that destination's steps on its own copy. What fails where
    decides what is dead-lettered:

    | Fails in | Dead-lettered | `dlq` payload | Other branches |
    |---|---|---|---|
    | head or splitter | the consumed record (by the loop) | the raw payload | none run |
    | tail, for one child | that child | the child as the splitter produced it | continue |
    | a destination's steps | that message for that destination, `tb-destination` set | the destination steps' input | continue |

    `Reject` fails at once; any other exception after `pipeline.stepRetries` in-place retries. The
    consumed offset is committed by the loop only after this returns, and this returns only after every
    `out` and `dlq` record it queued has been flushed (or, in a transaction, joined it), so "every child
    and destination produced or dead-lettered" holds before the commit.
    """

    def __init__(
        self,
        *,
        env: str,
        spec_provider: Callable[[], ChannelSpec],
        producer: RecordProducer,
        emitter: EventEmitter,
        log: Logger,
        instance: str,
        steps_override: tuple[Node, ...] | None = None,
        resolve: Resolver = load_step,
        flush_timeout_s: float = 30.0,
        transactional: bool = False,
        stats_interval_s: float = 60.0,
    ) -> None:
        self._env = env
        self._transactional = transactional
        self._warned_version: int | None = None
        self._spec_provider = spec_provider
        self._producer = producer
        self._emitter = emitter
        self._log = log
        self._instance = instance
        self._steps_override = steps_override
        self._resolve = resolve
        self._flush_timeout_s = flush_timeout_s
        self._plan: ChannelPlan | None = None
        self._plan_spec: ChannelSpec | None = None
        self._refused_version: int | None = None
        self._stats_interval_s = stats_interval_s
        self._stats_logged_at = time.monotonic()
        self.stats = StepStats()
        self.dead_lettered = 0
        self.filtered = 0

    # -- plan ------------------------------------------------------------------------------------

    def prepare(self, spec: ChannelSpec) -> ChannelPlan:
        """Build the plan for `spec` now. At startup this is what makes a bad `with` stop the worker
        before it consumes anything, rather than on the first message."""
        plan = build_plan(spec, resolve=self._resolve, pipeline_override=self._steps_override, stats=self.stats)
        self._plan, self._plan_spec = plan, spec
        return plan

    def _current(self) -> tuple[ChannelSpec, ChannelPlan]:
        """The plan for the latest spec, or the previous plan when the latest one is refused.

        Channel-spec README 3.3: a binding failure is refused loudly at load "and the previous spec keeps
        running". Logged once per refused version, with the step name and key names only.
        """
        spec = self._spec_provider()
        plan, plan_spec = self._plan, self._plan_spec
        if plan is not None and plan_spec is not None and plan.version == spec.version:
            return plan_spec, plan
        if plan is not None and plan_spec is not None and self._refused_version == spec.version:
            return plan_spec, plan
        try:
            return spec, self.prepare(spec)
        except ChannelSpecError as exc:
            if plan is None or plan_spec is None:
                raise
            self._refused_version = spec.version
            self._log.error(
                "channel_spec_refused",
                channel=spec.name,
                channel_version=spec.version,
                running_version=plan.version,
                reason=str(exc)[:240].replace("\n", " "),
            )
            return plan_spec, plan

    # -- one record ------------------------------------------------------------------------------

    def handle(self, record: ConsumedRecord) -> None:
        spec, plan = self._current()
        if spec.pipeline.transactional != self._transactional and self._warned_version != spec.version:
            # The producer's transactional identity is fixed at construction: say so, once per version.
            self._warned_version = spec.version
            self._log.warning(
                "transactional_change_needs_restart",
                channel=spec.name,
                channel_version=spec.version,
                running_transactional=self._transactional,
            )
        message = record.message
        ctx = WorkerContext(
            channel=spec.name,
            channel_version=spec.version,
            instance=self._instance,
            log=self._log,
            attempt=message.attempt,
            traceparent=message.traceparent,
        )
        # A StepFailure here propagates: the loop dead-letters the consumed record (stage transformed).
        result = plan.head.run(message, ctx)
        if result.drop is not None:
            self.filtered += 1
            self._emit(
                spec, record, message, stage="filtered", status="dropped", ctx=ctx, error_message=result.drop.reason
            )
            return
        transformed = result.message
        if transformed is None:  # pragma: no cover - ExecutionResult always carries one or the other
            raise DeliveryError("PIPELINE_EMPTY_RESULT", retryable=False)

        branches: list[tuple[Message, WorkerContext]]
        if plan.splitter is None:
            branches = [(transformed, ctx)]
            duration_ms = result.duration_ms
        else:
            started = time.perf_counter()
            split = run_splitter(plan.splitter, transformed, ctx, step_retries=plan.step_retries, stats=plan.stats)
            duration_ms = result.duration_ms + (time.perf_counter() - started) * 1000
            if isinstance(split, Drop):
                self.filtered += 1
                self._emit(
                    spec, record, message, stage="filtered", status="dropped", ctx=ctx, error_message=split.reason
                )
                return
            branches = [(child, ctx.fork(traceparent=child.traceparent)) for child in split]
        self._emit(
            spec,
            record,
            message,
            stage="transformed",
            status="ok",
            ctx=ctx,
            duration_ms=duration_ms,
            size_bytes=transformed.payload_size,
        )

        pending = _Pending()
        for subject, branch_ctx in branches:
            if plan.splitter is not None:
                child = self._run_child(spec, plan, record, subject, branch_ctx, pending)
                if child is None:
                    continue
                subject = child
            self._route(spec, plan, record, subject, branch_ctx, pending)

        # Commit-after-delivery: the loop commits the input offset when this returns, so every `out`
        # and `dlq` record must be acknowledged by the broker first (invariants 1 and 4). Inside a
        # transaction the commit itself flushes and fails - then aborts - if any record did not land.
        if not self._transactional:
            self._producer.flush_pending(self._flush_timeout_s)

        for subject, destination, error_code in pending.dead:
            self._emit_dead(spec, record, subject, destination=destination, error_code=error_code)
        for routed in pending.routed:
            self._emit(
                spec,
                record,
                routed.subject,
                stage="routed",
                status="ok",
                ctx=None,
                attrs=routed.attrs,
                destination=routed.destination,
            )
        self._maybe_log_stats(spec)

    def _run_child(
        self,
        spec: ChannelSpec,
        plan: ChannelPlan,
        record: ConsumedRecord,
        child: Message,
        ctx: WorkerContext,
        pending: _Pending,
    ) -> Message | None:
        """Run the post-split steps on one child. `None` when the child was filtered or dead-lettered."""
        try:
            result = plan.tail.run(child, ctx)
        except StepFailure as exc:
            # The child as the splitter produced it: the input of the subtree that failed.
            self._dead_letter(spec, child, error_code=exc.error_code, destination=None, pending=pending)
            return None
        if result.drop is not None:
            self.filtered += 1
            self._emit(
                spec, record, child, stage="filtered", status="dropped", ctx=ctx, error_message=result.drop.reason
            )
            return None
        if result.message is None:  # pragma: no cover
            return None
        self._emit(
            spec,
            record,
            result.message,
            stage="transformed",
            status="ok",
            ctx=ctx,
            duration_ms=result.duration_ms,
            size_bytes=result.message.payload_size,
        )
        return result.message

    def _route(
        self,
        spec: ChannelSpec,
        plan: ChannelPlan,
        record: ConsumedRecord,
        subject: Message,
        ctx: WorkerContext,
        pending: _Pending,
    ) -> None:
        """Produce one branch to every destination, running each destination's own steps first."""
        for destination in spec.destinations:
            executor = plan.destinations.get(destination.name)
            outgoing, usr, attrs = subject, ctx.usr_headers(), ctx.attrs()
            if executor is not None and len(executor):
                destination_ctx = ctx.fork(destination=destination.name)
                try:
                    result = executor.run(subject, destination_ctx)
                except StepFailure as exc:
                    # README 3.5: the dlq payload is the destination step's input, not the raw payload.
                    self._dead_letter(
                        spec, subject, error_code=exc.error_code, destination=destination.name, pending=pending
                    )
                    continue
                if result.drop is not None:
                    self.filtered += 1
                    self._emit(
                        spec,
                        record,
                        subject,
                        stage="filtered",
                        status="dropped",
                        ctx=destination_ctx,
                        destination=destination.name,
                        error_message=result.drop.reason,
                    )
                    continue
                if result.message is None:  # pragma: no cover
                    continue
                outgoing, usr, attrs = result.message, destination_ctx.usr_headers(), destination_ctx.attrs()
            self._produce(
                out_topic(self._env, spec.name, destination.name),
                out_message(outgoing, destination.name, usr=dict(usr)),
                topic_class="out",
                ordering=spec.ordering,
            )
            pending.routed.append(_Routed(subject=subject, destination=destination.name, attrs=dict(attrs)))

    def _dead_letter(
        self,
        spec: ChannelSpec,
        subject: Message,
        *,
        error_code: str,
        destination: str | None,
        pending: _Pending,
    ) -> None:
        """Queue a branch-scoped dead letter. Joins the same flush (or transaction) as the `out` records."""
        failed = subject if destination is None else subject.with_header(h.TB_DESTINATION, destination)
        self._produce(
            dlq_topic(self._env, spec.name),
            dlq_message(failed, error_code=error_code, stage="transformed"),
            topic_class="dlq",
            ordering="none",
        )
        pending.dead.append((subject, destination, error_code))
        self.dead_lettered += 1

    def _produce(self, topic: str, message: Message, *, topic_class: TopicClass, ordering: Ordering) -> None:
        """Produce, draining the local queue once if a large fan-out filled it.

        A split can queue thousands of records in one `handle`. `PRODUCER_QUEUE_FULL` is back-pressure,
        so wait for deliveries and try once more before handing the failure to the loop's blocking retry,
        which would otherwise re-run the whole fan-out and duplicate everything already queued.
        """
        try:
            self._producer.produce(topic, message, topic_class=topic_class, ordering=ordering)
        except DeliveryError as exc:
            if exc.error_code != "PRODUCER_QUEUE_FULL":
                raise
            self._producer.flush_pending(self._flush_timeout_s)
            self._producer.produce(topic, message, topic_class=topic_class, ordering=ordering)

    # -- events and stats ------------------------------------------------------------------------

    def _emit_dead(
        self,
        spec: ChannelSpec,
        record: ConsumedRecord,
        subject: Message,
        *,
        destination: str | None,
        error_code: str,
    ) -> None:
        self._log.warning(
            "dead_lettered",
            message_id=subject.get(h.TB_MSG_ID) or "",
            parent_id=subject.get(h.TB_PARENT_ID),
            destination=destination,
            error_code=error_code,
            stage="transformed",
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        self._emit(
            spec,
            record,
            subject,
            stage="dlq",
            status="error",
            ctx=None,
            destination=destination,
            error_code=error_code,
            error_message="routed to the dead-letter queue",
        )

    def _emit(
        self,
        spec: ChannelSpec,
        record: ConsumedRecord,
        subject: Message,
        *,
        stage: str,
        status: str,
        ctx: WorkerContext | None,
        attrs: Mapping[str, str] | None = None,
        destination: str | None = None,
        duration_ms: float | None = None,
        size_bytes: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        event_attrs = dict(attrs if attrs is not None else ctx.attrs() if ctx is not None else {})
        self._emitter.emit(
            MessageEvent(
                stage=stage,
                status=status,  # type: ignore[arg-type]  # callers pass contract statuses only
                channel=spec.name,
                channel_version=spec.version,
                instance=self._instance,
                message_id=subject.require(h.TB_MSG_ID),
                destination=destination,
                attempt=record.message.attempt,
                content_type=subject.get(h.TB_CONTENT_TYPE),
                size_bytes=size_bytes,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                duration_ms=duration_ms,
                error_code=error_code,
                error_message=error_message,
                traceparent=subject.traceparent,
                parent_id=subject.get(h.TB_PARENT_ID),
                correlation_id=subject.get(h.TB_CORRELATION_ID),
                attrs=event_attrs or None,
            )
        )

    def _maybe_log_stats(self, spec: ChannelSpec) -> None:
        now = time.monotonic()
        if now - self._stats_logged_at >= self._stats_interval_s:
            self._stats_logged_at = now
            self.stats.log_summary(self._log, channel=spec.name, channel_version=spec.version)

    def close(self) -> None:
        """Log the step timing summary once more at shutdown."""
        if self._plan_spec is not None:
            self.stats.log_summary(self._log, channel=self._plan_spec.name, channel_version=self._plan_spec.version)


def check_partition_counts(client: Any, env: str, spec: ChannelSpec, *, timeout_s: float = 10.0) -> None:
    """Refuse to start when an `out.<dest>` topic has a different partition count from `raw`.

    Only on an ordered channel, and it is a startup refusal rather than a warning: a different count
    changes the key-to-partition mapping, so two messages for one patient can be delivered by different
    workers out of order — invariant 5 violated with every setting apparently correct
    (`contracts/topics/v0` section 2).
    """
    if not spec.ordered:
        return
    metadata = client.list_topics(timeout=timeout_s)
    raw_name = raw_topic(env, spec.name)
    raw_meta = metadata.topics.get(raw_name)
    if raw_meta is None:
        raise PartitionMismatchError(f"raw topic {safe_token(raw_name)} does not exist")
    expected = len(raw_meta.partitions)
    for destination in spec.destinations:
        name = out_topic(env, spec.name, destination.name)
        out_meta = metadata.topics.get(name)
        if out_meta is None:
            raise PartitionMismatchError(f"out topic {safe_token(name)} does not exist")
        found = len(out_meta.partitions)
        if found != expected:
            raise PartitionMismatchError(
                f"{safe_token(name)} has {found} partitions and {safe_token(raw_name)} has {expected}: "
                "per-key ordering would break silently"
            )


@dataclass(slots=True)
class PipelineWorker:
    """Wires the cache, the executor, the producer and the loop together for one channel."""

    settings: WorkerSettings
    steps_override: tuple[Node, ...] | None = None
    defaults: TopicDefaults | None = None
    log: Logger = field(default_factory=lambda: JsonLogger("worker"))
    loop: ConsumerLoop | None = field(default=None, init=False)
    specs: LiveSpecSource | None = field(default=None, init=False)
    emitter: EventEmitter = field(default_factory=NullEmitter, init=False)
    handler: PipelineHandler | None = field(default=None, init=False)

    def build(self) -> ConsumerLoop:
        from confluent_kafka import Consumer, Producer

        defaults = self.defaults or load_topic_defaults(self.settings.contracts_dir)
        env, channel = self.settings.env, self.settings.channel
        # Before any client is built: a warning emitted after a failed connection is a warning nobody
        # sees on the run where it matters most.
        emit_profile_warning(self.log, defaults, profile=self.settings.profile)

        cache_consumer = cast(
            "ChannelsConsumerLike",
            Consumer(
                consumer_config(
                    self.settings.kafka,
                    defaults,
                    group_id=f"tb.{env}.{channel}.channels.{self.settings.instance_id}",
                    enable_partition_eof=True,
                )
            ),
        )
        self.specs = open_spec_source(
            channel=channel,
            consumer=cache_consumer,
            topic=channels_topic(env),
            log=self.log,
        )
        spec = self.specs.current()

        transactional = spec.pipeline.transactional
        base_config = producer_config(
            self.settings.kafka, defaults, client_id=f"tb-{env}-{channel}-{self.settings.instance_id}"
        )
        if transactional:
            base_config = transactional_producer_config(
                base_config,
                transactional_id_value=transactional_id(
                    defaults.transactional_id_template, env=env, channel=channel, instance=self.settings.instance_id
                ),
            )
        data_producer = Producer(base_config)
        producer = RecordProducer(cast("KafkaProducerLike", data_producer), log=self.log)
        check_partition_counts(data_producer, env, spec)

        self.emitter = KafkaEventEmitter(
            cast(
                "KafkaProducerLike",
                Producer(
                    events_producer_config(self.settings.kafka, client_id=f"tb-events-{self.settings.instance_id}")
                ),
            ),
            events_topic(env),
            log=self.log,
        )

        handler = PipelineHandler(
            env=env,
            spec_provider=self._current_spec,
            producer=producer,
            emitter=self.emitter,
            log=self.log,
            instance=self.settings.instance,
            steps_override=self.steps_override,
            flush_timeout_s=self.settings.flush_timeout_s,
            transactional=transactional,
        )
        # Resolve and bind every step now: a bad `with`, or a splitter on a per-key channel, stops the
        # worker before it consumes anything rather than on the first message (README 3.3, 3.3.2).
        handler.prepare(spec)
        self.handler = handler

        consumer = cast(
            "KafkaConsumerLike",
            Consumer(
                consumer_config(
                    self.settings.kafka,
                    defaults,
                    group_id=consumer_group(env, channel, "pipeline", shadow=self.settings.shadow),
                    group_instance_id=self.settings.instance_id,
                    max_poll_interval=max_poll_interval_ms(spec),
                )
            ),
        )
        committer = None
        if transactional:
            committer = TransactionalCommitter(
                cast("TransactionalProducerLike", data_producer),
                cast("GroupMetadataSource", consumer),
                log=self.log,
            )
            committer.initialise()  # fences a previous incarnation and aborts what it left open
        self.loop = ConsumerLoop(
            consumer,
            handler,
            committer=committer,
            topics=[raw_topic(env, channel)],
            retry=PRODUCE_RETRY,
            settings=LoopSettings(
                commit_every=defaults.consumer.commit_every,
                poll_timeout_s=self.settings.poll_timeout_s,
                flush_timeout_s=self.settings.flush_timeout_s,
            ),
            emitter=self.emitter,
            log=self.log,
            channel=channel,
            channel_version=spec.version,
            instance=self.settings.instance,
            dlq=KafkaDlqWriter(
                producer,
                dlq_topic(env, channel),
                log=self.log,
                source=self.settings.instance,
                flush_timeout_s=self.settings.flush_timeout_s,
            ),
        )
        self.loop.start()
        self.log.info(
            "worker_started",
            channel=channel,
            channel_version=spec.version,
            commit_every=defaults.consumer.commit_every,
            session_timeout_ms=defaults.consumer.session_timeout_ms,
            destinations=len(spec.destinations),
            transactional=transactional,
        )
        return self.loop

    def _current_spec(self) -> ChannelSpec:
        """The latest applied spec, refreshed without blocking (`LiveSpecSource`, the one reload path)."""
        if self.specs is None:
            raise RuntimeConfigurationError("worker was not built: no channel-spec source")
        return self.specs.current()

    def run(self, *, max_iterations: int | None = None) -> LoopStats:
        loop = self.loop or self.build()
        try:
            return loop.run(max_iterations=max_iterations)
        finally:
            if self.handler is not None:
                self.handler.close()
            # The loop has already committed; this only flushes the event tail, briefly.
            self.emitter.close()
            if self.specs is not None:
                self.specs.close()

    def stop(self) -> None:
        if self.loop is not None:
            self.loop.stop()


def main() -> int:
    """Entry point for the container image: configuration from the environment, SIGTERM-aware."""
    settings = WorkerSettings(
        env=os.environ.get("TB_ENV", "dev"),
        channel=os.environ["TB_CHANNEL"],
        kafka=KafkaSettings(bootstrap_servers=os.environ.get("TB_BOOTSTRAP_SERVERS", "localhost:9092")),
        instance_id=os.environ.get("TB_INSTANCE_ID", f"pipeline-{os.getpid()}"),
        profile=os.environ.get("TB_PROFILE", "lite"),
        contracts_dir=os.environ.get("TB_CONTRACTS_DIR"),
    )
    worker = PipelineWorker(settings)
    worker.build()

    def _shutdown(signum: int, _frame: object) -> None:
        # Ask the loop to finish its iteration and drain; the commit happens there, not here.
        worker.log.info("shutdown_signal", signal_number=signum)
        worker.stop()

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    started = time.monotonic()
    stats = worker.run()
    worker.log.info("worker_exit", handled=stats.handled, uptime_s=round(time.monotonic() - started, 1))
    return 0


if __name__ == "__main__":  # pragma: no cover - container entry point
    raise SystemExit(main())
