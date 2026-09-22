"""The pipeline worker: `<env>.<channel>.raw` -> steps in process -> `<env>.<channel>.out.<dest>`.

One Kafka hop in, one per destination out, and everything between them in memory (invariant 2). The
worker commits its input offset only after every `out.<dest>` record has a delivery callback, so a
crash mid-flight replays rather than drops (invariants 1 and 4).

Hot reload without a control plane: the channel-spec cache is polled once per message, so a new
`metadata.version` published to `_tb.<env>.channels` takes effect on the next record. PostgreSQL and
the API can be down throughout (invariant 3).
"""

from __future__ import annotations

import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from tributary.sdk import Message, Step
from tributary.sdk import headers as h
from tributary.sdk.context import Logger
from tributary.sdk.ids import child_traceparent, new_traceparent, uuid7

from .channels import ChannelCache, ChannelsConsumerLike
from .clients import KafkaSettings, consumer_config, events_producer_config, max_poll_interval_ms, producer_config
from .config import TopicDefaults, load_topic_defaults
from .consumer import ConsumerLoop, KafkaConsumerLike, LoopSettings, LoopStats
from .context import WorkerContext
from .errors import DeliveryError, PartitionMismatchError, RuntimeConfigurationError, safe_token
from .events import EventEmitter, MessageEvent, NullEmitter
from .executor import PipelineExecutor
from .logs import JsonLogger
from .producer import KafkaEventEmitter, KafkaProducerLike, RecordProducer
from .records import RECORD_UNDECODABLE, ConsumedRecord, quarantine_blob
from .spec import ChannelSpec, RetryPolicy
from .steps import load_steps
from .topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic, raw_topic

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
    contracts_dir: str | None = None
    poll_timeout_s: float = 0.2
    flush_timeout_s: float = 30.0
    shadow: str | None = None

    @property
    def instance(self) -> str:
        """The `instance` field of every event: `<component>:<instance>@<host>` (events contract 2)."""
        import socket

        return f"worker:{self.instance_id}@{socket.gethostname()}"


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


class PipelineHandler:
    """Runs the channel's steps and produces the result to every destination."""

    def __init__(
        self,
        *,
        env: str,
        spec_provider: Callable[[], ChannelSpec],
        producer: RecordProducer,
        emitter: EventEmitter,
        log: Logger,
        instance: str,
        steps_override: tuple[Step, ...] | None = None,
        flush_timeout_s: float = 30.0,
    ) -> None:
        self._env = env
        self._spec_provider = spec_provider
        self._producer = producer
        self._emitter = emitter
        self._log = log
        self._instance = instance
        self._steps_override = steps_override
        self._flush_timeout_s = flush_timeout_s
        self._executors: dict[int, PipelineExecutor] = {}

    def _executor(self, spec: ChannelSpec) -> PipelineExecutor:
        """One executor per channel version, so a hot reload swaps the steps and nothing else."""
        cached = self._executors.get(spec.version)
        if cached is None:
            steps = self._steps_override if self._steps_override is not None else load_steps(spec.pipeline)
            cached = PipelineExecutor(steps, step_retries=spec.pipeline.step_retries)
            self._executors = {spec.version: cached}  # drop older versions; only one is ever current
        return cached

    def handle(self, record: ConsumedRecord) -> None:
        spec = self._spec_provider()
        ctx = WorkerContext(
            channel=spec.name,
            channel_version=spec.version,
            instance=self._instance,
            log=self._log,
            attempt=record.message.attempt,
            traceparent=record.message.traceparent,
        )
        result = self._executor(spec).run(record.message, ctx)

        if result.dropped and result.drop is not None:
            self._emit(spec, record, stage="filtered", status="dropped", ctx=ctx, error_message=result.drop.reason)
            return

        transformed = result.message
        if transformed is None:  # pragma: no cover - ExecutionResult always carries one or the other
            raise DeliveryError("PIPELINE_EMPTY_RESULT", retryable=False)
        self._emit(
            spec,
            record,
            stage="transformed",
            status="ok",
            ctx=ctx,
            duration_ms=result.duration_ms,
            size_bytes=transformed.payload_size,
        )

        usr = dict(ctx.usr_headers())
        for destination in spec.destinations:
            outgoing = out_message(transformed, destination.name, usr=usr)
            self._producer.produce(
                out_topic(self._env, spec.name, destination.name),
                outgoing,
                topic_class="out",
                ordering=spec.ordering,
            )

        # Commit-after-delivery: the loop commits the input offset when this returns, so every
        # destination record must be acknowledged by the broker first (invariants 1 and 4).
        self._producer.flush_pending(self._flush_timeout_s)

        for destination in spec.destinations:
            self._emit(spec, record, stage="routed", status="ok", ctx=ctx, destination=destination.name)

    def _emit(
        self,
        spec: ChannelSpec,
        record: ConsumedRecord,
        *,
        stage: str,
        status: str,
        ctx: WorkerContext,
        destination: str | None = None,
        duration_ms: float | None = None,
        size_bytes: int | None = None,
        error_message: str | None = None,
    ) -> None:
        self._emitter.emit(
            MessageEvent(
                stage=stage,
                status=status,  # type: ignore[arg-type]  # callers pass contract statuses only
                channel=spec.name,
                channel_version=spec.version,
                instance=self._instance,
                message_id=record.message.require(h.TB_MSG_ID),
                destination=destination,
                attempt=record.message.attempt,
                content_type=record.message.get(h.TB_CONTENT_TYPE),
                size_bytes=size_bytes,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                duration_ms=duration_ms,
                error_message=error_message,
                traceparent=record.message.traceparent,
                parent_id=record.message.get(h.TB_PARENT_ID),
                correlation_id=record.message.get(h.TB_CORRELATION_ID),
                attrs=dict(ctx.attrs()) or None,
            )
        )


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
    steps_override: tuple[Step, ...] | None = None
    defaults: TopicDefaults | None = None
    log: Logger = field(default_factory=lambda: JsonLogger("worker"))
    loop: ConsumerLoop | None = field(default=None, init=False)
    cache: ChannelCache | None = field(default=None, init=False)
    emitter: EventEmitter = field(default_factory=NullEmitter, init=False)

    def build(self) -> ConsumerLoop:
        from confluent_kafka import Consumer, Producer

        defaults = self.defaults or load_topic_defaults(self.settings.contracts_dir)
        env, channel = self.settings.env, self.settings.channel

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
        self.cache = ChannelCache(cache_consumer, channels_topic(env), log=self.log)
        self.cache.start()
        self.cache.catch_up()
        spec = self.cache.require(channel)

        data_producer = Producer(
            producer_config(self.settings.kafka, defaults, client_id=f"tb-{env}-{channel}-{self.settings.instance_id}")
        )
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
        )

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
        self.loop = ConsumerLoop(
            consumer,
            handler,
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
        )
        return self.loop

    def _current_spec(self) -> ChannelSpec:
        """The latest applied spec, refreshed without blocking: this is the hot-reload path."""
        if self.cache is None:
            raise RuntimeConfigurationError("worker was not built: no channel-spec cache")
        self.cache.poll_once(timeout_s=0.0)
        return self.cache.require(self.settings.channel)

    def run(self, *, max_iterations: int | None = None) -> LoopStats:
        loop = self.loop or self.build()
        try:
            return loop.run(max_iterations=max_iterations)
        finally:
            # The loop has already committed; this only flushes the event tail, briefly.
            self.emitter.close()
            if self.cache is not None:
                self.cache.close()

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
