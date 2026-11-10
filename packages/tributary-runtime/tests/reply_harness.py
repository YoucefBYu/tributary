"""Shared doubles for the synchronous-reply tests: no Kafka, but the orderings that matter are observable.

`OrderedProducer` logs every `produce` and `flush` in one sequence, so "the reply follows every `out`
and `dlq` delivery callback" is an assertion about positions in that sequence rather than a hope. It
also records the explicit partition (the reply names one) and serves delivery callbacks the way
librdkafka does: only inside `poll()` and `flush()`, never inside `produce()`.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.reply import ReplyPublisher
from tributary.runtime.spec import ChannelSpec, StepRef
from tributary.runtime.stage_timing import StageTimer
from tributary.runtime.steps import Node
from tributary.runtime.worker import PRODUCE_RETRY, KafkaDlqWriter, PipelineHandler
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.ids import new_correlation_id, new_traceparent, uuid7
from tributary.sdk.testing import make_message

from .fakes import FakeConsumer, FakeRecord

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
EXAMPLES = CONTRACTS / "channel-spec" / "v0" / "examples" / "valid"

ENV = "dev"
CHANNEL = "adt-passthrough"
RAW = f"{ENV}.{CHANNEL}.raw"
REPLY = f"{ENV}.{CHANNEL}.reply"
DLQ = f"{ENV}.{CHANNEL}.dlq"
REPLY_PARTITIONS = 3

#: PHI stand-ins. Neither may appear in any reply header, reply key, log line or event.
KEY_MARKER = b"SYNTH-KEY-MARKER-4471"
CONTROL_ID_MARKER = "SYNTH-CTRL-MARKER-9f2a"
REQUEST_PAYLOAD = (
    f"MSH|^~\\&|TRIBUTARY-SYNTH|LAB|ENGINE|LAB|20260928091502||QBP^Q11|{CONTROL_ID_MARKER}|P|2.5.1\r"
).encode()

NOW = datetime(2026, 9, 28, 9, 15, 2, tzinfo=UTC)


class KafkaErrorStub:
    """What a failed delivery callback receives: an error with a fixed `name()`."""

    def __init__(self, name: str = "_MSG_TIMED_OUT") -> None:
        self._name = name

    def name(self) -> str:
        return self._name

    def __str__(self) -> str:  # never used by the runtime for a reply: it logs name() only
        return f"{self._name}: free text"


@dataclass
class Produced:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: dict[str, str]
    partition: int


@dataclass
class OrderedProducer:
    """A `KafkaProducerLike` that logs produce/flush order and serves callbacks on poll and flush."""

    ops: list[str] = field(default_factory=list)
    produced: list[Produced] = field(default_factory=list)
    fail_delivery_topics: set[str] = field(default_factory=set)
    refuse_topics: set[str] = field(default_factory=set)
    _pending: list[tuple[Callable[[Any, Any], None] | None, str]] = field(default_factory=list)

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: Any = None,
        headers: Any = None,
    ) -> None:
        if topic in self.refuse_topics:
            raise BufferError("queue full")
        decoded = {name: blob.decode("utf-8") for name, blob in headers or []}
        self.produced.append(Produced(topic, key, value, decoded, partition))
        self.ops.append(f"produce:{topic}")
        self._pending.append((on_delivery, topic))

    def _serve(self) -> None:
        pending, self._pending = self._pending, []
        for callback, topic in pending:
            if callback is not None:
                callback(KafkaErrorStub() if topic in self.fail_delivery_topics else None, None)

    def poll(self, timeout: float) -> int:
        self._serve()
        return 0

    def flush(self, timeout: float) -> int:
        self.ops.append("flush")
        self._serve()
        return 0

    def records(self, topic: str) -> list[Produced]:
        return [record for record in self.produced if record.topic == topic]

    def replies(self) -> list[Produced]:
        return self.records(REPLY)


def spec_document(
    *,
    reply: Mapping[str, Any] | None = None,
    destinations: Sequence[tuple[str, Sequence[str]]] = (("http-stub", ()),),
    transactional: bool = False,
    version: int = 3,
) -> dict[str, Any]:
    """The walking-skeleton spec, optionally in sync mode, with named destination steps."""
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": version}
    template = document["spec"]["destinations"][0]
    document["spec"]["destinations"] = [
        {**template, "name": name, "steps": [{"name": step, "ref": f"tests.{step}"} for step in steps]}
        for name, steps in destinations
    ]
    if reply is not None:
        document["spec"]["source"]["reply"] = dict(reply)
    if transactional:
        document["spec"]["pipeline"]["transactional"] = True
    return document


def make_spec(**kwargs: Any) -> ChannelSpec:
    return ChannelSpec.from_document(spec_document(**kwargs))


def sync_request(
    *,
    partition: str | None = "1",
    deadline: str | None = None,
    correlation_id: str | None = None,
    extra: Mapping[str, str] | None = None,
) -> Message:
    """A `raw` record as a sync-mode receiver produces it (envelope example raw-mllp-sync-request)."""
    headers: dict[str, str] = {}
    if partition is not None:
        headers[h.TB_REPLY_PARTITION] = partition
    if deadline is not None or partition is not None:
        headers[h.TB_REPLY_DEADLINE] = deadline or h.format_timestamp(NOW + timedelta(seconds=15))
    headers.update(extra or {})
    return make_message(
        REQUEST_PAYLOAD,
        key=KEY_MARKER,
        channel=CHANNEL,
        msg_id=uuid7(),
        content_type="application/hl7-v2+er7",
        traceparent=new_traceparent(),
        correlation_id=correlation_id or new_correlation_id(),
        headers=headers,
    )


@dataclass
class Rig:
    """One handler wired to one producer, one emitter and one reply publisher, plus a loop around it."""

    producer: OrderedProducer
    emitter: RecordingEmitter
    handler: PipelineHandler
    replies: ReplyPublisher
    log: io.StringIO
    stage_timer: StageTimer | None = None

    def loop(self, consumer: FakeConsumer) -> ConsumerLoop:
        record_producer = RecordProducer(self.producer, log=JsonLogger("test", stream=self.log))
        return ConsumerLoop(
            consumer,
            self.handler,
            topics=[RAW],
            retry=PRODUCE_RETRY,
            settings=LoopSettings(commit_every=1),
            emitter=self.emitter,
            log=JsonLogger("loop", stream=self.log),
            channel=CHANNEL,
            channel_version=3,
            instance="worker:test@localhost",
            dlq=KafkaDlqWriter(record_producer, DLQ, log=JsonLogger("dlq", stream=self.log), source="worker:test"),
            on_dead_lettered=self.handler.after_dead_letter,
            stage_timer=self.stage_timer,
        )

    def run(self, message: Message, *, offset: int = 0) -> ConsumerLoop:
        """Feed one record through a real `ConsumerLoop`, so head failures take the dead-letter hook."""
        consumer = FakeConsumer()
        consumer.append(FakeRecord(RAW, 0, offset, message.key, message.payload, to_raw_headers(message)))
        loop = self.loop(consumer)
        loop.run(max_iterations=1)
        return loop


def to_raw_headers(message: Message) -> list[tuple[str, bytes]]:
    return [(name, value.encode("utf-8")) for name, value in message.headers.items()]


def rig(
    *,
    channel_spec: ChannelSpec | None = None,
    steps: tuple[Node, ...] | None = None,
    resolve: Mapping[str, Node] | None = None,
    transactional: bool = False,
    now: datetime = NOW,
    partitions: int | None = REPLY_PARTITIONS,
    stage_timer: StageTimer | None = None,
) -> Rig:
    producer = OrderedProducer()
    emitter = RecordingEmitter()
    stream = io.StringIO()
    replies = ReplyPublisher(
        producer,
        REPLY,
        emitter=emitter,
        log=JsonLogger("reply", stream=stream),
        instance="worker:test@localhost",
        partition_count=lambda _wanted: partitions,
        now=lambda: now,
    )
    chosen = channel_spec or make_spec(reply={})
    table = dict(resolve or {})

    def resolver(reference: StepRef) -> Node:
        return table[reference.name]

    handler = PipelineHandler(
        env=ENV,
        spec_provider=lambda: chosen,
        producer=RecordProducer(producer, log=JsonLogger("producer", stream=stream)),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream),
        instance="worker:test@localhost",
        steps_override=steps,
        resolve=resolver,
        transactional=transactional,
        replies=replies,
        stage_timer=stage_timer,
    )
    handler.prepare(chosen)
    return Rig(
        producer=producer, emitter=emitter, handler=handler, replies=replies, log=stream, stage_timer=stage_timer
    )
