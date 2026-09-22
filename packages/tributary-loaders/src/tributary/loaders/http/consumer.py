"""`OutConsumer`: consume `<env>.<channel>.out.<destination>`, deliver, commit after delivery,
blocking retry with per-partition pause/seek/resume.

There is no `tributary-runtime` yet (W1-4) to drive this loader, so this is a wave-1 stand-in the
same way `tributary.receivers.mllp` stands in for the pipeline on the receive side: it owns its own
consumer, its own commit and its own retry loop. Agreed with `platform-engineer` (the runtime's
`out.<dest>` produce-path owner) as the target shape: consume, commit only after a successful
delivery, blocking retry preserves order by pausing and reseeking the *one* partition that is
retrying rather than the whole consumer, so unrelated partitions keep flowing.

Per message: attempt delivery once. Success -> commit, reset the attempt counter for that
partition. Failure, budget remaining -> pause that partition, sleep the backoff delay, seek back to
the same offset, resume — the next `poll()` redelivers the identical record (order preserved,
invariant 5; retried record is not a duplicate downstream because it never left this process, unlike
a sender-level resend). Budget exhausted (or a non-retryable 4xx immediately) -> `onExhaust`:
`dlq-and-continue` publishes to `<env>.<channel>.dlq` and commits past it (never drops the record: a
DLQ publish failure retries the DLQ write itself rather than committing), `halt-partition` pauses
the partition forever and does not resume it.

NOT validated against a real cluster in this wave — Phase 1's shared cluster was stopped for other
agents' work while this was written. `tests/http/test_consumer.py` exercises the retry/DLQ/halt
state machine against a fake consumer and a fake `HttpLoader`; `test_integration_http_loader.py` is
the guarded (`pytest.importorskip`), real-Kafka version, to run once `testcontainers[kafka]` is a
dependency (see the W1-5 handoff to the lead) and the cluster is available again.
"""

from __future__ import annotations

from typing import Any, Protocol

from tributary.sdk import Message
from tributary.sdk import headers as h

from ..errors import ConfigError
from .backoff import next_delay_ms
from .config import HttpDestinationConfig
from .dlq import DlqPublisher, build_dlq_envelope
from .loader import DeliveryOutcome, HttpLoader, Reason
from .retry_policy import decide

_ERROR_CODES: dict[Reason, str] = {
    "circuit-open": "CIRCUIT_OPEN",
    "timeout": "TIMEOUT",
    "transport-error": "TRANSPORT_ERROR",
    "http-5xx": "HTTP_5XX",
    "http-429": "HTTP_429",
    "http-4xx": "HTTP_4XX",
}


class KafkaMessage(Protocol):
    """The subset of `confluent_kafka.Message` this module reads. A `Protocol`, not an import, so
    unit tests can hand it a plain fake with no `confluent_kafka.Consumer` behind it."""

    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> list[tuple[str, bytes]] | None: ...
    def error(self) -> Any: ...


def message_from_kafka(msg: KafkaMessage) -> Message:
    return Message(headers=h.decode_headers(msg.headers() or []), payload=msg.value(), key=msg.key())


class OutConsumer:
    """The confluent_kafka wiring is created lazily in `run()` (or supplied via `consumer=` for
    tests), matching the lazy-import discipline the receivers package uses so unit tests never need
    a broker client installed to exercise the retry state machine."""

    def __init__(
        self,
        config: HttpDestinationConfig,
        loader: HttpLoader,
        *,
        bootstrap_servers: str | None = None,
        group_id: str = "tributary-loader",
        topic: str | None = None,
        dlq_publisher: DlqPublisher | None = None,
        consumer: Any = None,
    ) -> None:
        if config.on_exhaust == "dlq-and-continue" and dlq_publisher is None and consumer is None:
            raise ConfigError("onExhaust: dlq-and-continue needs a dlq_publisher")
        self.config = config
        self.loader = loader
        self.dlq_publisher = dlq_publisher
        self.group_id = group_id
        self.topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._consumer = consumer
        self._attempts: dict[tuple[str, int], int] = {}
        self._halted: set[tuple[str, int]] = set()
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def _build_consumer(self) -> Any:
        from confluent_kafka import Consumer

        return Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "isolation.level": "read_committed",
            }
        )

    async def run(self) -> None:
        import asyncio

        if self._consumer is None:
            self._consumer = self._build_consumer()
        if self.topic is None:
            raise ConfigError("topic is required when not supplied by a test")
        self._consumer.subscribe([self.topic])
        loop = asyncio.get_running_loop()
        if self.dlq_publisher is not None:
            self.dlq_publisher.start(loop)
        try:
            while not self._stop:
                msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
                if msg is None or msg.error():
                    continue
                await self.handle(msg)
        finally:
            self._consumer.close()
            if self.dlq_publisher is not None:
                self.dlq_publisher.close()

    async def handle(self, msg: KafkaMessage) -> None:
        """One record: deliver, then commit, retry or dispose of it. Public (not `_handle`) so unit
        tests drive it directly with a fake `KafkaMessage`, no poll loop needed."""

        from confluent_kafka import TopicPartition

        tp_key = (msg.topic(), msg.partition())
        if tp_key in self._halted:
            return  # onExhaust: halt-partition already fired for this partition; never resumed
        message = message_from_kafka(msg)
        attempt = self._attempts.get(tp_key, 0) + 1
        outcome = await self.loader.deliver(message)
        if outcome.success:
            self._attempts.pop(tp_key, None)
            self._consumer.commit(message=msg, asynchronous=False)
            return

        decision = decide(
            retryable=outcome.retryable,
            attempt=attempt,
            max_attempts=self.config.max_attempts,
            on_exhaust=self.config.on_exhaust,
        )
        tp = TopicPartition(msg.topic(), msg.partition())
        if decision == "retry":
            self._attempts[tp_key] = attempt
            delay_s = (
                next_delay_ms(
                    self.config.backoff,
                    attempt,
                    initial_delay_ms=self.config.initial_delay_ms,
                    max_delay_ms=self.config.max_delay_ms,
                )
                / 1000
            )
            await self._redeliver(tp, msg.offset(), delay_s)
            return
        if decision == "dlq":
            await self._to_dlq(tp, msg, message, outcome, attempt)
            return
        self._consumer.pause([tp])  # decision == "halt": stays paused, an operator must intervene
        self._halted.add(tp_key)

    async def _redeliver(self, tp: Any, offset: int, delay_s: float) -> None:
        import asyncio

        from confluent_kafka import TopicPartition

        self._consumer.pause([tp])
        await asyncio.sleep(delay_s)
        self._consumer.seek(TopicPartition(tp.topic, tp.partition, offset))
        self._consumer.resume([tp])

    async def _to_dlq(
        self, tp: Any, msg: KafkaMessage, message: Message, outcome: DeliveryOutcome, attempt: int
    ) -> None:
        if self.dlq_publisher is None:
            raise ConfigError("onExhaust: dlq-and-continue but no dlq_publisher is configured")
        dlq_message = build_dlq_envelope(message, error_code=_ERROR_CODES[outcome.reason], final_attempt=attempt)
        ok = await self.dlq_publisher.publish(dlq_message, timeout_s=self.config.timeout_s + 5)
        tp_key = (msg.topic(), msg.partition())
        if ok:
            self._attempts.pop(tp_key, None)
            self._consumer.commit(message=msg, asynchronous=False)
        else:
            # never drop a record: retry the DLQ write itself rather than committing past it
            await self._redeliver(tp, msg.offset(), 1.0)
