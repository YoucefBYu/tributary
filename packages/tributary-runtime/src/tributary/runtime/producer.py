"""Producing records, and the one rule that makes invariant 1 and invariant 4 true.

**A commit follows a delivery; it never precedes one.** `produce()` only queues a record with
librdkafka; the broker has not acknowledged anything when it returns. So the worker produces, then
`flush_pending()` waits for every delivery callback, and only a clean result lets the caller commit the
input offset. Committing on `produce()` returning would acknowledge a message that is still in a
buffer, which is the "ACK and drop" invariant 1 exists to forbid.

Lifecycle events are the deliberate exception: `KafkaEventEmitter` is fire-and-forget, because
invariant 9 puts the archive off the critical path. A full queue drops the event and counts it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from tributary.sdk import Message
from tributary.sdk.context import Logger
from tributary.sdk.projection import Ordering, TopicClass, validate_for_produce

from .errors import DeliveryError
from .events import LifecycleEvent, serialize
from .records import to_kafka_headers


class KafkaProducerLike(Protocol):
    """The slice of `confluent_kafka.Producer` the runtime uses."""

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: Any = None,
        headers: Any = None,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class RecordProducer:
    """Produces envelope-validated records and reports whether they were really delivered."""

    __slots__ = ("_failures", "_log", "_producer")

    def __init__(self, producer: KafkaProducerLike, *, log: Logger) -> None:
        self._producer = producer
        self._log = log
        self._failures: list[str] = []

    def produce(
        self,
        topic: str,
        message: Message,
        *,
        topic_class: TopicClass,
        ordering: Ordering = "per-key",
    ) -> None:
        """Queue one record after checking it against the envelope contract.

        Validating here rather than at the broker means a contract violation is a refusal by this
        worker, with a code, instead of a record on a topic that every downstream consumer inherits.
        """
        validate_for_produce(message, topic_class=topic_class, ordering=ordering)
        try:
            self._producer.produce(
                topic,
                value=message.payload,
                key=message.key,
                headers=to_kafka_headers(message),
                on_delivery=self._on_delivery,
            )
        except BufferError as exc:
            # The local queue is full. Back-pressure, not a lost message: the caller retries.
            raise DeliveryError("PRODUCER_QUEUE_FULL", retryable=True) from exc
        except Exception as exc:
            raise DeliveryError("PRODUCE_REFUSED", retryable=True, detail=type(exc).__name__) from exc
        self._producer.poll(0)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            # str(err) is a librdkafka error string: a code and a topic, no record content.
            self._failures.append(str(err))

    def flush_pending(self, timeout_s: float = 30.0) -> None:
        """Wait for every queued record. Raises `DeliveryError` unless all of them were delivered."""
        remaining = self._producer.flush(timeout_s)
        if remaining > 0:
            raise DeliveryError("PRODUCE_TIMEOUT", retryable=True, detail=f"{remaining} record(s) still queued")
        if self._failures:
            count = len(self._failures)
            self._failures.clear()
            raise DeliveryError("PRODUCE_FAILED", retryable=True, detail=f"{count} delivery callback(s) failed")

    def poll(self, timeout_s: float = 0.0) -> None:
        """Serve delivery callbacks without waiting for anything."""
        self._producer.poll(timeout_s)


class KafkaEventEmitter:
    """Fire-and-forget lifecycle events on `_tb.<env>.events`.

    Nothing in message flow waits on this (invariant 9). A full queue drops the event and increments
    `dropped`; the alternative — blocking the data plane until the archive path recovers — would make
    an observability topic a delivery dependency.
    """

    __slots__ = ("_dropped", "_log", "_producer", "_topic")

    def __init__(self, producer: KafkaProducerLike, topic: str, *, log: Logger) -> None:
        self._producer = producer
        self._topic = topic
        self._log = log
        self._dropped = 0

    @property
    def dropped(self) -> int:
        return self._dropped

    def emit(self, event: LifecycleEvent) -> None:
        headers: Mapping[str, str] = {"traceparent": event.traceparent} if event.traceparent else {}
        try:
            self._producer.produce(
                self._topic,
                value=serialize(event),
                key=event.key,
                headers=[(name, value.encode("utf-8")) for name, value in headers.items()],
                on_delivery=None,
            )
            self._producer.poll(0)
        except BufferError:
            self._dropped += 1
            if self._dropped in (1, 10, 100, 1000):  # log the shape of the loss, not every instance
                self._log.warning("event_dropped", stage=event.stage, dropped_total=self._dropped)
        except Exception as exc:
            self._dropped += 1
            self._log.warning("event_emit_failed", stage=event.stage, error_type=type(exc).__name__)
