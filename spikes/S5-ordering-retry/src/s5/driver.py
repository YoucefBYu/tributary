"""The client seam: where the loader gets its consumer and producers from.

Why this exists. The blocking-retry algorithm is the part of S5 that has to be
*proved*, and proving it needs hundreds of randomised runs with injected
failures, rebalances and kills. Doing that against a real broker takes minutes
per case. So the loader takes a :class:`Driver` instead of calling
``tbcommon.clients`` directly:

* :class:`KafkaDriver` is the real thing and is what every integration run and
  every measurement uses;
* :class:`s5.fakekafka.FakeCluster` is an in-process model of the librdkafka
  behaviours that the algorithm depends on, used by the fast property tests.

The seam is deliberately thin — it is a factory, not an abstraction layer. The
loader still speaks librdkafka's vocabulary (``pause``/``resume``/``seek``/
``store_offsets``/``commit``), because those semantics are exactly what is
under test. A model that "fixed" them would prove nothing.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Protocol

from confluent_kafka import TopicPartition
from tbcommon import clients

RebalanceHook = Callable[[Any, list[TopicPartition]], None]


class MessageLike(Protocol):
    """Subset of ``confluent_kafka.Message`` the loader uses."""

    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> list[tuple[str, bytes]] | None: ...
    def error(self) -> Any: ...


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...
    def poll(self, timeout: float = ...) -> int: ...


class ConsumerLike(Protocol):
    def poll(self, timeout: float = ...) -> MessageLike | None: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    def store_offsets(self, message: Any = ..., offsets: Any = ...) -> None: ...
    def commit(self, **kwargs: Any) -> Any: ...
    def assignment(self) -> list[TopicPartition]: ...
    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...
    def committed(self, partitions: list[TopicPartition], **kwargs: Any) -> list[TopicPartition]: ...
    def close(self) -> None: ...


class Driver(Protocol):
    """Factory for the clients a stage needs."""

    def consumer(
        self,
        group: str,
        topics: Sequence[str],
        *,
        instance_id: str | None = None,
        on_assign: RebalanceHook | None = None,
        on_revoke: RebalanceHook | None = None,
        on_lost: RebalanceHook | None = None,
        **overrides: Any,
    ) -> ConsumerLike: ...

    def producer(self, **overrides: Any) -> ProducerLike: ...


class KafkaDriver:
    """The real driver: `tbcommon.clients` with the ADR-mandated settings."""

    def consumer(
        self,
        group: str,
        topics: Sequence[str],
        *,
        instance_id: str | None = None,
        on_assign: RebalanceHook | None = None,
        on_revoke: RebalanceHook | None = None,
        on_lost: RebalanceHook | None = None,
        **overrides: Any,
    ) -> ConsumerLike:
        return clients.make_consumer(
            group,
            topics,
            instance_id=instance_id,
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
            **overrides,
        )

    def producer(self, **overrides: Any) -> ProducerLike:
        return clients.make_producer(**overrides)
