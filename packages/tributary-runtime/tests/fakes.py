"""In-memory Kafka doubles for the loop tests.

The fake consumer models the three librdkafka behaviours the loop depends on, because a fake that
models none of them would let the loop pass while doing the wrong thing:

1. **`pause()` purges the prefetch queue** and `resume()` continues from last-consumed + 1. A loop that
   pauses without seeking therefore SKIPS the failed record here, exactly as it does on a real broker —
   which is what makes `test_a_pause_without_a_seek_would_skip_the_failed_record` a real control.
2. A paused partition yields nothing from `poll()`.
3. `seek()` moves the next read position for that partition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import TopicPartition


@dataclass(frozen=True, slots=True)
class FakeError:
    _code: int
    _fatal: bool = False

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return self._fatal


@dataclass(frozen=True, slots=True)
class FakeRecord:
    """Mimics `confluent_kafka.Message`'s accessor methods."""

    _topic: str
    _partition: int
    _offset: int
    _key: bytes | None
    _value: bytes | None
    _headers: list[tuple[str, bytes]] | None
    _error: FakeError | None = None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def error(self) -> FakeError | None:
        return self._error


@dataclass
class FakeConsumer:
    """A single-partition-per-topic log with pause, resume, seek and commit."""

    log: dict[tuple[str, int], list[FakeRecord]] = field(default_factory=dict)
    position: dict[tuple[str, int], int] = field(default_factory=dict)
    paused: set[tuple[str, int]] = field(default_factory=set)
    committed: list[list[tuple[str, int, int]]] = field(default_factory=list)
    closed: bool = False
    subscribed: list[str] = field(default_factory=list)
    assigned: list[tuple[str, int]] = field(default_factory=list)
    unassigned: list[tuple[str, int]] = field(default_factory=list)
    pause_calls: list[tuple[str, int]] = field(default_factory=list)
    seek_calls: list[tuple[str, int, int]] = field(default_factory=list)
    resume_calls: list[tuple[str, int]] = field(default_factory=list)
    honour_prefetch_purge: bool = True

    def append(self, record: FakeRecord) -> None:
        self.log.setdefault((record.topic(), record.partition()), []).append(record)

    # -- consumer API ----------------------------------------------------------------------------

    def subscribe(self, topics: list[str], on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None) -> None:
        self.subscribed = list(topics)
        self.on_assign, self.on_revoke, self.on_lost = on_assign, on_revoke, on_lost

    def poll(self, timeout: float) -> FakeRecord | None:
        for key, records in self.log.items():
            if key in self.paused:
                continue
            index = self.position.get(key, 0)
            if index < len(records):
                self.position[key] = index + 1
                return records[index]
        return None

    def commit(self, offsets: list[TopicPartition] | None = None, asynchronous: bool = True) -> Any:
        self.committed.append([(tp.topic, tp.partition, tp.offset) for tp in (offsets or [])])
        return offsets

    def pause(self, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self.paused.add(key)
            self.pause_calls.append(key)
            if self.honour_prefetch_purge:
                # librdkafka drops the prefetched batch here; the read position stays where it was, so
                # a resume with no seek continues AFTER the failed record.
                pass

    def resume(self, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self.paused.discard(key)
            self.resume_calls.append(key)

    def seek(self, partition: TopicPartition) -> None:
        key = (partition.topic, partition.partition)
        self.seek_calls.append((partition.topic, partition.partition, partition.offset))
        self.position[key] = partition.offset

    def incremental_assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned.extend((tp.topic, tp.partition) for tp in partitions)

    def incremental_unassign(self, partitions: list[TopicPartition]) -> None:
        self.unassigned.extend((tp.topic, tp.partition) for tp in partitions)

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeProducer:
    """Records what was produced and whether it was flushed."""

    produced: list[tuple[str, bytes | None, bytes | None, list[tuple[str, bytes]] | None]] = field(default_factory=list)
    flushes: int = 0
    remaining: int = 0
    fail_next: bool = False

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: Any = None,
        headers: Any = None,
    ) -> None:
        if self.fail_next:
            self.fail_next = False
            raise BufferError("queue full")
        self.produced.append((topic, key, value, headers))
        self._on_delivery = on_delivery

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self.flushes += 1
        return self.remaining

    def topics(self) -> list[str]:
        return [topic for topic, _key, _value, _headers in self.produced]
