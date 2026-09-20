"""An in-process model of the librdkafka behaviours the ordered loader relies on.

**This is a model, not a broker.** It proves the *algorithm* is correct given
these semantics; the integration tests prove the semantics are real. Every
modelled behaviour below is stated as a claim that `test_fakekafka.py` pins
down and that the integration run re-checks against Apache Kafka 4.0.

Modelled, in order of how much damage each one does when you get it wrong:

1. **`pause()` purges the partition's local prefetch queue, and a later
   `resume()` refetches from _last consumed offset + 1_.** So pausing on a
   failed message and resuming *without* seeking back silently skips that
   message. The fake reproduces the skip (see
   `test_resume_without_seek_skips_the_failed_message`), which is what makes
   the loader's seek-before-resume testable instead of merely asserted.
2. **A partition comes back from a rebalance un-paused, and reading restarts
   at the last committed offset.** Pause state is per-assignment and lives in
   the client, not the group.
3. **Offsets are stored client-side and only become durable on `commit`.** A
   process that dies loses everything stored but not committed; that is the
   entire at-least-once/no-loss argument.
4. **The revoke callback runs _before_ the partitions go away** (so a loader
   can still commit), while the lost callback runs after they are already
   someone else's (so it must not commit).
5. **`commit()` with nothing stored raises** rather than succeeding quietly.

Deliberately *not* modelled, because the algorithm does not depend on it:
Kafka's murmur2 partitioner (any stable key->partition function preserves the
property under test), replication, the LSO/read_committed visibility rule, and
broker-side timeouts. Transactions are not modelled at all — EOS is broker
semantics, so `txn_stage` is integration-tested only.

No payload bytes are ever logged here (invariant 6); the fake stores values as
opaque bytes and never decodes them.
"""

from __future__ import annotations

import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaException, TopicPartition

#: librdkafka's "no committed offset" sentinel.
OFFSET_INVALID = -1001


class SimulatedCrash(RuntimeError):
    """Raised out of ``poll()`` to model a ``kill -9`` of the worker process.

    After it fires the consumer is dead: stores, commits and closes are no-ops,
    so anything the loader's shutdown path tries to do is correctly ignored.
    """


@dataclass(slots=True)
class FakeRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]


class FakeMessage:
    """Duck-types ``confluent_kafka.Message``."""

    __slots__ = ("_r",)

    def __init__(self, record: FakeRecord) -> None:
        self._r = record

    def topic(self) -> str:
        return self._r.topic

    def partition(self) -> int:
        return self._r.partition

    def offset(self) -> int:
        return self._r.offset

    def key(self) -> bytes | None:
        return self._r.key

    def value(self) -> bytes | None:
        return self._r.value

    def headers(self) -> list[tuple[str, bytes]]:
        return list(self._r.headers)

    def error(self) -> None:
        return None

    def __repr__(self) -> str:  # no payload in the repr
        return f"FakeMessage({self._r.topic}[{self._r.partition}]@{self._r.offset})"


@dataclass(slots=True)
class FetchState:
    """Per-partition client state. ``position`` is the next offset to fetch."""

    position: int = 0
    paused: bool = False
    #: Last offset actually handed to the application by ``poll()``.
    last_consumed: int = -1


@dataclass(slots=True)
class CommitEvent:
    topic: str
    partition: int
    previous: int
    committed: int
    consumer: str


class FakeCluster:
    """A tiny in-memory Kafka. Also serves as an `s5.driver.Driver`."""

    def __init__(self, default_partitions: int = 4) -> None:
        self.default_partitions = default_partitions
        self.logs: dict[str, list[list[FakeRecord]]] = {}
        #: group -> (topic, partition) -> committed offset
        self.offsets: dict[str, dict[tuple[str, int], int]] = {}
        self.commits: list[CommitEvent] = []
        #: Called on every commit, before it is applied. Property tests use it
        #: to assert commit-after-delivery online rather than after the fact.
        self.commit_hook: Callable[[CommitEvent], None] | None = None
        self.consumers: list[FakeConsumer] = []

    # ------------------------------------------------------------------ topics
    def create_topic(self, name: str, partitions: int | None = None) -> None:
        if name not in self.logs:
            self.logs[name] = [[] for _ in range(partitions or self.default_partitions)]

    def log(self, topic: str, partition: int) -> list[FakeRecord]:
        self.create_topic(topic)
        return self.logs[topic][partition]

    def partitions(self, topic: str) -> int:
        self.create_topic(topic)
        return len(self.logs[topic])

    def records(self, topic: str) -> list[FakeRecord]:
        """Every record in a topic, partition by partition."""
        self.create_topic(topic)
        return [r for plog in self.logs[topic] for r in plog]

    def partition_for(self, topic: str, key: bytes | None) -> int:
        n = self.partitions(topic)
        if key is None:
            return 0
        # NOT murmur2. Only the property "one key -> one partition" matters.
        return zlib.crc32(key) % n

    def append(
        self,
        topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        partition: int | None = None,
    ) -> FakeRecord:
        p = self.partition_for(topic, key) if partition is None else partition
        plog = self.log(topic, p)
        record = FakeRecord(topic, p, len(plog), key, value, list(headers or []))
        plog.append(record)
        return record

    # ------------------------------------------------------------ Driver impl
    def consumer(
        self,
        group: str,
        topics: Sequence[str],
        *,
        instance_id: str | None = None,
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
        **_overrides: Any,
    ) -> FakeConsumer:
        c = FakeConsumer(
            self,
            group,
            list(topics),
            instance_id=instance_id,
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
        )
        self.consumers.append(c)
        return c

    def producer(self, **_overrides: Any) -> FakeProducer:
        return FakeProducer(self)

    # ------------------------------------------------------------- group state
    def group_offsets(self, group: str) -> dict[tuple[str, int], int]:
        return self.offsets.setdefault(group, {})

    def committed_offset(self, group: str, topic: str, partition: int) -> int:
        return self.group_offsets(group).get((topic, partition), OFFSET_INVALID)


class FakeProducer:
    """Synchronous, always-durable producer. Delivery callbacks fire inline."""

    def __init__(self, cluster: FakeCluster, fail: Callable[[str], bool] | None = None) -> None:
        self._cluster = cluster
        self._fail = fail
        self.produced = 0

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        on_delivery: Callable[[Any, Any], None] | None = None,
        **_kw: Any,
    ) -> None:
        if self._fail is not None and self._fail(topic):
            raise BufferError("simulated produce failure")
        record = self._cluster.append(topic, key, value, headers)
        self.produced += 1
        if on_delivery is not None:
            on_delivery(None, FakeMessage(record))

    def flush(self, timeout: float = 0.0) -> int:
        return 0

    def poll(self, timeout: float = 0.0) -> int:
        return 0


class FakeConsumer:
    """Models the client-side half of a Kafka consumer.

    Rebalances are explicit and deterministic: call :meth:`revoke`,
    :meth:`assign_partitions` or :meth:`lose`, and the callbacks fire on the
    next ``poll()`` — which is where librdkafka fires them too.
    """

    _counter = 0

    def __init__(
        self,
        cluster: FakeCluster,
        group: str,
        topics: list[str],
        *,
        instance_id: str | None = None,
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None:
        FakeConsumer._counter += 1
        self.name = instance_id or f"fake-consumer-{FakeConsumer._counter}"
        self.group = group
        self.topics = topics
        self._cluster = cluster
        self._on_assign = on_assign
        self._on_revoke = on_revoke
        self._on_lost = on_lost
        self._assigned: dict[tuple[str, int], FetchState] = {}
        self._stored: dict[tuple[str, int], int] = {}
        self._pending: list[tuple[str, list[TopicPartition]]] = []
        self._cursor = 0
        self._dead = False
        self._closed = False
        self.polls = 0
        self.pause_calls: list[tuple[str, int]] = []
        self.resume_calls: list[tuple[str, int]] = []
        self.seeks: list[tuple[str, int, int]] = []
        self._actions: dict[int, list[Callable[[], None]]] = {}
        # Subscribing claims every partition of every subscribed topic, which
        # is what a single-member group gets.
        self.assign_partitions(
            [
                TopicPartition(t, p)
                for t in topics
                for p in range(cluster.partitions(t))
            ]
        )

    # ------------------------------------------------------- test control API
    def schedule(self, at_poll: int, action: Callable[[], None]) -> None:
        """Run ``action`` at the start of poll number ``at_poll`` (1-based)."""
        self._actions.setdefault(at_poll, []).append(action)

    def crash(self) -> None:
        """Model ``kill -9``: no revoke, no commit, nothing drains."""
        self._dead = True

    def assign_partitions(self, partitions: Sequence[TopicPartition]) -> None:
        self._pending.append(("assign", list(partitions)))

    def revoke(self, partitions: Sequence[TopicPartition] | None = None) -> None:
        """Cooperative revoke: the callback runs before the partitions go."""
        self._pending.append(("revoke", self._as_tps(partitions)))

    def lose(self, partitions: Sequence[TopicPartition] | None = None) -> None:
        """Membership lost (session timeout / max.poll.interval exceeded)."""
        self._pending.append(("lost", self._as_tps(partitions)))

    def rebalance(self, partitions: Sequence[TopicPartition] | None = None) -> None:
        """Revoke then immediately re-assign: the classic survivor's rebalance.

        This is the case that breaks naive loaders: the partition comes back
        un-paused and rewound to the last commit.
        """
        tps = self._as_tps(partitions)
        self.revoke(tps)
        self.assign_partitions(tps)

    def _as_tps(self, partitions: Sequence[TopicPartition] | None) -> list[TopicPartition]:
        if partitions is not None:
            return list(partitions)
        return [TopicPartition(t, p) for (t, p) in sorted(self._assigned)]

    # ---------------------------------------------------------- consumer API
    def poll(self, timeout: float = 0.0) -> FakeMessage | None:
        if self._dead:
            raise SimulatedCrash(f"{self.name} was killed")
        self.polls += 1
        for action in self._actions.pop(self.polls, []):
            action()
        if self._dead:
            raise SimulatedCrash(f"{self.name} was killed")
        self._drain_rebalances()
        return self._fetch()

    def _drain_rebalances(self) -> None:
        while self._pending:
            kind, tps = self._pending.pop(0)
            if kind == "assign":
                for tp in tps:
                    committed = self._cluster.committed_offset(self.group, tp.topic, tp.partition)
                    start = 0 if committed < 0 else committed  # auto.offset.reset=earliest
                    # Un-paused, rewound to the last commit. Both matter.
                    self._assigned[(tp.topic, tp.partition)] = FetchState(
                        position=start, paused=False, last_consumed=start - 1
                    )
                if self._on_assign is not None:
                    self._on_assign(self, list(tps))
            elif kind == "revoke":
                # Callback first: the owner may still commit here.
                if self._on_revoke is not None:
                    self._on_revoke(self, list(tps))
                for tp in tps:
                    self._assigned.pop((tp.topic, tp.partition), None)
                    self._stored.pop((tp.topic, tp.partition), None)
            else:  # lost
                for tp in tps:
                    self._assigned.pop((tp.topic, tp.partition), None)
                    self._stored.pop((tp.topic, tp.partition), None)
                # Callback after: these partitions are already someone else's.
                if self._on_lost is not None:
                    self._on_lost(self, list(tps))
            self._cursor = 0

    def _fetch(self) -> FakeMessage | None:
        keys = sorted(self._assigned)
        if not keys:
            return None
        n = len(keys)
        for i in range(n):
            idx = (self._cursor + i) % n
            topic, partition = keys[idx]
            st = self._assigned[(topic, partition)]
            if st.paused:
                continue
            plog = self._cluster.log(topic, partition)
            if st.position < len(plog):
                record = plog[st.position]
                st.position += 1
                st.last_consumed = record.offset
                self._cursor = (idx + 1) % n  # round-robin keeps partitions interleaved
                return FakeMessage(record)
        return None

    def pause(self, partitions: Sequence[TopicPartition]) -> None:
        for tp in partitions:
            st = self._assigned.get((tp.topic, tp.partition))
            if st is None:
                continue
            st.paused = True
            # The prefetch queue is purged: a later resume refetches from
            # last_consumed + 1, NOT from wherever the prefetch had reached.
            st.position = st.last_consumed + 1
            self.pause_calls.append((tp.topic, tp.partition))

    def resume(self, partitions: Sequence[TopicPartition]) -> None:
        for tp in partitions:
            st = self._assigned.get((tp.topic, tp.partition))
            if st is None:
                continue
            st.paused = False
            self.resume_calls.append((tp.topic, tp.partition))

    def seek(self, partition: TopicPartition) -> None:
        st = self._assigned.get((partition.topic, partition.partition))
        if st is None:
            # Never interpolate the TopicPartition itself: on Windows its repr
            # raises SystemError (see the note in `driver` / the S5 report).
            raise KafkaException(
                f"Local: Erroneous state: {partition.topic}[{partition.partition}] is not assigned"
            )
        st.position = partition.offset
        st.last_consumed = partition.offset - 1
        self.seeks.append((partition.topic, partition.partition, partition.offset))

    def store_offsets(self, message: Any = None, offsets: Any = None) -> None:
        if self._dead:
            return
        if message is not None:
            self._stored[(message.topic(), message.partition())] = message.offset() + 1
        for tp in offsets or []:
            self._stored[(tp.topic, tp.partition)] = tp.offset

    def commit(self, **kwargs: Any) -> list[TopicPartition]:
        if self._dead:
            return []
        offsets = kwargs.get("offsets")
        pending = (
            {(tp.topic, tp.partition): tp.offset for tp in offsets}
            if offsets
            else dict(self._stored)
        )
        if not pending:
            # librdkafka: _NO_OFFSET. Callers must treat it as benign.
            raise KafkaException("Local: No offset stored")
        group = self._cluster.group_offsets(self.group)
        out: list[TopicPartition] = []
        for (topic, partition), offset in sorted(pending.items()):
            event = CommitEvent(
                topic=topic,
                partition=partition,
                previous=group.get((topic, partition), 0),
                committed=offset,
                consumer=self.name,
            )
            if self._cluster.commit_hook is not None:
                self._cluster.commit_hook(event)
            self._cluster.commits.append(event)
            group[(topic, partition)] = offset
            out.append(TopicPartition(topic, partition, offset))
        self._stored.clear()
        return out

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(t, p) for (t, p) in sorted(self._assigned)]

    def position(self, partitions: Sequence[TopicPartition]) -> list[TopicPartition]:
        out = []
        for tp in partitions:
            st = self._assigned.get((tp.topic, tp.partition))
            out.append(
                TopicPartition(tp.topic, tp.partition, st.position if st else OFFSET_INVALID)
            )
        return out

    def committed(
        self, partitions: Sequence[TopicPartition], **_kwargs: Any
    ) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic,
                tp.partition,
                self._cluster.committed_offset(self.group, tp.topic, tp.partition),
            )
            for tp in partitions
        ]

    def close(self) -> None:
        if self._dead or self._closed:
            return
        self._closed = True
        # A clean close revokes the assignment, so the owner gets its last
        # chance to commit. This is what "graceful shutdown drains" means.
        if self._assigned:
            self.revoke()
            self._drain_rebalances()


@dataclass(slots=True)
class DlqRecord:
    """A DLQ entry decoded from headers, for assertions. No payload."""

    msg_id: str
    reason: str
    source_topic: str
    source_partition: int
    source_offset: int


def dlq_records(cluster: FakeCluster, topic: str) -> list[DlqRecord]:
    out: list[DlqRecord] = []
    for record in cluster.records(topic):
        h = {k: v.decode() for k, v in record.headers}
        out.append(
            DlqRecord(
                msg_id=h.get("tb-parent-id", h.get("tb-msg-id", "")),
                reason=h.get("tb-dlq-reason", ""),
                source_topic=h.get("tb-source-topic", ""),
                source_partition=int(h.get("tb-source-partition", -1)),
                source_offset=int(h.get("tb-source-offset", -1)),
            )
        )
    return out


@dataclass(slots=True)
class Handled:
    """Which (partition, offset) pairs a run has durably dealt with.

    "Dealt with" = written to the sink, or produced to the DLQ. The
    commit-after-delivery property is: a commit of offset ``O`` on a partition
    implies every offset below ``O`` is in here already.
    """

    offsets: dict[tuple[str, int], set[int]] = field(default_factory=dict)

    def add(self, topic: str, partition: int, offset: int) -> None:
        self.offsets.setdefault((topic, partition), set()).add(offset)

    def unhandled_below(self, topic: str, partition: int, commit_offset: int) -> list[int]:
        done = self.offsets.get((topic, partition), set())
        return [o for o in range(commit_offset) if o not in done]
