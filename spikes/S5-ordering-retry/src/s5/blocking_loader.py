"""Ordered destination loader: `blocking` retry via partition pause/resume.

This is the heart of S5 and the prototype for `tributary-runtime`'s ordered
loader. Invariants defended here:

* **5 (ordering).** A failed message blocks *its own partition only*. The
  partition is paused, the fetch offset is seeked back to the failed message,
  and no later offset from that partition is delivered until it succeeds or is
  explicitly sent to the DLQ. Other partitions keep flowing.
* **4 (at-least-once).** The offset is stored only after the sink write is
  durable, and committed after that. A crash replays, never skips.
* **9 / 6.** Lifecycle events are fire-and-forget and carry ids only.

Two librdkafka behaviours make or break this:

1. ``pause()`` purges the partition's local prefetch queue, and ``resume()``
   refetches from *last consumed offset + 1*. Without an explicit ``seek()``
   back to the failed offset, resume **skips the message that failed** — a
   silent data-loss bug. We seek while still paused, then resume; doing it in
   the other order races an in-flight fetch.
2. Pause state is per-assignment. After a rebalance re-assigns a partition it
   comes back **un-paused**, so per-partition state must be rebuilt in the
   assign callback, and the replay comes from the last committed offset.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field
from enum import StrEnum

from confluent_kafka import KafkaException, TopicPartition
from tbcommon.envelope import Envelope

from .backoff import Backoff
from .driver import ConsumerLike, Driver, KafkaDriver, MessageLike, ProducerLike
from .events import EventEmitter, Stage
from .sink import FileSink, SinkRejected

log = logging.getLogger("s5.blocking_loader")


class OnExhaust(StrEnum):
    HALT_PARTITION = "halt-partition"
    DLQ_AND_CONTINUE = "dlq-and-continue"


@dataclass(slots=True)
class PartitionState:
    attempt: int = 0
    paused: bool = False
    halted: bool = False
    resume_at: float = 0.0
    retry_offset: int = -1
    last_delay: float | None = None
    #: Offset we expect next. -1 = unknown (just assigned).
    next_offset: int = -1


@dataclass(slots=True)
class LoaderStats:
    delivered: int = 0
    first_deliveries: int = 0
    redeliveries: int = 0
    failures: int = 0
    pauses: int = 0
    resumes: int = 0
    dlq: int = 0
    halted_partitions: int = 0
    commits: int = 0
    offset_skips_corrected: int = 0
    assigns: int = 0
    revokes: int = 0
    lost: int = 0
    total_backoff_s: float = 0.0

    def as_dict(self) -> dict[str, float | int]:
        return {
            "delivered": self.delivered,
            "first_deliveries": self.first_deliveries,
            "redeliveries": self.redeliveries,
            "failures": self.failures,
            "pauses": self.pauses,
            "resumes": self.resumes,
            "dlq": self.dlq,
            "halted_partitions": self.halted_partitions,
            "commits": self.commits,
            "offset_skips_corrected": self.offset_skips_corrected,
            "assigns": self.assigns,
            "revokes": self.revokes,
            "lost": self.lost,
            "total_backoff_s": round(self.total_backoff_s, 3),
        }


@dataclass(slots=True)
class LoaderConfig:
    topic: str
    dlq_topic: str
    group: str
    instance_id: str | None
    destination: str = "fhir"
    max_attempts: int = 8
    on_exhaust: OnExhaust = OnExhaust.DLQ_AND_CONTINUE
    backoff: Backoff = field(default_factory=Backoff)
    #: Synchronous commit every N successful deliveries. 1 = strictest, and the
    #: duplicate window on a kill is then a single message.
    commit_every: int = 1
    commit_interval_s: float = 1.0
    poll_timeout_s: float = 0.2
    #: Stop after this long with no message and no paused partition.
    idle_exit_s: float | None = None
    seed: int = 1


class BlockingLoader:
    def __init__(
        self,
        cfg: LoaderConfig,
        sink: FileSink,
        *,
        events_topic: str | None = None,
        worker: str | None = None,
        driver: Driver | None = None,
        consumer_overrides: dict[str, object] | None = None,
    ) -> None:
        self.cfg = cfg
        self.sink = sink
        self.worker = worker or f"loader-{os.getpid()}"
        self.stats = LoaderStats()
        self.states: dict[tuple[str, int], PartitionState] = {}
        self._rng = random.Random(cfg.seed)
        self._since_commit = 0
        self._last_commit = time.monotonic()

        # KafkaDriver in production; s5.fakekafka.FakeCluster in the fast
        # property tests. See driver.py for why the seam exists.
        self._driver: Driver = driver or KafkaDriver()
        self._dlq_producer: ProducerLike = self._driver.producer()
        self._emitter: EventEmitter | None = (
            EventEmitter(self._driver.producer(**{"linger.ms": 50}), events_topic, self.worker)
            if events_topic
            else None
        )
        self._consumer: ConsumerLike = self._driver.consumer(
            cfg.group,
            [cfg.topic],
            instance_id=cfg.instance_id,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
            **(consumer_overrides or {}),
        )

    # ---------------------------------------------------------------- events
    def _emit(self, stage: Stage, env: Envelope, **kw: object) -> None:
        if self._emitter is not None:
            self._emitter.emit(stage, env, destination=self.cfg.destination, **kw)  # type: ignore[arg-type]

    # ----------------------------------------------------------- rebalancing
    def _on_assign(self, _c: ConsumerLike, partitions: list[TopicPartition]) -> None:
        self.stats.assigns += len(partitions)
        for tp in partitions:
            # Fresh state: a re-assigned partition is never still paused.
            self.states[(tp.topic, tp.partition)] = PartitionState()

    def _on_revoke(self, c: ConsumerLike, partitions: list[TopicPartition]) -> None:
        self.stats.revokes += len(partitions)
        # Commit what we have delivered before giving the partitions up. This
        # is what keeps the duplicate rate low across a planned rebalance.
        self._commit(force=True, consumer=c)
        for tp in partitions:
            self.states.pop((tp.topic, tp.partition), None)

    def _on_lost(self, _c: ConsumerLike, partitions: list[TopicPartition]) -> None:
        # Do NOT commit: another member already owns these.
        self.stats.lost += len(partitions)
        for tp in partitions:
            self.states.pop((tp.topic, tp.partition), None)

    # --------------------------------------------------------------- commits
    def _commit(self, *, force: bool, consumer: ConsumerLike | None = None) -> None:
        c = consumer or self._consumer
        due = force or (
            self._since_commit >= self.cfg.commit_every
            or (time.monotonic() - self._last_commit) >= self.cfg.commit_interval_s
        )
        if not due or (self._since_commit == 0 and not force):
            return
        try:
            c.commit(asynchronous=False)
            self.stats.commits += 1
        except KafkaException as exc:
            # _NO_OFFSET simply means nothing was stored; anything else matters.
            if "No offset stored" not in str(exc):
                log.warning("commit failed: %s", exc)
                return
        self._since_commit = 0
        self._last_commit = time.monotonic()

    # -------------------------------------------------------- pause / resume
    def _pause(self, tp: TopicPartition, st: PartitionState, offset: int, delay: float) -> None:
        self._consumer.pause([TopicPartition(tp.topic, tp.partition)])
        st.paused = True
        st.retry_offset = offset
        st.resume_at = time.monotonic() + delay
        st.last_delay = delay
        self.stats.pauses += 1
        self.stats.total_backoff_s += delay

    def _resume_due(self) -> None:
        now = time.monotonic()
        for (topic, partition), st in self.states.items():
            if not st.paused or st.halted or now < st.resume_at:
                continue
            tp = TopicPartition(topic, partition, st.retry_offset)
            # seek BEFORE resume: resume alone refetches from last-consumed+1
            # and would skip the failed message.
            if not self._seek(tp):
                continue
            self._consumer.resume([TopicPartition(topic, partition)])
            st.paused = False
            st.next_offset = st.retry_offset
            self.stats.resumes += 1

    def _seek(self, tp: TopicPartition) -> bool:
        """seek() can transiently raise _STATE before the toppar is fetch-ready.

        The log calls pass ``tp``'s *fields*, never ``tp`` itself: on Windows
        ``repr(TopicPartition)`` raises SystemError, and inside logging that
        does not crash -- it silently drops the record. These two lines are the
        only evidence that a seek failed, which is the difference between a
        skipped message and a stuck partition, so losing them is not an option.
        """
        for _ in range(20):
            try:
                self._consumer.seek(tp)
                return True
            except KafkaException as exc:
                if "Erroneous state" not in str(exc) and "_STATE" not in str(exc):
                    log.warning(
                        "seek(%s[%d]@%d) failed: %s", tp.topic, tp.partition, tp.offset, exc
                    )
                    return False
                time.sleep(0.05)
        log.error("seek(%s[%d]@%d) never became possible", tp.topic, tp.partition, tp.offset)
        return False

    # ------------------------------------------------------------ main loop
    def run(self, stop: threading.Event | None = None) -> LoaderStats:
        stop = stop or threading.Event()
        last_activity = time.monotonic()
        try:
            while not stop.is_set():
                self._resume_due()
                msg = self._consumer.poll(self.cfg.poll_timeout_s)
                if msg is None:
                    self._commit(force=False)
                    if self._idle_expired(last_activity):
                        break
                    continue
                if msg.error() is not None:
                    log.warning("consume error: %s", msg.error())
                    continue
                last_activity = time.monotonic()
                self._handle(msg)
        finally:
            self._shutdown()
        return self.stats

    def _idle_expired(self, last_activity: float) -> bool:
        if self.cfg.idle_exit_s is None:
            return False
        # A *paused* partition is mid-backoff and will make progress shortly, so
        # it must hold the loop open. A *halted* one will never make progress
        # without a human, so it must not: treating halted as activity made
        # `run()` un-exitable, and `idle_exit_s` silently stopped working the
        # moment any partition halted.
        if any(st.paused and not st.halted for st in self.states.values()):
            return False
        return (time.monotonic() - last_activity) >= self.cfg.idle_exit_s

    def _handle(self, msg: MessageLike) -> None:
        tp = TopicPartition(msg.topic(), msg.partition())
        st = self.states.setdefault((msg.topic(), msg.partition()), PartitionState())
        offset = msg.offset()

        # Ordering guard. If librdkafka ever hands us an offset past what we
        # expect, we would be skipping a message. Seek back instead.
        if st.next_offset >= 0 and offset > st.next_offset:
            self.stats.offset_skips_corrected += 1
            log.error(
                "offset skip on %s[%d]: got %d expected %d, seeking back",
                msg.topic(),
                msg.partition(),
                offset,
                st.next_offset,
            )
            self._seek(TopicPartition(msg.topic(), msg.partition(), st.next_offset))
            return
        if st.next_offset >= 0 and offset < st.next_offset:
            return  # stale message from a purged fetch; already handled

        env = Envelope.from_headers(msg.headers())
        key = (msg.key() or b"").decode()
        seq = env.seq if env.seq is not None else -1
        self._emit(Stage.PENDING, env, size_bytes=len(msg.value() or b""))

        try:
            first = self.sink.deliver(
                msg_id=env.msg_id,
                key=key,
                seq=seq,
                attempt=st.attempt,
                worker=self.worker,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=offset,
            )
        except SinkRejected as exc:
            self.stats.failures += 1
            self._on_failure(tp, st, msg, env, exc)
            return

        # --- success: store, then commit. Never the other way round. --------
        self.stats.delivered += 1
        self.stats.first_deliveries += int(first)
        self.stats.redeliveries += int(not first)
        st.attempt = 0
        st.last_delay = None
        st.next_offset = offset + 1
        self._consumer.store_offsets(message=msg)
        self._since_commit += 1
        self._emit(Stage.SENT, env, status="ok")
        self._commit(force=False)

    def _on_failure(
        self,
        tp: TopicPartition,
        st: PartitionState,
        msg: MessageLike,
        env: Envelope,
        exc: SinkRejected,
    ) -> None:
        st.attempt += 1
        exhausted = exc.permanent or st.attempt >= self.cfg.max_attempts
        retry_env = env.with_attempt(st.attempt)

        if not exhausted:
            delay = self.cfg.backoff.delay(st.attempt - 1, self._rng, previous=st.last_delay)
            # Commit everything already delivered on this partition before we
            # stall it, so a crash during the backoff replays as little as
            # possible.
            self._commit(force=True)
            self._pause(tp, st, msg.offset(), delay)
            self._emit(
                Stage.RETRYING,
                retry_env,
                status="retrying",
                error_code=exc.reason,
                extra={"backoff_s": round(delay, 3)},
            )
            self._emit(
                Stage.PARTITION_PAUSED,
                retry_env,
                status="paused",
                extra={"partition": msg.partition(), "resume_in_s": round(delay, 3)},
            )
            return

        if self.cfg.on_exhaust is OnExhaust.HALT_PARTITION:
            self._commit(force=True)
            self._consumer.pause([TopicPartition(msg.topic(), msg.partition())])
            st.paused = True
            st.halted = True
            st.retry_offset = msg.offset()
            self.stats.halted_partitions += 1
            self._emit(
                Stage.PARTITION_HALTED,
                retry_env,
                status="halted",
                error_code=exc.reason,
                extra={"partition": msg.partition(), "offset": msg.offset()},
            )
            log.error(
                "partition %s[%d] halted at offset %d after %d attempts",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                st.attempt,
            )
            return

        self._to_dlq(msg, env, exc)
        st.attempt = 0
        st.last_delay = None
        st.next_offset = msg.offset() + 1
        self._consumer.store_offsets(message=msg)
        self._since_commit += 1
        self._commit(force=True)

    def _to_dlq(self, msg: MessageLike, env: Envelope, exc: SinkRejected) -> None:
        dlq_env = env.next_stage("dlq")
        dlq_env.parent_id = env.msg_id
        headers = dlq_env.to_headers()
        headers.extend(
            [
                ("tb-dlq-reason", exc.reason.encode()),
                ("tb-dlq-stage", f"loader:{self.cfg.destination}".encode()),
                ("tb-dlq-attempts", str(self.cfg.max_attempts).encode()),
                ("tb-source-topic", msg.topic().encode()),
                ("tb-source-partition", str(msg.partition()).encode()),
                ("tb-source-offset", str(msg.offset()).encode()),
            ]
        )
        self._dlq_producer.produce(
            self.cfg.dlq_topic, key=env.msg_id.encode(), value=msg.value(), headers=headers
        )
        # Flush BEFORE storing the offset: committing past a message that never
        # made it to the DLQ would be silent loss (invariant 1/4).
        remaining = self._dlq_producer.flush(30.0)
        if remaining:
            raise RuntimeError(f"DLQ produce not durable ({remaining} pending); refusing to commit")
        self.stats.dlq += 1
        self._emit(Stage.DLQ, dlq_env, status="dlq", error_code=exc.reason)

    def _shutdown(self) -> None:
        """Graceful drain: commit what we delivered, then leave the group."""
        try:
            self._commit(force=True)
        except Exception:  # noqa: BLE001
            log.warning("final commit failed", exc_info=True)
        try:
            self._dlq_producer.flush(15.0)
        except Exception:  # noqa: BLE001
            log.warning("dlq flush failed", exc_info=True)
        if self._emitter is not None:
            self._emitter.flush(10.0)
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            log.warning("consumer close failed", exc_info=True)
        self.sink.close()
