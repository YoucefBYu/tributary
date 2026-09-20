"""Transactional Kafka-to-Kafka stage: `raw` -> `out.<dest>`.

Read-process-write with the consumer offsets inside the transaction
(ADR-001 6.4: exactly-once applies only to Kafka-to-Kafka stages).

Notes that cost time to learn:

* Use **one ``transactional.id`` per worker**, not per partition. Since
  KIP-447 (Kafka 2.5+) ``send_offsets_to_transaction`` fences zombies through
  the consumer group metadata (generation id), so a stable per-worker id is
  correct even as the assignment changes. The old per-partition-id rule is
  EOS v1 advice and does not survive cooperative-sticky assignment.
* The consumer must never commit by itself. ``enable.auto.commit=False`` is
  not optional — an auto-commit races the transaction and breaks atomicity.
* ``consumer.position(assignment)`` is the *next* offset to read, which is
  exactly what goes into the transaction. Do not send ``msg.offset()``.
* Downstream consumers must run ``isolation.level=read_committed`` or they see
  aborted records. `tbcommon.clients` defaults to that.
* On revoke you must abort any open transaction *before* giving up the
  partitions, otherwise the next owner blocks on the LSO until the
  transaction times out.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from tbcommon.clients import make_consumer, make_producer, make_transactional_producer
from tbcommon.envelope import Envelope

from .events import EventEmitter, Stage

log = logging.getLogger("s5.txn_stage")


@dataclass(slots=True)
class TxnStats:
    consumed: int = 0
    produced: int = 0
    committed_txns: int = 0
    aborted_txns: int = 0
    abortable_errors: int = 0
    retriable_errors: int = 0
    fatal_errors: int = 0
    revokes: int = 0
    max_batch: int = 0
    commit_latency_s: list[float] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        lat = sorted(self.commit_latency_s)
        return {
            "consumed": self.consumed,
            "produced": self.produced,
            "committed_txns": self.committed_txns,
            "aborted_txns": self.aborted_txns,
            "abortable_errors": self.abortable_errors,
            "retriable_errors": self.retriable_errors,
            "fatal_errors": self.fatal_errors,
            "revokes": self.revokes,
            "max_batch": self.max_batch,
            "txn_commit_p50_s": round(lat[len(lat) // 2], 4) if lat else None,
            "txn_commit_p99_s": round(lat[int(len(lat) * 0.99)], 4) if lat else None,
        }


@dataclass(slots=True)
class TxnConfig:
    in_topic: str
    out_topic: str
    group: str
    instance_id: str | None
    transactional_id: str
    batch_size: int = 200
    batch_max_wait_s: float = 0.25
    idle_exit_s: float | None = None
    destination: str = "fhir"


class TransactionalStage:
    def __init__(self, cfg: TxnConfig, *, events_topic: str | None = None) -> None:
        self.cfg = cfg
        self.worker = f"txn-{os.getpid()}"
        self.stats = TxnStats()
        self._in_txn = False

        self._producer = make_transactional_producer(cfg.transactional_id)
        # Lifecycle events go through a SEPARATE plain producer. A transactional
        # producer can only produce inside a transaction, and events written
        # inside it would be rolled back by an abort -- but the archive wants to
        # see the attempt. Events are at-least-once and off the critical path.
        self._emitter = (
            EventEmitter(make_producer(**{"linger.ms": 50}), events_topic, self.worker)
            if events_topic
            else None
        )
        self._consumer: Consumer = make_consumer(
            cfg.group,
            [cfg.in_topic],
            instance_id=cfg.instance_id,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    # ----------------------------------------------------------- rebalancing
    def _on_revoke(self, _c: Consumer, _partitions: list[TopicPartition]) -> None:
        self.stats.revokes += 1
        self._abort_if_open("revoke")

    def _on_lost(self, _c: Consumer, _partitions: list[TopicPartition]) -> None:
        self._abort_if_open("lost")

    def _abort_if_open(self, why: str) -> None:
        if not self._in_txn:
            return
        try:
            self._producer.abort_transaction(30)
            self.stats.aborted_txns += 1
            log.info("aborted open transaction on %s", why)
        except KafkaException:
            log.warning("abort_transaction failed on %s", why, exc_info=True)
        finally:
            self._in_txn = False

    # ------------------------------------------------------------ main loop
    def run(self, stop: threading.Event | None = None) -> TxnStats:
        stop = stop or threading.Event()
        last_activity = time.monotonic()
        try:
            while not stop.is_set():
                batch = self._collect(stop)
                if not batch:
                    if self.cfg.idle_exit_s is not None and (
                        time.monotonic() - last_activity
                    ) >= self.cfg.idle_exit_s:
                        break
                    continue
                last_activity = time.monotonic()
                self._process(batch)
        finally:
            self._shutdown()
        return self.stats

    def _collect(self, stop: threading.Event) -> list[Message]:
        batch: list[Message] = []
        deadline = time.monotonic() + self.cfg.batch_max_wait_s
        while len(batch) < self.cfg.batch_size and time.monotonic() < deadline:
            if stop.is_set():
                break
            msg = self._consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error() is not None:
                log.warning("consume error: %s", msg.error())
                continue
            batch.append(msg)
        return batch

    def _process(self, batch: list[Message]) -> None:
        self.stats.consumed += len(batch)
        self.stats.max_batch = max(self.stats.max_batch, len(batch))

        self._producer.begin_transaction()
        self._in_txn = True
        try:
            for msg in batch:
                env = Envelope.from_headers(msg.headers())
                out_env = env.next_stage("transformed")
                self._producer.produce(
                    self.cfg.out_topic,
                    # Same key -> same partition -> per-key order preserved.
                    key=msg.key(),
                    value=_transform(msg.value()),
                    headers=out_env.to_headers(),
                )
                self.stats.produced += 1
                self._emit(Stage.TRANSFORMED, out_env, len(msg.value() or b""))

            positions = self._consumer.position(self._consumer.assignment())
            positions = [tp for tp in positions if tp.offset >= 0]
            self._producer.send_offsets_to_transaction(
                positions, self._consumer.consumer_group_metadata(), 30
            )
            t0 = time.monotonic()
            self._producer.commit_transaction(60)
            self.stats.commit_latency_s.append(time.monotonic() - t0)
            self.stats.committed_txns += 1
            self._in_txn = False
        except KafkaException as exc:
            self._handle_txn_error(exc)

    def _handle_txn_error(self, exc: KafkaException) -> None:
        err: KafkaError = exc.args[0]
        if err.fatal():
            self.stats.fatal_errors += 1
            self._in_txn = False
            raise RuntimeError(f"fatal transaction error: {err}") from exc
        if err.txn_requires_abort():
            self.stats.abortable_errors += 1
            self._abort_if_open("abortable-error")
            # Rewind to the last committed offsets; the aborted batch is replayed.
            self._rewind_to_committed()
            return
        if err.retriable():
            self.stats.retriable_errors += 1
            log.warning("retriable transaction error, retrying: %s", err)
            return
        self._in_txn = False
        raise RuntimeError(f"unexpected transaction error: {err}") from exc

    def _rewind_to_committed(self) -> None:
        assignment = self._consumer.assignment()
        if not assignment:
            return
        committed = self._consumer.committed(assignment, timeout=30)
        for tp in committed:
            # offset < 0 means "nothing committed yet" -> fall back to the
            # configured auto.offset.reset by seeking to the beginning.
            target = tp.offset if tp.offset >= 0 else 0
            try:
                self._consumer.seek(TopicPartition(tp.topic, tp.partition, target))
            except KafkaException:
                # Fields, not `tp`: repr(TopicPartition) raises SystemError on
                # Windows and logging would drop this record entirely.
                log.warning(
                    "rewind seek failed for %s[%d]@%d",
                    tp.topic,
                    tp.partition,
                    target,
                    exc_info=True,
                )

    def _emit(self, stage: Stage, env: Envelope, size: int) -> None:
        if self._emitter is not None:
            self._emitter.emit(stage, env, destination=self.cfg.destination, size_bytes=size)

    def _shutdown(self) -> None:
        self._abort_if_open("shutdown")
        if self._emitter is not None:
            self._emitter.flush(10.0)
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            log.warning("consumer close failed", exc_info=True)


def _transform(value: bytes | None) -> bytes:
    """Stand-in for the pipeline. Deliberately trivial: S1 measures transforms."""
    return value or b""
