"""Producer / consumer factories with the settings ADR-001 mandates.

Every spike builds clients through here so that "what settings did you run
with?" has one answer in the reports.

Non-obvious librdkafka behaviour encoded below:

* ``enable.idempotence=true`` *forces* ``acks=all``, ``retries=INT32_MAX`` and
  ``max.in.flight<=5``. Passing a conflicting value raises at construction.
* With ``partition.assignment.strategy=cooperative-sticky`` and *custom*
  rebalance callbacks you MUST call ``incremental_assign`` /
  ``incremental_unassign``. Calling plain ``assign()`` throws
  ``Local: Erroneous state``. If you pass no callbacks librdkafka does the
  right thing itself.
* ``on_lost`` fires when the member fell out of the group (max.poll.interval
  exceeded, session timeout). Offsets must NOT be committed there: the
  partitions already belong to someone else.
* ``pause()`` purges the partition's local pre-fetch queue. On ``resume()``
  librdkafka refetches from the last consumed offset, so pausing never drops
  messages -- but any message already returned by ``poll()`` before the pause
  is still yours to handle.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from . import config

log = logging.getLogger("tbcommon.clients")

RebalanceHook = Callable[[Consumer, list[TopicPartition]], None]


def producer_config(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "bootstrap.servers": config.bootstrap(),
        # Invariant 1: ACK after commit needs a durable write.
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 5,
        "socket.keepalive.enable": True,
        # librdkafka's equivalent of delivery.timeout.ms. A produce that cannot
        # be made durable must fail loudly rather than silently expire.
        "message.timeout.ms": 120_000,
        "statistics.interval.ms": 0,
    }
    cfg.update(overrides)
    return cfg


def make_producer(**overrides: Any) -> Producer:
    return Producer(producer_config(**overrides))


def transactional_producer_config(transactional_id: str, **overrides: Any) -> dict[str, Any]:
    cfg = producer_config(**overrides)
    cfg["transactional.id"] = transactional_id
    # Must be <= broker transaction.max.timeout.ms (compose sets 900000).
    cfg.setdefault("transaction.timeout.ms", 60_000)
    return cfg


def make_transactional_producer(transactional_id: str, **overrides: Any) -> Producer:
    """Producer for a read-process-write stage.

    ``transactional.id`` must be **one per worker and stable across restarts**:
    that is what lets ``init_transactions`` fence the previous incarnation and
    abort whatever it left open. Since KIP-447 (Kafka 2.5+),
    ``send_offsets_to_transaction`` fences zombies through the consumer group
    metadata, so the id does not need to follow the partition assignment -- and
    must not, because under cooperative-sticky the assignment changes while the
    worker lives. Deriving the id per input partition is EOS v1 advice; it is
    wrong here and breaks as soon as a rebalance moves a partition.
    """
    p = Producer(transactional_producer_config(transactional_id, **overrides))
    p.init_transactions(30)
    return p


def consumer_config(
    group_id: str,
    *,
    instance_id: str | None = None,
    read_committed: bool = True,
    **overrides: Any,
) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "bootstrap.servers": config.bootstrap(),
        "group.id": group_id,
        # Invariant 4: commit only after downstream success.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        "isolation.level": "read_committed" if read_committed else "read_uncommitted",
        "session.timeout.ms": 45_000,
        "heartbeat.interval.ms": 3_000,
        "max.poll.interval.ms": 300_000,
        "socket.keepalive.enable": True,
    }
    if instance_id is not None:
        # Static membership: a restart inside session.timeout.ms rejoins with
        # the same assignment and causes no rebalance. The flip side, which
        # S5 measures: a *dead* static member keeps its partitions until the
        # session timeout expires.
        cfg["group.instance.id"] = instance_id
    cfg.update(overrides)
    return cfg


def make_consumer(
    group_id: str,
    topics: Sequence[str],
    *,
    instance_id: str | None = None,
    read_committed: bool = True,
    on_assign: RebalanceHook | None = None,
    on_revoke: RebalanceHook | None = None,
    on_lost: RebalanceHook | None = None,
    **overrides: Any,
) -> Consumer:
    """Subscribed consumer with cooperative-sticky-safe rebalance callbacks."""
    consumer = Consumer(
        consumer_config(
            group_id, instance_id=instance_id, read_committed=read_committed, **overrides
        )
    )

    def _assign(c: Consumer, partitions: list[TopicPartition]) -> None:
        # cooperative: `partitions` is the INCREMENT, not the full assignment.
        c.incremental_assign(partitions)
        log.info("assign +%d", len(partitions))
        if on_assign is not None:
            on_assign(c, partitions)

    def _revoke(c: Consumer, partitions: list[TopicPartition]) -> None:
        if on_revoke is not None:
            on_revoke(c, partitions)
        try:
            c.incremental_unassign(partitions)
        except Exception:  # noqa: BLE001 - consumer may already be closing
            log.warning("incremental_unassign failed during revoke", exc_info=True)
        log.info("revoke -%d", len(partitions))

    def _lost(c: Consumer, partitions: list[TopicPartition]) -> None:
        # Do NOT commit here: these partitions are no longer ours.
        if on_lost is not None:
            on_lost(c, partitions)
        try:
            c.incremental_unassign(partitions)
        except Exception:  # noqa: BLE001
            log.warning("incremental_unassign failed during lost", exc_info=True)
        log.warning("lost -%d", len(partitions))

    consumer.subscribe(list(topics), on_assign=_assign, on_revoke=_revoke, on_lost=_lost)
    return consumer


def is_fatal(err: KafkaError | None) -> bool:
    return err is not None and err.fatal()


def flush_or_raise(producer: Producer, timeout: float = 30.0) -> None:
    """Flush and fail loudly if anything is still queued.

    Invariant 1: never report success for a message that is not durable.
    """
    remaining = producer.flush(timeout)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) still unflushed after {timeout}s")
