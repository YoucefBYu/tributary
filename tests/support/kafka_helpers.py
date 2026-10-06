"""Shared raw-Kafka plumbing for `tests/chaos` and `tests/e2e`: free ports, broker readiness, publishing
synthetic `raw` records directly (bypassing a receiver, when the scenario under test is the worker or a
loader rather than ingest), and draining a topic to completion.

Centralised here (rather than copied per test module, as `tests/chaos/test_kafka_broker_kill.py`,
`tests/e2e/test_mllp_to_http_e2e.py` and `packages/tributary-runtime/tests/test_integration_kafka.py`
each grew their own `_free_port`/`_await_broker`/`drain`) because W2-9's chaos scenarios and the
per-loader duplicate-delivery harness need the same primitives, and a second copy is how the three
existing ones drift from each other.
"""

from __future__ import annotations

import socket
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tributary.sdk import Message
from tributary.sdk import headers as h

# Error substrings from a `CreateTopics` future that mean "the cluster has not finished settling yet,
# try again" rather than "this request is wrong". `_DESTROY_BROKER`/`_TRANSPORT`/`_TIMED_OUT` are
# librdkafka-local codes for a broker connection that reset mid-request (real on a freshly-started
# multi-broker KRaft cluster: controller-quorum leader election bounces broker connections during the
# same window `list_topics` first reports all N brokers present -- `await_broker` proves the brokers
# exist, not that the controller has finished settling enough to serve `CreateTopics`).
# `NOT_CONTROLLER`/`BROKER_NOT_AVAILABLE` are the broker-side equivalents. `INVALID_REPLICATION_FACTOR`
# is only transient when the message says there are fewer live brokers than the requested RF (an RF=3
# request racing the third broker still joining); any other `INVALID_REPLICATION_FACTOR` is a real
# request error and must not be retried.
_TRANSIENT_CREATE_TOPICS_ERRORS = (
    "_DESTROY_BROKER",
    "_TRANSPORT",
    "_TIMED_OUT",
    "NOT_CONTROLLER",
    "BROKER_NOT_AVAILABLE",
)


def _is_transient_create_topics_error(exc: Exception) -> bool:
    text = str(exc)
    if any(code in text for code in _TRANSIENT_CREATE_TOPICS_ERRORS):
        return True
    return "INVALID_REPLICATION_FACTOR" in text and "fewer" in text.lower()


def create_topics_with_retry(bootstrap: str, topics: list[NewTopic], *, deadline_s: float = 90.0) -> None:
    """Create `topics`, retrying transient `CreateTopics` failures on a fresh `AdminClient` each attempt.

    A single `create_topics` call proves nothing about whether the cluster can actually serve it: on a
    multi-broker cluster, `await_broker` returning (metadata lists every broker) races the controller
    quorum finishing leader election, and a `CreateTopics` sent into that window can see a broker
    connection reset (`_DESTROY_BROKER`) instead of an answer. This retries only errors that mean "not
    ready yet" (see `_is_transient_create_topics_error`); anything else, or `TOPIC_ALREADY_EXISTS`,
    resolves immediately (success or re-raise). It then blocks until every partition of every requested
    topic has a leader and as many in-sync replicas as the topic's replication factor, since a
    `CreateTopics` future can resolve before the partitions it created are actually able to serve
    produce/consume at the requested durability.
    """
    by_name = {topic.topic: topic for topic in topics}
    deadline = time.monotonic() + deadline_s
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        futures = admin.create_topics(topics)
        pending: list[Exception] = []
        for _topic, future in futures.items():
            try:
                future.result(timeout=30)
            except Exception as exc:  # triaged below by message, not swallowed
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    continue
                if not _is_transient_create_topics_error(exc):
                    raise
                pending.append(exc)
        if not pending:
            _await_topics_replicated(
                bootstrap, list(by_name.values()), deadline_s=max(deadline - time.monotonic(), 5.0)
            )
            return
        last_exc = pending[-1]
        time.sleep(1.0)
    raise KafkaException(f"CreateTopics did not succeed within {deadline_s}s: {last_exc}") from last_exc


def _await_topics_replicated(bootstrap: str, topics: list[NewTopic], *, deadline_s: float) -> None:
    """Block until every partition of `topics` has a leader and its full replica set in-sync.

    A `CreateTopics` future resolving does not mean the partitions are ready: on a cluster still
    settling, metadata can briefly show a partition with no leader or with fewer in-sync replicas than
    its replication factor. A test that proceeds to produce/consume (or, worse, to kill a broker) before
    this holds could observe the fault it thinks it is injecting for a reason that is really "the
    cluster was never fully up," not the fault.
    """
    deadline = time.monotonic() + deadline_s
    last_reason = "no metadata observed yet"
    while time.monotonic() < deadline:
        try:
            metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            not_ready: list[str] = []
            for topic in topics:
                info = metadata.topics.get(topic.topic)
                if info is None or info.error is not None:
                    not_ready.append(f"{topic.topic}: missing from metadata")
                    continue
                for partition_id, partition in info.partitions.items():
                    if partition.leader < 0:
                        not_ready.append(f"{topic.topic}-{partition_id}: no leader")
                    elif len(partition.isrs) < topic.replication_factor:
                        not_ready.append(
                            f"{topic.topic}-{partition_id}: {len(partition.isrs)}/{topic.replication_factor} ISR"
                        )
            if not not_ready:
                return
            last_reason = "; ".join(not_ready)
        except Exception as exc:  # any failure here means the cluster is not ready yet
            last_reason = str(exc)
        time.sleep(1.0)
    raise RuntimeError(f"topics did not reach full ISR within {deadline_s}s: {last_reason}")


def free_port() -> int:
    """A loopback port nothing is listening on yet, for a container's advertised listener."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def await_broker(bootstrap: str, *, expected_brokers: int = 1, timeout_s: float = 90.0) -> None:
    """Wait for metadata naming every expected broker, not just a quorum answering `list_topics`.

    Requiring `expected_brokers` (not just "any response") matters for a multi-broker cluster: one node
    can answer before the others finish joining, which would let a later "kill the leader" step target a
    broker whose peers are not actually up yet.
    """
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            if len(metadata.brokers) >= expected_brokers:
                return
        except Exception as exc:  # any failure here means the cluster is not ready yet
            last = exc
        time.sleep(1.0)
    raise RuntimeError(
        f"Kafka did not reach {expected_brokers} broker(s): {type(last).__name__ if last else 'timeout'}"
    )


def header_map(record: Any) -> dict[str, str]:
    return {name: value.decode("utf-8") for name, value in (record.headers() or [])}


def publish_messages(bootstrap: str, topic: str, messages: list[Message]) -> list[str]:
    """Produce a batch of already-built `Message`s straight to `topic` (a `raw` topic, typically),
    `acks=all` with the idempotent producer -- the same durability floor a real receiver uses. Returns
    the `tb-msg-id` of each in send order, for a caller to check against what a later drain observes."""
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    ids: list[str] = []
    for message in messages:
        ids.append(message.require(h.TB_MSG_ID))
        producer.produce(
            topic,
            key=message.key,
            value=message.payload,
            headers=[(name, value.encode()) for name, value in message.headers.items()],
        )
    assert producer.flush(30) == 0, "producer.flush timed out or left records undelivered"
    return ids


def drain_topic(
    bootstrap: str,
    topic: str,
    *,
    partitions: int,
    expected: int = 0,
    timeout_s: float = 30.0,
) -> list[Any]:
    """Read `topic` from the beginning until every partition has hit EOF and at least `expected`
    records have been collected.

    EOF, not the high watermark: the watermark counts transaction control records that are never handed
    to the application, so a watermark-based loop can fail to terminate (the same reasoning as
    `tests/chaos/test_kafka_broker_kill.py::_drain_raw`).
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([topic])
    collected: list[Any] = []
    eofs: set[int] = set()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            record = consumer.poll(1.0)
            if record is None:
                if collected and len(collected) >= expected:
                    break
                continue
            if record.error() is not None:
                eofs.add(record.partition())
                if len(eofs) >= partitions and len(collected) >= expected:
                    break
                continue
            collected.append(record)
    finally:
        consumer.close()
    return collected


def wait_for_at_least(bootstrap: str, topic: str, *, partitions: int, count: int, timeout_s: float = 60.0) -> list[Any]:
    """Poll `topic` until it holds at least `count` records, or raise. Used to synchronise a fault
    injection (a kill, a partition, a stop) against real progress instead of a fixed sleep."""
    deadline = time.monotonic() + timeout_s
    last: list[Any] = []
    while time.monotonic() < deadline:
        last = drain_topic(bootstrap, topic, partitions=partitions, expected=count, timeout_s=5.0)
        if len(last) >= count:
            return last
        time.sleep(0.5)
    raise AssertionError(f"{topic} did not reach {count} records within {timeout_s}s (last saw {len(last)})")


def first_delivery_order(records: list[Any], *, sequence_header: str) -> dict[bytes, list[int]]:
    """Per key, the sequence numbers in the order they were FIRST delivered (S5's ordering check,
    PHASE-0-DECISION section 3.1): a redelivered sequence number is a duplicate, not a reordering, which
    is exactly what at-least-once means and why invariant 4 requires loader idempotency on `tb-msg-id`.
    """
    order: dict[bytes, list[int]] = {}
    for record in records:
        key = record.key()
        sequence = int(header_map(record)[sequence_header])
        seen = order.setdefault(key, [])
        if sequence not in seen:
            seen.append(sequence)
    return order
