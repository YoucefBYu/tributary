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

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from tributary.sdk import Message
from tributary.sdk import headers as h


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
