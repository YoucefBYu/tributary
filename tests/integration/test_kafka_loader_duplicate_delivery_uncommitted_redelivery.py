"""W2-9-style duplicate-delivery harness, `kafka` destination: proves invariant 4 for the Kafka loader
against `contracts/channel-spec/v0/README.md` 5.1's bar for wave 2 exit criterion 3 -- *"deliver the
same `out` record twice -- the second as a genuine redelivery from an uncommitted offset, not a second
call in the same process -- and assert what the sink holds."*

`packages/tributary-loaders/tests/kafka/test_loader.py::test_duplicate_delivery_carries_the_same_header`
(unit-level, connector-engineer's) calls `KafkaLoader.deliver(message)` twice in the same process. This
is the missing half: one record, published once to `out.<dest>`, delivered through the real
`python -m tributary.loaders.kafka` entry point (`tests.support.processes.spawn_kafka_loader`) against a
**real** destination topic on a **real** broker.

**The fault, and why it looks different from the HTTP/PostgreSQL reference tests.** README 5.1 says
kafka's sink is "someone else's": there is no stub this test controls the way `StubHttpServer` or a
`testcontainers` PostgreSQL server can be paused mid-attempt. Instead this test uses a fault a real
broker genuinely produces: the destination topic does not exist yet when the loader's first delivery
attempt reaches the broker (`AUTO_CREATE_TOPICS_ENABLE=false` on both the "engine" and "destination"
connections -- **the same physical broker**, addressed through two different, explicit connections
purely to keep the test to one container; ADR-007 D11 is about the *connection* always being explicit
and separately credentialed, not about the physical topology). That first attempt genuinely reaches the
broker and genuinely fails (`_MSG_TIMED_OUT`, `loader.py`'s `TIMEOUT` classification) -- confirmed by
`partition_paused`, the real `ConsumerLoop._pause_and_rewind`/`_resume_due_partitions` blocking-retry
cycle, not a second in-process call. Once the test creates the destination topic, the SAME record is
redelivered from the SAME (still-uncommitted) offset and the second attempt lands.

**What this proves, and what it does not.** Because the first attempt never reaches durability (the
broker refuses it before any record is appended), only the *second* attempt's header can be read back
from the topic -- unlike HTTP's stub, which receives and records both attempts' bodies and headers even
though the first is answered with failure. `test_duplicate_delivery_carries_the_same_header` is what
proves this loader sends the *identical* `tb-msg-id` header value across attempts; this test proves the
other half README 5.1 asks for: that a **real** `ConsumerLoop` blocking-retry cycle against a **real**
broker fires, and that the record which does land carries the correct header, is not swallowed, and is
not duplicated once landed.
"""

from __future__ import annotations

import copy
import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import spawn_kafka_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-kafka-duplicate-delivery"
DESTINATION = "partner-feed"
EXTERNAL_TOPIC = "partner.lab-results"
PARTITIONS = 3

#: Short: `message.timeout.ms` (derived from `timeoutMs` by `kafka/client.py`) is the real fault clock
#: here (the missing-topic produce genuinely times out), so this only needs to survive one such cycle.
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 100,
    "maxDelayMs": 100,
    "maxAttempts": 20,
    "onExhaust": "halt-partition",
}


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "dup-delivery-kafka")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def topics(kafka: str) -> str:
    """Every engine-side topic, but deliberately NOT `EXTERNAL_TOPIC` -- that absence is the fault
    (module docstring)."""
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for _topic, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # pragma: no cover - already exists on a re-run
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return kafka


@pytest.fixture(scope="module")
def deployed_spec(topics: str) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run; see the module docstring
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "kafka",
        "with": {
            "connection": {
                "bootstrapServers": [topics],
                "security": {"protocol": "PLAINTEXT"},
            },
            "topic": EXTERNAL_TOPIC,
            "key": "none",
            "timeoutMs": 3000,
        },
        "idempotency": {"technique": "idempotency-key-header", "header": "tb-msg-id"},
        "retry": dict(RETRY_OVERRIDE),
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return copy.deepcopy(document)


def test_a_blocking_retry_redelivers_from_the_uncommitted_offset_and_lands_one_record_with_the_right_header(
    topics: str, deployed_spec: dict[str, Any]
) -> None:
    message = make_message(
        json.dumps({"marker": "kafka-duplicate-delivery-harness"}).encode(),
        key=None,
        channel=CHANNEL,
        destination=DESTINATION,
        content_type="application/json",
    )
    published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
    assert len(published_ids) == 1

    loader = spawn_kafka_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics)
    try:
        # The fault, confirmed rather than assumed: the loader must actually observe a real,
        # broker-returned failure (the missing topic) and enter blocking retry before a later
        # successful produce can be trusted as a genuine redelivery.
        paused = loader.wait_for_event("partition_paused", timeout_s=30.0)
        assert paused["error_code"] == "TIMEOUT"

        admin = AdminClient({"bootstrap.servers": topics})
        for _topic, future in admin.create_topics([NewTopic(EXTERNAL_TOPIC, 1, 1)]).items():
            future.result(timeout=30)

        records = _consume_all(topics, EXTERNAL_TOPIC, expected=1, timeout_s=30.0)
    finally:
        loader.terminate_and_wait()

    assert_fault_fired(
        len(records) >= 1, description=f"expected at least 1 record on {EXTERNAL_TOPIC!r} after the retry, got 0"
    )
    assert len(records) == 1, (
        "the destination topic did not exist for the first attempt, so only the successful redelivery "
        "could ever land a record -- more than one means something replayed after commit, invariant 4"
    )
    landed = records[0]
    assert landed.value() == message.payload
    header_map = dict(landed.headers() or [])
    assert header_map.get("tb-msg-id") == message.msg_id.encode("utf-8")


def _consume_all(bootstrap: str, topic: str, *, expected: int, timeout_s: float) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "kafka-duplicate-delivery-test-reader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    records: list[Any] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and len(records) < expected:
            record = consumer.poll(1.0)
            if record is None:
                continue
            if record.error() is not None:
                continue
            records.append(record)
    finally:
        consumer.close()
    return records
