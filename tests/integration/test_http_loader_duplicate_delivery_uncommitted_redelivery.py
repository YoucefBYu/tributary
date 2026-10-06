"""W2-9 duplicate-delivery harness, HTTP reference implementation: proves invariant 4 for the HTTP
loader against `contracts/channel-spec/v0/README.md` 5.1's exact bar for wave 2 exit criterion 3 --
*"deliver the same `out` record twice -- the second as a genuine redelivery from an uncommitted offset,
not a second call in the same process -- and assert what the sink holds."*

`packages/tributary-loaders/tests/http/test_loader.py::test_duplicate_delivery_is_harmless` (unit-level,
connector-engineer's) calls `HttpLoader.deliver(message)` twice in the same process. That is a real and
useful test of the loader's own behaviour (it proves the loader sends the same idempotency header every
attempt), but it is not what README 5.1 now asks for: it never exercises `ConsumerLoop`'s blocking-retry
pause/seek/resume, so it cannot catch a bug in *that* path -- the actual mechanism that produces a
redelivery in production. This test is the missing half: one record, published once, delivered through
the real `python -m tributary.loaders.http` entry point
(`tests.support.processes.spawn_http_loader`) against a destination
(`tests.support.http_stub.StubHttpServer(fail_first_n=1)`) that fails the *first* real HTTP attempt with
a genuine 503 and succeeds on the second -- so the second delivery is the loader's real
`ConsumerLoop._begin_retry`/`_pause_and_rewind`/`_resume_due_partitions` cycle reading the same Kafka
offset again, not a test calling anything twice.

**Reference implementation for W2-6's remaining loaders**: `StubHttpServer.fail_first_n` and this test's
shape (publish once to `out.<dest>`, run the real loader against a destination that fails deterministically
N times, assert on what the destination captured) is the general pattern; a PostgreSQL or ClickHouse
version swaps the destination for a real testcontainers backend and a failure trigger connector-engineer
controls (e.g. a proxy/network fault, or a destination briefly paused) -- coordinate with qa-engineer
before building a second one, since a shared `tests/support` helper may be worth factoring out once a
second concrete case exists.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.http_stub import StubHttpServer
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import spawn_http_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-duplicate-delivery"
DESTINATION = "http-stub"
PARTITIONS = 3

#: Short and bounded: this scenario only needs to survive exactly one forced failure.
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 100,
    "maxDelayMs": 100,
    "maxAttempts": 10,
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
        .with_env("CLUSTER_ID", "dup-delivery-http")
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
def destination_port() -> int:
    return free_port()


@pytest.fixture(scope="module")
def deployed_spec(topics: str, destination_port: int) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url=f"http://127.0.0.1:{destination_port}/ingest",
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0]["retry"] = dict(RETRY_OVERRIDE)
    document["spec"]["destinations"][0]["idempotency"] = {
        "technique": "idempotency-key-header",
        "header": "Idempotency-Key",
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def test_a_blocking_retry_redelivers_from_the_uncommitted_offset_and_the_loader_keeps_one_identity(
    topics: str, deployed_spec: dict[str, Any], destination_port: int
) -> None:
    stub = StubHttpServer(port=destination_port, fail_first_n=1)
    stub.start()
    try:
        message = make_message(
            json.dumps({"marker": "duplicate-delivery-harness"}).encode(),
            key=None,
            channel=CHANNEL,
            destination=DESTINATION,
            content_type="application/json",
        )
        published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
        assert len(published_ids) == 1

        loader = spawn_http_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics)
        try:
            # The fault, confirmed rather than assumed: the loader must actually observe the forced
            # 503 and enter blocking retry before we trust that a second stub request is a genuine
            # redelivery and not a fluke of two unrelated successful sends.
            loader.wait_for_event("partition_paused", timeout_s=30.0)

            deadline_requests = 2
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline and len(stub.requests) < deadline_requests:
                time.sleep(0.2)
        finally:
            loader.terminate_and_wait()
    finally:
        stub.stop()

    requests = stub.requests
    assert_fault_fired(
        len(requests) == 2,
        description=f"expected exactly 2 requests (one forced failure, one redelivery), got {len(requests)}",
    )

    first, second = requests
    assert first.body == second.body == message.payload, (
        "both deliveries must carry the identical original record -- this is a redelivery of the same "
        "message, not two different ones"
    )
    first_key = first.headers.get("Idempotency-Key")
    second_key = second.headers.get("Idempotency-Key")
    assert first_key is not None and first_key == second_key, (
        "the loader must send the same idempotency key on every attempt at the same message "
        "(invariant 4), even when the redelivery came from a real Kafka offset replay rather than a "
        "second in-process call"
    )
    assert first_key == message.msg_id
