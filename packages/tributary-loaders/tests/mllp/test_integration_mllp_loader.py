"""W2-9-style genuine-redelivery harness for the `mllp` destination, matching
`tests.integration.test_http_loader_duplicate_delivery_uncommitted_redelivery`'s reference shape
(`contracts/channel-spec/v0/README.md` 5.1, wave 2 exit criterion 3): one record, published once to
`out.<dest>`, delivered through the real `python -m tributary.loaders.mllp` entry point
(`tests.support.processes.spawn_mllp_loader`) against a destination that fails the *first* real attempt
and succeeds on the second -- so the second delivery is `ConsumerLoop`'s real blocking-retry
pause/seek/resume cycle reading the same Kafka offset again, not a second in-process call.

**The destination is a real in-process MLLP listener** (`test_client.FakeMllpServer`, the same
hand-rolled TCP server `test_client.py`/`test_loader.py` already drive) -- a real socket accepting a
real MLLP frame and writing back a real ACK frame, not a stub `MllpConnection`. **Kafka is a real
`testcontainers` broker** -- the engine's own cluster the loader reads `out.<dest>`/
`_tb.<env>.channels` from, same as every other loader's redelivery harness in this repo.

**What README 5.1 allows this test to assert.** For `mllp`, "where the sink is someone else's, the test
asserts that both deliveries carry the same tb-msg-id, which is all the engine can promise" -- here,
the same `MSH-10` control id, since `loader.py` never rewrites `MSH` and the wire protocol has no
`tb-msg-id` header of its own. This test cannot and does not assert anything about deduplication at the
destination (`config.py`'s module docstring: no MLLP-side idempotency proof exists for this loader to
make).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.loaders.mllp.framing import control_id, frame
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import spawn_mllp_loader

from .test_client import FakeMllpServer

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-mllp-duplicate-delivery"
DESTINATION = "downstream-lis"
PARTITIONS = 3

#: Short and bounded: this scenario only needs to survive exactly one forced negative acknowledgement.
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 100,
    "maxDelayMs": 100,
    "maxAttempts": 10,
    "onExhaust": "halt-partition",
}

MSH = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|RECV|FAC2|20260101000000||ADT^A01|CTRL-DUP-1|P|2.5"


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
        .with_env("CLUSTER_ID", "dup-delivery-mllp")
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


@pytest.fixture
def mllp_server() -> Iterator[tuple[FakeMllpServer, Callable[[], list[str]]]]:
    """The real in-process MLLP listener this scenario's destination is: `AE` (retryable, per
    `onNegativeAck: "retry"`) for the first attempt at a given control id, `AA` for every attempt after
    that -- driving a genuine `ConsumerLoop` blocking-retry cycle, not a fixed script length."""
    seen: dict[str, int] = {}
    received_control_ids: list[str] = []

    def respond(message: bytes) -> bytes:
        ctrl = control_id(message)
        received_control_ids.append(ctrl)
        attempt = seen.get(ctrl, 0) + 1
        seen[ctrl] = attempt
        code = "AE" if attempt == 1 else "AA"
        return frame(f"MSH|^~\\&\rMSA|{code}|{ctrl}".encode())

    server = FakeMllpServer(respond)
    try:
        yield server, lambda: list(received_control_ids)
    finally:
        server.close()


@pytest.fixture
def deployed_spec(topics: str, mllp_server: tuple[FakeMllpServer, Callable[[], list[str]]]) -> dict[str, Any]:
    server, _ = mllp_server
    document: dict[str, Any] = {
        "apiVersion": "tributary.dev/v1",
        "kind": "Channel",
        "metadata": {"name": CHANNEL, "version": 1, "description": "MLLP loader duplicate-delivery harness"},
        "spec": {
            "ordering": "none",
            "source": {
                "type": "mllp",
                "bind": "127.0.0.1",
                "port": free_port(),  # unused: this test publishes straight to out.<dest>
                "ack": {"mode": "after-commit", "nackOn": ["parse-error", "queue-full"]},
            },
            "pipeline": {
                "bundle": "ghcr.io/tributary-synth/adt-integration@sha256:3f7a1c9e2b5d48a06e1f8c3d7b2a95e04c6d8f1a3b5c7d9e0f2a4b6c8d0e2f46",
                "steps": [{"name": "passthrough", "uses": "tributary.core.passthrough"}],
            },
            "destinations": [
                {
                    "name": DESTINATION,
                    "type": "mllp",
                    "with": {
                        "host": "127.0.0.1",
                        "port": server.port,
                        "timeoutMs": 5000,
                        "onNegativeAck": "retry",
                    },
                    "idempotency": {
                        "technique": "none",
                        "note": "Test stub counts deliveries; duplicates are the assertion, not a defect.",
                    },
                    "retry": dict(RETRY_OVERRIDE),
                }
            ],
        },
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def test_a_blocking_retry_redelivers_from_the_uncommitted_offset_and_the_loader_sends_one_identity(
    topics: str, deployed_spec: dict[str, Any], mllp_server: tuple[FakeMllpServer, Callable[[], list[str]]]
) -> None:
    _server, received_control_ids = mllp_server
    message = make_message(MSH, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2")
    published_ids = publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])
    assert len(published_ids) == 1

    loader = spawn_mllp_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics)
    try:
        # The fault, confirmed rather than assumed: the loader must actually observe the forced AE and
        # enter blocking retry before a second frame at this server is trusted as a genuine redelivery
        # rather than a fluke of two unrelated sends.
        loader.wait_for_event("partition_paused", timeout_s=30.0)

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and len(received_control_ids()) < 2:
            time.sleep(0.2)
    finally:
        loader.terminate_and_wait()

    attempts = received_control_ids()
    assert_fault_fired(
        len(attempts) == 2,
        description=f"expected exactly 2 attempts (one forced AE, one redelivery), got {len(attempts)}",
    )
    assert attempts[0] == attempts[1] == control_id(MSH), (
        "both deliveries must carry the identical MSH-10 control id -- this is a redelivery of the same "
        "message, not two different ones (README 5.1: 'the same tb-msg-id, which is all the engine can "
        "promise' -- MSH-10 is this loader's only wire-visible identity, since it never rewrites MSH)"
    )
