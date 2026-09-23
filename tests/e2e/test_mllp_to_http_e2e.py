"""W1-8 verify line: a synthetic ADT message sent over MLLP reaches a stub HTTP server unchanged,
through the real receiver, the real pipeline worker and the real HTTP loader.

Marked `integration`: it needs Docker (a Kafka broker via testcontainers) and spawns three real
subprocesses running the actual entry points `deploy/compose/docker-compose.yml` runs —
`tributary.receivers.mllp`, the pipeline worker, `tributary.loaders.http`. `uv run pytest -m
integration tests/e2e`.

Two things stand in for pieces wave 1 has not built yet, both intentional and both documented where
they live:

* The channel's pipeline step resolves to `tests.support.demo_steps.passthrough` rather than the
  contract example's `adt_flow.steps.passthrough` — nothing in the repo can fetch an OCI bundle yet, and
  no package ships `adt_flow` (confirmed against the compose stack's own worker image; reported to
  architect/devops-engineer 2026-09-23). Wave 1 has no HL7-to-FHIR mapping regardless, so a no-op step
  is the correct stand-in, not a shortcut around what this test proves.
* Kafka is single-node/RF=1 (the lite profile), built the same way
  `packages/tributary-runtime/tests/test_integration_kafka.py` builds its testcontainers broker
  (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` included — the compose stack's own kafka service is
  missing that and cannot form a consumer group at all; also reported 2026-09-23). The chaos test in
  `tests/chaos` is where a 3-broker cluster belongs.

Assertions, in order: the stub sees nothing before we send anything (so "it received something" is not
vacuously true); the receiver ACKs with `MSA|AA` carrying our control id; the stub's request carries
the pass-through payload byte-for-byte, the receiver's `Content-Type`, and our control id — tying the
observed HTTP delivery back to the specific message this test sent, not a leftover from another run.
"""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic

from benchmarks.gen import generate
from tests.support.channel_spec import walking_skeleton_document, write_channel_spec_file
from tests.support.http_stub import StubHttpServer
from tests.support.mllp_client import is_application_accept, send_mllp
from tests.support.processes import ManagedProcess, spawn_http_loader, spawn_mllp_receiver, spawn_pipeline_worker

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough"
DESTINATION = "http-stub"
PARTITIONS = 3


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _await_broker(bootstrap: str, *, timeout_s: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            return
        except Exception as exc:  # any failure here means the broker is not ready yet
            last = exc
            time.sleep(1.0)
    raise RuntimeError(f"Kafka did not become reachable: {type(last).__name__ if last else 'timeout'}")


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    """Single-node Kafka 4.x in KRaft mode, the lite profile, same as
    `packages/tributary-runtime/tests/test_integration_kafka.py::kafka` (see that fixture for why
    `apache/kafka:4.0.0` directly rather than testcontainers' `KafkaContainer`)."""
    port = _free_port()
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
        .with_env("CLUSTER_ID", "e2e-mllp-http-skeleton")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        _await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def topics(kafka: str) -> str:
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
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
def stub() -> Iterator[StubHttpServer]:
    server = StubHttpServer()
    server.start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def mllp_port() -> int:
    return _free_port()


@pytest.fixture(scope="module")
def channel_spec_document(mllp_port: int, stub: StubHttpServer) -> dict[str, Any]:
    return walking_skeleton_document(
        mllp_port=mllp_port, http_url=stub.url(), channel=CHANNEL, destination=DESTINATION, version=1
    )


@pytest.fixture(scope="module")
def channel_spec_file(tmp_path_factory: pytest.TempPathFactory, channel_spec_document: dict[str, Any]) -> Path:
    path = tmp_path_factory.mktemp("e2e-channel-spec") / "channel.json"
    return write_channel_spec_file(path, channel_spec_document)


@pytest.fixture(scope="module")
def deployed_spec(topics: str, channel_spec_document: dict[str, Any]) -> dict[str, Any]:
    """Publish the channel spec to `_tb.<env>.channels` — the worker's and loader's only source of
    truth (invariant 3), same as `deploy/compose/docker-compose.yml`'s `channel-publish` job."""
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(channel_spec_document).encode())
    assert producer.flush(30) == 0
    return channel_spec_document


@pytest.fixture(scope="module")
def receiver(topics: str, channel_spec_file: Path) -> Iterator[ManagedProcess]:
    proc = spawn_mllp_receiver(channel_spec=channel_spec_file, env=ENV, bootstrap=topics, instance="mllp-e2e")
    try:
        yield proc
    finally:
        proc.terminate_and_wait()


@pytest.fixture(scope="module")
def worker(topics: str, deployed_spec: dict[str, Any]) -> Iterator[ManagedProcess]:
    proc = spawn_pipeline_worker(env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-e2e")
    try:
        yield proc
    finally:
        proc.terminate_and_wait()


@pytest.fixture(scope="module")
def loader(topics: str, deployed_spec: dict[str, Any]) -> Iterator[ManagedProcess]:
    proc = spawn_http_loader(
        channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics, instance_id="loader-e2e"
    )
    try:
        yield proc
    finally:
        proc.terminate_and_wait()


def test_synthetic_adt_flows_from_mllp_to_the_http_stub(
    mllp_port: int,
    stub: StubHttpServer,
    receiver: ManagedProcess,
    worker: ManagedProcess,
    loader: ManagedProcess,
) -> None:
    assert stub.requests == [], "nothing must have reached the stub before this test sends anything"

    message = next(iter(generate(1, seed=2026)))
    assert message.startswith("MSH|^~\\&|TRIBUTARY-SYNTH|"), "hook-enforced: MSH-3 must be TRIBUTARY-SYNTH"
    control_id = message.split("\r", 1)[0].split("|")[9]
    assert control_id, "the generator always sets MSH-10; a blank control id breaks this test's own tracing"

    ack = send_mllp("127.0.0.1", mllp_port, message.encode("ascii"), timeout_s=15.0)
    assert is_application_accept(ack), f"expected MSA|AA, got {ack!r}"
    assert control_id.encode("ascii") in ack, "the ACK must reference the message it is acknowledging"

    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline and not stub.requests:
        time.sleep(0.2)

    delivered = stub.requests
    assert delivered, (
        f"the stub never received a delivery within 30s; worker output: {worker.lines}; loader output: {loader.lines}"
    )
    assert len(delivered) == 1, f"expected exactly one delivery for one message, got {delivered}"
    request = delivered[0]
    assert request.method == "POST"
    assert request.path == "/ingest"
    assert request.headers.get("Content-Type") == "application/hl7-v2+er7"
    assert request.body == message.encode("ascii"), "wave 1 is pass-through: no HL7-to-FHIR mapping yet"
    assert control_id.encode("ascii") in request.body
