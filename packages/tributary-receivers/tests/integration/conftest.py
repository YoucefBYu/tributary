"""One single-node Kafka 4.x (KRaft, RF=1: the lite profile) shared by the receivers' integration
tests. `DockerContainer` rather than testcontainers' `KafkaContainer` for the reason given in
`tributary-runtime`'s `test_integration_kafka.py`. Broker readiness and topic creation come from the
shared `tests.support.kafka_helpers` (imported, not modified)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer

from tests.support.kafka_helpers import await_broker, free_port


@pytest.fixture(autouse=True)
def _ephemeral_ops_port(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every spawned receiver binds its ops listener (`contracts/observability/v0` O2) on an ephemeral port, so
    two receivers in one test, or one left over from another run, never collide on 9180."""
    monkeypatch.setenv("TB_OPS_PORT", "0")


def _broker(port: int, name: str | None = None) -> DockerContainer:
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
        .with_env("CLUSTER_ID", "receivers-integration")
        .with_bind_ports(9092, port)
    )
    return container if name is None else container.with_name(name)


@pytest.fixture(scope="session")
def kafka() -> Iterator[str]:
    port = free_port()
    container = _broker(port)
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture
def private_kafka() -> Iterator[tuple[str, DockerContainer]]:
    """A broker of this test's own, that it may stop and start (the shared one must stay up). Uniquely named;
    only this fixture's container is ever touched."""
    port = free_port()
    container = _broker(port, f"tb-receivers-ready-{uuid.uuid4().hex[:12]}")
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap, container
    finally:
        container.stop()
