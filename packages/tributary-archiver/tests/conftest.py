"""Fail a hung test fast instead of burning CI time (same layer as `tributary-loaders/tests/conftest.py`)."""

from __future__ import annotations

import faulthandler
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _hang_guard() -> Iterator[None]:
    faulthandler.dump_traceback_later(300, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()


@pytest.fixture(scope="module")
def kafka_bootstrap() -> Iterator[str]:
    """A real single-node KRaft Kafka (own container per module). Used by the W3-3 integration tests."""
    from testcontainers.core.container import DockerContainer

    from tests.support.kafka_helpers import await_broker, free_port

    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093")
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
        .with_env("CLUSTER_ID", "archiver-w33")
        .with_bind_ports(port, port)  # the broker dials its own advertised address (transaction verification)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def ch_container() -> Iterator[object]:
    from testcontainers.community.clickhouse import ClickHouseContainer

    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container
