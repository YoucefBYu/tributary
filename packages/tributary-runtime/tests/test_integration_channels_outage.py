"""`_tb.<env>.channels` through a real broker outage: the cache says it may be stale and keeps serving.

The unit tests (`test_channels.py`) script `list_topics`; this one stops and restarts a real Kafka 4.0
broker under a real `confluent_kafka.Consumer` built by the runtime's own `consumer_config`, because the
behaviour it relies on is librdkafka's and was found by measurement, not documentation: during an outage
`poll()` returns no error event at all, and after one it returns nothing either (module docstring of
`tributary.runtime.channels`). If a librdkafka upgrade changes that, this is the test that notices.

`uv run pytest -m integration packages/tributary-runtime/tests/test_integration_channels_outage.py`
"""

from __future__ import annotations

import io
import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.channels import LiveSpecSource, open_spec_source
from tributary.runtime.clients import KafkaSettings, consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.errors import ChannelsUnavailableError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import channels_topic

from tests.support.kafka_helpers import await_broker, create_topics_with_retry, free_port

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough"
EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "channel-spec"
    / "v0"
    / "examples"
    / "valid"
    / "walking-skeleton-loopback.json"
)


@pytest.fixture(scope="module")
def broker() -> Iterator[tuple[DockerContainer, str]]:
    """Single-node KRaft, lite profile. A fixed host port, so the restarted broker is at the same address."""
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
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield container, bootstrap
    finally:
        container.stop()


def publish_spec(bootstrap: str, *, version: int) -> None:
    document = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "name": CHANNEL, "version": version}
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0


def poll_until(source: LiveSpecSource, *, raising: bool, timeout_s: float) -> float:
    """Poll the cache like the MLLP receiver's feed does until it does (or stops) raising. Returns seconds."""
    started = time.monotonic()
    while time.monotonic() - started < timeout_s:
        try:
            source.cache.poll_once(timeout_s=0.1)
        except ChannelsUnavailableError:
            if raising:
                return time.monotonic() - started
        else:
            if not raising:
                return time.monotonic() - started
    raise AssertionError(f"cache {'never reported' if raising else 'never recovered from'} the outage")


def logged(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    return [line for line in map(json.loads, stream.getvalue().splitlines()) if line["event"] == event]


def test_a_broker_outage_is_reported_and_the_last_spec_keeps_serving(broker: tuple[DockerContainer, str]) -> None:
    container, bootstrap = broker
    create_topics_with_retry(bootstrap, [NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"})])
    publish_spec(bootstrap, version=1)
    stream = io.StringIO()
    consumer: Any = Consumer(
        consumer_config(
            KafkaSettings(bootstrap_servers=bootstrap),
            load_topic_defaults(),
            group_id="tb.dev.spec-reader.outage-test",
            enable_partition_eof=True,
        )
    )
    source = open_spec_source(
        channel=CHANNEL,
        consumer=consumer,
        topic=channels_topic(ENV),
        log=JsonLogger("test", stream=stream),
        probe_interval_s=1.0,
        probe_timeout_s=0.5,
    )
    try:
        assert source.current().version == 1 and source.available

        container.get_wrapped_container().stop(timeout=5)
        detected_after_s = poll_until(source, raising=True, timeout_s=20.0)
        assert source.available is False
        assert source.current().version == 1, "invariant 3: the applied spec is served through the outage"
        assert len(logged(stream, "channels_unavailable")) == 1

        container.get_wrapped_container().start()
        recovered_after_s = poll_until(source, raising=False, timeout_s=90.0)
        assert source.available is True
        assert len(logged(stream, "channels_recovered")) == 1

        # Hot reload works again after the outage, on the same consumer.
        await_broker(bootstrap)
        publish_spec(bootstrap, version=2)
        deadline = time.monotonic() + 30.0
        while source.current().version != 2:
            assert time.monotonic() < deadline, "the republished spec never arrived after recovery"
            time.sleep(0.1)
        print(f"outage detected after {detected_after_s:.1f}s, recovery after {recovered_after_s:.1f}s")
    finally:
        source.close()
