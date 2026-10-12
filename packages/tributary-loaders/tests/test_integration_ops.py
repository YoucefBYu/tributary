"""W3-26 / W3-28b against a real broker: the real `python -m tributary.loaders.http` process serves `/livez` and
`/metrics` on its ops port, and `tb_deliveries_total` counts a genuine redelivery (the destination fails the first
attempt, `ConsumerLoop` rewinds the uncommitted offset and delivers again). A loader started without an instance id
exits 2. Synthetic data only.

`uv run pytest -m integration packages/tributary-loaders -k test_integration_ops` (Docker).
"""

from __future__ import annotations

import http.client
import json
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.engine_kafka import engine_kafka_env, receiver_env
from tests.support.http_stub import StubHttpServer
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import ManagedProcess

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "loader-metrics"
DESTINATION = "http-stub"
CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "observability" / "v0" / "metrics.json"


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_name(f"tb-connector-w326-loader-kafka-{uuid.uuid4().hex[:8]}")
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
        .with_env("CLUSTER_ID", "loader-ops-integration")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


def loader_args(instance: str | None) -> list[str]:
    args = [sys.executable, "-m", "tributary.loaders.http", "--channel", CHANNEL, "--destination", DESTINATION]
    args += ["--env", ENV]
    return args + (["--instance-id", instance] if instance else [])


def get(port: int, path: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        return response.status, response.read().decode()
    finally:
        conn.close()


def series(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.rpartition(" ")
            out[name] = float(value)
    return out


def test_a_loader_without_an_instance_id_exits_2(kafka: str) -> None:
    proc = ManagedProcess(loader_args(None), env=receiver_env(engine_kafka_env(kafka)))
    assert proc.process.wait(timeout=30) == 2
    proc.collect_pending()
    refused = [r for r in proc.lines if r.get("event") == "startup_refused"]
    assert [r["code"] for r in refused] == ["INSTANCE_REQUIRED"]


def test_metrics_scrape_counts_a_failed_attempt_and_its_redelivery(kafka: str) -> None:
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), 1, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for future in admin.create_topics(wanted).values():
        future.result(timeout=30)
    stub_port = free_port()
    document: dict[str, Any] = walking_skeleton_document(
        mllp_port=free_port(),
        http_url=f"http://127.0.0.1:{stub_port}/ingest",
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0]["retry"] = {
        "policy": "blocking",
        "backoff": "fixed",
        "initialDelayMs": 100,
        "maxDelayMs": 100,
        "maxAttempts": 10,
        "onExhaust": "halt-partition",
    }
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    message = make_message(
        json.dumps({"marker": "loader-metrics"}).encode(),
        key=None,
        channel=CHANNEL,
        destination=DESTINATION,
        content_type="application/json",
    )
    publish_messages(kafka, out_topic(ENV, CHANNEL, DESTINATION), [message])

    stub = StubHttpServer(port=stub_port, fail_first_n=1)
    stub.start()
    loader = ManagedProcess(loader_args("loader-metrics-0"), env=receiver_env(engine_kafka_env(kafka)))
    try:
        loader.wait_for_event("loader_started", timeout_s=60)
        port = next(int(r["ops_port"]) for r in loader.lines if r.get("event") == "ops_listening")
        deadline = time.monotonic() + 60
        while len(stub.requests) < 2 and time.monotonic() < deadline:
            time.sleep(0.2)
        assert len(stub.requests) == 2, "the forced failure and its redelivery"

        assert get(port, "/livez")[0] == 200
        assert get(port, "/readyz")[0] == 404  # a loader contracts no readiness
        scope = f'channel="{CHANNEL}",destination="{DESTINATION}",destination_type="http"'
        deadline = time.monotonic() + 30
        while True:
            status, text = get(port, "/metrics")
            counted = series(text)
            ok = counted.get(f'tb_deliveries_total{{{scope},outcome="success",code="NONE"}}', 0)
            if ok >= 1 or time.monotonic() > deadline:
                break
            time.sleep(0.2)
        assert status == 200
        assert counted[f'tb_deliveries_total{{{scope},outcome="success",code="NONE"}}'] == 1
        assert counted[f'tb_deliveries_total{{{scope},outcome="failure",code="HTTP_503"}}'] == 1
        assert counted[f'tb_spec_version{{component="loader",channel="{CHANNEL}"}}'] == 1
        registered = {m["name"] for m in json.loads(CONTRACT.read_text(encoding="utf-8"))["metrics"]}
        names = {n.split("{")[0].removesuffix("_bucket").removesuffix("_sum").removesuffix("_count") for n in counted}
        assert names <= registered
    finally:
        loader.terminate_and_wait()
        stub.stop()
