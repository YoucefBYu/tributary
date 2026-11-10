"""W2-9 duplicate-delivery harness, `s3` destination: invariant 4 for the S3 loader against
`contracts/channel-spec/v0/README.md` 5.1's bar for wave 2 exit criterion 3 -- *"deliver the same `out`
record twice -- the second as a genuine redelivery from an uncommitted offset, not a second call in the
same process -- and assert what the sink holds."*

**The fault, and why it is genuine.** One record is published once to `out.<dest>`. The real
`python -m tributary.loaders.s3` process delivers it to a **real MinIO server** (`testcontainers`): the
object lands. The test then **SIGKILLs the loader before it commits its Kafka offset** (`commit_every` is
50, so a single record is only committed at a graceful stop): the sink applied the write, the source offset
did not move. A second loader process, same static instance id, resumes from the uncommitted offset and
receives the *same record* again.

The assertions: the object existed and the group's committed offset was still 0 when the first loader died
(the fault fired); the replayed delivery was absorbed as a duplicate by MinIO's own conditional-put `412`
(the second loader's `delivery_duplicate` counter, not an inference from the bucket); the bucket holds
exactly one object for the message id, byte-identical to the payload; and the offset is committed only
after that confirmed delivery.

**Broken-implementation control.** `test_a_reprocessed_copy_with_a_new_msg_id_is_a_second_object` shows the
key really is derived from `tb-msg-id`: the same bytes under a new id (what reprocess mints, README 5) are
a second object, so "exactly one" above is not the bucket refusing to grow.
"""

from __future__ import annotations

import base64
import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.loaders.s3.client import build_client
from tributary.loaders.s3.config import S3DestinationConfig
from tributary.loaders.s3.loader import S3Loader
from tributary.loaders.secrets import DirSecretResolver
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, committed_with_retry, free_port, publish_messages
from tests.support.processes import ManagedProcess, spawn_s3_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-s3-duplicate-delivery"
DESTINATION = "s3-out"
PARTITIONS = 3
ROOT_USER = "synthminio"
ROOT_LOGIN = uuid.uuid4().hex  # a fresh throwaway login for a container that lives for this module
CREDS_DIR = "s3-out-creds"  # a secretRef name
BUCKET = "tributary-dup"
INSTANCE = "loader-s3-dup"


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
        .with_env("CLUSTER_ID", "dup-delivery-sftp")
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


class Minio:
    def __init__(self, container: DockerContainer, secrets: Path) -> None:
        self.endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        self.secrets = secrets

    def client(self) -> Any:
        return build_client(self.config("adt"), DirSecretResolver(self.secrets))

    def config(self, prefix: str) -> S3DestinationConfig:
        return S3DestinationConfig(
            channel=CHANNEL,
            destination=DESTINATION,
            bucket=BUCKET,
            region="us-east-1",
            prefix=prefix,
            endpoint=self.endpoint,
            sse="AES256",
            kms_key_id=None,
            auth_type="access-key",
            auth_secret_ref=CREDS_DIR,
            file_extension=".hl7",
            timeout_s=10.0,
            circuit_breaker=CircuitBreakerSpec(5, 30.0),
            idempotency_technique="deterministic-name",
            backoff="fixed",
            initial_delay_ms=10,
            max_delay_ms=100,
            max_attempts=3,
            on_exhaust="halt-partition",
        )

    def keys(self, containing: str) -> list[str]:
        listing = self.client().list_objects_v2(Bucket=BUCKET)
        return [o["Key"] for o in listing.get("Contents", []) if containing in o["Key"]]


@pytest.fixture(scope="module")
def minio(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Minio]:
    secrets = tmp_path_factory.mktemp("s3-dup") / "secrets"
    (secrets / CREDS_DIR).mkdir(parents=True)
    (secrets / CREDS_DIR / "access_key_id").write_text(ROOT_USER, encoding="utf-8")
    (secrets / CREDS_DIR / "secret_access_key").write_text(ROOT_LOGIN, encoding="utf-8")
    kms_key = "test-key:" + base64.b64encode(os.urandom(32)).decode()  # SSE-S3 needs a KMS key configured
    container = (
        DockerContainer("cgr.dev/chainguard/minio:latest")
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", ROOT_USER)
        .with_env("MINIO_ROOT_PASSWORD", ROOT_LOGIN)
        .with_env("MINIO_KMS_SECRET_KEY", kms_key)
        .with_command("server /data --address :9000")
        .waiting_for(HttpWaitStrategy(9000, "/minio/health/live").with_startup_timeout(60))
    )
    with container:
        store = Minio(container, secrets)
        store.client().create_bucket(Bucket=BUCKET)
        yield store


@pytest.fixture(scope="module")
def deployed_spec(topics: str, minio: Minio) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "s3",
        "with": {
            "bucket": BUCKET,
            "region": "us-east-1",
            "prefix": "adt",
            "endpoint": minio.endpoint,
            "serverSideEncryption": "AES256",
            "auth": {"type": "access-key", "secretRef": CREDS_DIR},
            "fileExtension": ".hl7",
            "timeoutMs": 10000,
        },
        "idempotency": {"technique": "deterministic-name"},
        "retry": {
            "policy": "blocking",
            "backoff": "fixed",
            "initialDelayMs": 100,
            "maxDelayMs": 100,
            "maxAttempts": 10,
            "onExhaust": "halt-partition",
        },
    }
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def _committed_total(bootstrap: str) -> int:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": consumer_group(ENV, CHANNEL, "loader", DESTINATION)}
    )
    try:
        partitions = [TopicPartition(out_topic(ENV, CHANNEL, DESTINATION), p) for p in range(PARTITIONS)]
        return sum(max(tp.offset, 0) for tp in committed_with_retry(consumer, partitions, timeout_s=10))
    finally:
        consumer.close()


def _spawn(topics: str, minio: Minio) -> ManagedProcess:
    return spawn_s3_loader(
        channel=CHANNEL,
        destination=DESTINATION,
        env=ENV,
        bootstrap=topics,
        instance_id=INSTANCE,
        secrets_dir=minio.secrets,
    )


def test_a_loader_killed_after_the_write_and_before_the_commit_replays_into_exactly_one_object(
    topics: str, minio: Minio, deployed_spec: dict[str, Any]
) -> None:
    payload = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC\rPID|1||SYNTH-0001\r"
    message = make_message(
        payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2"
    )
    assert len(publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])) == 1

    first = _spawn(topics, minio)
    try:
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and not minio.keys(message.msg_id):
            time.sleep(0.2)
    finally:
        first.kill_and_wait()  # SIGKILL: no graceful commit
    committed_after_kill = _committed_total(topics)
    assert_fault_fired(
        bool(minio.keys(message.msg_id)) and committed_after_kill == 0,
        description=f"expected the object applied and the offset uncommitted, got committed={committed_after_kill}",
    )

    second = _spawn(topics, minio)
    try:
        second.wait_for_event("partitions_assigned", timeout_s=60.0)
        time.sleep(6.0)  # one small record: consumed, found already present (412), and counted
    finally:
        second.terminate_and_wait()
    second.collect_pending()
    exits = [line for line in second.lines if line.get("event") == "loader_exit"]
    metrics = exits[-1]["delivery_metrics"] if exits else {}
    assert_fault_fired(
        metrics.get("delivery_duplicate") == 1,
        description=f"the replay was not absorbed as a duplicate by the second loader: {metrics!r}",
    )

    keys = minio.keys(message.msg_id)
    assert len(keys) == 1, "exactly one object for the message id"
    body = minio.client().get_object(Bucket=BUCKET, Key=keys[0])["Body"].read()
    assert body == payload
    assert _committed_total(topics) == 1, "the offset must be committed only after the confirmed delivery"


def test_a_reprocessed_copy_with_a_new_msg_id_is_a_second_object(minio: Minio) -> None:
    """Broken-implementation control (module docstring): the key is derived from `tb-msg-id`, so the same
    bytes under a new id are a different object."""
    loader = S3Loader(minio.config("ctl"), minio.client())
    payload = b"same bytes\r"
    a = make_message(payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2")
    b = make_message(payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2")
    try:
        assert loader.deliver(a).success and loader.deliver(b).success
    finally:
        loader.close()
    assert len(minio.keys(a.msg_id)) == 1 and len(minio.keys(b.msg_id)) == 1
