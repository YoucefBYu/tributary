"""W2-9 duplicate-delivery harness, `sftp` destination: invariant 4 for the SFTP loader against
`contracts/channel-spec/v0/README.md` 5.1's bar for wave 2 exit criterion 3 -- *"deliver the same `out`
record twice -- the second as a genuine redelivery from an uncommitted offset, not a second call in the
same process -- and assert what the sink holds."*

**The fault, and why it is genuine.** One record is published once to `out.<dest>`. The real
`python -m tributary.loaders.sftp` process delivers it to a **real OpenSSH server** (`atmoz/sftp`,
testcontainers): the file lands. The test then **SIGKILLs the loader before it commits its Kafka offset**
(`commit_every` is 50, so a single record is only committed at a graceful stop). That is the at-least-once
crash window in its purest form: the sink applied the write, the source offset did not move. A second
loader process, same static instance id, resumes from the uncommitted offset and receives the *same
record* again.

The assertions: the file existed and the group's committed offset was still 0 when the first loader died
(the fault fired); the replayed delivery was absorbed as a duplicate (the second loader's own
`delivery_duplicate` counter, not an inference from the directory); the directory holds exactly one file
for the message id, byte-identical to the payload, no `.part`; and the offset is committed only after that
confirmed delivery.

**Broken-implementation control.** `test_a_reprocessed_copy_with_a_new_msg_id_is_a_second_file` shows the
name really is keyed on `tb-msg-id`: the same bytes under a new id (what reprocess mints, README 5) are a
second file, so "exactly one" above is not the directory refusing to grow.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import paramiko
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.loaders.secrets import DirSecretResolver
from tributary.loaders.sftp.config import SftpDestinationConfig
from tributary.loaders.sftp.loader import SftpLoader
from tributary.loaders.sftp.session import connect, fingerprint, resolve_credentials
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import walking_skeleton_document
from tests.support.chaos_assertions import assert_fault_fired
from tests.support.kafka_helpers import await_broker, committed_with_retry, free_port, publish_messages
from tests.support.processes import ManagedProcess, spawn_sftp_loader

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "adt-passthrough-sftp-duplicate-delivery"
DESTINATION = "sftp-out"
PARTITIONS = 3
USER = "synth"
PASSWORD = uuid.uuid4().hex  # a fresh throwaway login for a container that lives for this module
CREDS_DIR = "sftp-out-creds"
INSTANCE = "loader-sftp-dup"


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


class Sftp:
    def __init__(self, container: DockerContainer, pin: str, secrets: Path) -> None:
        self.container = container
        self.host = container.get_container_host_ip()
        self.port = int(container.get_exposed_port(22))
        self.pin = pin
        self.secrets = secrets

    def listing(self) -> list[str]:
        result = self.container.exec(["ls", "-1", f"/home/{USER}/upload"])
        assert result.exit_code == 0, result.output
        return [line for line in result.output.decode().splitlines() if line]

    def read(self, name: str) -> bytes:
        return bytes(self.container.exec(["cat", f"/home/{USER}/upload/{name}"]).output)


@pytest.fixture(scope="module")
def sftp(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Sftp]:
    base = tmp_path_factory.mktemp("sftp-dup")
    host_key = paramiko.RSAKey.generate(2048)
    key_path = base / "ssh_host_rsa_key"
    host_key.write_private_key_file(str(key_path))
    key_path.chmod(0o600)
    secrets = base / "secrets"
    (secrets / CREDS_DIR).mkdir(parents=True)
    (secrets / CREDS_DIR / "username").write_text(USER, encoding="utf-8")
    (secrets / CREDS_DIR / "password").write_text(PASSWORD, encoding="utf-8")
    container = (
        DockerContainer("atmoz/sftp:alpine")
        .with_exposed_ports(22)
        .with_command(f"{USER}:{PASSWORD}:::upload")
        .with_volume_mapping(str(key_path), "/etc/ssh/ssh_host_rsa_key", "ro")
        .waiting_for(LogMessageWaitStrategy("Server listening on").with_startup_timeout(60))
    )
    with container:
        yield Sftp(container, fingerprint(host_key.asbytes()), secrets)


@pytest.fixture(scope="module")
def deployed_spec(topics: str, sftp: Sftp) -> dict[str, Any]:
    document = walking_skeleton_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/unused",  # this spec's source is never run
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0] = {
        "name": DESTINATION,
        "type": "sftp",
        "with": {
            "host": sftp.host,
            "port": sftp.port,
            "hostKeys": [sftp.pin],
            "auth": {"type": "password", "secretRef": CREDS_DIR},
            "directory": "/upload",
            "fileNamePrefix": "adt-",
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


def _spawn(topics: str, sftp: Sftp) -> ManagedProcess:
    return spawn_sftp_loader(
        channel=CHANNEL,
        destination=DESTINATION,
        env=ENV,
        bootstrap=topics,
        instance_id=INSTANCE,
        secrets_dir=sftp.secrets,
    )


def test_a_loader_killed_after_the_write_and_before_the_commit_replays_into_exactly_one_file(
    topics: str, sftp: Sftp, deployed_spec: dict[str, Any]
) -> None:
    payload = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC\rPID|1||SYNTH-0001\r"
    message = make_message(
        payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2"
    )
    name = f"adt-{message.msg_id}.hl7"
    assert len(publish_messages(topics, out_topic(ENV, CHANNEL, DESTINATION), [message])) == 1

    first = _spawn(topics, sftp)
    try:
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and name not in sftp.listing():
            time.sleep(0.2)
    finally:
        first.kill_and_wait()  # SIGKILL: no graceful commit
    committed_after_kill = _committed_total(topics)
    assert_fault_fired(
        name in sftp.listing() and committed_after_kill == 0,
        description=f"expected the file applied and the offset uncommitted, got committed={committed_after_kill}",
    )

    second = _spawn(topics, sftp)
    try:
        second.wait_for_event("partitions_assigned", timeout_s=60.0)
        time.sleep(6.0)  # one small record: consumed, found already present, and counted
    finally:
        second.terminate_and_wait()
    second.collect_pending()
    exits = [line for line in second.lines if line.get("event") == "loader_exit"]
    metrics = exits[-1]["delivery_metrics"] if exits else {}
    assert_fault_fired(
        metrics.get("delivery_duplicate") == 1,
        description=f"the replay was not absorbed as a duplicate by the second loader: {metrics!r}",
    )

    assert [f for f in sftp.listing() if message.msg_id in f] == [name], "exactly one file, and no .part"
    assert sftp.read(name) == payload
    assert _committed_total(topics) == 1, "the offset must be committed only after the confirmed delivery"


def test_a_reprocessed_copy_with_a_new_msg_id_is_a_second_file(sftp: Sftp) -> None:
    """Broken-implementation control (module docstring): the name is keyed on `tb-msg-id`, so the same bytes
    under a new id are a different file."""
    config = SftpDestinationConfig(
        channel=CHANNEL,
        destination=DESTINATION,
        host=sftp.host,
        port=sftp.port,
        host_keys=(sftp.pin,),
        auth_type="password",
        auth_secret_ref=CREDS_DIR,
        directory="/upload",
        file_name_prefix="ctl-",
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
    credentials = resolve_credentials(config, DirSecretResolver(sftp.secrets))
    loader = SftpLoader(config, lambda: connect(config, credentials))
    payload = b"same bytes\r"
    a = make_message(payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2")
    b = make_message(payload, key=None, channel=CHANNEL, destination=DESTINATION, content_type="application/hl7-v2")
    try:
        assert loader.deliver(a).success and loader.deliver(b).success
    finally:
        loader.close()
    assert sum(1 for f in sftp.listing() if f in (f"ctl-{a.msg_id}.hl7", f"ctl-{b.msg_id}.hl7")) == 2
