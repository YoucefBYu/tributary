"""`engine-kafka/v0` against a real SASL_SSL broker with deny-by-default ACLs (ADR-008, section 5).

The unit tests prove what the loader *builds*; only a broker proves librdkafka *accepts* it and that the
link is what the contract says: TLS verified against the document's CA with hostname checking and the
AEAD pin, SCRAM-SHA-512 as the document's principal, and ACLs applying to that principal.

Broker: `apache/kafka:4.0.0` (KRaft), configured from a mounted `server.properties` (the image's own
mechanism). Listener `EXT` is SASL_SSL (SCRAM-SHA-512, PEM keystore signed by a per-run CA, SAN
`DNS:localhost` only). Listener `ADMIN` is PLAINTEXT on the host's loopback for the test's own set-up
tooling; `ANONYMOUS` is super user **in this test container only** (`engine-kafka/v0` 6.3 forbids it in
the Compose stack). `StandardAuthorizer` with `allow.everyone.if.no.acl.found=false`.

Each claim has a control that fails for the reason named:

* the worker runs `raw` -> `out` with only the document's link (the PLAINTEXT listener is not in it);
* the principal is authenticated, so an ungranted topic is refused (`TOPIC_AUTHORIZATION_FAILED`);
* a wrong password fails SASL authentication;
* a CA that did not sign the broker fails verification;
* `127.0.0.1` fails hostname verification against a certificate that names only `localhost`.

The CA, keys and passwords are generated per run in a temporary directory and never printed. Synthetic
payloads only.
"""

from __future__ import annotations

import json
import secrets
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import (
    AclBinding,
    AclOperation,
    AclPermissionType,
    AdminClient,
    NewTopic,
    ResourcePatternType,
    ResourceType,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialUpsertion,
)
from testcontainers.core.container import DockerContainer
from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import headers as h

from .test_integration_kafka import (
    CHANNEL,
    CONTRACTS,
    DESTINATION,
    ENV,
    PARTITIONS,
    PER_KEY,
    drain,
    header_map,
    passthrough,
    publish,
)

pytestmark = pytest.mark.integration

PRINCIPAL = "tb-worker-synth"
FORBIDDEN_TOPIC = "forbidden.probe"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)  # noqa: S603, S607


def _make_ca(directory: Path, name: str) -> None:
    _openssl("genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", f"{name}.key", cwd=directory)
    _openssl(
        "req", "-x509", "-new", "-key", f"{name}.key", "-sha256", "-days", "2",
        "-subj", f"/CN=Tributary synthetic test CA {name}", "-out", f"{name}.crt",
        cwd=directory,
    )  # fmt: skip


@dataclass(frozen=True)
class Pki:
    certs: Path  # mounted into the broker
    secrets: Path  # TB_SECRETS_DIR for the clients


@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Pki:
    work = tmp_path_factory.mktemp("pki")
    _make_ca(work, "ca")
    _make_ca(work, "rogue")
    _openssl("genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", "broker.key", cwd=work)
    _openssl("req", "-new", "-key", "broker.key", "-subj", "/CN=localhost", "-out", "broker.csr", cwd=work)
    (work / "san.ext").write_text("subjectAltName=DNS:localhost\n", encoding="utf-8")
    _openssl(
        "x509", "-req", "-in", "broker.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
        "-days", "2", "-sha256", "-extfile", "san.ext", "-out", "broker.crt",
        cwd=work,
    )  # fmt: skip
    certs = work / "mounted"
    certs.mkdir()
    # Kafka's PEM keystore: an unencrypted PKCS#8 key followed by the certificate chain.
    pem = (work / "broker.key").read_text() + (work / "broker.crt").read_text() + (work / "ca.crt").read_text()
    (certs / "broker.pem").write_text(pem, encoding="utf-8")
    certs.chmod(0o755)
    (certs / "broker.pem").chmod(0o644)  # the broker runs as a non-root user; synthetic test key

    root = work / "secrets"
    for ref, files in {
        PRINCIPAL: {"username": PRINCIPAL, "password": secrets.token_urlsafe(24)},
        "tb-wrong-password": {"username": PRINCIPAL, "password": secrets.token_urlsafe(24)},
        "tb-ca": {"ca.crt": (work / "ca.crt").read_text()},
        "tb-rogue-ca": {"ca.crt": (work / "rogue.crt").read_text()},
    }.items():
        (root / ref).mkdir(parents=True)
        for key, value in files.items():
            (root / ref / key).write_text(value, encoding="utf-8")
            (root / ref / key).chmod(0o600)  # whatever the umask: group- or world-writable is refused
    return Pki(certs=certs, secrets=root)


@dataclass(frozen=True)
class Broker:
    sasl_port: int
    admin: str  # PLAINTEXT on 127.0.0.1, the test's own tooling only


def _server_properties(sasl_port: int, admin_port: int) -> str:
    return "\n".join(
        [
            "process.roles=broker,controller",
            "node.id=1",
            "controller.quorum.voters=1@localhost:9093",
            "controller.listener.names=CONTROLLER",
            "listeners=EXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,ADMIN://0.0.0.0:9094",
            f"advertised.listeners=EXT://localhost:{sasl_port},ADMIN://127.0.0.1:{admin_port}",
            "listener.security.protocol.map=EXT:SASL_SSL,CONTROLLER:PLAINTEXT,ADMIN:PLAINTEXT",
            "inter.broker.listener.name=ADMIN",
            "listener.name.ext.sasl.enabled.mechanisms=SCRAM-SHA-512",
            "listener.name.ext.scram-sha-512.sasl.jaas.config="
            "org.apache.kafka.common.security.scram.ScramLoginModule required;",
            "listener.name.ext.ssl.keystore.type=PEM",
            "listener.name.ext.ssl.keystore.location=/certs/broker.pem",
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "allow.everyone.if.no.acl.found=false",
            "super.users=User:ANONYMOUS",
            "offsets.topic.replication.factor=1",
            "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1",
            "group.initial.rebalance.delay.ms=0",
            "auto.create.topics.enable=false",
            "log.dirs=/tmp/kraft-combined-logs",
            "",
        ]
    )


@pytest.fixture(scope="module")
def broker(pki: Pki, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Broker]:
    sasl_port, admin_port = _free_port(), _free_port()
    config = tmp_path_factory.mktemp("broker-config")
    (config / "server.properties").write_text(_server_properties(sasl_port, admin_port), encoding="utf-8")
    config.chmod(0o755)
    (config / "server.properties").chmod(0o644)
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_volume_mapping(str(pki.certs), "/certs", "ro")
        .with_volume_mapping(str(config), "/mnt/shared/config", "ro")
        .with_bind_ports(9092, sasl_port)
        .with_bind_ports(9094, admin_port)
    )
    container.start()
    admin_bootstrap = f"127.0.0.1:{admin_port}"
    try:
        _await_admin(admin_bootstrap)
        _provision(admin_bootstrap, pki)
        yield Broker(sasl_port=sasl_port, admin=admin_bootstrap)
    finally:
        container.stop()


def _await_admin(bootstrap: str, *, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            return
        except Exception:  # not up yet
            time.sleep(1.0)
    raise RuntimeError("the SASL_SSL test broker's admin listener did not become reachable")


def _provision(bootstrap: str, pki: Pki) -> None:
    """Topics, the principal's SCRAM credential, and its grants: the worker's own prefixes only."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = [
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
        NewTopic(FORBIDDEN_TOPIC, 1, 1),
    ]
    for future in admin.create_topics(wanted).values():
        future.result(timeout=30)

    password = (pki.secrets / PRINCIPAL / "password").read_text(encoding="utf-8")
    upsert = UserScramCredentialUpsertion(
        PRINCIPAL, ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), password.encode("utf-8")
    )
    for future in admin.alter_user_scram_credentials([upsert]).values():
        future.result(timeout=30)

    user = f"User:{PRINCIPAL}"
    grants = [
        AclBinding(kind, name, ResourcePatternType.PREFIXED, user, "*", AclOperation.ALL, AclPermissionType.ALLOW)
        for kind, name in (
            (ResourceType.TOPIC, f"{ENV}."),
            (ResourceType.TOPIC, f"_tb.{ENV}."),
            (ResourceType.GROUP, f"tb.{ENV}."),
        )
    ]
    for future in admin.create_acls(grants).values():
        future.result(timeout=30)


def _document(tmp_path: Path, broker: Broker, *, host: str = "localhost", principal: str = PRINCIPAL,
              ca: str = "tb-ca") -> Path:  # fmt: skip
    path = tmp_path / "engine-kafka.json"
    path.write_text(
        json.dumps(
            {
                "apiVersion": "tributary.dev/v1",
                "kind": "EngineKafkaConnection",
                "bootstrapServers": [f"{host}:{broker.sasl_port}"],
                "security": {
                    "protocol": "SASL_SSL",
                    "saslMechanism": "SCRAM-SHA-512",
                    "secretRef": principal,
                    "tls": {"secretRef": ca},
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _settings(tmp_path: Path, broker: Broker, pki: Pki, **kwargs: str) -> KafkaSettings:
    environ = {"TB_ENGINE_KAFKA_FILE": str(_document(tmp_path, broker, **kwargs)), "TB_SECRETS_DIR": str(pki.secrets)}
    return load_engine_kafka(environ)


def _produce_once(settings: KafkaSettings, topic: str, *, timeout_s: float = 20.0) -> tuple[Any, list[Any]]:
    """Produce one synthetic record through the contract producer config. Returns (delivery error, client errors)."""
    config = producer_config(settings, load_topic_defaults(), client_id=f"probe-{uuid.uuid4().hex[:8]}")
    errors: list[Any] = []
    config["error_cb"] = errors.append
    config["message.timeout.ms"] = int(timeout_s * 1000) - 2000
    outcome: list[Any] = []
    producer = Producer(config)
    producer.produce(topic, value=b'{"synthetic": true}', on_delivery=lambda err, _msg: outcome.append(err))
    producer.flush(timeout_s)
    assert outcome, "no delivery report within the timeout"
    return outcome[0], errors


def test_the_worker_runs_raw_to_out_over_the_documents_sasl_ssl_link(broker: Broker, pki: Pki, tmp_path: Path) -> None:
    spec = json.loads(
        (CONTRACTS / "channel-spec" / "v0" / "examples" / "valid" / "walking-skeleton-loopback.json").read_text(
            encoding="utf-8"
        )
    )
    spec["metadata"] = {**spec["metadata"], "version": 1}
    admin_producer = Producer({"bootstrap.servers": broker.admin, "acks": "all", "enable.idempotence": True})
    admin_producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(spec).encode())
    assert admin_producer.flush(30) == 0
    expected = set(publish(broker.admin, PER_KEY))

    settings = _settings(tmp_path, broker, pki)
    assert settings.describe() == {
        "protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "client_certificate": False,
        "brokers": 1,
    }
    running = PipelineWorker(
        settings=WorkerSettings(
            env=ENV, channel=CHANNEL, kafka=settings, instance_id="pipeline-sasl", poll_timeout_s=0.1
        ),
        steps_override=(passthrough,),
        defaults=load_topic_defaults(),
    )
    running.build()
    running.run(max_iterations=400)

    delivered = drain(broker.admin, out_topic(ENV, CHANNEL, DESTINATION), expected=len(expected))
    assert expected <= {header_map(raw)[h.TB_MSG_ID] for raw in delivered}


def test_the_principal_is_authenticated_so_its_acls_apply(broker: Broker, pki: Pki, tmp_path: Path) -> None:
    settings = _settings(tmp_path, broker, pki)
    granted, _ = _produce_once(settings, dlq_topic(ENV, CHANNEL))
    assert granted is None, "control: a granted topic accepts the principal's write"
    refused, _ = _produce_once(settings, FORBIDDEN_TOPIC)
    assert refused is not None and refused.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED


def test_a_wrong_password_fails_sasl_authentication(broker: Broker, pki: Pki, tmp_path: Path) -> None:
    settings = _settings(tmp_path, broker, pki, principal="tb-wrong-password")
    failed, errors = _produce_once(settings, dlq_topic(ENV, CHANNEL), timeout_s=12.0)
    assert failed is not None
    assert any(error.code() == KafkaError._AUTHENTICATION for error in errors), [e.code() for e in errors]


def test_a_ca_that_did_not_sign_the_broker_fails_verification(broker: Broker, pki: Pki, tmp_path: Path) -> None:
    settings = _settings(tmp_path, broker, pki, ca="tb-rogue-ca")
    failed, errors = _produce_once(settings, dlq_topic(ENV, CHANNEL), timeout_s=12.0)
    assert failed is not None
    assert any(error.code() == KafkaError._SSL for error in errors), [e.code() for e in errors]
    assert not any(error.code() == KafkaError._AUTHENTICATION for error in errors), "failed before SASL"


def test_hostname_verification_is_on(broker: Broker, pki: Pki, tmp_path: Path) -> None:
    """The certificate names only `DNS:localhost`; the same broker reached as `127.0.0.1` must fail."""
    settings = _settings(tmp_path, broker, pki, host="127.0.0.1")
    failed, errors = _produce_once(settings, dlq_topic(ENV, CHANNEL), timeout_s=12.0)
    assert failed is not None
    assert any(error.code() == KafkaError._SSL for error in errors), [e.code() for e in errors]
