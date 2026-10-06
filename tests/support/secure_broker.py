"""A SASL_SSL, deny-by-default Kafka broker for tests that must run under the contract's exact ACL grants.

What it is for: `contracts/topics/v0` 3.4.1 says what each role may touch. Nothing is verified until a
principal holding exactly those grants (and nothing else) runs the real process against a broker whose
authorizer denies everything not granted. This module is that broker plus the helpers around it:

* `apache/kafka:4.0.0` (KRaft), `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`.
* Listener `EXT` is SASL_SSL (SCRAM-SHA-512, PEM keystore signed by a per-run CA, SAN `DNS:localhost`). It
  is the only listener a process under test is given.
* Listener `ADMIN` is PLAINTEXT on the host's loopback, for the test's own set-up tooling, and also serves
  as the "foreign" source cluster for the kafka-source receiver. `User:ANONYMOUS` is a super user **in this
  test container only** (`engine-kafka/v0` 6 rule 3 forbids it in the Compose stack).
* Grants are **derived from the contract**, not restated: `contracts/topics/v0/topic-defaults.json`
  `security.acls`, parsed by the lite stack's own `bootstrap_topics.derive_acls`, so a contract change that
  breaks a role shows up here. The sync-reply sentence the parser deliberately skips is added by hand from
  3.4.1 (`SYNC_REPLY_RECEIVER`).
* The broker's authorizer log is read back (`authorizer_denials`), which is how a negative case proves the
  broker said "denied" rather than the client timing out.

CA, keys and passwords are generated per run in a temporary directory and never printed. Container names
are unique per run, so a parallel session's containers are never touched.
"""

from __future__ import annotations

import importlib.util
import json
import re
import secrets
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

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

REPO = Path(__file__).resolve().parents[2]
TOPIC_DEFAULTS = REPO / "contracts" / "topics" / "v0" / "topic-defaults.json"
BOOTSTRAP_TOPICS = REPO / "deploy" / "compose" / "topic-bootstrap" / "bootstrap_topics.py"
IMAGE = "apache/kafka:4.0.0"


def load_bootstrap_topics() -> ModuleType:
    """The lite stack's grant parser, loaded by path (it is a script, not a package)."""
    spec = importlib.util.spec_from_file_location("tb_qa_bootstrap_topics", BOOTSTRAP_TOPICS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True, slots=True)
class Grant:
    """One ACL binding in plain terms: `resource_type` is TOPIC, GROUP or CLUSTER."""

    resource_type: str
    name: str
    pattern: str
    operation: str

    def as_tuple(self) -> tuple[str, str, str, str]:
        return (self.resource_type, self.name, self.pattern, self.operation)


def sync_reply_receiver_grants(env: str, channel: str) -> list[Grant]:
    """3.4.1: sync-reply channels only. READ `reply` and the LITERAL replier group; never WRITE on `reply`."""
    return [
        Grant("TOPIC", f"{env}.{channel}.reply", "LITERAL", "READ"),
        Grant("GROUP", f"tb.{env}.{channel}.replier", "LITERAL", "READ"),
    ]


def contract_grants(role: str, *, env: str, channel: str, destinations: Sequence[str] = ("stub",)) -> list[Grant]:
    """The grants the contract gives `role` (`receiver`, `pipeline_worker`, `loader`) for `channel`."""
    module = load_bootstrap_topics()
    defaults = json.loads(TOPIC_DEFAULTS.read_text(encoding="utf-8"))
    parsed = module.parse_grants(
        defaults["security"]["acls"][role], env=env, channel=channel, destinations=tuple(destinations)
    )
    return [Grant(g.resource_type, g.name, g.pattern, g.operation) for g in parsed]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)  # noqa: S603, S607


@dataclass(frozen=True)
class SecureBroker:
    """A running broker. `secrets_dir` is a `TB_SECRETS_DIR` holding one directory per principal."""

    name: str
    sasl_port: int
    admin: str  # PLAINTEXT on 127.0.0.1: set-up tooling and the foreign source cluster, nothing under test
    secrets_dir: Path
    _passwords: dict[str, str] = field(default_factory=dict, repr=False)

    @property
    def bootstrap(self) -> str:
        """What a process under test dials: `localhost` so the certificate's SAN matches."""
        return f"localhost:{self.sasl_port}"

    def admin_client(self) -> AdminClient:
        return AdminClient({"bootstrap.servers": self.admin})

    def create_topics(self, topics: Iterable[NewTopic]) -> None:
        admin = self.admin_client()  # keep a reference: a collected client fails its pending futures
        for future in admin.create_topics(list(topics)).values():
            try:
                future.result(timeout=30)
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    def add_principal(self, name: str) -> None:
        """A SCRAM-SHA-512 user and its `<secrets_dir>/<name>/{username,password}` (0600, synthetic)."""
        password = secrets.token_urlsafe(24)
        upsert = UserScramCredentialUpsertion(
            name, ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 8192), password.encode("utf-8")
        )
        admin = self.admin_client()
        for future in admin.alter_user_scram_credentials([upsert]).values():
            future.result(timeout=30)
        directory = self.secrets_dir / name
        directory.mkdir(parents=True, exist_ok=True)
        for key, value in (("username", name), ("password", password)):
            (directory / key).write_text(value, encoding="utf-8")
            (directory / key).chmod(0o600)

    def grant(self, principal: str, grants: Iterable[Grant]) -> int:
        """Create the ACLs for `User:<principal>`; returns how many bindings were created."""
        bindings = [
            AclBinding(
                # confluent-kafka names the cluster resource ("kafka-cluster") ResourceType.BROKER.
                getattr(ResourceType, "BROKER" if g.resource_type == "CLUSTER" else g.resource_type),
                "kafka-cluster" if g.resource_type == "CLUSTER" else g.name,
                getattr(ResourcePatternType, g.pattern),
                f"User:{principal}",
                "*",
                getattr(AclOperation, g.operation),
                AclPermissionType.ALLOW,
            )
            for g in grants
        ]
        if not bindings:
            return 0
        admin = self.admin_client()
        for future in admin.create_acls(bindings).values():
            future.result(timeout=30)
        return len(bindings)

    def provision(self, principal: str, grants: Iterable[Grant]) -> int:
        self.add_principal(principal)
        return self.grant(principal, grants)

    def engine_kafka_file(self, directory: Path, principal: str) -> Path:
        """An `engine-kafka/v0` SASL_SSL document for `principal`, trusting this broker's CA."""
        path = directory / f"engine-kafka-{principal}.json"
        path.write_text(
            json.dumps(
                {
                    "apiVersion": "tributary.dev/v1",
                    "kind": "EngineKafkaConnection",
                    "bootstrapServers": [self.bootstrap],
                    "security": {
                        "protocol": "SASL_SSL",
                        "saslMechanism": "SCRAM-SHA-512",
                        "secretRef": principal,
                        "tls": {"secretRef": "tb-ca"},
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def process_env(self, directory: Path, principal: str, base: dict[str, str] | None = None) -> dict[str, str]:
        """The environment of a data-plane process that runs as `principal`."""
        import os

        env = dict(os.environ if base is None else base)
        env.pop("TB_BOOTSTRAP_SERVERS", None)
        env["TB_ENGINE_KAFKA_FILE"] = str(self.engine_kafka_file(directory, principal))
        env["TB_SECRETS_DIR"] = str(self.secrets_dir)
        return env

    def authorizer_denials(self) -> list[str]:
        """Denied-operation lines from the broker's authorizer log, oldest first. Each reads like
        `Principal = User:x is Denied operation = READ from host = h on resource = Topic:LITERAL:t for request = ...`.
        Resource and principal names only: no payload ever reaches this log."""
        result = subprocess.run(  # noqa: S603
            ["docker", "exec", self.name, "sh", "-c", "cat /opt/kafka/logs/kafka-authorizer.log 2>/dev/null; true"],  # noqa: S607
            capture_output=True,
            check=False,
            timeout=60,
        )
        text = result.stdout.decode("utf-8", "replace")
        return [line for line in text.splitlines() if "Denied operation" in line]

    def denials_for(self, principal: str, *, operation: str | None = None, resource: str | None = None) -> list[str]:
        """Authorizer denials for `User:<principal>`, optionally narrowed to an operation (`READ`, `WRITE`,
        `DESCRIBE`) and a resource such as `Topic:LITERAL:<name>` or `Group:LITERAL:<name>`."""
        out: list[str] = []
        for line in self.authorizer_denials():
            parsed = parse_denial(line)
            if parsed is None or parsed[0] != f"User:{principal}":
                continue
            if operation is not None and parsed[1] != operation:
                continue
            if resource is not None and parsed[2] != resource:
                continue
            out.append(line)
        return out


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


def _make_pki(work: Path, secrets_dir: Path) -> Path:
    """Throwaway CA plus a `localhost` broker certificate. Returns the directory to mount at `/certs`."""
    _openssl("genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", "ca.key", cwd=work)
    _openssl(
        "req", "-x509", "-new", "-key", "ca.key", "-sha256", "-days", "2",
        "-subj", "/CN=Tributary synthetic test CA", "-out", "ca.crt",
        cwd=work,
    )  # fmt: skip
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
    certs.chmod(0o755)
    pem = (work / "broker.key").read_text() + (work / "broker.crt").read_text() + (work / "ca.crt").read_text()
    (certs / "broker.pem").write_text(pem, encoding="utf-8")
    (certs / "broker.pem").chmod(0o644)  # the broker runs as a non-root user; synthetic test key
    (secrets_dir / "tb-ca").mkdir(parents=True)
    (secrets_dir / "tb-ca" / "ca.crt").write_text((work / "ca.crt").read_text(), encoding="utf-8")
    (secrets_dir / "tb-ca" / "ca.crt").chmod(0o600)
    return certs


def _await_admin(bootstrap: str, *, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError("the SASL_SSL test broker's admin listener did not become reachable")


@contextmanager
def secure_broker(work: Path) -> Iterator[SecureBroker]:
    """Start the broker; stop only the container this call started. `work` is a scratch directory."""
    work.mkdir(parents=True, exist_ok=True)
    secrets_dir = work / "secrets"
    secrets_dir.mkdir()
    certs = _make_pki(work, secrets_dir)
    sasl_port, admin_port = _free_port(), _free_port()
    config = work / "broker-config"
    config.mkdir()
    (config / "server.properties").write_text(_server_properties(sasl_port, admin_port), encoding="utf-8")
    config.chmod(0o755)
    (config / "server.properties").chmod(0o644)
    name = f"tb-qa-acl-{uuid.uuid4().hex[:10]}"
    container = (
        DockerContainer(IMAGE)
        .with_name(name)
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_volume_mapping(str(certs), "/certs", "ro")
        .with_volume_mapping(str(config), "/mnt/shared/config", "ro")
        .with_bind_ports(9092, sasl_port)
        .with_bind_ports(9094, admin_port)
    )
    container.start()
    try:
        _await_admin(f"127.0.0.1:{admin_port}")
        yield SecureBroker(name=name, sasl_port=sasl_port, admin=f"127.0.0.1:{admin_port}", secrets_dir=secrets_dir)
    finally:
        container.stop()


_DENIAL = re.compile(
    r"Principal = (?P<principal>\S+) is Denied operation = (?P<op>\S+) from host = \S+ on resource = (?P<res>\S+)"
)


def parse_denial(line: str) -> tuple[str, str, str] | None:
    """(principal, operation, resource) of an authorizer denial line, else None."""
    match = _DENIAL.search(line)
    return (match["principal"], match["op"], match["res"]) if match else None


__all__: list[Any] = [
    "Grant",
    "SecureBroker",
    "contract_grants",
    "parse_denial",
    "secure_broker",
    "sync_reply_receiver_grants",
]
