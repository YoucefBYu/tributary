"""`engine-kafka/v0` (contract set 1.3, ADR-008): the loader, the `KafkaSettings` link rules, the worker.

Section 5 acceptance for the worker role is here: a non-loopback PLAINTEXT document is refused, SSL
without `security.secretRef` is refused, a security map carrying `acks` is refused, and the effective
producer config has `acks=all` and `enable.idempotence=true` whatever the document says.

Every host, `secretRef` and credential is synthetic. The marker strings are planted so a test can prove
they never reach an exception message, a `repr` or a log line.
"""

from __future__ import annotations

import dataclasses
import io
import json
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator
from tributary.runtime import engine_kafka as ek
from tributary.runtime.clients import (
    KafkaSettings,
    SecurityProperties,
    consumer_config,
    events_producer_config,
    producer_config,
)
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.errors import (
    ChannelSpecError,
    EngineKafkaError,
    KafkaSecurityError,
    RuntimeConfigurationError,
)
from tributary.runtime.kafka_security import PINNED_CIPHER_SUITES, is_loopback_host_port, split_bootstrap
from tributary.runtime.logs import JsonLogger
from tributary.runtime.secrets import DirSecretResolver, SecretResolutionError

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
CONTRACT = CONTRACTS / "engine-kafka" / "v0"
SCHEMA = json.loads((CONTRACT / "engine-kafka.schema.json").read_text(encoding="utf-8"))
MANIFEST = json.loads((CONTRACT / "examples" / "manifest.json").read_text(encoding="utf-8"))
PASSWORD_MARKER = "SYNTH-PASSWORD-MARKER-7f3a"  # noqa: S105 - synthetic marker, planted to prove it never leaks
USERNAME = "tb-synthetic-worker"

TLS_PINS: dict[str, Any] = {
    "enable.ssl.certificate.verification": True,
    "ssl.endpoint.identification.algorithm": "https",
    "ssl.cipher.suites": PINNED_CIPHER_SUITES,
}
SASL_SSL: dict[str, Any] = {
    "security.protocol": "SASL_SSL",
    **TLS_PINS,
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": USERNAME,
    "sasl.password": PASSWORD_MARKER,
}


def document(protocol: str = "SASL_SSL", servers: list[str] | None = None, **security: Any) -> dict[str, Any]:
    block: dict[str, Any] = {"protocol": protocol}
    if protocol == "SASL_SSL":
        block |= {"saslMechanism": "SCRAM-SHA-512", "secretRef": "tb-worker-synth", "tls": {"secretRef": "tb-ca"}}
    elif protocol == "SSL":
        block |= {"secretRef": "tb-worker-synth", "tls": {"secretRef": "tb-ca"}}
    block |= security
    return {
        "apiVersion": "tributary.dev/v1",
        "kind": "EngineKafkaConnection",
        "bootstrapServers": servers or (["127.0.0.1:9092"] if protocol == "PLAINTEXT" else ["kafka-0.synth:9093"]),
        "security": block,
    }


@pytest.fixture
def secrets_dir(tmp_path: Path) -> Path:
    root = tmp_path / "secrets"
    (root / "tb-worker-synth").mkdir(parents=True)
    (root / "tb-ca").mkdir()
    (root / "tb-worker-synth" / "username").write_text(USERNAME + "\n", encoding="utf-8")
    (root / "tb-worker-synth" / "password").write_text(PASSWORD_MARKER + "\n", encoding="utf-8")
    (root / "tb-worker-synth" / "tls.crt").write_text("SYNTHETIC CERT\n", encoding="utf-8")
    (root / "tb-worker-synth" / "tls.key").write_text("SYNTHETIC KEY\n", encoding="utf-8")
    (root / "tb-ca" / "ca.crt").write_text("SYNTHETIC CA\n", encoding="utf-8")
    for key in root.glob("*/*"):
        key.chmod(0o600)  # independent of the umask: group- or world-writable secrets are refused
    return root


def environ_for(tmp_path: Path, doc: object, secrets_dir: Path | None = None, **extra: str) -> dict[str, str]:
    path = tmp_path / "engine-kafka.json"
    path.write_text(json.dumps(doc) if not isinstance(doc, str) else doc, encoding="utf-8")
    env = {"TB_ENGINE_KAFKA_FILE": str(path), **extra}
    if secrets_dir is not None:
        env["TB_SECRETS_DIR"] = str(secrets_dir)
    return env


# --- the contract's own examples --------------------------------------------------------------------


@pytest.mark.parametrize("entry", MANIFEST["valid"], ids=lambda e: e["file"])
def test_every_valid_contract_example_parses(entry: Mapping[str, str]) -> None:
    doc = json.loads((CONTRACT / "examples" / entry["file"]).read_text(encoding="utf-8"))
    connection = ek.parse_engine_kafka(doc)
    assert list(connection.bootstrap_servers) == doc["bootstrapServers"]
    assert connection.protocol == doc["security"]["protocol"]


@pytest.mark.parametrize("entry", MANIFEST["invalid"], ids=lambda e: e["file"])
def test_every_invalid_contract_example_is_refused_at_its_pinned_path(entry: Mapping[str, str]) -> None:
    doc = json.loads((CONTRACT / "examples" / entry["file"]).read_text(encoding="utf-8"))
    with pytest.raises(EngineKafkaError) as refused:
        ek.parse_engine_kafka(doc)
    message = str(refused.value)
    assert f"at {entry['expected_json_path']}" in message
    assert "SYNTHETIC-PLACEHOLDER-NOT-A-SECRET" not in message  # inline-password.json's planted value


# --- differential: the stdlib mirror against jsonschema on the schema file ---------------------------

#: Mostly valid building blocks, so the generator reaches acceptance often; the odd ones are the known
#: bypass shapes and the schema/runtime disagreements the module docstring documents.
GOOD_HOSTS = ["localhost", "127.0.0.1", "127.9.8.7", "[::1]", "kafka", "kafka-0.kafka-brokers.tributary.svc"]
ODD_HOSTS = [
    "127.1.1.300",  # schema: loopback. Runtime: not an IPv4 literal
    "999.1.1.1",
    "[0:0:0:0:0:0:0:1]",
    "[::ffff:127.0.0.1]",
    "::1",
    "host.docker.internal",
    "10.0.0.5",
    "LOCALHOST",
    "localhost.",
    "PLAINTEXT://127.0.0.1",
    "-bad",
    "",
]
GOOD_PORTS = ["9092", "9093", "65535", "10"]
ODD_PORTS = ["1", "0", "00", "65536", "99999", "x"]
REFS = ["tb-worker-synth", "Upper", "tb-worker-synth\n", "../escape", "a" * 65, ""]


def _set(path: tuple[str, ...], value: Any) -> Any:
    def apply(doc: dict[str, Any]) -> None:
        target: Any = doc
        for key in path[:-1]:
            target = target.setdefault(key, {}) if isinstance(target, dict) else None
        if isinstance(target, dict):
            target[path[-1]] = value

    return apply


def _drop(*path: str) -> Any:
    def apply(doc: dict[str, Any]) -> None:
        target: Any = doc
        for key in path[:-1]:
            target = target.get(key) if isinstance(target, dict) else None
        if isinstance(target, dict):
            target.pop(path[-1], None)

    return apply


PERTURBATIONS: list[Any] = [
    _set(("security", "protocol"), "SASL_SSL"),
    _set(("security", "protocol"), "SSL"),
    _set(("security", "protocol"), "PLAINTEXT"),
    _set(("security", "protocol"), "sasl_ssl"),
    _set(("security", "protocol"), "SASL_PLAINTEXT"),
    _drop("security", "secretRef"),
    _drop("security", "saslMechanism"),
    _drop("security", "protocol"),
    _set(("security", "saslMechanism"), "PLAIN"),
    _set(("security", "saslMechanism"), "SCRAM-SHA-512"),
    _set(("security", "secretRef"), "tb-worker-synth"),
    _set(("security", "tls", "secretRef"), "tb-ca"),
    _set(("security", "tls", "verifyHostname"), False),
    _set(("security", "password"), PASSWORD_MARKER),
    _set(("security",), "SASL_SSL"),
    _set(("properties",), {"acks": "1"}),
    _set(("apiVersion",), "tributary.dev/v2"),
    _set(("kind",), "Other"),
    _drop("kind"),
    _set(("bootstrapServers",), []),
    _set(("bootstrapServers",), ["kafka:9093", "kafka:9093"]),
    _set(("bootstrapServers",), [f"b{i}:9093" for i in range(17)]),
    _set(("bootstrapServers",), ["127.0.0.1:9092\n"]),
    _set(("bootstrapServers",), ["127.0.0.1:9092,kafka:9092"]),
    _set(("bootstrapServers",), [9092]),
    _set(("bootstrapServers",), "127.0.0.1:9092"),
]


@st.composite
def documents(draw: st.DrawFn) -> dict[str, Any]:
    def entry() -> str:
        host = draw(st.sampled_from(GOOD_HOSTS if draw(st.integers(0, 4)) else ODD_HOSTS))
        port = draw(st.sampled_from(GOOD_PORTS if draw(st.integers(0, 4)) else ODD_PORTS))
        return f"{host}:{port}"

    protocol = draw(st.sampled_from(["SASL_SSL", "SSL", "PLAINTEXT"]))
    security: dict[str, Any] = {"protocol": protocol}
    if protocol == "SASL_SSL":
        security |= {"saslMechanism": "SCRAM-SHA-512", "secretRef": "tb-worker-synth"}
    elif protocol == "SSL":
        security["secretRef"] = "tb-worker-synth"
    if protocol != "PLAINTEXT" and draw(st.booleans()):
        security["tls"] = {"secretRef": "tb-ca"}
    doc: dict[str, Any] = {
        "apiVersion": "tributary.dev/v1",
        "kind": "EngineKafkaConnection",
        "bootstrapServers": list(dict.fromkeys(entry() for _ in range(draw(st.integers(1, 3))))),
        "security": security,
    }
    for perturb in draw(st.lists(st.sampled_from(PERTURBATIONS), max_size=2)):
        perturb(doc)
    if draw(st.integers(0, 5)) == 0 and isinstance(doc["security"], dict):
        doc["security"]["secretRef"] = draw(st.sampled_from(REFS))
    return doc


def _entries(doc: Any) -> list[Any]:
    servers = doc.get("bootstrapServers") if isinstance(doc, dict) else None
    return servers if isinstance(servers, list) else []


def _runtime_stricter(doc: Any) -> bool:
    """A feature the schema's regexes admit and the runtime refuses by design (engine_kafka docstring)."""
    for entry in _entries(doc):
        if not isinstance(entry, str):
            continue
        if entry.endswith("\n"):
            return True
        host, _, port = entry.rpartition(":")
        if port.isdigit() and not 1 <= int(port) <= 65535:
            return True
        if re.fullmatch(r"[0-9.]+", host) and not _valid_ipv4(host):
            return True
    security = doc.get("security") if isinstance(doc, dict) else None
    if isinstance(security, dict):
        tls = security.get("tls")
        refs = [security.get("secretRef"), tls.get("secretRef") if isinstance(tls, dict) else None]
        if any(isinstance(ref, str) and ref.endswith("\n") for ref in refs):
            return True
    return False


def _valid_ipv4(host: str) -> bool:
    import ipaddress

    try:
        ipaddress.IPv4Address(host)
    except ValueError:
        return False
    return True


OUTCOMES: dict[tuple[bool, bool], int] = {}


@settings(max_examples=1500, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(doc=documents())
def test_the_runtime_validator_never_accepts_what_the_schema_refuses(doc: dict[str, Any]) -> None:
    """Accepts => schema accepts. Where the schema accepts and the runtime refuses, the document has one
    of the documented stricter features and nothing else explains it."""
    schema_ok = not list(Draft202012Validator(SCHEMA).iter_errors(doc))
    try:
        ek.parse_engine_kafka(doc)
        runtime_ok = True
    except EngineKafkaError as exc:
        runtime_ok = False
        assert PASSWORD_MARKER not in str(exc)
    OUTCOMES[(schema_ok, runtime_ok)] = OUTCOMES.get((schema_ok, runtime_ok), 0) + 1
    if runtime_ok:
        assert schema_ok, doc
    elif schema_ok:
        assert _runtime_stricter(doc), doc


def test_the_differential_reached_every_outcome_it_can() -> None:
    """A control: a generator that only produced refusals would pass the property above vacuously."""
    if not OUTCOMES:
        pytest.skip("run together with the property test")
    assert OUTCOMES.get((True, True), 0) >= 100
    assert OUTCOMES.get((False, False), 0) >= 100
    assert OUTCOMES.get((True, False), 0) >= 10  # the documented stricter rules were exercised
    assert OUTCOMES.get((False, True), 0) == 0


# --- the loader ---------------------------------------------------------------------------------------


def test_sasl_ssl_document_resolves_to_the_exact_pinned_map(tmp_path: Path, secrets_dir: Path) -> None:
    settings_ = ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))
    assert settings_.bootstrap_servers == "kafka-0.synth:9093"
    assert dict(settings_.security) == {
        **SASL_SSL,
        "ssl.ca.location": str((secrets_dir / "tb-ca" / "ca.crt").resolve()),
    }


def test_ssl_document_resolves_the_client_certificate_from_security_secret_ref(
    tmp_path: Path, secrets_dir: Path
) -> None:
    settings_ = ek.load_engine_kafka(environ_for(tmp_path, document("SSL"), secrets_dir))
    security = dict(settings_.security)
    assert security["ssl.certificate.location"].endswith("tb-worker-synth/tls.crt")
    assert security["ssl.key.location"].endswith("tb-worker-synth/tls.key")
    assert security["ssl.ca.location"].endswith("tb-ca/ca.crt")
    assert not any(key.startswith("sasl.") for key in security)
    assert settings_.describe() == {
        "protocol": "SSL",
        "sasl_mechanism": None,
        "client_certificate": True,
        "brokers": 1,
    }


def test_sasl_ssl_without_tls_uses_the_platform_trust_store(tmp_path: Path, secrets_dir: Path) -> None:
    doc = document()
    del doc["security"]["tls"]
    security = dict(ek.load_engine_kafka(environ_for(tmp_path, doc, secrets_dir)).security)
    assert "ssl.ca.location" not in security
    assert all(security[key] == value for key, value in TLS_PINS.items())


def test_plaintext_loopback_needs_no_secrets(tmp_path: Path) -> None:
    doc = document("PLAINTEXT", ["127.0.0.1:9092", "localhost:9093", "[::1]:9094"])
    settings_ = ek.load_engine_kafka(environ_for(tmp_path, doc))
    assert dict(settings_.security) == {"security.protocol": "PLAINTEXT"}
    assert settings_.describe()["brokers"] == 3


@pytest.mark.parametrize(
    "servers",
    [
        ["kafka:9092"],  # the Compose lite stack before ADR-008 D5
        ["127.0.0.1:9092", "kafka:9092"],  # mixed list
        ["127.1.1.300:9092"],  # the receivers' regex accepted this
        ["host.docker.internal:9092"],
        ["[::ffff:127.0.0.1]:9092"],
        ["[0:0:0:0:0:0:0:1]:9092"],
        ["::1:9092"],
        ["PLAINTEXT://127.0.0.1:9092"],
        ["127.0.0.1:9092,kafka:9092"],  # one item smuggling a second server
        ["127.0.0.1:0"],
        ["127.0.0.1:65536"],
        ["127.0.0.1:9092\n"],
        ["LOCALHOST:9092"],
    ],
)
def test_plaintext_to_anything_but_loopback_is_refused(tmp_path: Path, servers: list[str]) -> None:
    with pytest.raises(EngineKafkaError, match=r"bootstrapServers\[\d\]"):
        ek.load_engine_kafka(environ_for(tmp_path, document("PLAINTEXT", servers)))


def test_ssl_without_a_client_certificate_is_refused(tmp_path: Path, secrets_dir: Path) -> None:
    doc = document("SSL")
    del doc["security"]["secretRef"]
    with pytest.raises(EngineKafkaError, match="SSL requires secretRef"):
        ek.load_engine_kafka(environ_for(tmp_path, doc, secrets_dir))


@pytest.mark.parametrize(
    ("mutate", "rule"),
    [
        (lambda d: d.update(properties={"acks": "1"}), r"at \$: 1 unknown field"),
        (lambda d: d["security"].update(password=PASSWORD_MARKER), r"at \$\.security: 1 unknown field"),
        (lambda d: d["security"]["tls"].update(verifyHostname=False), r"at \$\.security\.tls: 1 unknown"),
        (lambda d: d["security"].update(saslMechanism="PLAIN"), "SCRAM-SHA-512"),
        (lambda d: d["security"].update(protocol="SASL_PLAINTEXT"), "must be SASL_SSL, SSL or PLAINTEXT"),
        (lambda d: d.update(apiVersion="tributary.dev/v2"), r"\$\.apiVersion"),
        (lambda d: d.update(bootstrapServers=[]), "non-empty array"),
        (lambda d: d.update(bootstrapServers=["a:9093", "a:9093"]), "unique"),
        (lambda d: d.update(bootstrapServers=[f"b{i}:9093" for i in range(17)]), "at most 16"),
        (lambda d: d["security"].update(secretRef="../../etc"), "secretRef"),
    ],
)
def test_document_rules(tmp_path: Path, secrets_dir: Path, mutate: Any, rule: str) -> None:
    doc = document()
    mutate(doc)
    with pytest.raises(EngineKafkaError, match=rule) as refused:
        ek.load_engine_kafka(environ_for(tmp_path, doc, secrets_dir))
    assert PASSWORD_MARKER not in str(refused.value)


def test_the_file_is_required_and_there_is_no_default_broker() -> None:
    with pytest.raises(EngineKafkaError, match="TB_ENGINE_KAFKA_FILE is not set"):
        ek.load_engine_kafka({})


def test_a_relative_path_is_refused(tmp_path: Path) -> None:
    with pytest.raises(EngineKafkaError, match="absolute"):
        ek.load_engine_kafka({"TB_ENGINE_KAFKA_FILE": "engine-kafka.json"})


@pytest.mark.parametrize(
    ("environ", "argv", "named"),
    [
        ({"TB_BOOTSTRAP_SERVERS": "127.0.0.1:9092"}, (), "TB_BOOTSTRAP_SERVERS"),
        ({}, ("--bootstrap", "127.0.0.1:9092"), "--bootstrap"),
        ({}, ("--bootstrap=127.0.0.1:9092",), "--bootstrap"),
        ({}, ("--engine-security", "/x.json"), "--engine-security"),
    ],
)
def test_retired_inputs_refuse_to_start_and_name_the_replacement(
    tmp_path: Path, environ: dict[str, str], argv: tuple[str, ...], named: str
) -> None:
    env = environ_for(tmp_path, document("PLAINTEXT")) | environ
    with pytest.raises(EngineKafkaError, match=f"{named} is retired.*TB_ENGINE_KAFKA_FILE"):
        ek.load_engine_kafka(env, argv=argv)


@pytest.mark.parametrize(
    "content",
    [
        '{"apiVersion": "tributary.dev/v1", "apiVersion": "tributary.dev/v1"}',
        '{"bootstrapServers": NaN}',
        "not json " + PASSWORD_MARKER,
        "[" * 5000 + "]" * 5000,
        "﻿{}",
    ],
)
def test_unparseable_documents_are_refused_without_quoting_them(tmp_path: Path, content: str) -> None:
    with pytest.raises(EngineKafkaError) as refused:
        ek.load_engine_kafka(environ_for(tmp_path, content))
    assert PASSWORD_MARKER not in str(refused.value)
    assert refused.value.__cause__ is None


def test_an_oversized_document_is_refused(tmp_path: Path) -> None:
    with pytest.raises(EngineKafkaError, match="larger than"):
        ek.load_engine_kafka(environ_for(tmp_path, " " * 70_000 + "{}"))


@pytest.mark.parametrize("missing", ["password", "username"])
def test_missing_or_empty_credentials_are_refused_before_connecting(
    tmp_path: Path, secrets_dir: Path, missing: str
) -> None:
    (secrets_dir / "tb-worker-synth" / missing).unlink()
    with pytest.raises(EngineKafkaError, match=f"tb-worker-synth/{missing} does not exist") as refused:
        ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))
    assert str(secrets_dir) not in str(refused.value)

    (secrets_dir / "tb-worker-synth" / missing).write_text("  \n", encoding="utf-8")
    with pytest.raises(EngineKafkaError, match="is empty"):
        ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))


def test_an_empty_client_key_is_refused(tmp_path: Path, secrets_dir: Path) -> None:
    (secrets_dir / "tb-worker-synth" / "tls.key").write_bytes(b"")
    with pytest.raises(EngineKafkaError, match=r"tls\.key is empty"):
        ek.load_engine_kafka(environ_for(tmp_path, document("SSL"), secrets_dir))


def test_a_symlink_out_of_the_secrets_dir_is_refused(tmp_path: Path, secrets_dir: Path) -> None:
    outside = tmp_path / "elsewhere"
    outside.write_text("SYNTHETIC\n", encoding="utf-8")
    (secrets_dir / "tb-ca" / "ca.crt").unlink()
    (secrets_dir / "tb-ca" / "ca.crt").symlink_to(outside)
    with pytest.raises(EngineKafkaError, match="outside the secrets directory"):
        ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))


def test_a_resolver_that_returns_an_empty_password_is_still_refused(tmp_path: Path) -> None:
    class Hollow:
        def resolve_file(self, ref: str, key: str) -> Path:
            return tmp_path / key

        def resolve_text(self, ref: str, key: str) -> str:
            return "" if key == "password" else USERNAME

    connection = ek.parse_engine_kafka(document())
    with pytest.raises(EngineKafkaError, match="no username or password"):
        ek.engine_kafka_settings(connection, Hollow())


def test_a_foreign_resolver_os_error_is_wrapped_without_its_message(tmp_path: Path) -> None:
    class Raising:
        def resolve_file(self, ref: str, key: str) -> Path:
            raise FileNotFoundError(f"/secret/path/{PASSWORD_MARKER}")

        def resolve_text(self, ref: str, key: str) -> str:
            raise FileNotFoundError(f"/secret/path/{PASSWORD_MARKER}")

    with pytest.raises(EngineKafkaError, match="FileNotFoundError") as refused:
        ek.engine_kafka_settings(ek.parse_engine_kafka(document()), Raising())
    assert PASSWORD_MARKER not in str(refused.value)
    assert refused.value.__cause__ is None


def test_resolver_text_cap_and_repr(tmp_path: Path, secrets_dir: Path) -> None:
    (secrets_dir / "tb-worker-synth" / "password").write_text("x" * 5000, encoding="utf-8")
    resolver = DirSecretResolver(secrets_dir)
    with pytest.raises(SecretResolutionError, match="larger than"):
        resolver.resolve_text("tb-worker-synth", "password")
    assert str(secrets_dir) not in repr(resolver)


def test_nothing_secret_reaches_repr_asdict_or_describe(tmp_path: Path, secrets_dir: Path) -> None:
    settings_ = ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))
    for rendering in (repr(settings_), str(settings_), repr(dataclasses.asdict(settings_)), str(settings_.security)):
        assert PASSWORD_MARKER not in rendering
        assert USERNAME not in rendering
    described = json.dumps(settings_.describe())
    assert PASSWORD_MARKER not in described and USERNAME not in described and "synth" not in described
    connection = ek.read_engine_kafka_file(environ_for(tmp_path, document(), secrets_dir)["TB_ENGINE_KAFKA_FILE"])
    assert PASSWORD_MARKER not in repr(connection)


# --- KafkaSettings: the rules for every link, however it is built -------------------------------------


@pytest.mark.parametrize(
    "extra",
    [
        {"bootstrap.servers": "evil:9092"},
        {"metadata.broker.list": "evil:9092"},
        {"acks": "1"},
        {"enable.idempotence": False},
        {"ssl.engine.location": "/nonexistent/x.so"},
        {"ssl.providers": "legacy"},
        {"ssl.key.pem": "SYNTHETIC"},
        {"sasl.kerberos.kinit.cmd": "touch /tmp/x"},
        {"sasl.mechanisms": "PLAIN"},
        {"client.id": "x"},
    ],
    ids=lambda e: next(iter(e)),
)
def test_the_security_map_carries_security_keys_only(extra: dict[str, Any]) -> None:
    with pytest.raises(KafkaSecurityError, match="outside the allow-list") as refused:
        KafkaSettings("kafka-0.synth:9093", {**SASL_SSL, **extra})
    assert PASSWORD_MARKER not in str(refused.value)
    assert "evil" not in str(refused.value)


@pytest.mark.parametrize(
    ("security", "rule"),
    [
        ({**SASL_SSL, "enable.ssl.certificate.verification": False}, "verify"),
        ({**SASL_SSL, "enable.ssl.certificate.verification": "true"}, "verify"),
        ({**SASL_SSL, "ssl.endpoint.identification.algorithm": "none"}, "verify"),
        ({**SASL_SSL, "ssl.cipher.suites": "ALL"}, "verify"),
        ({k: v for k, v in SASL_SSL.items() if k != "ssl.cipher.suites"}, "verify"),
        ({**SASL_SSL, "sasl.mechanism": "PLAIN"}, "SCRAM-SHA-512"),
        ({**SASL_SSL, "sasl.password": ""}, "no username or password"),
        ({k: v for k, v in SASL_SSL.items() if k != "sasl.username"}, "no username or password"),
        ({**SASL_SSL, "security.protocol": "sasl_ssl"}, "security.protocol"),
        ({**SASL_SSL, "security.protocol": "SASL_PLAINTEXT"}, "security.protocol"),
        ({"security.protocol": "SSL", **TLS_PINS, "sasl.username": USERNAME}, "no sasl"),
        ({"security.protocol": "SSL", **TLS_PINS, "ssl.certificate.location": "/c"}, "both"),
        ({"security.protocol": "SSL", **TLS_PINS, "ssl.ca.location": ""}, "non-empty path"),
        ({"security.protocol": "PLAINTEXT"}, "non-loopback"),
        ({}, "non-loopback"),
    ],
)
def test_link_rules(security: dict[str, Any], rule: str) -> None:
    with pytest.raises(KafkaSecurityError, match=rule) as refused:
        KafkaSettings("kafka-0.synth:9093", security)
    assert PASSWORD_MARKER not in str(refused.value)


def test_plaintext_to_loopback_carries_no_credentials() -> None:
    with pytest.raises(KafkaSecurityError, match="no ssl"):
        KafkaSettings("127.0.0.1:9092", {"security.protocol": "PLAINTEXT", "sasl.password": PASSWORD_MARKER})


def test_server_authenticated_ssl_is_valid_for_a_foreign_cluster() -> None:
    """The kafka source builds `KafkaSettings` for a foreign cluster, where SSL without a client
    certificate is valid (ADR-007 D11). Only the engine loader requires the certificate."""
    settings_ = KafkaSettings("kafka.partner.synth:9093", {"security.protocol": "SSL", **TLS_PINS})
    assert settings_.protocol == "SSL"


@pytest.mark.parametrize(
    "bootstrap",
    ["", ",", "127.0.0.1:9092,", " 127.0.0.1:9092", "127.0.0.1:9092, localhost:9092", "PLAINTEXT://kafka:9092"],
)
def test_malformed_bootstrap_strings_are_refused(bootstrap: str) -> None:
    with pytest.raises(KafkaSecurityError):
        KafkaSettings(bootstrap, SASL_SSL)


@pytest.mark.parametrize(
    ("entry", "loopback"),
    [
        ("127.0.0.1:9092", True),
        ("127.255.255.254:10", True),
        ("127.0.0.1:1", False),  # the contract grammar needs 2-5 digits
        ("localhost:65535", True),
        ("[::1]:9092", True),
        ("127.1.1.300:9092", False),
        ("127.0.0.1:0", False),
        ("128.0.0.1:9092", False),
        ("[::ffff:127.0.0.1]:9092", False),
        ("::1:9092", False),
        ("localhost:9092\n", False),
        ("0.0.0.0:9092", False),
    ],
)
def test_is_loopback_host_port(entry: str, loopback: bool) -> None:
    assert is_loopback_host_port(entry) is loopback


def test_split_bootstrap_refuses_invalid_ipv4_even_when_not_plaintext() -> None:
    with pytest.raises(KafkaSecurityError, match="IPv4"):
        split_bootstrap("10.0.0.300:9093")


def test_security_cannot_be_mutated_after_the_check() -> None:
    raw = dict(SASL_SSL)
    settings_ = KafkaSettings("kafka-0.synth:9093", raw)
    raw["acks"] = "0"
    raw["ssl.cipher.suites"] = "ALL"
    assert isinstance(settings_.security, SecurityProperties)
    assert "acks" not in settings_.security
    assert settings_.security["ssl.cipher.suites"] == PINNED_CIPHER_SUITES


# --- invariant 1: contract values win (ADR-008 D4) ------------------------------------------------------


def _smuggle(settings_: KafkaSettings, extra: Mapping[str, Any]) -> KafkaSettings:
    """Bypass the allow-list as a future defect would, to prove the merge order holds on its own."""
    object.__setattr__(settings_, "security", SecurityProperties({**settings_.security, **extra}))
    return settings_


def test_contract_client_settings_win_over_the_security_map_even_if_one_slips_through() -> None:
    defaults = load_topic_defaults()
    hostile = {
        "acks": "0",
        "enable.idempotence": False,
        "bootstrap.servers": "evil:9092",
        "enable.auto.commit": True,
        "group.id": "evil",
    }
    settings_ = _smuggle(KafkaSettings("kafka-0.synth:9093", SASL_SSL), hostile)

    producer = producer_config(settings_, defaults, client_id="tb-dev-ch-1")
    assert producer["acks"] == "all" and producer["enable.idempotence"] is True
    assert producer["bootstrap.servers"] == "kafka-0.synth:9093"
    assert all(producer[key] == value for key, value in defaults.producer.items())

    consumer = consumer_config(settings_, defaults, group_id="tb.dev.ch.pipeline")
    assert consumer["enable.auto.commit"] is False and consumer["group.id"] == "tb.dev.ch.pipeline"
    assert consumer["bootstrap.servers"] == "kafka-0.synth:9093"

    events = events_producer_config(settings_, client_id="tb-events-1")
    assert events["acks"] == 1 and events["bootstrap.servers"] == "kafka-0.synth:9093"


def test_effective_producer_config_from_a_document_is_the_invariant_1_profile(
    tmp_path: Path, secrets_dir: Path
) -> None:
    settings_ = ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))
    producer = producer_config(settings_, load_topic_defaults(), client_id="tb-dev-ch-1")
    assert producer["acks"] == "all"
    assert producer["enable.idempotence"] is True
    assert producer["sasl.password"] == PASSWORD_MARKER  # librdkafka needs it; nothing else sees it
    assert producer["ssl.cipher.suites"] == PINNED_CIPHER_SUITES


# --- the worker ---------------------------------------------------------------------------------------


@pytest.fixture
def worker_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in ("TB_BOOTSTRAP_SERVERS", "TB_ENGINE_KAFKA_FILE", "TB_SECRETS_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TB_CHANNEL", "adt-passthrough")
    yield


def _refused(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines() if line.strip()]
    (refused,) = [line for line in lines if line["event"] == "startup_refused"]
    assert refused["component"] == "worker"
    return refused


def _exits_2(main: Any) -> None:
    with pytest.raises(SystemExit) as exited:
        main([])
    assert exited.value.code == 2


@pytest.mark.usefixtures("worker_env")
def test_worker_main_refuses_without_a_document_and_builds_no_client(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import confluent_kafka
    from tributary.runtime import worker

    def no_client(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("a Kafka client was built before the link was validated")

    monkeypatch.setattr(confluent_kafka, "Consumer", no_client)
    monkeypatch.setattr(confluent_kafka, "Producer", no_client)
    _exits_2(worker.main)
    refused = _refused(capsys)
    assert refused["code"] == "ENGINE_KAFKA_FILE_UNSET"
    assert "TB_ENGINE_KAFKA_FILE is not set" in refused["reason"]


@pytest.mark.usefixtures("worker_env")
def test_worker_main_refuses_the_retired_bootstrap_variable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    from tributary.runtime import worker

    for name, value in environ_for(tmp_path, document("PLAINTEXT")).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("TB_BOOTSTRAP_SERVERS", "localhost:9092")
    _exits_2(worker.main)
    refused = _refused(capsys)
    assert refused["code"] == "ENGINE_KAFKA_RETIRED_INPUT"
    assert "TB_BOOTSTRAP_SERVERS is retired" in refused["reason"]


@pytest.mark.usefixtures("worker_env")
def test_worker_main_refuses_a_non_loopback_plaintext_document(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    from tributary.runtime import worker

    for name, value in environ_for(tmp_path, document("PLAINTEXT", ["kafka:9092"])).items():
        monkeypatch.setenv(name, value)
    _exits_2(worker.main)
    refused = _refused(capsys)
    assert refused["error"] == "EngineKafkaError" and "no opt-out" in refused["reason"]
    assert refused["code"] == "ENGINE_KAFKA_DOCUMENT_INVALID"


class _Stop(Exception):
    pass


@pytest.mark.usefixtures("worker_env")
def test_the_channels_reader_is_the_first_client_and_uses_the_loaded_link(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, secrets_dir: Path
) -> None:
    """ADR-008: the `_tb.<env>.channels` reader is the most exposed client, because invariant 3 makes the
    worker trust what it reads. It must be built from the document's SASL_SSL link, not a default."""
    import confluent_kafka
    from tributary.runtime.worker import PipelineWorker, WorkerSettings

    built: list[dict[str, Any]] = []

    def capture(config: dict[str, Any]) -> None:
        built.append(config)
        raise _Stop

    monkeypatch.setattr(confluent_kafka, "Consumer", capture)
    stream = io.StringIO()
    kafka = ek.load_engine_kafka(environ_for(tmp_path, document(), secrets_dir))
    worker = PipelineWorker(
        WorkerSettings(env="dev", channel="adt-passthrough", kafka=kafka, instance_id="pipeline-1"),
        log=JsonLogger("worker", stream=stream),
    )
    with pytest.raises(_Stop):
        worker.build()
    (config,) = built
    assert config["group.id"] == "tb.dev.adt-passthrough.channels.pipeline-1"
    assert config["security.protocol"] == "SASL_SSL"
    assert all(config[key] == value for key, value in TLS_PINS.items())
    assert config["bootstrap.servers"] == "kafka-0.synth:9093"
    assert PASSWORD_MARKER not in stream.getvalue() and USERNAME not in stream.getvalue()


def test_refusals_are_runtime_configuration_errors() -> None:
    assert issubclass(EngineKafkaError, RuntimeConfigurationError)
    assert issubclass(KafkaSecurityError, RuntimeConfigurationError)


# --- the shared entry point (load_engine_kafka_or_exit) ------------------------------------------------


def _or_exit(environ: Mapping[str, str], **kwargs: Any) -> tuple[int | str | None, list[dict[str, Any]], str]:
    stream = io.StringIO()
    with pytest.raises(SystemExit) as exited:
        ek.load_engine_kafka_or_exit(environ, log=JsonLogger("receiver-synth", stream=stream), **kwargs)
    raw = stream.getvalue()
    return exited.value.code, [json.loads(line) for line in raw.splitlines()], raw


def test_the_entry_point_returns_the_settings_and_logs_the_link_once(tmp_path: Path, secrets_dir: Path) -> None:
    stream = io.StringIO()
    settings_ = ek.load_engine_kafka_or_exit(
        environ_for(tmp_path, document(), secrets_dir), log=JsonLogger("receiver-synth", stream=stream)
    )
    assert settings_.protocol == "SASL_SSL"
    (line,) = [json.loads(raw) for raw in stream.getvalue().splitlines()]
    assert line["event"] == "engine_kafka_link" and line["component"] == "receiver-synth"
    assert {k: line[k] for k in ("protocol", "sasl_mechanism", "client_certificate", "brokers")} == (
        settings_.describe()
    )
    assert PASSWORD_MARKER not in stream.getvalue() and USERNAME not in stream.getvalue()


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        (lambda tmp, sec: {}, "ENGINE_KAFKA_FILE_UNSET"),
        (
            lambda tmp, sec: {**environ_for(tmp, document(), sec), "TB_BOOTSTRAP_SERVERS": "x:1"},
            "ENGINE_KAFKA_RETIRED_INPUT",
        ),
        (lambda tmp, sec: {"TB_ENGINE_KAFKA_FILE": "relative.json"}, "ENGINE_KAFKA_FILE_UNREADABLE"),
        (lambda tmp, sec: {"TB_ENGINE_KAFKA_FILE": str(tmp / "absent.json")}, "ENGINE_KAFKA_FILE_UNREADABLE"),
        (lambda tmp, sec: environ_for(tmp, "{not json"), "ENGINE_KAFKA_DOCUMENT_INVALID"),
        (lambda tmp, sec: environ_for(tmp, document("PLAINTEXT", ["kafka:9092"])), "ENGINE_KAFKA_DOCUMENT_INVALID"),
        (lambda tmp, sec: environ_for(tmp, document(), tmp / "no-secrets"), "ENGINE_KAFKA_SECRET_UNRESOLVED"),
    ],
    ids=["unset", "retired-env", "relative", "absent", "not-json", "plaintext-remote", "no-secrets"],
)
def test_every_refusal_is_one_line_with_its_code_and_exit_2(
    tmp_path: Path, secrets_dir: Path, setup: Any, code: str
) -> None:
    status, lines, raw = _or_exit(setup(tmp_path, secrets_dir))
    assert status == 2
    (line,) = lines
    assert line["event"] == "startup_refused" and line["level"] == "error"
    assert line["component"] == "receiver-synth" and line["error"] == "EngineKafkaError"
    assert line["code"] == code and line["reason"]
    assert PASSWORD_MARKER not in raw and str(secrets_dir) not in raw


def test_a_retired_flag_is_refused_through_argv(tmp_path: Path, secrets_dir: Path) -> None:
    status, (line,), raw = _or_exit(
        environ_for(tmp_path, document(), secrets_dir), argv=["--bootstrap=" + PASSWORD_MARKER]
    )
    assert status == 2 and line["code"] == "ENGINE_KAFKA_RETIRED_INPUT"
    assert line["reason"].startswith("--bootstrap is retired") and PASSWORD_MARKER not in raw


def test_the_entry_point_catches_every_runtime_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """KafkaSecurityError raised directly, and any other RuntimeConfigurationError: exit 2, one line. Only
    the classes whose messages are fixed sentences have a `reason`; any other logs its class and code."""
    for raised, code, has_reason in [
        (KafkaSecurityError("a PLAINTEXT link carries no ssl.* or sasl.* property"), "KAFKA_LINK_REFUSED", True),
        (ChannelSpecError(f"free text {PASSWORD_MARKER}"), "CHANNEL_SPEC_INVALID", False),
        (RuntimeConfigurationError(f"free text {PASSWORD_MARKER}"), "RUNTIME_CONFIG_INVALID", False),
    ]:

        def raise_it(*_a: object, _exc: Exception = raised, **_k: object) -> KafkaSettings:
            raise _exc

        monkeypatch.setattr(ek, "load_engine_kafka", raise_it)
        status, (line,), raw = _or_exit({})
        assert status == 2 and line["error"] == type(raised).__name__ and line["code"] == code
        assert ("reason" in line) is has_reason
        assert PASSWORD_MARKER not in raw


def test_an_unexpected_error_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bug is a traceback, not a configuration refusal: exit 2 would tell the operator to fix a file."""

    def broken(*_a: object, **_k: object) -> KafkaSettings:
        raise ZeroDivisionError

    monkeypatch.setattr(ek, "load_engine_kafka", broken)
    with pytest.raises(ZeroDivisionError):
        ek.load_engine_kafka_or_exit({}, log=JsonLogger("receiver-synth", stream=io.StringIO()))


def test_a_logger_that_refuses_the_reason_still_gets_one_line_and_exit_2() -> None:
    class Picky:
        def __init__(self) -> None:
            self.lines: list[dict[str, object]] = []

        def error(self, event: str, /, **fields: object) -> None:
            if "reason" in fields:
                raise ValueError("guard refused the field")
            self.lines.append({"event": event, **fields})

    picky = Picky()
    with pytest.raises(SystemExit) as exited:
        ek.load_engine_kafka_or_exit({}, log=picky)  # type: ignore[arg-type]
    assert exited.value.code == 2
    assert picky.lines == [{"event": "startup_refused", "error": "EngineKafkaError", "code": "ENGINE_KAFKA_FILE_UNSET"}]


def test_the_package_exports_the_entry_point_api() -> None:
    import tributary.runtime as runtime
    from tributary.runtime import secrets

    assert runtime.load_engine_kafka is ek.load_engine_kafka
    assert runtime.load_engine_kafka_or_exit is ek.load_engine_kafka_or_exit
    assert runtime.EngineKafkaError is EngineKafkaError and runtime.KafkaSecurityError is KafkaSecurityError
    assert runtime.DirSecretResolver is secrets.DirSecretResolver
    assert runtime.SecretResolver is secrets.SecretResolver
    assert set(runtime.__all__) >= {
        "load_engine_kafka",
        "load_engine_kafka_or_exit",
        "EngineKafkaError",
        "KafkaSecurityError",
        "DirSecretResolver",
        "SecretResolver",
    }


def test_error_codes_are_stable_tokens() -> None:
    from tributary.runtime.errors import ENGINE_KAFKA_CODES

    for code in [*ENGINE_KAFKA_CODES, KafkaSecurityError.code, SecretResolutionError.code]:
        assert re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", code)
    assert EngineKafkaError("x", code="NOT_A_CODE").code == "ENGINE_KAFKA_DOCUMENT_INVALID"
