"""The shared SASL_SSL settings for the compose helper jobs, and the committed engine-kafka documents."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "common"))
from kafka_conn import KafkaConnError, load_client_config  # noqa: E402

DOCS = sorted((HERE.parent / "engine-kafka").glob("*.json"))
SCHEMA = HERE.parents[2] / "contracts" / "engine-kafka" / "v0" / "engine-kafka.schema.json"


def _secrets(tmp_path: Path, ref: str = "tb-lite-x") -> Path:
    (tmp_path / ref).mkdir()
    (tmp_path / ref / "username").write_text("user-x")
    (tmp_path / ref / "password").write_text("not-a-real-password\n")
    (tmp_path / "tb-lite-kafka-ca").mkdir()
    return tmp_path


def _doc(tmp_path: Path, **security: object) -> Path:
    sec = {
        "protocol": "SASL_SSL",
        "saslMechanism": "SCRAM-SHA-512",
        "secretRef": "tb-lite-x",
        "tls": {"secretRef": "tb-lite-kafka-ca"},
    } | security
    p = tmp_path / "doc.json"
    p.write_text(json.dumps({"bootstrapServers": ["kafka:9094"], "security": sec}))
    return p


def test_config_is_verified_pinned_sasl_ssl(tmp_path: Path) -> None:
    cfg = load_client_config(_doc(tmp_path), _secrets(tmp_path))
    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-512"
    assert cfg["sasl.username"] == "user-x"
    assert cfg["sasl.password"] == "not-a-real-password"  # trailing newline stripped
    assert cfg["enable.ssl.certificate.verification"] is True
    assert cfg["ssl.endpoint.identification.algorithm"] == "https"
    assert cfg["ssl.cipher.suites"] == "ECDHE+AESGCM:ECDHE+CHACHA20"
    assert cfg["ssl.ca.location"].endswith("tb-lite-kafka-ca/ca.crt")
    assert set(cfg) == {
        "bootstrap.servers",
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "enable.ssl.certificate.verification",
        "ssl.endpoint.identification.algorithm",
        "ssl.cipher.suites",
        "ssl.ca.location",
    }


@pytest.mark.parametrize("override", [{"protocol": "PLAINTEXT"}, {"saslMechanism": "PLAIN"}])
def test_refuses_anything_but_scram_over_tls(tmp_path: Path, override: dict[str, str]) -> None:
    with pytest.raises(KafkaConnError):
        load_client_config(_doc(tmp_path, **override), _secrets(tmp_path))


def test_missing_or_empty_secret_is_refused_without_leaking_it(tmp_path: Path) -> None:
    secrets = _secrets(tmp_path)
    (secrets / "tb-lite-x" / "password").write_text("  \n")
    with pytest.raises(KafkaConnError, match="empty") as exc:
        load_client_config(_doc(tmp_path), secrets)
    assert "user-x" not in str(exc.value)
    with pytest.raises(KafkaConnError):
        load_client_config(_doc(tmp_path, secretRef="absent"), secrets)


def test_document_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TB_ENGINE_KAFKA_FILE", raising=False)
    with pytest.raises(KafkaConnError, match="TB_ENGINE_KAFKA_FILE"):
        load_client_config()


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.stem)
def test_committed_documents_are_valid_and_hold_no_secret(doc: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    body = json.loads(doc.read_text())
    jsonschema.validate(body, json.loads(SCHEMA.read_text()))
    assert body["security"]["protocol"] == "SASL_SSL"
    assert body["security"]["secretRef"] == f"tb-lite-{doc.stem}"
    assert "password" not in doc.read_text().lower()
