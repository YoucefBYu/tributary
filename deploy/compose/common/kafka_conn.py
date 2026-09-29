"""Kafka client settings for the two Compose helper jobs (topic-bootstrap, channel-publish).

They are not data-plane processes, so engine-kafka/v0 does not cover them (its section 1); they reuse
its document shape and secret layout (sections 2 and 3) so the lite stack has one convention, and
they enforce the same rules a data-plane process does (section 4): SASL_SSL only, TLS always
verified, the AEAD cipher pin, credentials present, and nothing but the fixed key set in the result.
Stdlib only: the job images do not carry `tributary-runtime`.

Secrets are read from files under `TB_SECRETS_DIR/<secretRef>/<key>` at call time and are never
logged or placed in an exception message.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_SECRETS_DIR = Path("/var/run/secrets/tributary")
CIPHER_SUITES = "ECDHE+AESGCM:ECDHE+CHACHA20"


class KafkaConnError(Exception):
    """Invalid connection document or missing credentials. Never carries a secret value."""


def _read_secret(root: Path, ref: str, key: str) -> str:
    path = root / ref / key
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise KafkaConnError(f"cannot read secret {ref}/{key}") from exc
    if not value:
        raise KafkaConnError(f"secret {ref}/{key} is empty")
    return value


def load_client_config(path: Path | None = None, secrets_dir: Path | None = None) -> dict[str, Any]:
    """The librdkafka properties for one `EngineKafkaConnection` document (SASL_SSL/SCRAM only)."""
    doc_path = path or (Path(v) if (v := os.environ.get("TB_ENGINE_KAFKA_FILE")) else None)
    if doc_path is None:
        raise KafkaConnError("TB_ENGINE_KAFKA_FILE is required")
    root = secrets_dir or Path(os.environ.get("TB_SECRETS_DIR", str(DEFAULT_SECRETS_DIR)))
    try:
        doc = json.loads(doc_path.read_text(encoding="utf-8"))
        servers = list(doc["bootstrapServers"])
        security = doc["security"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise KafkaConnError("engine-kafka document unreadable or invalid") from exc
    if security.get("protocol") != "SASL_SSL" or security.get("saslMechanism") != "SCRAM-SHA-512":
        raise KafkaConnError("only SASL_SSL with SCRAM-SHA-512 is supported by the compose helpers")
    if not servers:
        raise KafkaConnError("bootstrapServers is empty")
    config: dict[str, Any] = {
        "bootstrap.servers": ",".join(servers),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": _read_secret(root, security["secretRef"], "username"),
        "sasl.password": _read_secret(root, security["secretRef"], "password"),
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        "ssl.cipher.suites": CIPHER_SUITES,
    }
    if tls_ref := (security.get("tls") or {}).get("secretRef"):
        config["ssl.ca.location"] = str(root / tls_ref / "ca.crt")
    return config
