"""The engine-side Kafka link: no receiver may produce `raw` over an unauthenticated, unverified link to a
non-loopback broker (`contracts/topics/v0` 3.4). Every entry point is exercised, and `RawPublisher`
refuses on its own so none can bypass the check."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.engine_kafka import engine_security_properties, require_secure_link
from tributary.receivers.errors import ConfigError
from tributary.receivers.http.server import serve_from_channel_spec as serve_http
from tributary.receivers.kafka_source.receiver import serve_from_channel_spec as serve_kafka
from tributary.receivers.kafka_source.security import PINNED_CIPHER_SUITES
from tributary.receivers.mllp.kafka_publisher import RawPublisher
from tributary.receivers.mllp.server import serve_from_channel_spec as serve_mllp
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.sftp.poller import serve_from_channel_spec as serve_sftp

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"

ENTRY_POINTS: list[tuple[str, Callable[..., Awaitable[None]]]] = [
    ("http-fhir-ingest-to-fhir-and-postgresql.json", serve_http),
    ("walking-skeleton-loopback.json", serve_mllp),
    ("sftp-drop-to-s3-and-sftp.json", serve_sftp),
    ("kafka-lab-events-to-clickhouse-and-kafka.json", serve_kafka),
]


@pytest.mark.parametrize(("example", "serve"), ENTRY_POINTS, ids=["http", "mllp", "sftp", "kafka-source"])
@pytest.mark.parametrize("bootstrap", ["kafka:9092", "10.0.0.5:9092", "127.0.0.1:9092,kafka:9092"])
def test_every_receiver_refuses_a_plaintext_non_loopback_engine_link_at_startup(
    example: str, serve: Callable[..., Awaitable[None]], bootstrap: str
) -> None:
    spec: dict[str, Any] = json.loads((EXAMPLES / example).read_text(encoding="utf-8"))
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        asyncio.run(serve(spec, env="dev", bootstrap_servers=bootstrap))


def test_raw_publisher_refuses_on_its_own() -> None:
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        RawPublisher("kafka:9092", "dev.c.raw")
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        RawPublisher("kafka:9092", "dev.c.raw", security={"security.protocol": "PLAINTEXT"})


@pytest.mark.parametrize(
    "bootstrap", ["localhost:9092", "127.0.0.1:9092", "[::1]:9092", "localhost:9092,127.0.0.2:9093"]
)
def test_plaintext_to_loopback_is_allowed(bootstrap: str) -> None:
    require_secure_link(bootstrap, None)


@pytest.mark.parametrize(
    "props",
    [
        {"security.protocol": "SSL"},  # no pin, no verification settings
        {"security.protocol": "SSL", "ssl.cipher.suites": "ALL", "ssl.endpoint.identification.algorithm": "https"},
        {
            "security.protocol": "SSL",
            "ssl.cipher.suites": PINNED_CIPHER_SUITES,
            "ssl.endpoint.identification.algorithm": "none",
            "enable.ssl.certificate.verification": True,
        },
        {
            "security.protocol": "SASL_SSL",
            "ssl.cipher.suites": PINNED_CIPHER_SUITES,
            "ssl.endpoint.identification.algorithm": "https",
            "enable.ssl.certificate.verification": True,
        },  # no credentials
        {"security.protocol": "SASL_PLAINTEXT"},
    ],
)
def test_an_insecure_or_unverified_tls_config_is_refused(props: dict[str, Any]) -> None:
    with pytest.raises(ConfigError):
        require_secure_link("kafka.example:9093", props)


def test_sasl_ssl_from_secret_ref_passes_and_carries_the_pin(tmp_path: Path) -> None:
    (tmp_path / "eng").mkdir()
    (tmp_path / "eng" / "username").write_text("receiver-synth", encoding="utf-8")
    (tmp_path / "eng" / "password").write_text("synthpw", encoding="utf-8")  # tb-secret-scan: allow
    doc = tmp_path / "sec.json"
    doc.write_text(
        json.dumps({"protocol": "SASL_SSL", "saslMechanism": "SCRAM-SHA-512", "secretRef": "eng", "tls": {}}),
        encoding="utf-8",
    )
    props = engine_security_properties("kafka.example:9093", str(doc), DirSecretResolver(tmp_path))
    assert props["security.protocol"] == "SASL_SSL" and props["sasl.mechanism"] == "SCRAM-SHA-512"
    assert props["ssl.cipher.suites"] == PINNED_CIPHER_SUITES
    assert props["enable.ssl.certificate.verification"] is True


def test_security_file_with_plaintext_to_a_remote_broker_is_refused(tmp_path: Path) -> None:
    doc = tmp_path / "sec.json"
    doc.write_text(json.dumps({"protocol": "PLAINTEXT"}), encoding="utf-8")
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        engine_security_properties("kafka.example:9093", str(doc), DirSecretResolver(tmp_path))


def test_a_missing_credential_secret_fails_closed(tmp_path: Path) -> None:
    doc = tmp_path / "sec.json"
    doc.write_text(
        json.dumps({"protocol": "SASL_SSL", "saslMechanism": "SCRAM-SHA-512", "secretRef": "absent"}), encoding="utf-8"
    )
    with pytest.raises(FileNotFoundError):
        engine_security_properties("kafka.example:9093", str(doc), DirSecretResolver(tmp_path))
