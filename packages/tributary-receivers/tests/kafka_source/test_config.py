"""`kafka_source_settings_from_channel_spec` against the contract's own example."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.kafka_source.config import KafkaSourceSettings, kafka_source_settings_from_channel_spec
from tributary.receivers.kafka_source.security import PINNED_CIPHER_SUITES, security_properties
from tributary.receivers.secrets import DirSecretResolver

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load() -> dict[str, Any]:
    return copy.deepcopy(json.loads((EXAMPLES / "kafka-lab-events-to-clickhouse-and-kafka.json").read_text("utf-8")))


def build(spec: dict[str, Any], **kw: Any) -> KafkaSourceSettings:
    return kafka_source_settings_from_channel_spec(
        spec, env="dev", bootstrap_servers="engine:9092", commit_every=kw.pop("commit_every", 50), **kw
    )


def test_contract_example_is_accepted() -> None:
    cfg = build(load())
    assert cfg.topic == "lab.results.v1" and cfg.start_from == "earliest"
    assert cfg.source_bootstrap == ("kafka-1.lab.example.org:9093", "kafka-2.lab.example.org:9093")
    assert cfg.security.protocol == "SASL_SSL" and cfg.security.tls is not None
    assert cfg.group_id == "tb.dev.lab-event-bridge.receiver"
    assert cfg.bootstrap_servers == "engine:9092"  # the engine cluster, distinct from the source's
    assert cfg.key_expression is not None  # record:key


def test_secret_refs_are_not_in_repr() -> None:
    text = repr(build(load()))
    assert "lab-kafka-scram" not in text and "lab-kafka-ca" not in text


def test_commit_every_defaults_to_the_topics_contract() -> None:
    cfg = kafka_source_settings_from_channel_spec(load(), env="dev", bootstrap_servers="e:9092")
    assert cfg.commit_every == 50


@pytest.mark.parametrize("topic", ["_tb.dev.channels", "dev.adt.raw", "dev.adt.out.fhir", "prod.x-y.dlq", ".."])
def test_platform_and_channel_shaped_topics_are_refused(topic: str) -> None:
    spec = load()
    spec["spec"]["source"]["kafka"]["topic"] = topic
    with pytest.raises(ConfigError, match="topic"):
        build(spec)


def test_plaintext_to_a_non_loopback_broker_is_refused() -> None:
    spec = load()
    conn = spec["spec"]["source"]["kafka"]["connection"]
    conn["security"] = {"protocol": "PLAINTEXT"}
    with pytest.raises(ConfigError, match="loopback"):
        build(spec)
    conn["bootstrapServers"] = ["127.0.0.1:9092", "localhost:9093"]
    assert build(spec).security.protocol == "PLAINTEXT"


def test_tls_min_version_other_than_default_is_refused_not_silently_ignored() -> None:
    spec = load()
    spec["spec"]["source"]["kafka"]["connection"]["security"]["tls"]["minVersion"] = "1.3"
    with pytest.raises(ConfigError, match="minVersion"):
        build(spec)


@pytest.mark.parametrize("nack_on", [["queue-full"], ["spec-invalid"]])
def test_nack_conditions_a_kafka_source_cannot_produce_are_refused(nack_on: list[str]) -> None:
    spec = load()
    spec["spec"]["source"]["ack"]["nackOn"] = nack_on
    with pytest.raises(ConfigError, match="nackOn"):
        build(spec)


def test_record_key_scheme_needs_a_key_and_per_key_is_validated_at_startup() -> None:
    spec = load()
    spec["spec"]["partitionKey"] = "hl7:PID.3.1"  # content type is application/json: cannot apply
    with pytest.raises(ConfigError):
        build(spec)


def test_engine_cluster_is_never_used_for_the_source_connection() -> None:
    cfg = build(load())
    assert "engine" not in ",".join(cfg.source_bootstrap)


def _secrets(tmp_path: Path) -> DirSecretResolver:
    (tmp_path / "lab-kafka-scram").mkdir()
    (tmp_path / "lab-kafka-scram" / "username").write_text("svc-synth\n", encoding="utf-8")
    (tmp_path / "lab-kafka-scram" / "password").write_text("synthpw\n", encoding="utf-8")  # tb-secret-scan: allow
    (tmp_path / "lab-kafka-ca").mkdir()
    (tmp_path / "lab-kafka-ca" / "ca.crt").write_text("-", encoding="utf-8")
    return DirSecretResolver(tmp_path)


def test_security_properties_verify_the_broker_and_pin_the_cipher_floor(tmp_path: Path) -> None:
    props = security_properties(build(load()).security, _secrets(tmp_path))
    assert props["security.protocol"] == "SASL_SSL" and props["sasl.mechanism"] == "SCRAM-SHA-512"
    assert props["sasl.username"] == "svc-synth" and props["sasl.password"] == "synthpw"  # tb-secret-scan: allow
    assert props["ssl.cipher.suites"] == PINNED_CIPHER_SUITES
    assert props["ssl.endpoint.identification.algorithm"] == "https"
    assert props["enable.ssl.certificate.verification"] is True
    assert props["ssl.ca.location"].endswith("lab-kafka-ca/ca.crt")


def test_empty_tls_object_still_verifies_against_the_platform_trust_store(tmp_path: Path) -> None:
    spec = load()
    spec["spec"]["source"]["kafka"]["connection"]["security"]["tls"] = {}
    cfg = build(spec)
    assert cfg.security.tls is not None  # `tls: {}` is configured, not absent
    props = security_properties(cfg.security, _secrets(tmp_path))
    assert props["ssl.endpoint.identification.algorithm"] == "https" and "ssl.ca.location" not in props
