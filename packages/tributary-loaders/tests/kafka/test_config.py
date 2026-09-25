"""`kafka_destination_config_from_destination`: refuses what this loader cannot honour (wrong type,
unsupported idempotency technique, a malformed connection, a platform/channel-shaped topic) rather than
silently ignoring it. The published example fixture exercises the real, deploy-time-validated shape;
the rest build `Destination` objects directly, matching `tests/postgresql/test_config.py`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.kafka.config import kafka_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load_example(name: str, destination_name: str) -> Destination:
    spec = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    return ChannelSpec.from_document(spec).destination(destination_name)


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "partner-feed",
        "type": "kafka",
        "params": {
            "connection": {
                "bootstrapServers": ["kafka.partner.example.org:9093"],
                "security": {"protocol": "SSL", "tls": {"secretRef": "partner-kafka-mtls"}},
            },
            "topic": "partner.lab-results",
            "key": "preserve",
            "timeoutMs": 10000,
        },
        "idempotency": Idempotency(technique="idempotency-key-header", header="tb-msg-id"),
        "retry": RetryPolicy(policy="blocking", max_attempts=10, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def test_builds_config_from_the_published_example() -> None:
    destination = load_example("kafka-lab-events-to-clickhouse-and-kafka.json", "partner-feed")
    cfg = kafka_destination_config_from_destination("lab-event-bridge", destination)
    assert cfg.connection.bootstrap_servers == ("kafka.partner.example.org:9093",)
    assert cfg.connection.security.protocol == "SSL"
    assert cfg.connection.security.tls is not None
    assert cfg.connection.security.tls.secret_ref == "partner-kafka-mtls"  # noqa: S105 - a secretRef name, not a credential value
    assert cfg.topic == "partner.lab-results"
    assert cfg.key_mode == "preserve"
    assert cfg.timeout_s == 10.0
    assert cfg.idempotency_technique == "idempotency-key-header"
    assert cfg.idempotency_header == "tb-msg-id"


def test_wrong_type_is_refused() -> None:
    with pytest.raises(ConfigError, match="not 'kafka'"):
        kafka_destination_config_from_destination("c", make_destination(type="http"))


def test_sink_native_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        kafka_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="sink-native"))
        )


def test_conditional_update_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        kafka_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="conditional-update", note="n/a"))
        )


def test_none_is_accepted_with_a_note() -> None:
    cfg = kafka_destination_config_from_destination(
        "c", make_destination(idempotency=Idempotency(technique="none", note="no consumer-side dedupe"))
    )
    assert cfg.idempotency_technique == "none"
    assert cfg.idempotency_header is None


def test_idempotency_key_header_without_a_header_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="header"):
        kafka_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="idempotency-key-header"))
        )


def test_sasl_ssl_requires_a_mechanism_and_secret_ref() -> None:
    with pytest.raises(ConfigError, match="secretRef"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.example.org:9093"],
                        "security": {"protocol": "SASL_SSL", "saslMechanism": "SCRAM-SHA-512"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_sasl_ssl_rejects_an_unsupported_mechanism() -> None:
    with pytest.raises(ConfigError, match="saslMechanism"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.example.org:9093"],
                        "security": {
                            "protocol": "SASL_SSL",
                            "saslMechanism": "PLAIN",
                            "secretRef": "kafka-creds",
                        },
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_plaintext_with_a_secret_ref_is_refused() -> None:
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["127.0.0.1:9092"],
                        "security": {"protocol": "PLAINTEXT", "secretRef": "should-not-be-here"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_unsupported_protocol_is_refused() -> None:
    with pytest.raises(ConfigError, match="protocol"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.example.org:9093"],
                        "security": {"protocol": "SASL_PLAINTEXT"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_empty_bootstrap_servers_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="bootstrapServers"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {"bootstrapServers": [], "security": {"protocol": "SSL"}},
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


@pytest.mark.parametrize(
    "bad_topic",
    ["_tb.dev.events", "dev.channel-name.out.dest", "dev.channel-name.raw", "..", "has a space", ""],
)
def test_platform_or_channel_shaped_topic_is_refused(bad_topic: str) -> None:
    with pytest.raises(ConfigError, match="topic"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.example.org:9093"],
                        "security": {"protocol": "SSL"},
                    },
                    "topic": bad_topic,
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_malformed_key_mode_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match=r"with\.key"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.example.org:9093"],
                        "security": {"protocol": "SSL"},
                    },
                    "topic": "partner.events",
                    "key": "round-robin",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_circuit_breaker_is_optional_and_parsed_when_present() -> None:
    cfg = kafka_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "connection": {
                    "bootstrapServers": ["kafka.example.org:9093"],
                    "security": {"protocol": "SSL"},
                },
                "topic": "partner.events",
                "key": "none",
                "timeoutMs": 5000,
                "circuitBreaker": {"failureThreshold": 4, "openSeconds": 20},
            },
            idempotency=Idempotency(technique="none", note="n/a"),
        ),
    )
    assert cfg.circuit_breaker is not None
    assert cfg.circuit_breaker.failure_threshold == 4
    assert cfg.circuit_breaker.open_seconds == 20.0
