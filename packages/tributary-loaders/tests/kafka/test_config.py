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


def test_plaintext_to_a_non_loopback_broker_is_refused() -> None:
    """Security review, High: `$defs/kafkaConnection.allOf`'s "PLAINTEXT only when every bootstrap
    server is this host" is a deploy-time-only schema rule; `tributary.runtime.spec.ChannelSpec.
    from_document` (invariant 3: the data plane does not depend on the control plane) never runs the
    full JSON Schema, so this loader's own config builder must refuse the same shape on its own or a
    `kafka` destination could send PHI to a real, non-loopback broker in the clear."""
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.partner.example.org:9092"],
                        "security": {"protocol": "PLAINTEXT"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


@pytest.mark.parametrize("loopback", ["localhost:9092", "127.0.0.1:9092", "[::1]:9092"])
def test_plaintext_to_a_loopback_broker_is_accepted(loopback: str) -> None:
    cfg = kafka_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "connection": {"bootstrapServers": [loopback], "security": {"protocol": "PLAINTEXT"}},
                "topic": "partner.events",
                "key": "none",
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.connection.security.protocol == "PLAINTEXT"


def test_plaintext_is_refused_if_any_one_bootstrap_server_is_not_loopback() -> None:
    """A mixed list must not let one loopback entry vouch for the rest."""
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["127.0.0.1:9092", "kafka.partner.example.org:9092"],
                        "security": {"protocol": "PLAINTEXT"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_plaintext_is_refused_for_a_bootstrap_server_with_a_trailing_newline() -> None:
    """Security review, Low: `_LOOPBACK_HOST_PORT_RE` is matched with `.fullmatch()`, not `.match()` --
    with `.match()`, an unescaped trailing `$` in the pattern matches just before a trailing newline as
    well as the true end of string, so `"127.0.0.1:9092\\n"` would have been accepted as loopback."""
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["127.0.0.1:9092\n"],
                        "security": {"protocol": "PLAINTEXT"},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_empty_tls_block_is_not_treated_as_absent() -> None:
    """Security review, High, the same class of bug as the mllp `with.tls: {}` finding: `tls: {}` is
    present-but-empty -- `$defs/clientTls`'s own comment reads it as "use the platform trust store" --
    and must still produce a `KafkaTlsSpec` (so `client.py` sets
    `ssl.endpoint.identification.algorithm=https` explicitly), not `None` the way an absent `tls` key
    does for a `PLAINTEXT` destination."""
    cfg = kafka_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "connection": {
                    "bootstrapServers": ["kafka.partner.example.org:9093"],
                    "security": {"protocol": "SSL", "tls": {}},
                },
                "topic": "partner.events",
                "key": "none",
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.connection.security.tls is not None
    assert cfg.connection.security.tls.secret_ref is None
    assert cfg.connection.security.tls.min_version == "1.2"


def test_plaintext_with_an_empty_tls_block_is_still_refused() -> None:
    """The truthiness fix must not accidentally let `tls: {}` slip past the PLAINTEXT-cannot-carry-tls
    check the way it used to slip past `tls_obj else None`."""
    with pytest.raises(ConfigError, match="PLAINTEXT"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["127.0.0.1:9092"],
                        "security": {"protocol": "PLAINTEXT", "tls": {}},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_non_default_min_version_is_refused() -> None:
    """Security review, High: librdkafka has no property to enforce a TLS minimum version
    (`client.py`'s module docstring) -- a `minVersion` other than the default is refused at config-build
    time rather than silently accepted and never actually enforced."""
    with pytest.raises(ConfigError, match="minVersion"):
        kafka_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "connection": {
                        "bootstrapServers": ["kafka.partner.example.org:9093"],
                        "security": {"protocol": "SSL", "tls": {"minVersion": "1.3"}},
                    },
                    "topic": "partner.events",
                    "key": "none",
                    "timeoutMs": 5000,
                }
            ),
        )


def test_default_min_version_is_accepted() -> None:
    cfg = kafka_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "connection": {
                    "bootstrapServers": ["kafka.partner.example.org:9093"],
                    "security": {"protocol": "SSL", "tls": {"minVersion": "1.2"}},
                },
                "topic": "partner.events",
                "key": "none",
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.connection.security.tls is not None
    assert cfg.connection.security.tls.min_version == "1.2"


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
