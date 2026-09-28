"""`producer_settings_from_config`: the destination's own connection and credentials become
librdkafka properties, never the engine's (ADR-007 D11) — `acks=all` plus idempotence always on, TLS
verification never turned off, and `secretRef` resolved through the same `resolve_text`/`resolve_file`
split `postgresql/client.py`'s tests use."""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.loaders.kafka.client import TransportError, producer_settings_from_config
from tributary.loaders.kafka.config import (
    KafkaConnectionSpec,
    KafkaDestinationConfig,
    KafkaSecuritySpec,
    KafkaTlsSpec,
)


class FakeSecretResolver:
    def __init__(self, texts: dict[tuple[str, str], str], files: dict[tuple[str, str], Path] | None = None) -> None:
        self.texts = texts
        self.files = files or {}

    def resolve_text(self, ref: str, key: str) -> str:
        try:
            return self.texts[(ref, key)]
        except KeyError:
            raise FileNotFoundError(f"no {key!r} under {ref!r}") from None

    def resolve_file(self, ref: str, key: str) -> Path:
        try:
            return self.files[(ref, key)]
        except KeyError:
            raise FileNotFoundError(f"no {key!r} under {ref!r}") from None


def make_config(**overrides: object) -> KafkaDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "partner-feed",
        "connection": KafkaConnectionSpec(("kafka.example.org:9093",), KafkaSecuritySpec("SSL")),
        "topic": "partner.events",
        "key_mode": "preserve",
        "timeout_s": 5.0,
        "idempotency_technique": "idempotency-key-header",
        "idempotency_header": "tb-msg-id",
        "circuit_breaker": None,
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return KafkaDestinationConfig(**base)  # type: ignore[arg-type]


def test_plaintext_has_no_ssl_or_sasl_properties() -> None:
    config = make_config(connection=KafkaConnectionSpec(("127.0.0.1:9092",), KafkaSecuritySpec("PLAINTEXT")))
    settings = producer_settings_from_config(config, FakeSecretResolver({}))
    assert settings.config["security.protocol"] == "PLAINTEXT"
    assert "sasl.username" not in settings.config
    assert "ssl.ca.location" not in settings.config


def test_ssl_loads_a_trust_bundle_when_a_tls_secret_ref_is_given(tmp_path: Path) -> None:
    ca = tmp_path / "ca.crt"
    resolver = FakeSecretResolver({}, {("partner-ca", "ca.crt"): ca})
    config = make_config(
        connection=KafkaConnectionSpec(
            ("kafka.example.org:9093",), KafkaSecuritySpec("SSL", tls=KafkaTlsSpec("partner-ca"))
        )
    )
    settings = producer_settings_from_config(config, resolver)
    assert settings.config["ssl.ca.location"] == str(ca)
    assert settings.config["ssl.endpoint.identification.algorithm"] == "https"
    # No `ssl.min.version`: not a real librdkafka property (security review, High, client.py's module
    # docstring) -- `config.py` refuses any non-default minVersion long before this function runs, so
    # this dict never claims an enforcement this loader cannot actually perform.
    assert "ssl.min.version" not in settings.config


def test_ssl_with_no_tls_block_does_not_load_a_trust_bundle() -> None:
    config = make_config(connection=KafkaConnectionSpec(("kafka.example.org:9093",), KafkaSecuritySpec("SSL")))
    settings = producer_settings_from_config(config, FakeSecretResolver({}))
    assert "ssl.ca.location" not in settings.config


def test_ssl_with_a_secret_ref_loads_a_client_certificate(tmp_path: Path) -> None:
    cert = tmp_path / "tls.crt"
    key = tmp_path / "tls.key"
    resolver = FakeSecretResolver({}, {("partner-mtls", "tls.crt"): cert, ("partner-mtls", "tls.key"): key})
    config = make_config(
        connection=KafkaConnectionSpec(
            ("kafka.example.org:9093",),
            KafkaSecuritySpec("SSL", secret_ref="partner-mtls"),  # noqa: S106 - a secretRef name, not a credential value
        )
    )
    settings = producer_settings_from_config(config, resolver)
    assert settings.config["ssl.certificate.location"] == str(cert)
    assert settings.config["ssl.key.location"] == str(key)


def test_sasl_ssl_resolves_username_and_password() -> None:
    resolver = FakeSecretResolver({("lab-scram", "username"): "loader", ("lab-scram", "password"): "s3cret"})
    config = make_config(
        connection=KafkaConnectionSpec(
            ("kafka.lab.example.org:9093",),
            KafkaSecuritySpec("SASL_SSL", sasl_mechanism="SCRAM-SHA-512", secret_ref="lab-scram"),  # noqa: S106 - a secretRef name, not a credential value
        )
    )
    settings = producer_settings_from_config(config, resolver)
    assert settings.config["sasl.mechanism"] == "SCRAM-SHA-512"
    assert settings.config["sasl.username"] == "loader"
    assert settings.config["sasl.password"] == "s3cret"


def test_acks_all_and_idempotence_are_always_on() -> None:
    settings = producer_settings_from_config(make_config(), FakeSecretResolver({}))
    assert settings.config["acks"] == "all"
    assert settings.config["enable.idempotence"] is True


def test_bootstrap_servers_join_the_destinations_own_list_not_the_engines() -> None:
    config = make_config(
        connection=KafkaConnectionSpec(("a.example.org:9093", "b.example.org:9093"), KafkaSecuritySpec("SSL"))
    )
    settings = producer_settings_from_config(config, FakeSecretResolver({}))
    assert settings.config["bootstrap.servers"] == "a.example.org:9093,b.example.org:9093"


def test_message_timeout_is_derived_from_the_destinations_timeout() -> None:
    settings = producer_settings_from_config(make_config(timeout_s=8.0), FakeSecretResolver({}))
    assert settings.config["message.timeout.ms"] == 8000


def test_producer_settings_repr_never_shows_the_config_dict() -> None:
    """Security review, Low: the default dataclass `repr` would otherwise print `config` verbatim,
    including `sasl.password` in the clear for a SASL_SSL destination."""
    settings = producer_settings_from_config(
        make_config(
            connection=KafkaConnectionSpec(
                ("kafka.example.org:9093",),
                KafkaSecuritySpec("SASL_SSL", sasl_mechanism="SCRAM-SHA-512", secret_ref="partner-sasl"),  # noqa: S106
            )
        ),
        FakeSecretResolver({("partner-sasl", "username"): "svc", ("partner-sasl", "password"): "s3cret!"}),
    )
    assert "s3cret" not in repr(settings)
    assert "config=" not in repr(settings)


@pytest.mark.parametrize(
    "security",
    [
        KafkaSecuritySpec("SSL"),
        KafkaSecuritySpec("SASL_SSL", sasl_mechanism="SCRAM-SHA-512", secret_ref="partner-sasl"),  # noqa: S106
    ],
)
def test_settings_construct_a_real_producer_with_no_broker_needed(security: KafkaSecuritySpec) -> None:
    """Security review, High: a fake opener or a plain dict assertion (the shape this suite's other
    tests use) cannot catch a property librdkafka itself rejects at construction time -- `ssl.min.
    version` (dropped by this loader's own fix) passed every existing test here while making every real
    `confluent_kafka.Producer(...)` call for an SSL/SASL_SSL destination fail outright. Constructing a
    real `Producer` needs no broker: librdkafka validates configuration synchronously and connects to
    the (here, unreachable) broker asynchronously in the background."""
    from confluent_kafka import Producer

    resolver = FakeSecretResolver({("partner-sasl", "username"): "svc", ("partner-sasl", "password"): "s3cret!"})
    settings = producer_settings_from_config(
        make_config(connection=KafkaConnectionSpec(("127.0.0.1:1",), security)), resolver
    )
    Producer(settings.config)  # raises if librdkafka rejects any property in `settings.config`


def test_missing_sasl_secret_ref_raises_when_used_without_config_validation() -> None:
    """`config.py` already refuses this at parse time (`test_config.py::
    test_sasl_ssl_requires_a_mechanism_and_secret_ref`); this asserts `client.py`'s own assumption
    holds if that check were ever bypassed by a hand-built config."""
    config = make_config(
        connection=KafkaConnectionSpec(
            ("kafka.example.org:9093",), KafkaSecuritySpec("SASL_SSL", sasl_mechanism="SCRAM-SHA-512")
        )
    )
    with pytest.raises(TransportError):
        producer_settings_from_config(config, FakeSecretResolver({}))
