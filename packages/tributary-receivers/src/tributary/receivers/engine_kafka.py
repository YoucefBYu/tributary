"""The engine-side Kafka link a receiver produces `<env>.<channel>.raw` over.

`contracts/topics/v0` 3.4 requires `SASL_SSL` (or `SSL` with mTLS) on any listener a non-local client
can reach and allows `PLAINTEXT` only on a loopback-bound lite listener; the receiver principal then
holds WRITE on `raw` only, which needs an authenticated connection. So a receiver's producer is
refused at startup unless it is either (a) authenticated and TLS-verified with the AEAD cipher pin, or
(b) plaintext to loopback brokers only. `RawPublisher` enforces this itself (`require_secure_link`), so
no entry point can build a producer that bypasses it.

PROVISIONAL (pending an architect ruling, see the W2-7 security-review handoff): the contract does not
yet say where a receiver's engine-side credentials come from. Until it does, `--engine-security` names a
JSON file in the shape of `channel-spec/v0` `$defs/kafkaSecurity` (`protocol`, `saslMechanism`,
`secretRef`, `tls.secretRef`), resolved through the same `secretRef` mechanism as every other secret;
no new field is added to any contract. Absent, the link is plaintext and therefore loopback-only.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .errors import ConfigError
from .kafka_source.config import LOOPBACK_HOST_PORT, SourceSecurity, parse_security
from .kafka_source.security import PINNED_CIPHER_SUITES, security_properties
from .secrets import SecretResolver

__all__ = ["engine_security_properties", "require_secure_link"]


def _entries(bootstrap_servers: str) -> list[str]:
    return [b.strip() for b in bootstrap_servers.split(",") if b.strip()]


def require_secure_link(bootstrap_servers: str, security: Mapping[str, Any] | None) -> None:
    """Raise `ConfigError` for a producer configuration that would send `raw` unauthenticated or
    unverified to a non-loopback broker. Messages name no host, credential or path."""
    props = security or {}
    protocol = props.get("security.protocol", "PLAINTEXT")
    entries = _entries(bootstrap_servers)
    if not entries:
        raise ConfigError("engine bootstrap servers are empty")
    if protocol == "PLAINTEXT":
        if not all(LOOPBACK_HOST_PORT.fullmatch(e) for e in entries):
            raise ConfigError(
                "refusing a PLAINTEXT engine Kafka link to a non-loopback broker: configure SASL_SSL or "
                "SSL/mTLS via --engine-security (contracts/topics/v0 3.4)"
            )
        return
    if protocol not in ("SSL", "SASL_SSL"):
        raise ConfigError("engine Kafka security.protocol must be PLAINTEXT (loopback only), SSL or SASL_SSL")
    if (
        props.get("ssl.cipher.suites") != PINNED_CIPHER_SUITES
        or props.get("ssl.endpoint.identification.algorithm") != "https"
        or props.get("enable.ssl.certificate.verification") is not True
    ):
        raise ConfigError("engine Kafka TLS must verify the broker and use the pinned AEAD cipher list")
    if protocol == "SASL_SSL" and not (props.get("sasl.username") and props.get("sasl.password")):
        raise ConfigError("engine Kafka SASL_SSL has no credentials")


def engine_security_properties(
    bootstrap_servers: str, security_file: str | None, resolver: SecretResolver
) -> dict[str, Any]:
    """librdkafka properties for the engine link, or `ConfigError` if it would be insecure."""
    if security_file is None:
        security = SourceSecurity("PLAINTEXT")
    else:
        with open(security_file, encoding="utf-8") as f:
            document = json.load(f)
        if not isinstance(document, dict):
            raise ConfigError("--engine-security must be a JSON object")
        security = parse_security(document, "engine")
    if security.protocol == "PLAINTEXT" and not all(
        LOOPBACK_HOST_PORT.fullmatch(e) for e in _entries(bootstrap_servers)
    ):
        raise ConfigError(
            "refusing a PLAINTEXT engine Kafka link to a non-loopback broker: pass --engine-security "
            "(SASL_SSL with SCRAM-SHA-512, or SSL with mTLS)"
        )
    props = security_properties(security, resolver)
    require_secure_link(bootstrap_servers, props)
    return props
