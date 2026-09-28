"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: fhir`) into
`FhirDestinationConfig`. Shape: `channel.schema.json` `$defs/fhirParams`, README section 3.4.2.

`auth` and `circuitBreaker` are the same `$defs/httpAuth`/`$defs/circuitBreaker` shapes the `http`
destination uses, so this module reuses `tributary.loaders.http.config`'s `AuthSpec`, `CircuitBreakerSpec`
and `SUPPORTED_AUTH_TYPES` rather than declaring a second, driftable copy.

Checks that repeat the schema on purpose (a hand-built or lenient-parsed document can skip the schema,
and the wave 2 reviews found this each time): plaintext HTTP is refused for a non-loopback host, `url`
carries no userinfo, query or fragment, `interaction` is `transaction`, and an empty optional block
(`circuitBreaker: {}`) means "configured with defaults", never "absent" (`is not None`, not truthiness).
"""

from __future__ import annotations

import ipaddress
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError
from ..http.config import SUPPORTED_AUTH_TYPES, AuthSpec, CircuitBreakerSpec

#: `idempotency.technique` values this loader accepts (README 5.1, the `fhir` row).
SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"conditional-update", "idempotency-key-header", "none"})

#: `fhirParams.interaction` is a schema `const`; checked again here as defence in depth.
SUPPORTED_INTERACTIONS = frozenset({"transaction"})

SUPPORTED_FHIR_VERSIONS = frozenset({"4.0.1", "4.3.0", "5.0.0"})

#: Applied when `circuitBreaker` is absent or empty, same as the clickhouse loader: a breaker is never
#: silently off.
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

#: A static header may not carry a credential (README 3.4 rule 4, schema `staticHeaders` comment:
#: "Credentials go through auth.secretRef").
_FORBIDDEN_STATIC_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie", "x-api-key"})

__all__ = [
    "SUPPORTED_AUTH_TYPES",
    "SUPPORTED_FHIR_VERSIONS",
    "SUPPORTED_IDEMPOTENCY_TECHNIQUES",
    "SUPPORTED_INTERACTIONS",
    "FhirDestinationConfig",
    "fhir_destination_config_from_channel_spec",
    "fhir_destination_config_from_destination",
]


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class FhirDestinationConfig:
    channel: str
    destination: str
    url: str
    fhir_version: str
    timeout_s: float
    headers: Mapping[str, str]
    auth: AuthSpec | None
    circuit_breaker: CircuitBreakerSpec
    idempotency_technique: str
    idempotency_header: str | None
    backoff: str
    initial_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    on_exhaust: str


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def _check_url(name: str, url: str) -> None:
    """Never echoes the URL: it is operator-authored but this module's errors are logged."""
    try:
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname
        parts.port  # noqa: B018 - raises ValueError for a malformed port
    except ValueError:
        raise ConfigError(f"destination {name!r}: with.url is not a valid URL") from None
    if parts.scheme not in ("http", "https") or not host:
        raise ConfigError(f"destination {name!r}: with.url must be an http(s) URL with a host")
    if parts.username is not None or parts.password is not None:
        raise ConfigError(f"destination {name!r}: with.url must not carry credentials (use auth.secretRef)")
    if parts.query or parts.fragment or "?" in url or "#" in url:
        raise ConfigError(f"destination {name!r}: with.url must have no query or fragment (README 3.4.2)")
    if parts.scheme == "http" and not _is_loopback(host):
        raise ConfigError(
            f"destination {name!r}: plaintext http is only allowed to a loopback host; PHI in flight is TLS "
            "or it is a breach (README 10.1)"
        )


def fhir_destination_config_from_destination(channel: str, destination: Destination) -> FhirDestinationConfig:
    """Build `FhirDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this wave's loader cannot honour."""
    if destination.type != "fhir":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'fhir'")

    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {destination.name!r}: idempotency.technique "
            f"{destination.idempotency.technique!r} is not permitted for a fhir destination "
            f"(README 5.1: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)})"
        )
    if destination.idempotency.technique == "idempotency-key-header" and not destination.idempotency.header:
        raise ConfigError(f"destination {destination.name!r}: idempotency-key-header needs idempotency.header")

    params = destination.params
    name = destination.name

    interaction = _require(params, "interaction", "with")
    if interaction not in SUPPORTED_INTERACTIONS:
        raise ConfigError(
            f"destination {name!r}: with.interaction {interaction!r} is not supported (ADR-007 D12: transaction only)"
        )

    fhir_version = _require(params, "fhirVersion", "with")
    if fhir_version not in SUPPORTED_FHIR_VERSIONS:
        raise ConfigError(f"destination {name!r}: with.fhirVersion {fhir_version!r} is not supported")

    url = _require(params, "url", "with")
    if not isinstance(url, str):
        raise ConfigError(f"destination {name!r}: with.url must be a string")
    _check_url(name, url)

    timeout_ms = _require(params, "timeoutMs", "with")
    if not isinstance(timeout_ms, int) or isinstance(timeout_ms, bool) or timeout_ms < 1:
        raise ConfigError(f"destination {name!r}: with.timeoutMs must be a positive integer")

    headers = dict(params.get("headers") or {})
    for header_name in headers:
        if header_name.lower() in _FORBIDDEN_STATIC_HEADERS:
            raise ConfigError(
                f"destination {name!r}: with.headers must not carry a credential header; use auth.secretRef"
            )

    auth_obj = params.get("auth")
    auth = None
    if auth_obj is not None:
        if auth_obj["type"] not in SUPPORTED_AUTH_TYPES:
            raise ConfigError(
                f"destination {name!r}: auth.type {auth_obj['type']!r} is not supported by this wave's "
                f"fhir loader (supported: {sorted(SUPPORTED_AUTH_TYPES)})"
            )
        auth = AuthSpec(auth_obj["type"], auth_obj["secretRef"])

    breaker_obj = params.get("circuitBreaker")
    breaker = CircuitBreakerSpec(
        int((breaker_obj or {}).get("failureThreshold", DEFAULT_FAILURE_THRESHOLD)),
        float((breaker_obj or {}).get("openSeconds", DEFAULT_OPEN_SECONDS)),
    )

    retry = destination.retry
    return FhirDestinationConfig(
        channel=channel,
        destination=name,
        url=url,
        fhir_version=fhir_version,
        timeout_s=timeout_ms / 1000,
        headers=headers,
        auth=auth,
        circuit_breaker=breaker,
        idempotency_technique=destination.idempotency.technique,
        idempotency_header=destination.idempotency.header,
        backoff=retry.backoff,
        initial_delay_ms=retry.initial_delay_ms,
        max_delay_ms=retry.max_delay_ms,
        max_attempts=retry.max_attempts,
        on_exhaust=retry.on_exhaust,
    )


def fhir_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> FhirDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return fhir_destination_config_from_destination(channel_spec.name, destination)
