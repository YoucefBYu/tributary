"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: http`) into
`HttpDestinationConfig`.

Same split as `tributary-receivers`' `mllp/config.py`: the schema already constrains field shapes
(a producer validated it before publishing, invariant 3's "runtime must ignore unknown fields" is
the other half of that split), so this module trusts those and only refuses what is legal per the
schema but unsupported by this wave's loader — never silently drops a declared behaviour.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from ..errors import ConfigError

#: `auth.type` values this wave's loader can actually authenticate with. `oauth2-client-credentials`
#: needs a token endpoint and a refresh loop (a whole extra client), and `mtls` needs the same
#: secretRef-to-files plumbing `tributary-receivers.tls` has for the *server* side, built here for a
#: *client* — both real work, neither done yet. A destination declaring one is refused at startup.
SUPPORTED_AUTH_TYPES = frozenset({"basic", "bearer"})

#: `retry.policy` values: the channel-spec schema itself only allows `blocking` (`retry-topic` fails
#: validation, ADR-003 D1/D2), so this is a second, defensive check for a hand-built config.
SUPPORTED_RETRY_POLICIES = frozenset({"blocking"})

Method = Literal["POST", "PUT", "PATCH"]


@dataclass(frozen=True, slots=True)
class AuthSpec:
    type: str  # "basic" | "bearer" (SUPPORTED_AUTH_TYPES)
    secret_ref: str


@dataclass(frozen=True, slots=True)
class CircuitBreakerSpec:
    failure_threshold: int
    open_seconds: float


@dataclass(frozen=True, slots=True)
class HttpDestinationConfig:
    channel: str
    destination: str
    url: str
    method: Method
    timeout_s: float
    headers: Mapping[str, str]
    auth: AuthSpec | None
    circuit_breaker: CircuitBreakerSpec | None
    idempotency_technique: str
    idempotency_header: str | None
    backoff: str  # "exponential" | "fixed"
    initial_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    on_exhaust: str  # "halt-partition" | "dlq-and-continue"

    def __post_init__(self) -> None:
        if self.idempotency_technique == "idempotency-key-header" and not self.idempotency_header:
            raise ConfigError("idempotency.technique is idempotency-key-header but idempotency.header is not set")


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def find_destination(spec: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    destinations: list[Mapping[str, Any]] = spec.get("spec", {}).get("destinations", [])
    for entry in destinations:
        if entry.get("name") == name:
            return entry
    raise ConfigError(f"no destination named {name!r} in this channel spec")


def http_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> HttpDestinationConfig:
    """Build `HttpDestinationConfig` for one `spec.destinations[]` entry. Raises `ConfigError` for
    anything this wave's loader cannot honour."""
    channel = _require(_require(spec, "metadata", "channel spec"), "name", "metadata")
    dest = find_destination(spec, destination_name)
    if dest.get("type") != "http":
        raise ConfigError(f"destination {destination_name!r}: type is {dest.get('type')!r}, not 'http'")

    with_ = _require(dest, "with", "destination")
    auth_obj = with_.get("auth")
    auth = None
    if auth_obj:
        if auth_obj["type"] not in SUPPORTED_AUTH_TYPES:
            raise ConfigError(
                f"destination {destination_name!r}: auth.type {auth_obj['type']!r} is not supported by this "
                f"wave's loader (supported: {sorted(SUPPORTED_AUTH_TYPES)})"
            )
        auth = AuthSpec(auth_obj["type"], auth_obj["secretRef"])

    breaker_obj = with_.get("circuitBreaker")
    breaker = (
        CircuitBreakerSpec(breaker_obj["failureThreshold"], float(breaker_obj["openSeconds"])) if breaker_obj else None
    )

    idem = _require(dest, "idempotency", "destination")
    technique = _require(idem, "technique", "idempotency")

    retry = _require(dest, "retry", "destination")
    policy = _require(retry, "policy", "retry")
    if policy not in SUPPORTED_RETRY_POLICIES:
        raise ConfigError(f"destination {destination_name!r}: retry.policy {policy!r} is not supported")
    on_exhaust = _require(retry, "onExhaust", "retry")

    return HttpDestinationConfig(
        channel=channel,
        destination=destination_name,
        url=_require(with_, "url", "with"),
        method=_require(with_, "method", "with"),
        timeout_s=_require(with_, "timeoutMs", "with") / 1000,
        headers=dict(with_.get("headers", {})),
        auth=auth,
        circuit_breaker=breaker,
        idempotency_technique=technique,
        idempotency_header=idem.get("header"),
        backoff=retry.get("backoff", "exponential"),
        initial_delay_ms=retry.get("initialDelayMs", 250),
        max_delay_ms=retry.get("maxDelayMs", 60_000),
        max_attempts=_require(retry, "maxAttempts", "retry"),
        on_exhaust=on_exhaust,
    )
