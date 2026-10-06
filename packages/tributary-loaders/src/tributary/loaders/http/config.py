"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: http`) into
`HttpDestinationConfig`.

Parsing and validating the shared parts of a channel spec — `idempotency`, `retry` (including the
`retry-topic` refusal, ADR-003 D1/D2) and destination-name uniqueness — is `tributary.runtime.spec`'s
job now that `tributary-runtime` exists: this module builds on `ChannelSpec`/`Destination` rather than
re-parsing the document, so there is one parser for the contract, not two that can drift (platform-
engineer, W1-6 second round). What is left here is genuinely HTTP-specific: which `auth.type` values
this wave's loader can authenticate with, and the `circuitBreaker` mapping.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError

#: `auth.type` values this wave's loader can actually authenticate with. `oauth2-client-credentials`
#: needs a token endpoint and a refresh loop (a whole extra client), and `mtls` needs the same
#: secretRef-to-files plumbing `tributary-receivers.tls` has for the *server* side, built here for a
#: *client* — both real work, neither done yet. A destination declaring one is refused at startup.
SUPPORTED_AUTH_TYPES = frozenset({"basic", "bearer"})

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


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def check_static_headers(destination: str, headers: Mapping[str, Any]) -> dict[str, str]:
    """A static header name or value with a control character (a line break above all) would make
    `http.client` refuse every request, and is how a header gets injected. Refused at load time, without
    echoing the value (it may be a token someone pasted into the wrong field)."""
    out: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, str) or not name or ":" in name:
            raise ConfigError(f"destination {destination!r}: with.headers must map header names to strings")
        if any(ord(c) < 0x20 or ord(c) == 0x7F for c in name + value):
            raise ConfigError(f"destination {destination!r}: with.headers carries a control character")
        out[name] = value
    return out


def http_destination_config_from_destination(channel: str, destination: Destination) -> HttpDestinationConfig:
    """Build `HttpDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this wave's loader cannot honour."""
    if destination.type != "http":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'http'")

    params = destination.params
    auth_obj = params.get("auth")
    auth = None
    if auth_obj:
        if auth_obj["type"] not in SUPPORTED_AUTH_TYPES:
            raise ConfigError(
                f"destination {destination.name!r}: auth.type {auth_obj['type']!r} is not supported by this "
                f"wave's loader (supported: {sorted(SUPPORTED_AUTH_TYPES)})"
            )
        auth = AuthSpec(auth_obj["type"], auth_obj["secretRef"])

    breaker_obj = params.get("circuitBreaker")
    breaker = (
        CircuitBreakerSpec(breaker_obj["failureThreshold"], float(breaker_obj["openSeconds"])) if breaker_obj else None
    )

    retry = destination.retry
    return HttpDestinationConfig(
        channel=channel,
        destination=destination.name,
        url=_require(params, "url", "with"),
        method=_require(params, "method", "with"),
        timeout_s=_require(params, "timeoutMs", "with") / 1000,
        headers=check_static_headers(destination.name, params.get("headers") or {}),
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


def http_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> HttpDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return http_destination_config_from_destination(channel_spec.name, destination)
