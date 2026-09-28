"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: mllp`) into
`MllpDestinationConfig`. Shape: `channel.schema.json` `$defs/mllpParams`/`$defs/clientTls`, README
section 3.4.6.

## Idempotency (README 5.1) — be honest about what an MLLP receiver can prove

Only `sink-native` and `none` are permitted (schema `destination.allOf`, the `mllp` branch) — there is
no `conditional-update`, `deterministic-name` or `idempotency-key-header` analogue over MLLP: the wire
protocol carries one payload and one MSA reply, nothing this loader can attach a second identifier to
without rewriting `MSH` (which README 3.4.6 forbids: "the loader does not rewrite MSH"). `sink-native`
means *the receiving system* deduplicates on a control id a step upstream already set from `tb-msg-id`
(README 3.4.6's own caveat: `MSH-10` is 20 characters in HL7 v2.5 and a UUID is 36, so a step must say
how it fits — this loader has no visibility into that mapping and cannot verify it, unlike
`postgresql`/`clickhouse`'s startup proof). `none` is the honest alternative when the receiver has no
such mechanism, and needs the schema's `note`. **This loader does not, and cannot, start-up-verify
either technique** — say so in review rather than implying a check that does not exist.

## TLS (`$defs/clientTls`)

Required unless `host` is loopback (schema `mllpParams.allOf`). Server certificate and hostname
verification are always on — there is no field to turn them off, by design (schema comment). `secretRef`,
when present, names a trust bundle and *optionally* a client certificate and key (`client.py` loads the
certificate only if both files exist under that ref).
"""

from __future__ import annotations

import ipaddress
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError

_LOOPBACK_WORDS = frozenset({"localhost"})


def _is_loopback(host: str) -> bool:
    if host in _LOOPBACK_WORDS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


#: `idempotency.technique` values this loader accepts (README 5.1's `mllp` row).
SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"sink-native", "none"})

DEFAULT_MIN_TLS_VERSION = "1.2"


@dataclass(frozen=True, slots=True)
class MllpTlsSpec:
    secret_ref: str | None = None
    min_version: str = DEFAULT_MIN_TLS_VERSION


@dataclass(frozen=True, slots=True)
class CircuitBreakerSpec:
    failure_threshold: int
    open_seconds: float


@dataclass(frozen=True, slots=True)
class MllpDestinationConfig:
    channel: str
    destination: str
    host: str
    port: int
    tls: MllpTlsSpec | None  # None only when host is loopback and the spec omitted it
    timeout_s: float
    on_negative_ack: str  # "retry" | "fail"
    circuit_breaker: CircuitBreakerSpec | None
    idempotency_technique: str
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


def mllp_destination_config_from_destination(channel: str, destination: Destination) -> MllpDestinationConfig:
    """Build `MllpDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this loader cannot honour."""
    if destination.type != "mllp":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'mllp'")

    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {destination.name!r}: idempotency.technique "
            f"{destination.idempotency.technique!r} is not supported by this loader "
            f"(supported: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)}; README 5.1: HL7 v2 over MLLP has "
            "no general idempotency mechanism this loader can prove beyond the receiver's own)"
        )

    params = destination.params
    name = destination.name
    host = _require(params, "host", "with")

    tls_obj = params.get("tls")
    if tls_obj is None and not _is_loopback(host):
        raise ConfigError(
            f"destination {name!r}: with.tls is required for a non-loopback host {host!r} "
            "(README 3.4.6, $defs/mllpParams.allOf)"
        )
    # `tls_obj is not None`, never a truthiness check (security review, High): `with.tls: {}` is a
    # present-but-empty object -- "use the platform trust store" ($defs/clientTls's own comment) -- and
    # `if tls_obj` treated it exactly like `tls` being absent. The check two lines up already lets `{}`
    # through for a non-loopback host (`{} is None` is `False`), but `if tls_obj else None` then built
    # `tls = None` anyway, so `client.py`'s `connection_from_config` skipped `_build_ssl_context`
    # entirely and opened a **plaintext** socket to a real, non-loopback receiver -- PHI on the wire in
    # the clear, for exactly the shape `mllpParams.allOf`'s required-`tls` rule exists to prevent.
    tls = (
        MllpTlsSpec(tls_obj.get("secretRef"), tls_obj.get("minVersion", DEFAULT_MIN_TLS_VERSION))
        if tls_obj is not None
        else None
    )

    on_negative_ack = _require(params, "onNegativeAck", "with")
    if on_negative_ack not in ("retry", "fail"):
        raise ConfigError(f"destination {name!r}: with.onNegativeAck {on_negative_ack!r} must be 'retry' or 'fail'")

    breaker_obj = params.get("circuitBreaker")
    breaker = (
        CircuitBreakerSpec(breaker_obj["failureThreshold"], float(breaker_obj["openSeconds"])) if breaker_obj else None
    )

    retry = destination.retry
    return MllpDestinationConfig(
        channel=channel,
        destination=name,
        host=host,
        port=_require(params, "port", "with"),
        tls=tls,
        timeout_s=_require(params, "timeoutMs", "with") / 1000,
        on_negative_ack=on_negative_ack,
        circuit_breaker=breaker,
        idempotency_technique=destination.idempotency.technique,
        backoff=retry.backoff,
        initial_delay_ms=retry.initial_delay_ms,
        max_delay_ms=retry.max_delay_ms,
        max_attempts=retry.max_attempts,
        on_exhaust=retry.on_exhaust,
    )


def mllp_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> MllpDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return mllp_destination_config_from_destination(channel_spec.name, destination)
