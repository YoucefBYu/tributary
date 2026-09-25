"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: clickhouse`)
into `ClickhouseDestinationConfig`. Shape: `channel.schema.json` `$defs/sqlSinkParams` (shared with
`postgresql`), README section 3.4.3.

## Standard library only (human decision 2026-09-25)

The ClickHouse loader talks to ClickHouse's HTTP interface with the standard library, the same
`urllib`/`ssl` choice `http/client.py` makes, and for the same reason: `clickhouse-connect` is
Apache-2.0 on its own, but it unconditionally depends on `certifi` (MPL-2.0), and adding it to this
package's *runtime* dependencies pulled `certifi` into the runtime licence closure for the first time —
`certifi` is currently approved in `.github/scripts/licence_check.py` only as a **non-runtime** MPL
dependency (its one existing path is `testcontainers[kafka] -> docker -> requests -> certifi`, dev-group
only). Rather than ask for a new runtime-scope approval, the human decided: no new dependency, `certifi`
stays dev-only, this loader is `urllib` plus stdlib `ssl` — the same shape `postgresql`'s module
docstring already uses `asyncpg` to avoid (`psycopg`'s LGPL-3.0) and `http`'s avoids `httpx`/`aiohttp`
for the same MPL reason. This module (`config.py`) is unaffected either way — it is `client.py` that
changes shape.

## The row shape and idempotency

Identical to `postgresql`: **the payload is a row** (`loader.py` parses and validates it, not this
module), and idempotency is `sink-native` only — the loader proves it at startup rather than trusting
the declaration (README 3.4.3): a `ReplacingMergeTree`-family engine whose sorting key starts with
`idColumn`. Everything below mirrors `postgresql/config.py` field for field; see that module's docstring
for the reasoning this repeats (table/identifier validation, `tls.mode: disable` loopback-only,
`client-certificate` needing TLS).
"""

from __future__ import annotations

import ipaddress
import re
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


#: `idempotency.technique` values this loader accepts (README 5.1's `clickhouse` row: `sink-native` only).
SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"sink-native"})

#: `auth.type` values this loader can authenticate with — same enum as `postgresql`'s `sqlSinkParams.auth`.
SUPPORTED_AUTH_TYPES = frozenset({"password", "client-certificate"})

_TABLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}(\.[a-z][a-z0-9_]{0,62})?$")
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

DEFAULT_PORT = 8123  # ClickHouse's HTTP interface; 8443 is the HTTPS convention but not assumed here
DEFAULT_ID_COLUMN = "tb_msg_id"


@dataclass(frozen=True, slots=True)
class ChAuthSpec:
    type: str  # "password" | "client-certificate"
    secret_ref: str


@dataclass(frozen=True, slots=True)
class ChTlsSpec:
    mode: str  # "verify-full" | "disable"
    secret_ref: str | None = None


@dataclass(frozen=True, slots=True)
class CircuitBreakerSpec:
    failure_threshold: int
    open_seconds: float


@dataclass(frozen=True, slots=True)
class ClickhouseDestinationConfig:
    channel: str
    destination: str
    host: str
    port: int
    database: str
    table: str
    id_column: str
    auth: ChAuthSpec
    tls: ChTlsSpec
    timeout_s: float
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


def _identifier(value: Any, field: str, destination: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"destination {destination!r}: with.{field} {value!r} is not a valid identifier")
    return value


def clickhouse_destination_config_from_destination(
    channel: str, destination: Destination
) -> ClickhouseDestinationConfig:
    """Build `ClickhouseDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this loader cannot honour."""
    if destination.type != "clickhouse":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'clickhouse'")

    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {destination.name!r}: idempotency.technique "
            f"{destination.idempotency.technique!r} is not supported by this loader "
            f"(supported: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)}); the target table needs a "
            "ReplacingMergeTree-family engine sorted by idColumn for sink-native deduplication (loader.py)"
        )

    params = destination.params
    name = destination.name

    table = _require(params, "table", "with")
    if not isinstance(table, str) or not _TABLE_RE.match(table):
        raise ConfigError(f"destination {name!r}: with.table {table!r} is not a valid table identifier")

    database = _identifier(_require(params, "database", "with"), "database", name)
    id_column = _identifier(params.get("idColumn", DEFAULT_ID_COLUMN), "idColumn", name)

    auth_obj = _require(params, "auth", "with")
    if auth_obj.get("type") not in SUPPORTED_AUTH_TYPES:
        raise ConfigError(
            f"destination {name!r}: auth.type {auth_obj.get('type')!r} is not supported by this loader "
            f"(supported: {sorted(SUPPORTED_AUTH_TYPES)})"
        )
    auth = ChAuthSpec(auth_obj["type"], _require(auth_obj, "secretRef", "with.auth"))

    tls_obj = _require(params, "tls", "with")
    tls_mode = tls_obj.get("mode")
    if tls_mode not in ("verify-full", "disable"):
        raise ConfigError(f"destination {name!r}: with.tls.mode {tls_mode!r} must be verify-full or disable")
    tls = ChTlsSpec(tls_mode, tls_obj.get("secretRef"))
    if tls.mode == "disable" and auth.type == "client-certificate":
        raise ConfigError(
            f"destination {name!r}: auth.type client-certificate needs a TLS connection "
            "(tls.mode: disable has no channel to present a client certificate on)"
        )

    host = _require(params, "host", "with")
    if tls.mode == "disable" and not _is_loopback(host):
        raise ConfigError(
            f"destination {name!r}: tls.mode 'disable' is only permitted for a loopback host, "
            f"not {host!r} (README 3.4.3, $defs/sqlSinkParams.allOf; PHI in flight must be encrypted)"
        )

    breaker_obj = params.get("circuitBreaker")
    breaker = (
        CircuitBreakerSpec(breaker_obj["failureThreshold"], float(breaker_obj["openSeconds"])) if breaker_obj else None
    )

    retry = destination.retry
    return ClickhouseDestinationConfig(
        channel=channel,
        destination=name,
        host=host,
        port=params.get("port", DEFAULT_PORT),
        database=database,
        table=table,
        id_column=id_column,
        auth=auth,
        tls=tls,
        timeout_s=_require(params, "timeoutMs", "with") / 1000,
        circuit_breaker=breaker,
        idempotency_technique=destination.idempotency.technique,
        backoff=retry.backoff,
        initial_delay_ms=retry.initial_delay_ms,
        max_delay_ms=retry.max_delay_ms,
        max_attempts=retry.max_attempts,
        on_exhaust=retry.on_exhaust,
    )


def clickhouse_destination_config_from_channel_spec(
    spec: Mapping[str, Any], destination_name: str
) -> ClickhouseDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return clickhouse_destination_config_from_destination(channel_spec.name, destination)
