"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: postgresql`)
into `PostgresqlDestinationConfig`. Shape: `contracts/channel-spec/v0/channel.schema.json`
`$defs/sqlSinkParams` (`postgresql` and `clickhouse` share it), README section 3.4.3.

## Idempotency

`idempotency.technique` must be `sink-native`: the loader's own
`INSERT ... ON CONFLICT (<idColumn>) DO NOTHING` is the deduplication, keyed by `tb-msg-id` written into
`idColumn` (default `tb_msg_id`) — invariant 4. README 3.4.3: "the loader proves it at startup rather
than trusting the declaration" — `loader.py`'s `verify_idempotency_constraint` runs the startup check
that a unique index or primary key covers exactly `idColumn`; see that module's docstring for the rest
of the idempotency argument and `tests/postgresql/test_loader.py::test_duplicate_delivery_is_harmless`
for the proof.

## The row shape

The payload is not this module's concern — `loader.py` parses it per message, because which columns are
present varies row to row (README 3.4.3: "There is no column-mapping language in the spec"). What is
fixed per destination is `table`, `idColumn`, and the connection.
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

#: Duplicated from `tributary.receivers.net.is_loopback` — see `client.py`'s copy of this same
#: function for why there is no shared module yet.
_LOOPBACK_WORDS = frozenset({"localhost"})


def _is_loopback(host: str) -> bool:
    if host in _LOOPBACK_WORDS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


#: `idempotency.technique` values this loader accepts (schema `destination.allOf`, the `postgresql`
#: branch: `sink-native` only).
SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"sink-native"})

#: `auth.type` values this loader can authenticate with. Both are in the schema's `sqlSinkParams.auth`
#: enum; this loader implements both (unlike `http.config.SUPPORTED_AUTH_TYPES`, which refuses two of
#: the four the HTTP schema allows).
SUPPORTED_AUTH_TYPES = frozenset({"password", "client-certificate"})

#: `$defs/sqlSinkParams.table`: `name` or `schema.name`, lower-case.
_TABLE_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$")
#: `$defs/sqlIdentifier`, reused here for `database` and `idColumn`.
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

DEFAULT_PORT = 5432
DEFAULT_ID_COLUMN = "tb_msg_id"
DEFAULT_MIN_POOL_SIZE = 1
DEFAULT_MAX_POOL_SIZE = 4


@dataclass(frozen=True, slots=True)
class PgAuthSpec:
    type: str  # "password" | "client-certificate"
    secret_ref: str


@dataclass(frozen=True, slots=True)
class PgTlsSpec:
    mode: str  # "verify-full" | "disable"
    secret_ref: str | None = None


@dataclass(frozen=True, slots=True)
class CircuitBreakerSpec:
    failure_threshold: int
    open_seconds: float


@dataclass(frozen=True, slots=True)
class PostgresqlDestinationConfig:
    channel: str
    destination: str
    host: str
    port: int
    database: str
    table: str
    id_column: str
    auth: PgAuthSpec
    tls: PgTlsSpec
    timeout_s: float
    circuit_breaker: CircuitBreakerSpec | None
    idempotency_technique: str
    backoff: str
    initial_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    on_exhaust: str
    min_pool_size: int = DEFAULT_MIN_POOL_SIZE
    max_pool_size: int = DEFAULT_MAX_POOL_SIZE


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def _identifier(value: Any, field: str, destination: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"destination {destination!r}: with.{field} {value!r} is not a valid identifier")
    return value


def postgresql_destination_config_from_destination(
    channel: str, destination: Destination
) -> PostgresqlDestinationConfig:
    """Build `PostgresqlDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this loader cannot honour."""
    if destination.type != "postgresql":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'postgresql'")

    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {destination.name!r}: idempotency.technique "
            f"{destination.idempotency.technique!r} is not supported by this loader "
            f"(supported: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)}); the target table needs a unique "
            "constraint on idColumn for sink-native deduplication to work (see loader.py)"
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
    auth = PgAuthSpec(auth_obj["type"], _require(auth_obj, "secretRef", "with.auth"))

    tls_obj = _require(params, "tls", "with")
    tls_mode = tls_obj.get("mode")
    if tls_mode not in ("verify-full", "disable"):
        raise ConfigError(f"destination {name!r}: with.tls.mode {tls_mode!r} must be verify-full or disable")
    tls = PgTlsSpec(tls_mode, tls_obj.get("secretRef"))
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
    return PostgresqlDestinationConfig(
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


def postgresql_destination_config_from_channel_spec(
    spec: Mapping[str, Any], destination_name: str
) -> PostgresqlDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return postgresql_destination_config_from_destination(channel_spec.name, destination)
