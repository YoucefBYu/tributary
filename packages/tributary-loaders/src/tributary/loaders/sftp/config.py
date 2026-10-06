"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: sftp`) into
`SftpDestinationConfig`. Shape: `channel.schema.json` `$defs/sftpParams`, README 3.4.4 and 10.1.

Repeats the schema's checks on purpose (a hand-built or lenient-parsed document can skip it): at least one
and at most four `SHA256:` host-key pins, absolute `directory` with no `.`/`..` segment, and file-name
parts that cannot smuggle a path separator. There is no field, and no code path, that lets the connection
proceed without a pinned host key (README 10.2). `circuitBreaker: {}` is "configured with defaults".

`idempotency.technique` must be `deterministic-name`, the only technique this loader can honour (README
5.1): the remote name is a pure function of `tb-msg-id`, and a name that already exists is a duplicate.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError
from ..http.config import CircuitBreakerSpec

SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"deterministic-name"})
SUPPORTED_AUTH_TYPES = frozenset({"private-key", "password"})

DEFAULT_PORT = 22
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

_FINGERPRINT = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")
_HOST = re.compile(r"^(::1|[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?)$")
_DIRECTORY = re.compile(r"^/([A-Za-z0-9._-]{1,64}(/[A-Za-z0-9._-]{1,64}){0,15})?/?$")
_PREFIX = re.compile(r"^[A-Za-z0-9_-]{0,32}$")
_EXTENSION = re.compile(r"^\.[a-z0-9]{1,8}$")

__all__ = [
    "SUPPORTED_AUTH_TYPES",
    "SUPPORTED_IDEMPOTENCY_TECHNIQUES",
    "SftpDestinationConfig",
    "sftp_destination_config_from_channel_spec",
    "sftp_destination_config_from_destination",
]


@dataclass(frozen=True, slots=True)
class SftpDestinationConfig:
    channel: str
    destination: str
    host: str
    port: int
    host_keys: tuple[str, ...]
    auth_type: str  # "private-key" | "password"
    auth_secret_ref: str
    directory: str  # normalised: absolute, no trailing slash except the root
    file_name_prefix: str
    file_extension: str
    timeout_s: float
    circuit_breaker: CircuitBreakerSpec
    idempotency_technique: str
    backoff: str
    initial_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    on_exhaust: str


def _require(mapping: Mapping[str, Any], key: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"with: missing required field {key!r}") from None


def sftp_destination_config_from_destination(channel: str, destination: Destination) -> SftpDestinationConfig:
    """Build `SftpDestinationConfig` from an already-parsed `Destination`. Raises `ConfigError` for
    anything this loader cannot honour. Never echoes a fingerprint, path or credential name."""
    name = destination.name
    if destination.type != "sftp":
        raise ConfigError(f"destination {name!r}: type is {destination.type!r}, not 'sftp'")
    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {name!r}: idempotency.technique {destination.idempotency.technique!r} is not "
            f"permitted for an sftp destination (README 5.1: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)})"
        )

    params = destination.params
    host = _require(params, "host")
    if not isinstance(host, str) or not _HOST.fullmatch(host):
        raise ConfigError(f"destination {name!r}: with.host is not a valid host name")
    port = params.get("port", DEFAULT_PORT)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigError(f"destination {name!r}: with.port must be 1-65535")

    pins = _require(params, "hostKeys")
    if (
        not isinstance(pins, list)
        or not 1 <= len(pins) <= 4
        or len(set(pins)) != len(pins)
        or not all(isinstance(p, str) and _FINGERPRINT.fullmatch(p) for p in pins)
    ):
        raise ConfigError(
            f"destination {name!r}: with.hostKeys must be 1-4 distinct OpenSSH SHA256: fingerprints "
            "(there is no trust-on-first-use and no accept-any option)"
        )

    auth = _require(params, "auth")
    if not isinstance(auth, Mapping) or auth.get("type") not in SUPPORTED_AUTH_TYPES or not auth.get("secretRef"):
        raise ConfigError(f"destination {name!r}: with.auth needs type private-key|password and a secretRef")

    directory = _require(params, "directory")
    if (
        not isinstance(directory, str)
        or not _DIRECTORY.fullmatch(directory)
        or any(seg in (".", "..") for seg in directory.split("/"))
    ):
        raise ConfigError(f"destination {name!r}: with.directory must be an absolute path without '.' or '..'")
    directory = directory.rstrip("/") or "/"

    prefix = params.get("fileNamePrefix", "")
    extension = params.get("fileExtension", "")
    if not isinstance(prefix, str) or not _PREFIX.fullmatch(prefix):
        raise ConfigError(f"destination {name!r}: with.fileNamePrefix is not a safe name part")
    if not isinstance(extension, str) or (extension and not _EXTENSION.fullmatch(extension)):
        raise ConfigError(f"destination {name!r}: with.fileExtension must look like '.hl7'")

    timeout_ms = _require(params, "timeoutMs")
    if not isinstance(timeout_ms, int) or isinstance(timeout_ms, bool) or timeout_ms < 1:
        raise ConfigError(f"destination {name!r}: with.timeoutMs must be a positive integer")

    breaker_obj = params.get("circuitBreaker")
    breaker = CircuitBreakerSpec(
        int((breaker_obj or {}).get("failureThreshold", DEFAULT_FAILURE_THRESHOLD)),
        float((breaker_obj or {}).get("openSeconds", DEFAULT_OPEN_SECONDS)),
    )

    retry = destination.retry
    return SftpDestinationConfig(
        channel=channel,
        destination=name,
        host=host,
        port=port,
        host_keys=tuple(pins),
        auth_type=auth["type"],
        auth_secret_ref=auth["secretRef"],
        directory=directory,
        file_name_prefix=prefix,
        file_extension=extension,
        timeout_s=timeout_ms / 1000,
        circuit_breaker=breaker,
        idempotency_technique=destination.idempotency.technique,
        backoff=retry.backoff,
        initial_delay_ms=retry.initial_delay_ms,
        max_delay_ms=retry.max_delay_ms,
        max_attempts=retry.max_attempts,
        on_exhaust=retry.on_exhaust,
    )


def sftp_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> SftpDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return sftp_destination_config_from_destination(channel_spec.name, destination)
