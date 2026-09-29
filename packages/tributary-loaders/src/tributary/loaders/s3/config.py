"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: s3`) into
`S3DestinationConfig`. Shape: `channel.schema.json` `$defs/s3Params`, README 3.4.5 and 10.1.

Repeats the schema's checks on purpose (a hand-built or lenient-parsed document can skip it): plaintext
`http://` only to a loopback endpoint, no userinfo/path/query on the endpoint, `serverSideEncryption`
present, `kmsKeyId` only with `aws:kms`, `access-key` needs a `secretRef` and `ambient` must not have one,
prefix segments with no `.`/`..`. `circuitBreaker: {}` means "configured with defaults".

`idempotency.technique` must be `deterministic-name`, the only technique this loader can honour.
"""

from __future__ import annotations

import ipaddress
import re
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError
from ..http.config import CircuitBreakerSpec

SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"deterministic-name"})
SUPPORTED_AUTH_TYPES = frozenset({"access-key", "ambient"})
SUPPORTED_SSE = frozenset({"AES256", "aws:kms"})

DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_PREFIX = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}(/[a-z0-9][a-z0-9_-]{0,63}){0,7}$")
_REGION = re.compile(r"^[a-z0-9-]{1,32}$")
_KMS_KEY = re.compile(r"^[A-Za-z0-9:/_-]{1,256}$")
_EXTENSION = re.compile(r"^\.[a-z0-9]{1,8}$")

__all__ = [
    "SUPPORTED_AUTH_TYPES",
    "SUPPORTED_IDEMPOTENCY_TECHNIQUES",
    "S3DestinationConfig",
    "s3_destination_config_from_channel_spec",
    "s3_destination_config_from_destination",
]


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class S3DestinationConfig:
    channel: str
    destination: str
    bucket: str
    region: str
    prefix: str  # "" or "a/b" (no leading or trailing slash)
    endpoint: str | None  # scheme://host[:port] only; None means the AWS endpoint for `region`
    sse: str  # "AES256" | "aws:kms"
    kms_key_id: str | None
    auth_type: str  # "access-key" | "ambient"
    auth_secret_ref: str | None
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


def _check_endpoint(name: str, endpoint: object) -> str:
    if not isinstance(endpoint, str):
        raise ConfigError(f"destination {name!r}: with.endpoint must be a string")
    try:
        parts = urllib.parse.urlsplit(endpoint)
        host = parts.hostname
        parts.port  # noqa: B018 - raises ValueError for a malformed port
    except ValueError:
        raise ConfigError(f"destination {name!r}: with.endpoint is not a valid URL") from None
    if parts.scheme not in ("http", "https") or not host:
        raise ConfigError(f"destination {name!r}: with.endpoint must be an http(s) URL with a host")
    if parts.username is not None or parts.password is not None or parts.query or parts.fragment:
        raise ConfigError(f"destination {name!r}: with.endpoint must be scheme and host only")
    if parts.path not in ("", "/"):
        raise ConfigError(f"destination {name!r}: with.endpoint must be scheme and host only")
    if parts.scheme == "http" and not _is_loopback(host):
        raise ConfigError(
            f"destination {name!r}: plaintext http is only allowed to a loopback endpoint; PHI in flight is "
            "TLS or it is a breach (README 10.1)"
        )
    return f"{parts.scheme}://{parts.netloc}"


def s3_destination_config_from_destination(channel: str, destination: Destination) -> S3DestinationConfig:
    """Build `S3DestinationConfig` from an already-parsed `Destination`. Raises `ConfigError` for anything
    this loader cannot honour. Never echoes a key id, bucket policy or credential name."""
    name = destination.name
    if destination.type != "s3":
        raise ConfigError(f"destination {name!r}: type is {destination.type!r}, not 's3'")
    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {name!r}: idempotency.technique {destination.idempotency.technique!r} is not "
            f"permitted for an s3 destination (README 5.1: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)})"
        )

    params = destination.params
    bucket = _require(params, "bucket")
    if not isinstance(bucket, str) or not _BUCKET.fullmatch(bucket) or ".." in bucket:
        raise ConfigError(f"destination {name!r}: with.bucket is not a valid bucket name")
    region = _require(params, "region")
    if not isinstance(region, str) or not _REGION.fullmatch(region):
        raise ConfigError(f"destination {name!r}: with.region is not a valid region")

    prefix = params.get("prefix", "")
    if not isinstance(prefix, str) or (prefix and not _PREFIX.fullmatch(prefix)):
        raise ConfigError(f"destination {name!r}: with.prefix must be lowercase segments with no '.' or '..'")

    endpoint_obj = params.get("endpoint")
    endpoint = _check_endpoint(name, endpoint_obj) if endpoint_obj is not None else None

    sse = _require(params, "serverSideEncryption")
    if sse not in SUPPORTED_SSE:
        raise ConfigError(f"destination {name!r}: with.serverSideEncryption must be one of {sorted(SUPPORTED_SSE)}")
    kms_key_id = params.get("kmsKeyId")
    if kms_key_id is not None:
        if sse != "aws:kms":
            raise ConfigError(f"destination {name!r}: with.kmsKeyId is only valid with aws:kms")
        if not isinstance(kms_key_id, str) or not _KMS_KEY.fullmatch(kms_key_id):
            raise ConfigError(f"destination {name!r}: with.kmsKeyId is not a valid key identifier")

    auth = _require(params, "auth")
    if not isinstance(auth, Mapping) or auth.get("type") not in SUPPORTED_AUTH_TYPES:
        raise ConfigError(f"destination {name!r}: with.auth.type must be one of {sorted(SUPPORTED_AUTH_TYPES)}")
    secret_ref = auth.get("secretRef")
    if auth["type"] == "access-key" and not secret_ref:
        raise ConfigError(f"destination {name!r}: auth access-key needs a secretRef")
    if auth["type"] == "ambient" and secret_ref is not None:
        raise ConfigError(f"destination {name!r}: auth ambient takes credentials from the environment, no secretRef")

    extension = params.get("fileExtension", "")
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
    return S3DestinationConfig(
        channel=channel,
        destination=name,
        bucket=bucket,
        region=region,
        prefix=prefix,
        endpoint=endpoint,
        sse=sse,
        kms_key_id=kms_key_id,
        auth_type=auth["type"],
        auth_secret_ref=secret_ref,
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


def s3_destination_config_from_channel_spec(spec: Mapping[str, Any], destination_name: str) -> S3DestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return s3_destination_config_from_destination(channel_spec.name, destination)
