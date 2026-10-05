"""Archiver settings from the environment. Kafka comes from `TB_ENGINE_KAFKA_FILE` only (ADR-008)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from tributary.runtime.secrets import SecretResolver

from .errors import ArchiverConfigError
from .schema import DEFAULT_TTL_DAYS, MAX_TTL_DAYS
from .sink import ClickhouseTarget

ENV_NAME: Final = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_IDENT: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_SECRET_REF: Final = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_HOST: Final = re.compile(r"^[A-Za-z0-9._-]{1,253}$")


@dataclass(frozen=True, slots=True)
class ArchiverConfig:
    env: str
    target: ClickhouseTarget
    ttl_days: int
    group_id: str


def archiver_group(env: str) -> str:
    """`tb.<env>.archiver`, the literal group the ACL grants (set 1.8; `contracts/topics/v0`).

    Not `consumer_group(...)`: that grammar requires a channel segment, and the archiver is not per channel.
    """
    if not ENV_NAME.fullmatch(env):
        raise ArchiverConfigError("env does not match the topic-name segment grammar")
    return f"tb.{env}.archiver"


def _bool(environ: Mapping[str, str], name: str, *, default: bool) -> bool:
    """`true` or `false` only: an unrecognised value is refused, never read as the unsafe choice."""
    raw = environ.get(name)
    if raw is None or raw == "":
        return default
    if raw in ("true", "false"):
        return raw == "true"
    raise ArchiverConfigError(f"{name} must be true or false")


def _int(environ: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    raw = environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ArchiverConfigError(f"{name} must be an integer") from None
    if not low <= value <= high:
        raise ArchiverConfigError(f"{name} is out of range")
    return value


def load_config(
    env: str,
    environ: Mapping[str, str],
    resolver: SecretResolver,
    *,
    ref_env_name: str = "TB_ARCHIVE_CLICKHOUSE_SECRET_REF",
) -> ArchiverConfig:
    """Names and numbers only in errors: never a secret value."""
    host = environ.get("TB_ARCHIVE_CLICKHOUSE_HOST", "")
    if not _HOST.fullmatch(host):
        raise ArchiverConfigError("TB_ARCHIVE_CLICKHOUSE_HOST is unset or invalid")
    database = environ.get("TB_ARCHIVE_CLICKHOUSE_DATABASE", "tributary")
    if not _IDENT.fullmatch(database):
        raise ArchiverConfigError("TB_ARCHIVE_CLICKHOUSE_DATABASE is invalid")
    secure = _bool(environ, "TB_ARCHIVE_CLICKHOUSE_SECURE", default=True)
    ref = environ.get(ref_env_name, "")
    if not _SECRET_REF.fullmatch(ref):
        raise ArchiverConfigError(f"{ref_env_name} is unset or invalid")
    try:
        username = resolver.resolve_text(ref, "user")
        password = resolver.resolve_text(ref, "password")
    except Exception as exc:
        raise ArchiverConfigError(f"clickhouse secret {ref} could not be resolved ({type(exc).__name__})") from None
    target = ClickhouseTarget(
        host=host,
        port=_int(environ, "TB_ARCHIVE_CLICKHOUSE_PORT", 8443 if secure else 8123, 1, 65535),
        database=database,
        username=username,
        password=password,
        secure=secure,
        ca_cert=environ.get("TB_ARCHIVE_CLICKHOUSE_CA_CERT") or None,
    )
    return ArchiverConfig(
        env=env,
        target=target,
        ttl_days=_int(environ, "TB_ARCHIVE_TTL_DAYS", DEFAULT_TTL_DAYS, 1, MAX_TTL_DAYS),
        group_id=archiver_group(env),
    )
