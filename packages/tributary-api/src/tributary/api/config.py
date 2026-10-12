"""Settings from environment variables (ADR-009). Every refusal here is a startup refusal: a control
plane that starts with a weak authentication setting is worse than one that does not start.

No value is echoed in a message: the issuer, the role map and the audience are operator input, but the
rule is simpler to keep and to review when no error text ever contains an environment value.
"""

from __future__ import annotations

import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .errors import ConfigurationError

ROLES: frozenset[str] = frozenset({"admin", "developer", "operator", "auditor", "viewer"})
PROFILES = ("lite", "production")

#: ADR-009 D5 ranges.
HTTP_TIMEOUT_RANGE = (1.0, 10.0)
REFRESH_RANGE = (60.0, 3600.0)
MAX_STALE_RANGE = (300.0, 14400.0)
MAX_LEEWAY_S = 60.0

DEFAULT_HTTP_TIMEOUT_S = 5.0
DEFAULT_REFRESH_S = 300.0
DEFAULT_MAX_STALE_S = 3600.0
DEFAULT_LEEWAY_S = 30.0
DEFAULT_PUBLISH_TIMEOUT_S = 10.0
#: A deploy holds one database connection across its Kafka publish, so the publish timeout must stay well
#: under the pool's checkout timeout or queued requests fail with pool errors instead of waiting.
DB_POOL_TIMEOUT_S = 30.0
MAX_PUBLISH_TIMEOUT_S = 20.0


@dataclass(frozen=True, slots=True)
class OidcSettings:
    issuer: str
    audience: str
    role_map: Mapping[str, str]
    roles_claim: tuple[str, ...] = ("roles",)
    jwks_uri: str | None = None
    jwks_file: str | None = None
    accept_jwt_typ: bool = False
    ui_client_id: str | None = None
    http_timeout_s: float = DEFAULT_HTTP_TIMEOUT_S
    refresh_s: float = DEFAULT_REFRESH_S
    max_stale_s: float = DEFAULT_MAX_STALE_S
    leeway_s: float = DEFAULT_LEEWAY_S

    def __post_init__(self) -> None:
        _check_oidc(self)


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """PostgreSQL over TLS with `verify-full` semantics (certificate chain AND host name checked), ADR-004
    C1/C2. The credential is a `secretRef` file (`<TB_SECRETS_DIR>/<secret_ref>/password`), never a URL in
    the environment. `ca_secret_ref` names a trust bundle (`ca.crt`); absent means the system trust store."""

    host: str
    name: str
    user: str
    secret_ref: str
    port: int = 5432
    ca_secret_ref: str | None = None

    def __post_init__(self) -> None:
        if not (self.host and self.name and self.user and self.secret_ref):
            raise ConfigurationError(
                "DATABASE_SETTINGS_MISSING",
                "TB_DATABASE_HOST, _NAME, _USER and _SECRET_REF are required (ADR-004 C1/C2)",
            )
        if not 1 <= self.port <= 65535:
            raise ConfigurationError("PORT_INVALID", "TB_DATABASE_PORT is out of range")


_ENV_NAME = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_DB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
TAMPER_INTERVAL_RANGE = (60.0, 86400.0)
DEFAULT_TAMPER_INTERVAL_S = 900.0


@dataclass(frozen=True, slots=True)
class ArchiveSettings:
    """Read access to the archive for the `audit_log` tamper check (ADR-004 A5: the API holds SELECT).
    `targets` maps each environment to the ClickHouse database holding its `audit_log`. The password is a
    secretRef, never a value."""

    url: str
    user: str
    secret_ref: str
    targets: Mapping[str, str]
    interval_s: float = DEFAULT_TAMPER_INTERVAL_S


def archive_from_env(env: Mapping[str, str]) -> ArchiveSettings | None:
    """None when nothing is configured (the check is off and its series stay absent). Half a configuration, a
    bad value or an interval outside 60-86400 refuses to start."""
    url, user, ref = (
        env.get("TB_CLICKHOUSE_URL", ""),
        env.get("TB_CLICKHOUSE_USER", ""),
        env.get("TB_CLICKHOUSE_SECRET_REF", ""),
    )
    raw_targets = env.get("TB_AUDIT_TAMPER_TARGETS", "")
    interval = _number(
        env, "TB_AUDIT_TAMPER_CHECK_INTERVAL_S", DEFAULT_TAMPER_INTERVAL_S, "TAMPER_INTERVAL_OUT_OF_RANGE"
    )
    if not TAMPER_INTERVAL_RANGE[0] <= interval <= TAMPER_INTERVAL_RANGE[1]:
        raise ConfigurationError(
            "TAMPER_INTERVAL_OUT_OF_RANGE", "TB_AUDIT_TAMPER_CHECK_INTERVAL_S must be within 60-86400"
        )
    if not (url or user or ref or raw_targets):
        return None
    if not (url and user and ref and raw_targets):
        raise ConfigurationError(
            "AUDIT_TAMPER_CONFIG_INCOMPLETE",
            "TB_CLICKHOUSE_URL, _USER, _SECRET_REF and TB_AUDIT_TAMPER_TARGETS go together",
        )
    try:
        parsed = json.loads(raw_targets)
    except ValueError:
        raise ConfigurationError("AUDIT_TAMPER_TARGETS_INVALID", "TB_AUDIT_TAMPER_TARGETS is not JSON") from None
    if (
        not isinstance(parsed, dict)
        or not parsed
        or len(parsed) > 64
        or not all(
            isinstance(k, str) and isinstance(v, str) and _ENV_NAME.fullmatch(k) and _DB_NAME.fullmatch(v)
            for k, v in parsed.items()
        )
    ):
        raise ConfigurationError(
            "AUDIT_TAMPER_TARGETS_INVALID", "TB_AUDIT_TAMPER_TARGETS must map environment names to database names"
        )
    from .tamper import check_url

    try:
        check_url(url)
    except ValueError:
        raise ConfigurationError(
            "CLICKHOUSE_URL_INVALID", "TB_CLICKHOUSE_URL must be https (http only on loopback)"
        ) from None
    return ArchiveSettings(url=url, user=user, secret_ref=ref, targets=parsed, interval_s=interval)


@dataclass(frozen=True, slots=True)
class Settings:
    database: DatabaseSettings
    oidc: OidcSettings
    bind: str = "127.0.0.1"
    port: int = 8080
    profile: str = "production"
    cors_origins: tuple[str, ...] = field(default_factory=tuple)
    publish_timeout_s: float = DEFAULT_PUBLISH_TIMEOUT_S
    #: Seconds between reconciler passes (W3-5); 0 turns the loop off (`Reconciler.run_once` still works).
    reconcile_interval_s: float = 30.0
    reconcile_pending_grace_s: float = 120.0
    archive: ArchiveSettings | None = None

    def __post_init__(self) -> None:
        if self.profile not in PROFILES:
            raise ConfigurationError("PROFILE_INVALID", "TB_PROFILE must be lite or production")
        if "*" in self.cors_origins:
            raise ConfigurationError("CORS_WILDCARD", "a wildcard CORS origin is refused (ADR-009 D10)")
        if self.oidc.jwks_file is not None and not (self.profile == "lite" and is_loopback(self.bind)):
            raise ConfigurationError(
                "OIDC_JWKS_FILE_NOT_ALLOWED", "a static JWKS file needs profile lite and a loopback bind (ADR-009 D9)"
            )
        if not 0.0 <= self.reconcile_interval_s <= 3600.0:
            raise ConfigurationError("RECONCILE_INTERVAL_OUT_OF_RANGE", "TB_RECONCILE_INTERVAL_S must be within 0-3600")
        if not 2 * MAX_PUBLISH_TIMEOUT_S <= self.reconcile_pending_grace_s <= 86400.0:
            raise ConfigurationError(
                "RECONCILE_GRACE_OUT_OF_RANGE",
                "TB_RECONCILE_PENDING_GRACE_S must exceed twice the publish timeout cap, so a live deploy is never expired",
            )
        if not 1.0 <= self.publish_timeout_s <= MAX_PUBLISH_TIMEOUT_S:
            raise ConfigurationError(
                "PUBLISH_TIMEOUT_OUT_OF_RANGE",
                f"TB_PUBLISH_TIMEOUT_S must be within 1-{MAX_PUBLISH_TIMEOUT_S:g}, well under the {DB_POOL_TIMEOUT_S:g} s pool timeout",
            )


def is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def _https(url: str) -> bool:
    parts = urlsplit(url)
    return parts.scheme == "https" and bool(parts.hostname) and not parts.fragment and not parts.username


def _check_oidc(o: OidcSettings) -> None:
    if not _https(o.issuer):
        raise ConfigurationError("OIDC_ISSUER_INVALID", "TB_OIDC_ISSUER must be an https URL (ADR-009 D13)")
    if o.jwks_uri is not None and not _https(o.jwks_uri):
        raise ConfigurationError("OIDC_JWKS_URI_INVALID", "TB_OIDC_JWKS_URI must be an https URL (ADR-009 D13)")
    if o.jwks_uri is not None and o.jwks_file is not None:
        raise ConfigurationError("OIDC_KEY_SOURCE_AMBIGUOUS", "set TB_OIDC_JWKS_URI or TB_OIDC_JWKS_FILE, not both")
    if not o.audience:
        raise ConfigurationError("OIDC_AUDIENCE_MISSING", "TB_OIDC_AUDIENCE is required")
    if not o.role_map:
        raise ConfigurationError("OIDC_ROLE_MAP_INVALID", "TB_OIDC_ROLE_MAP is empty (ADR-009 D6)")
    if any(not k or not isinstance(k, str) or v not in ROLES for k, v in o.role_map.items()):
        raise ConfigurationError("OIDC_ROLE_MAP_INVALID", "TB_OIDC_ROLE_MAP maps to a name outside the five roles")
    if not o.roles_claim or any(not part for part in o.roles_claim):
        raise ConfigurationError("OIDC_ROLES_CLAIM_INVALID", "TB_OIDC_ROLES_CLAIM is not a dotted path")
    if o.accept_jwt_typ:
        if not o.ui_client_id:
            raise ConfigurationError(
                "OIDC_UI_CLIENT_ID_REQUIRED", "TB_OIDC_ACCEPT_JWT_TYP needs TB_OIDC_UI_CLIENT_ID (ADR-009 D4a)"
            )
        if o.ui_client_id == o.audience:
            raise ConfigurationError(
                "OIDC_AUDIENCE_IS_UI_CLIENT", "TB_OIDC_AUDIENCE equals the UI client id (ADR-009 D4a)"
            )
    for code, value, (low, high) in (
        ("OIDC_HTTP_TIMEOUT_OUT_OF_RANGE", o.http_timeout_s, HTTP_TIMEOUT_RANGE),
        ("OIDC_REFRESH_OUT_OF_RANGE", o.refresh_s, REFRESH_RANGE),
        ("OIDC_MAX_STALE_OUT_OF_RANGE", o.max_stale_s, MAX_STALE_RANGE),
    ):
        if not low <= value <= high:
            raise ConfigurationError(code, f"{code.removesuffix('_OUT_OF_RANGE').lower()} is outside {low:g}-{high:g}")
    if not 0.0 <= o.leeway_s <= MAX_LEEWAY_S:
        raise ConfigurationError("OIDC_LEEWAY_OUT_OF_RANGE", "TB_OIDC_LEEWAY_S must be within 0-60 (ADR-009 D2)")


def _number(env: Mapping[str, str], name: str, default: float, code: str) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigurationError(code, f"{name} is not a number") from None
    if value != value or value in (float("inf"), float("-inf")):  # NaN or infinite
        raise ConfigurationError(code, f"{name} is not finite")
    return value


def _flag(env: Mapping[str, str], name: str) -> bool:
    raw = env.get(name, "").strip().lower()
    if raw in ("", "0", "false"):
        return False
    if raw in ("1", "true"):
        return True
    raise ConfigurationError("FLAG_INVALID", f"{name} must be true or false")


def parse_role_map(raw: str | None) -> dict[str, str]:
    if raw is None or not raw.strip():
        raise ConfigurationError("OIDC_ROLE_MAP_INVALID", "TB_OIDC_ROLE_MAP is empty (ADR-009 D6)")
    try:
        parsed = json.loads(raw)
    except ValueError:
        raise ConfigurationError("OIDC_ROLE_MAP_INVALID", "TB_OIDC_ROLE_MAP is not JSON") from None
    if not isinstance(parsed, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in parsed.items()):
        raise ConfigurationError("OIDC_ROLE_MAP_INVALID", "TB_OIDC_ROLE_MAP must be a JSON object of strings")
    return parsed


def database_from_env(env: Mapping[str, str]) -> DatabaseSettings:
    if "TB_DATABASE_URL" in env:
        # A URL carries the password in the environment. Refuse it instead of ignoring it, so a stale
        # manifest cannot look configured (ADR-004 C1/C2).
        raise ConfigurationError(
            "DATABASE_URL_REFUSED", "TB_DATABASE_URL is retired: use TB_DATABASE_HOST/_NAME/_USER/_SECRET_REF"
        )
    try:
        db_port = int(env.get("TB_DATABASE_PORT", "5432"))
    except ValueError:
        raise ConfigurationError("PORT_INVALID", "TB_DATABASE_PORT is not an integer") from None
    database = DatabaseSettings(
        host=env.get("TB_DATABASE_HOST", ""),
        name=env.get("TB_DATABASE_NAME", ""),
        user=env.get("TB_DATABASE_USER", ""),
        secret_ref=env.get("TB_DATABASE_SECRET_REF", ""),
        port=db_port,
        ca_secret_ref=env.get("TB_DATABASE_CA_SECRET_REF") or None,
    )
    return database


def from_env(env: Mapping[str, str]) -> Settings:
    """Build and validate the settings. Raises `ConfigurationError`; never returns a weak configuration."""
    database = database_from_env(env)
    claim = tuple(env.get("TB_OIDC_ROLES_CLAIM", "roles").split("."))
    oidc = OidcSettings(
        issuer=env.get("TB_OIDC_ISSUER", ""),
        audience=env.get("TB_OIDC_AUDIENCE", ""),
        role_map=parse_role_map(env.get("TB_OIDC_ROLE_MAP")),
        roles_claim=claim,
        jwks_uri=env.get("TB_OIDC_JWKS_URI") or None,
        jwks_file=env.get("TB_OIDC_JWKS_FILE") or None,
        accept_jwt_typ=_flag(env, "TB_OIDC_ACCEPT_JWT_TYP"),
        ui_client_id=env.get("TB_OIDC_UI_CLIENT_ID") or None,
        http_timeout_s=_number(env, "TB_OIDC_HTTP_TIMEOUT_S", DEFAULT_HTTP_TIMEOUT_S, "OIDC_HTTP_TIMEOUT_OUT_OF_RANGE"),
        refresh_s=_number(env, "TB_OIDC_JWKS_REFRESH_S", DEFAULT_REFRESH_S, "OIDC_REFRESH_OUT_OF_RANGE"),
        max_stale_s=_number(env, "TB_OIDC_JWKS_MAX_STALE_S", DEFAULT_MAX_STALE_S, "OIDC_MAX_STALE_OUT_OF_RANGE"),
        leeway_s=_number(env, "TB_OIDC_LEEWAY_S", DEFAULT_LEEWAY_S, "OIDC_LEEWAY_OUT_OF_RANGE"),
    )
    try:
        port = int(env.get("TB_API_PORT", "8080"))
    except ValueError:
        raise ConfigurationError("PORT_INVALID", "TB_API_PORT is not an integer") from None
    if not 0 <= port <= 65535:
        raise ConfigurationError("PORT_INVALID", "TB_API_PORT is out of range")
    origins = tuple(o.strip() for o in env.get("TB_CORS_ORIGINS", "").split(",") if o.strip())
    return Settings(
        database=database,
        oidc=oidc,
        bind=env.get("TB_API_BIND", "127.0.0.1"),
        port=port,
        profile=env.get("TB_PROFILE", "production"),
        cors_origins=origins,
        publish_timeout_s=_number(
            env, "TB_PUBLISH_TIMEOUT_S", DEFAULT_PUBLISH_TIMEOUT_S, "PUBLISH_TIMEOUT_OUT_OF_RANGE"
        ),
        archive=archive_from_env(env),
        reconcile_interval_s=_number(env, "TB_RECONCILE_INTERVAL_S", 30.0, "RECONCILE_INTERVAL_OUT_OF_RANGE"),
        reconcile_pending_grace_s=_number(env, "TB_RECONCILE_PENDING_GRACE_S", 120.0, "RECONCILE_GRACE_OUT_OF_RANGE"),
    )
