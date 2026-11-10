"""The one way the control plane builds its PostgreSQL engine from the environment (ADR-004 C1/C2): the API and
the migrations both use it, so both get TLS `verify-full` and a secretRef credential."""

from __future__ import annotations

import ssl
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncEngine
from tributary.runtime.errors import RuntimeConfigurationError
from tributary.runtime.secrets import DirSecretResolver

from ..config import DatabaseSettings, database_from_env
from ..errors import ConfigurationError
from .engine import make_tls_engine


def engine_from_settings(db: DatabaseSettings, env: Mapping[str, str]) -> AsyncEngine:
    resolver = DirSecretResolver.from_environ(env)
    try:
        password = resolver.resolve_text(db.secret_ref, "password")
        ca_file = str(resolver.resolve_file(db.ca_secret_ref, "ca.crt")) if db.ca_secret_ref else None
    except RuntimeConfigurationError:
        raise ConfigurationError("DATABASE_SECRET_UNRESOLVED", "the database secretRef did not resolve") from None
    try:
        return make_tls_engine(
            host=db.host, port=db.port, name=db.name, user=db.user, password=password, ca_file=ca_file
        )
    except (ssl.SSLError, OSError):  # a CA bundle that is not a readable PEM certificate
        raise ConfigurationError("DATABASE_TLS_INVALID", "the database CA bundle is not a usable certificate") from None


def engine_from_environ(env: Mapping[str, str]) -> AsyncEngine:
    return engine_from_settings(database_from_env(env), env)
