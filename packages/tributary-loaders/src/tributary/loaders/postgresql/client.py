"""The PostgreSQL transport: `asyncpg`, native `asyncio` (no thread pool needed — unlike
`http/client.py`'s `urllib`, `asyncpg` never blocks the event loop).

Not `psycopg` (`psycopg[binary]`/`psycopg[c]` v3): its licence classifier is LGPL-3.0, which fails
`.github/scripts/licence_check.py`'s DENIED pattern outright — CLAUDE.md permits Apache-2.0, MIT, BSD,
ISC and PSF only. `asyncpg` is Apache-2.0 (`License-Expression: Apache-2.0`, confirmed by
`licence_check.py` 2026-09-24) — `contracts/channel-spec/v0/README.md` section 10.2 named it as the
likely candidate and asked this package to confirm it; confirmed. `asyncpg` ships no `py.typed` marker
(no such file in the installed package), so `mypy --strict` cannot check its stubs; the import below is
the one place that is silenced, matching `mllp/server.py`'s targeted `type: ignore` on the `uvloop`
import that has no Windows wheel — not a blanket exemption in `pyproject.toml` (which this package does
not own).

## TLS and auth (`$defs/dbTls`, `$defs/sqlSinkParams.auth`, README 3.4.3 / 10.1)

`tls.mode: verify-full` is certificate AND hostname verification, always — there is no "encrypted but
unauthenticated" mode in the contract, so `ssl.create_default_context()` (verifies against the OS trust
store) is the starting point every time TLS is on. `tls.secretRef`, when present, names an additional
trust bundle (`ca.crt`) loaded on top of that. `tls.mode: disable` is refused outside a loopback host at
config-parse time (`config.py`), not here.

`auth.secretRef` resolves to `user` always, plus `password` when `auth.type: password`, or `tls.crt`/
`tls.key` (a client certificate presented on the same TLS connection) when
`auth.type: client-certificate` — a data-plane detail the JSON Schema does not fix (it only fixes the
wire shape of `auth`), documented here the way `http.loader._auth_header`'s key names
(`credentials`, `token`) are documented at their one call site.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from typing import Any, Protocol

import asyncpg  # type: ignore[import-untyped]  # Apache-2.0, no py.typed marker (see module docstring)

from ..secrets import SecretResolver
from .config import PgAuthSpec, PgTlsSpec, PostgresqlDestinationConfig


class TransportError(Exception):
    """The connection could not be made or was lost mid-query: DNS failure, connection refused, pool
    exhausted. Always retryable (`loader.py`'s classification), never the database's own answer to the
    query."""


@dataclass(frozen=True, slots=True)
class PgConnectionSettings:
    host: str
    port: int
    database: str
    user: str
    password: str | None  # None when auth.type is client-certificate
    ssl_context: ssl.SSLContext | None  # None only when tls.mode is disable


def _build_ssl_context(tls: PgTlsSpec, auth: PgAuthSpec, resolver: SecretResolver) -> ssl.SSLContext | None:
    if tls.mode == "disable":
        return None
    ctx = ssl.create_default_context()
    if tls.secret_ref is not None:
        ctx.load_verify_locations(cafile=str(resolver.resolve_file(tls.secret_ref, "ca.crt")))
    if auth.type == "client-certificate":
        ctx.load_cert_chain(
            certfile=str(resolver.resolve_file(auth.secret_ref, "tls.crt")),
            keyfile=str(resolver.resolve_file(auth.secret_ref, "tls.key")),
        )
    return ctx


def connection_settings_from_config(
    config: PostgresqlDestinationConfig, resolver: SecretResolver
) -> PgConnectionSettings:
    ssl_context = _build_ssl_context(config.tls, config.auth, resolver)
    user = resolver.resolve_text(config.auth.secret_ref, "user")
    password = resolver.resolve_text(config.auth.secret_ref, "password") if config.auth.type == "password" else None
    return PgConnectionSettings(
        host=config.host,
        port=config.port,
        database=config.database,
        user=user,
        password=password,
        ssl_context=ssl_context,
    )


class PgPoolLike(Protocol):
    """The slice of `asyncpg.Pool` the loader uses. Small on purpose, so a unit test can supply a fake
    pool with no real PostgreSQL — the integration test (`test_integration_postgresql_loader.py`,
    `testcontainers`) is what actually proves this against the real driver and a real server."""

    async def execute(self, query: str, /, *args: Any, timeout: float | None = None) -> str: ...

    async def fetchval(self, query: str, /, *args: Any, timeout: float | None = None) -> Any: ...

    async def close(self) -> None: ...


async def create_pool(
    settings: PgConnectionSettings, *, min_size: int, max_size: int, command_timeout: float
) -> PgPoolLike:
    """One connection pool per `PostgresqlLoader` instance, opened once at startup — like `HttpLoader`'s
    dedicated thread pool, not shared process-wide, and not reopened per message. A destination whose
    connection settings are repointed by a hot-reloaded spec needs a loader restart until the runtime
    grows a way to hot-reload a stateful connection resource (`http/handler.py`'s docstring notes the
    same limit for `retry`)."""
    pool = await asyncpg.create_pool(
        host=settings.host,
        port=settings.port,
        database=settings.database,
        user=settings.user,
        password=settings.password,
        ssl=settings.ssl_context,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
    )
    return pool  # type: ignore[no-any-return]  # asyncpg.Pool has no stubs; PgPoolLike is the checked shape
