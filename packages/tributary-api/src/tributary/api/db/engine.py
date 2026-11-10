"""Engine construction. The driver lives only in the URL scheme, here, so swapping asyncpg for another
async driver (wave-3.md 7.1) is a change to `ASYNC_DRIVER` and nothing else."""

from __future__ import annotations

import ssl

from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

ASYNC_DRIVER = "postgresql+asyncpg"
POOL_TIMEOUT_S = 30.0  # keep in step with `config.DB_POOL_TIMEOUT_S`


def to_async_url(url: str) -> str:
    """Rewrite any `postgresql[+driver]://` URL to the configured async driver. Other schemes raise."""
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql":
        raise ValueError(f"unsupported database backend {parsed.get_backend_name()!r}; PostgreSQL only")
    return parsed.set(drivername=ASYNC_DRIVER).render_as_string(hide_password=False)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(to_async_url(url), pool_pre_ping=True, pool_timeout=POOL_TIMEOUT_S)


def make_tls_engine(*, host: str, port: int, name: str, user: str, password: str, ca_file: str | None) -> AsyncEngine:
    """The control plane's own connection (ADR-004 C1/C2): TLS 1.2+, the server certificate chain verified and
    the host name checked (`verify-full`). There is no setting that relaxes either. The password lives only
    in the `URL` object (whose `repr` hides it), never in a string the process logs."""
    context = ssl.create_default_context(cafile=ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:  # pragma: no cover - defaults
        raise RuntimeError("TLS verification is not fully on")
    url = URL.create(ASYNC_DRIVER, username=user, password=password, host=host, port=port, database=name)
    return create_async_engine(url, pool_pre_ping=True, pool_timeout=POOL_TIMEOUT_S, connect_args={"ssl": context})


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
