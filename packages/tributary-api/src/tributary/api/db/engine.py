"""Engine construction. The driver lives only in the URL scheme, here, so swapping asyncpg for another
async driver (wave-3.md 7.1) is a change to `ASYNC_DRIVER` and nothing else."""

from __future__ import annotations

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

ASYNC_DRIVER = "postgresql+asyncpg"


def to_async_url(url: str) -> str:
    """Rewrite any `postgresql[+driver]://` URL to the configured async driver. Other schemes raise."""
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql":
        raise ValueError(f"unsupported database backend {parsed.get_backend_name()!r}; PostgreSQL only")
    return parsed.set(drivername=ASYNC_DRIVER).render_as_string(hide_password=False)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(to_async_url(url), pool_pre_ping=True)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
