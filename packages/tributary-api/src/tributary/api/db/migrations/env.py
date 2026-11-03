"""Alembic environment (async). Production connection: `db.connect.engine_from_environ`
(TLS verify-full, secretRef credential). `Config.attributes['url']` is for tests only; never alembic.ini."""

from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from tributary.api.db.engine import to_async_url
from tributary.api.db.models import Base

config = context.config
target_metadata = Base.metadata


def _url() -> str:
    url = config.attributes.get("url")  # tests and offline SQL generation only
    if not url:
        raise RuntimeError("offline mode and tests need Config.attributes['url']")
    return to_async_url(str(url))


def _engine() -> AsyncEngine:
    """Tests pass a plain URL. Everything else (a migrate Job) uses the API's own TLS `verify-full` engine and
    secretRef credential from the TB_DATABASE_* environment, so the two cannot differ."""
    if config.attributes.get("url"):
        return create_async_engine(_url())
    from tributary.api.db.connect import engine_from_environ

    return engine_from_environ(os.environ)


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def _run_online() -> None:
    engine = _engine()
    try:
        async with engine.connect() as conn:
            await conn.run_sync(_run)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(url=_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(_run_online())
