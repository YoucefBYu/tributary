"""Alembic environment (async). The URL comes from `Config.attributes['url']` or `TRIBUTARY_DATABASE_URL`;
never from alembic.ini, so no credentials sit in a file."""

from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from tributary.api.db.engine import to_async_url
from tributary.api.db.models import Base

config = context.config
target_metadata = Base.metadata


def _url() -> str:
    url = config.attributes.get("url") or os.environ.get("TRIBUTARY_DATABASE_URL")
    if not url:
        raise RuntimeError("no database URL: set TRIBUTARY_DATABASE_URL or Config.attributes['url']")
    return to_async_url(str(url))


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def _run_online() -> None:
    engine = create_async_engine(_url())
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
