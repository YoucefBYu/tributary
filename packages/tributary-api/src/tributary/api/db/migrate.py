"""Programmatic Alembic entry points (no alembic.ini)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config

_SCRIPTS = Path(__file__).parent / "migrations"


def make_config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(_SCRIPTS))
    cfg.attributes["url"] = url
    return cfg


# `upgrade` / `downgrade` are synchronous and Alembic's env.py calls `asyncio.run`, so they MUST NOT be
# called from a running event loop. From async code use `aupgrade` / `adowngrade`, which run them in a
# worker thread.


def upgrade(url: str, revision: str = "head") -> None:
    command.upgrade(make_config(url), revision)


def downgrade(url: str, revision: str = "base") -> None:
    command.downgrade(make_config(url), revision)


async def aupgrade(url: str, revision: str = "head") -> None:
    await asyncio.to_thread(upgrade, url, revision)


async def adowngrade(url: str, revision: str = "base") -> None:
    await asyncio.to_thread(downgrade, url, revision)
