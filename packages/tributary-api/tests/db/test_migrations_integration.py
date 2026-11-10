"""Alembic upgrade, downgrade, upgrade against real PostgreSQL."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from tributary.api.db.engine import make_engine
from tributary.api.db.migrate import adowngrade, aupgrade, downgrade, upgrade

pytestmark = pytest.mark.integration

_TABLES = {"environments", "channels", "channel_versions", "deployments", "replays"}


async def _scalars(url: str, sql: str) -> set[str]:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            return {str(r[0]) for r in (await conn.execute(text(sql))).all()}
    finally:
        await engine.dispose()


def _tables(url: str) -> set[str]:
    return asyncio.run(_scalars(url, "SELECT tablename FROM pg_tables WHERE schemaname='public'")) - {"alembic_version"}


def test_migrations_upgrade_downgrade_upgrade(pg_url: str) -> None:
    downgrade(pg_url)  # clean slate if another test in the session ran first
    assert _tables(pg_url) == set()
    upgrade(pg_url)
    assert _tables(pg_url) == _TABLES
    downgrade(pg_url)
    assert _tables(pg_url) == set()
    assert asyncio.run(_scalars(pg_url, "SELECT proname FROM pg_proc WHERE proname LIKE 'tb\\_%'")) == set()
    upgrade(pg_url)
    assert _tables(pg_url) == _TABLES
    upgrade(pg_url)  # idempotent at head
    assert _tables(pg_url) == _TABLES


def test_migrated_schema_matches_models_and_constraints_exist(pg_url: str) -> None:
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from tributary.api.db.models import Base

    downgrade(pg_url)
    upgrade(pg_url)

    async def drift() -> list[object]:
        engine = make_engine(pg_url)
        try:
            async with engine.connect() as conn:

                def _diff(sync_conn: object) -> list[object]:
                    ctx = MigrationContext.configure(sync_conn, opts={"compare_type": True})  # type: ignore[arg-type]
                    return list(compare_metadata(ctx, Base.metadata))

                return await conn.run_sync(_diff)
        finally:
            await engine.dispose()

    assert asyncio.run(drift()) == []
    checks = asyncio.run(_scalars(pg_url, "SELECT conname FROM pg_constraint WHERE contype='c'"))
    assert {
        "ck_environments_mode",
        "ck_environments_name",
        "ck_channels_name",
        "ck_channel_versions_version",
        "ck_channel_versions_spec_sha256",
        "ck_deployments_status",
        "ck_deployments_image_digest",
        "ck_deployments_kind",
        "ck_deployments_undeploy_has_no_version",
        "ck_deployments_published_at",
        "ck_deployments_failed_has_code",
        "ck_environments_profile",
        "ck_replays_status",
        "ck_replays_range",
        "ck_replays_failed_has_code",
        "ck_replays_finished_at",
        "ck_replays_failure_code",
    } <= checks
    triggers = asyncio.run(_scalars(pg_url, "SELECT tgname FROM pg_trigger WHERE NOT tgisinternal"))
    assert "channel_versions_no_update" in triggers


def test_async_migration_variants_from_a_running_loop(pg_url: str) -> None:
    async def go() -> tuple[set[str], set[str]]:
        await adowngrade(pg_url)
        empty = await _scalars(pg_url, "SELECT tablename FROM pg_tables WHERE schemaname='public'")
        await aupgrade(pg_url)
        full = await _scalars(pg_url, "SELECT tablename FROM pg_tables WHERE schemaname='public'")
        return empty - {"alembic_version"}, full - {"alembic_version"}

    empty, full = asyncio.run(go())
    assert empty == set()
    assert full == _TABLES


def test_0002_maps_0001_rows_and_downgrade_restores_one_active(pg_url: str) -> None:
    """Data survives 0001 -> 0002 (active -> published, rolled_back -> superseded, restored_from -> rollback),
    and the lossy downgrade drops what 0001 cannot express and leaves exactly one active row."""
    digest = "sha256:" + "a" * 64

    async def seed() -> None:
        engine = make_engine(pg_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("INSERT INTO environments (id, name, mode) VALUES (gen_random_uuid(), 'dev', 'ui-managed')")
                )
                await conn.execute(
                    text("INSERT INTO channels (id, name, created_by) VALUES (gen_random_uuid(), 'adt', 'u')")
                )
                await conn.execute(
                    text(
                        "INSERT INTO channel_versions (id, channel_id, version, spec, spec_sha256, created_by) "
                        "SELECT gen_random_uuid(), id, 1, '{}', repeat('0', 64), 'u' FROM channels"
                    )
                )
                for status in ("rolled_back", "active"):
                    await conn.execute(
                        text(
                            "INSERT INTO deployments (id, channel_id, environment_id, version_id, image_digest, status, created_by) "
                            "SELECT gen_random_uuid(), c.id, e.id, v.id, :d, :s, 'u' FROM channels c, environments e, channel_versions v"
                        ),
                        {"d": digest, "s": status},
                    )
        finally:
            await engine.dispose()

    downgrade(pg_url)
    upgrade(pg_url, "0001")
    asyncio.run(seed())
    upgrade(pg_url)
    rows = asyncio.run(_scalars(pg_url, "SELECT kind || ':' || status FROM deployments ORDER BY seq"))
    assert rows == {"deploy:superseded", "deploy:published"}
    downgrade(pg_url, "0001")
    assert asyncio.run(_scalars(pg_url, "SELECT status FROM deployments")) == {"superseded", "active"}
    upgrade(pg_url)
