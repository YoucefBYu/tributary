"""Repository behaviour against real PostgreSQL (no pytest-asyncio: each test runs one coroutine)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.errors import (
    AlreadyExistsError,
    InUseError,
    InvalidValueError,
    NotFoundError,
    NothingToRollBackError,
)
from tributary.api.db.migrate import upgrade
from tributary.api.db.models import ChannelVersion
from tributary.api.db.repository import ChannelRepository, DeploymentRepository, EnvironmentRepository

pytestmark = pytest.mark.integration

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64
SPEC = {"apiVersion": "tributary.dev/v0", "kind": "Channel", "metadata": {"name": "adt"}, "spec": {}}


@pytest.fixture(scope="module")
def migrated(pg_url: str) -> str:
    upgrade(pg_url)
    return pg_url


def run(url: str, scenario: Callable[[AsyncSession], Awaitable[None]]) -> None:
    async def _go() -> None:
        engine = make_engine(url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE deployments, channel_versions, channels, environments RESTART IDENTITY CASCADE")
                )
            async with make_session_factory(engine)() as session, session.begin():
                await scenario(session)
        finally:
            await engine.dispose()

    asyncio.run(_go())


def test_versions_are_consecutive_and_digest_pinned(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        ch = ChannelRepository(s)
        await ch.create("adt", "user-1")
        v1 = await ch.add_version("adt", SPEC, "user-1")
        v2 = await ch.add_version("adt", {**SPEC, "spec": {"x": 1}}, "user-1")
        assert (v1.version, v2.version) == (1, 2)
        assert v1.spec_sha256 != v2.spec_sha256
        assert [v.version for v in await ch.list_versions("adt")] == [2, 1]
        with pytest.raises(AlreadyExistsError):
            await ch.create("adt", "user-1")
        with pytest.raises(NotFoundError):
            await ch.get_version("adt", 3)

    run(migrated, scenario)


def test_versions_are_immutable(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        ch = ChannelRepository(s)
        await ch.create("adt", "u")
        v = await ch.add_version("adt", SPEC, "u")
        with pytest.raises(DBAPIError, match="immutable"):
            await s.execute(update(ChannelVersion).where(ChannelVersion.id == v.id).values(spec={}))

    run(migrated, scenario)


def test_deploy_supersedes_and_one_active(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        await EnvironmentRepository(s).create("prod", "git-managed")
        ch = ChannelRepository(s)
        c = await ch.create("adt", "u")
        await ch.add_version("adt", SPEC, "u")
        await ch.add_version("adt", SPEC, "u")
        dr = DeploymentRepository(s)
        d1 = await dr.deploy("adt", 1, "dev", D1, "u")
        d2 = await dr.deploy("adt", 2, "dev", D2, "u")
        p1 = await dr.deploy("adt", 1, "prod", D1, "u")
        await s.refresh(d1)
        assert (d1.status, d2.status, p1.status) == ("superseded", "active", "active")
        envs = {e.name: e for e in await EnvironmentRepository(s).list()}
        active = await dr.active(c.id, envs["dev"].id)
        assert active is not None and active.id == d2.id
        assert len(await dr.history("adt")) == 3
        assert len(await dr.history("adt", "dev")) == 2

    run(migrated, scenario)


def test_tag_instead_of_digest_is_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        await ChannelRepository(s).create("adt", "u")
        await ChannelRepository(s).add_version("adt", SPEC, "u")
        with pytest.raises(InvalidValueError):
            await DeploymentRepository(s).deploy("adt", 1, "dev", "registry/img:latest", "u")

    run(migrated, scenario)


def test_rollback_walks_back_and_records_history(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        ch = ChannelRepository(s)
        await ch.create("adt", "u")
        for _ in range(3):
            await ch.add_version("adt", SPEC, "u")
        dr = DeploymentRepository(s)
        await dr.deploy("adt", 1, "dev", D1, "u")
        await dr.deploy("adt", 2, "dev", D2, "u")
        d3 = await dr.deploy("adt", 3, "dev", D3, "u")
        r1 = await dr.rollback("adt", "dev", "u")
        assert (r1.image_digest, r1.status) == (D2, "active")
        assert r1.restored_from is not None
        await s.refresh(d3)
        assert d3.status == "rolled_back"
        r2 = await dr.rollback("adt", "dev", "u")
        assert r2.image_digest == D1
        with pytest.raises(NothingToRollBackError):
            await dr.rollback("adt", "dev", "u")
        assert len(await dr.history("adt", "dev")) == 5  # append-only

    run(migrated, scenario)


def test_rollback_without_deployment_is_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        await ChannelRepository(s).create("adt", "u")
        with pytest.raises(NothingToRollBackError):
            await DeploymentRepository(s).rollback("adt", "dev", "u")
        with pytest.raises(NotFoundError):
            await DeploymentRepository(s).rollback("nope", "dev", "u")

    run(migrated, scenario)


def test_channel_with_deployments_cannot_be_deleted(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        ch = ChannelRepository(s)
        await ch.create("adt", "u")
        await ch.add_version("adt", SPEC, "u")
        await DeploymentRepository(s).deploy("adt", 1, "dev", D1, "u")
        with pytest.raises(InUseError):
            await ch.delete("adt")

    run(migrated, scenario)


def test_invalid_environment_mode_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        with pytest.raises(InvalidValueError):
            await EnvironmentRepository(s).create("dev", "whatever")
        await EnvironmentRepository(s).create("dev", "ui-managed")
        with pytest.raises(AlreadyExistsError):
            await EnvironmentRepository(s).create("dev", "ui-managed")

    run(migrated, scenario)


async def _seed(s: AsyncSession, versions: int = 3) -> None:
    await EnvironmentRepository(s).create("dev", "ui-managed")
    ch = ChannelRepository(s)
    await ch.create("adt", "u")
    for _ in range(versions):
        await ch.add_version("adt", SPEC, "u")


def test_failed_deploy_leaves_old_deployment_active(migrated: str) -> None:
    """Regression: the status flip and the insert share one savepoint."""

    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        dr = DeploymentRepository(s)
        d1 = await dr.deploy("adt", 1, "dev", D1, "u")
        with pytest.raises(InvalidValueError):
            await dr.deploy("adt", 2, "dev", "latest", "u")  # rejected in Python
        with pytest.raises(InvalidValueError):
            await dr.deploy("adt", 2, "dev", D2, "x" * 500)  # rejected by the database
        await s.refresh(d1)
        assert d1.status == "active"
        assert len(await dr.history("adt", "dev")) == 1

    run(migrated, scenario)


def test_deploy_after_rollback_then_rollback_again(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        dr = DeploymentRepository(s)
        await dr.deploy("adt", 1, "dev", D1, "u")
        await dr.deploy("adt", 2, "dev", D2, "u")
        r = await dr.rollback("adt", "dev", "u")
        assert r.image_digest == D1
        d3 = await dr.deploy("adt", 3, "dev", D3, "u")
        back = await dr.rollback("adt", "dev", "u")
        # d3 is replaced by the most recent superseded deployment that differs: the restored D1 copy
        assert back.image_digest == D1
        await s.refresh(d3)
        assert d3.status == "rolled_back"

    run(migrated, scenario)


def test_rollback_skips_target_identical_to_current(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        dr = DeploymentRepository(s)
        await dr.deploy("adt", 1, "dev", D1, "u")
        await dr.deploy("adt", 2, "dev", D2, "u")
        await dr.deploy("adt", 2, "dev", D2, "u")  # same version and image again
        r = await dr.rollback("adt", "dev", "u")
        assert r.image_digest == D1  # skipped the identical D2 row

    run(migrated, scenario)


def test_restored_from_walk_back_after_intervening_deploy(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s, versions=4)
        dr = DeploymentRepository(s)
        d1 = await dr.deploy("adt", 1, "dev", D1, "u")
        await dr.deploy("adt", 2, "dev", D2, "u")
        await dr.deploy("adt", 3, "dev", D3, "u")
        r = await dr.rollback("adt", "dev", "u")  # restores D2
        assert r.image_digest == D2
        d4 = await dr.deploy("adt", 4, "dev", "sha256:" + "4" * 64, "u")  # fresh lineage
        assert d4.restored_from is None
        r2 = await dr.rollback("adt", "dev", "u")
        assert r2.image_digest == D2  # most recent superseded differing row is the D2 copy
        r3 = await dr.rollback("adt", "dev", "u")
        assert r3.image_digest == D1  # then walks back past the restored deployment
        await s.refresh(d1)
        assert d1.status == "superseded"

    run(migrated, scenario)


def test_concurrent_deploys_leave_exactly_one_active(migrated: str) -> None:
    async def go() -> None:
        engine = make_engine(migrated)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE deployments, channel_versions, channels, environments RESTART IDENTITY CASCADE")
                )
            factory = make_session_factory(engine)
            async with factory() as s, s.begin():
                await _seed(s, versions=2)

            async def deploy(version: int, digest: str) -> None:
                async with factory() as s, s.begin():
                    await DeploymentRepository(s).deploy("adt", version, "dev", digest, "u")

            await asyncio.gather(*(deploy(1 + i % 2, "sha256:" + str(i % 10) * 64) for i in range(8)))
            async with factory() as s:
                hist = await DeploymentRepository(s).history("adt", "dev")
                assert len(hist) == 8
                assert [d.status for d in hist].count("active") == 1
                assert hist[0].status == "active"  # the last writer in seq order won
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_update_of_spec_sha256_is_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await ChannelRepository(s).create("adt", "u")
        v = await ChannelRepository(s).add_version("adt", SPEC, "u")
        with pytest.raises(DBAPIError, match="immutable"):
            await s.execute(update(ChannelVersion).where(ChannelVersion.id == v.id).values(spec_sha256="0" * 64))

    run(migrated, scenario)
