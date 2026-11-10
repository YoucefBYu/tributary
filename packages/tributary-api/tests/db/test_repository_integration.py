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
)
from tributary.api.db.migrate import upgrade
from tributary.api.db.models import ChannelVersion
from tributary.api.db.repository import (
    ChannelRepository,
    DeploymentRepository,
    EnvironmentRepository,
    image_digest_of,
    published_bytes,
    spec_digest,
)

pytestmark = pytest.mark.integration

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
SPEC = {
    "apiVersion": "tributary.dev/v1",
    "kind": "Channel",
    "metadata": {"name": "adt"},
    "spec": {"pipeline": {"bundle": f"reg.example/img@{D1}"}},
}


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


def test_versions_are_consecutive_stamped_and_hash_the_published_bytes(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        ch = ChannelRepository(s)
        await ch.create("adt", "user-1")
        v1 = await ch.add_version("adt", SPEC, "user-1", {"kind": "ui"})
        v2 = await ch.add_version("adt", {**SPEC, "spec": {**SPEC["spec"], "x": 1}}, "user-1")
        assert (v1.version, v2.version) == (1, 2)
        assert (v1.spec["metadata"]["version"], v2.spec["metadata"]["version"]) == (1, 2)
        assert "version" not in SPEC["metadata"]  # the caller's dict is not mutated
        assert v1.spec_sha256 == spec_digest(v1.spec) != v2.spec_sha256
        assert v1.origin == {"kind": "ui"} and v2.origin == {"kind": "api"}
        # JSONB round trip keeps the published bytes (the deploy path re-checks this at publish time)
        await s.refresh(v1)
        assert spec_digest(v1.spec) == v1.spec_sha256
        assert published_bytes(v1.spec).startswith(b'{"apiVersion"')
        assert [v.version for v in await ch.list_versions("adt")] == [2, 1]
        assert [v.version for v in await ch.list_versions("adt", limit=1, before_version=2)] == [1]
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


def test_image_digest_comes_from_the_bundle() -> None:
    assert image_digest_of(SPEC) == D1
    assert image_digest_of({"spec": {"pipeline": {"bundle": "img:latest"}}}) is None
    assert image_digest_of({"spec": {}}) is None


async def _seed(s: AsyncSession, versions: int = 3, mode: str = "ui-managed") -> None:
    await EnvironmentRepository(s).create("dev", mode)
    ch = ChannelRepository(s)
    await ch.create("adt", "u")
    for _ in range(versions):
        await ch.add_version("adt", SPEC, "u")


def test_request_supersedes_older_pending_and_current_is_newest_live(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        c = ChannelRepository(s)
        ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
        v1, v2 = await c.get_version("adt", 1), await c.get_version("adt", 2)
        dr = DeploymentRepository(s)
        assert await dr.current(ch.id, env.id) is None
        d1 = await dr.request(ch, env, "deploy", v1, None, "u")
        assert d1.status == "pending" and d1.image_digest == D1
        d2 = await dr.request(ch, env, "deploy", v2, 1, "u")
        await s.refresh(d1)
        assert (d1.status, d2.status) == ("superseded", "pending")
        current = await dr.current(ch.id, env.id)
        assert current is not None and current.id == d2.id
        await dr.mark_published(d2)
        assert d2.published_at is not None
        with pytest.raises(InvalidValueError):
            await dr.mark_published(d2)  # only pending moves

    run(migrated, scenario)


def test_failed_is_never_current_and_needs_a_code(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        c = ChannelRepository(s)
        ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
        dr = DeploymentRepository(s)
        d1 = await dr.request(ch, env, "deploy", await c.get_version("adt", 1), None, "u")
        await dr.mark_published(d1)
        d2 = await dr.request(ch, env, "deploy", await c.get_version("adt", 2), 1, "u")
        await dr.mark_failed(d2, "PUBLISH_TIMEOUT")
        current = await dr.current(ch.id, env.id)
        assert current is not None and current.id == d1.id
        states = await dr.states_for_channel(ch)
        assert [(n, d.status) for n, d in states] == [("dev", "failed")]  # the failure stays visible
        with pytest.raises(InvalidValueError):
            await dr.mark_failed(d2, "PUBLISH_FAILED")

    run(migrated, scenario)


def test_undeploy_tombstone_and_current_listing(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s)
        c = ChannelRepository(s)
        ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
        dr = DeploymentRepository(s)
        d1 = await dr.request(ch, env, "deploy", await c.get_version("adt", 1), None, "u")
        await dr.mark_published(d1)
        assert [n for n, _ in await dr.current_in_environment(env, limit=10)] == ["adt"]
        t = await dr.request(ch, env, "undeploy", None, 1, "u")
        assert (t.version_id, t.image_digest) == (None, None)
        await dr.mark_published(t)
        cur = await dr.current(ch.id, env.id)
        assert cur is not None and cur.kind == "undeploy"
        assert await dr.current_in_environment(env, limit=10) == []
        assert len(await dr.history(ch, env, limit=10)) == 2
        assert [d.kind for d in await dr.history(ch, env, limit=10, before_seq=t.seq)] == ["deploy"]

    run(migrated, scenario)


def test_previous_published_version_skips_unpublished_and_identical(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s, versions=4)
        c = ChannelRepository(s)
        ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
        dr = DeploymentRepository(s)

        async def go(version: int, *, publish: bool = True) -> object:
            d = await dr.request(ch, env, "deploy", await c.get_version("adt", version), None, "u")
            await (dr.mark_published(d) if publish else dr.mark_failed(d, "PUBLISH_FAILED"))
            return d

        await go(1)
        await go(2)
        await go(3, publish=False)  # failed: never published, so never a rollback target
        d2b = await go(2)  # the same version again
        target = await dr.previous_published_version(d2b)  # type: ignore[arg-type]
        assert target is not None and target.version == 1

    run(migrated, scenario)


def test_missing_bundle_and_bad_inputs_are_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await EnvironmentRepository(s).create("dev", "ui-managed")
        c = ChannelRepository(s)
        await c.create("nobundle", "u")
        await c.add_version("nobundle", {**SPEC, "spec": {}}, "u")
        ch, env = await c.get_by_name("nobundle"), await EnvironmentRepository(s).get_by_name("dev")
        with pytest.raises(InvalidValueError):
            await DeploymentRepository(s).request(ch, env, "deploy", await c.get_version("nobundle", 1), None, "u")
        await c.create("adt", "u")
        good = await c.add_version("adt", SPEC, "u")
        with pytest.raises(InvalidValueError):  # rejected by the database (created_by is 255 characters)
            await DeploymentRepository(s).request(ch, env, "deploy", good, None, "x" * 500)

    run(migrated, scenario)


def test_channel_with_deployments_cannot_be_deleted(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        await _seed(s, versions=1)
        c = ChannelRepository(s)
        ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
        await DeploymentRepository(s).request(ch, env, "deploy", await c.get_version("adt", 1), None, "u")
        with pytest.raises(InUseError):
            await c.delete("adt")

    run(migrated, scenario)


def test_invalid_environment_mode_and_profile_refused(migrated: str) -> None:
    async def scenario(s: AsyncSession) -> None:
        with pytest.raises(InvalidValueError):
            await EnvironmentRepository(s).create("dev", "whatever")
        env = await EnvironmentRepository(s).create("dev", "ui-managed")
        assert env.profile == "production"
        with pytest.raises(AlreadyExistsError):
            await EnvironmentRepository(s).create("dev", "ui-managed")

    run(migrated, scenario)


def test_concurrent_requests_under_the_channel_lock_leave_one_pending(migrated: str) -> None:
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

            async def request(version: int) -> None:
                async with factory() as s, s.begin():
                    dr = DeploymentRepository(s)
                    ch = await dr.lock_channel("adt")
                    env = await EnvironmentRepository(s).get_by_name("dev")
                    ver = await ChannelRepository(s).get_version("adt", version)
                    await dr.request(ch, env, "deploy", ver, None, "u")

            await asyncio.gather(*(request(1 + i % 2) for i in range(8)))
            async with factory() as s:
                c = ChannelRepository(s)
                ch, env = await c.get_by_name("adt"), await EnvironmentRepository(s).get_by_name("dev")
                hist = await DeploymentRepository(s).history(ch, env, limit=50)
                assert len(hist) == 8
                assert [d.status for d in hist].count("pending") == 1
                assert hist[0].status == "pending"  # the last writer in seq order is the live one
        finally:
            await engine.dispose()

    asyncio.run(go())
