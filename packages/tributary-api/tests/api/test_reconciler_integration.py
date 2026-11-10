"""The reconciler (W3-5) against real PostgreSQL, with an in-memory model of a compacted topic (the model
is both the publisher and the reader, so what the reconciler publishes is what it later observes).
Kafka and a real worker are in `test_hot_reload_integration.py`."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from api_kit import TEST_ACTOR, fake_audit, run
from sqlalchemy import text
from test_routes_integration import API, GIT, document
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import ChannelRepository, DeploymentRepository, EnvironmentRepository, published_bytes
from tributary.api.publisher import PublishError, PublishTimeoutError
from tributary.api.reconciler import Reconciler, TopicReadError
from tributary.api.service import ControlPlane
from tributary.runtime.logs import JsonLogger

pytestmark = pytest.mark.integration

LOG = JsonLogger("api", level="error")


class TopicModel:
    """A compacted topic per environment: last record wins, `None` is a tombstone."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, str], bytes | None] = {}
        self.published: list[tuple[str, str, bytes | None]] = []
        self.fail: type[PublishError] | None = None
        self.unreadable = False
        self.degraded = False
        self.stopped = False

    async def publish(self, env: str, channel: str, value: bytes | None) -> None:
        if self.fail is not None:
            raise self.fail
        self.records[(env, channel)] = value
        self.published.append((env, channel, value))

    def stop(self) -> None:
        self.stopped = True

    def snapshot(self, env: str) -> dict[str, bytes | None]:
        if self.unreadable:
            raise TopicReadError
        return {c: v for (e, c), v in self.records.items() if e == env}


class Rig:
    def __init__(self, url: str) -> None:
        self.engine = make_engine(url)
        self.sessions = make_session_factory(self.engine)
        self.topic = TopicModel()
        self.service = ControlPlane(self.sessions, self.topic, LOG, audit=fake_audit(), audit_home="dev")

    def reconciler(self, **kw: Any) -> Reconciler:
        return Reconciler(self.sessions, self.topic, self.topic, LOG, **kw)

    async def sweep(self, **kw: Any) -> dict[str, Any]:
        return {r.environment: r for r in await self.reconciler(**kw).run_once()}

    async def drop_topic(self, env: str, channel: str, value: bytes | str | None = "absent") -> None:
        if value == "absent":
            self.topic.records.pop((env, channel), None)
        else:
            self.topic.records[(env, channel)] = value  # type: ignore[assignment]

    async def spec_bytes(self, name: str, version: int) -> bytes:
        async with self.sessions() as s:
            return published_bytes((await ChannelRepository(s).get_version(name, version)).spec)


@pytest.fixture
def rig(pg_url: str) -> Callable[[], Rig]:
    upgrade(pg_url)

    async def reset() -> None:
        engine = make_engine(pg_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE deployments, channel_versions, channels, environments RESTART IDENTITY CASCADE")
                )
            async with make_session_factory(engine)() as s, s.begin():
                await EnvironmentRepository(s).create("dev", "ui-managed")
                await EnvironmentRepository(s).create("prod", "git-managed")
        finally:
            await engine.dispose()

    asyncio.run(reset())
    return lambda: Rig(pg_url)


async def two_versions(r: Rig, name: str = "adt", origin: dict[str, Any] = API) -> None:
    await r.service.create_channel(name, document(name), origin, TEST_ACTOR)
    await r.service.add_version(name, document(name, 4), origin, TEST_ACTOR)


def test_missing_and_stale_records_are_repaired_and_a_second_pass_publishes_nothing(rig: Callable[[], Rig]) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            d = await r.service.deploy("dev", "adt", 2, None, TEST_ACTOR)
            assert d["status"] == "published"
            v2 = await r.spec_bytes("adt", 2)
            assert r.topic.records[("dev", "adt")] == v2

            converged = await r.sweep()
            assert converged["dev"].converged and r.topic.published == [("dev", "adt", v2)]  # nothing new

            await r.drop_topic("dev", "adt")  # a lost record
            first = await r.sweep()
            assert first["dev"].repaired == ["adt"] and r.topic.records[("dev", "adt")] == v2
            await r.drop_topic("dev", "adt", await r.spec_bytes("adt", 1))  # a stale version
            second = await r.sweep()  # a fresh Reconciler each time: nothing is kept between passes (restart)
            assert second["dev"].repaired == ["adt"] and r.topic.records[("dev", "adt")] == v2
            before = len(r.topic.published)
            assert (await r.sweep())["dev"].converged and len(r.topic.published) == before  # idempotent
        finally:
            await r.engine.dispose()

    run(go())


def test_an_undeployed_channel_gets_a_tombstone_and_an_absent_key_is_already_converged(rig: Callable[[], Rig]) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            await r.service.deploy("dev", "adt", 1, None, TEST_ACTOR)
            await r.service.undeploy("dev", "adt", 1, TEST_ACTOR)
            assert r.topic.records[("dev", "adt")] is None
            await r.drop_topic("dev", "adt")  # compaction removed the tombstone: still converged
            before = len(r.topic.published)
            assert (await r.sweep())["dev"].converged and len(r.topic.published) == before
            await r.drop_topic("dev", "adt", await r.spec_bytes("adt", 1))  # the old spec came back
            assert (await r.sweep())["dev"].tombstoned == [] and r.topic.records[("dev", "adt")] is None
        finally:
            await r.engine.dispose()

    run(go())


def test_a_failed_deployment_is_never_published_and_its_leftover_is_overwritten(rig: Callable[[], Rig]) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            await r.service.deploy("dev", "adt", 1, None, TEST_ACTOR)
            v1, v2 = await r.spec_bytes("adt", 1), await r.spec_bytes("adt", 2)
            r.topic.fail = PublishTimeoutError
            failed = await r.service.deploy("dev", "adt", 2, 1, TEST_ACTOR)
            assert failed["status"] == "failed"
            r.topic.fail = None
            published_before = list(r.topic.published)
            assert (await r.sweep())["dev"].converged  # topic holds v1, PostgreSQL says v1
            assert r.topic.published == published_before and v2 not in [p[2] for p in r.topic.published]
            await r.drop_topic("dev", "adt", v2)  # the timed-out publish landed after all
            assert (await r.sweep())["dev"].repaired == ["adt"]
            assert r.topic.records[("dev", "adt")] == v1
        finally:
            await r.engine.dispose()

    run(go())


def test_a_young_pending_record_is_left_alone_and_an_orphaned_one_expires(rig: Callable[[], Rig]) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            await r.service.deploy("dev", "adt", 1, None, TEST_ACTOR)
            v1 = await r.spec_bytes("adt", 1)
            async with r.sessions() as s, s.begin():  # a deploy that crashed between its two transactions
                crepo, drepo = ChannelRepository(s), DeploymentRepository(s)
                ch = await crepo.get_by_name("adt")
                env = await EnvironmentRepository(s).get_by_name("dev")
                await drepo.request(ch, env, "deploy", await crepo.get_version("adt", 2), 1, "u")
            await r.drop_topic("dev", "adt")
            published = len(r.topic.published)

            young = await r.sweep()
            assert young["dev"].in_flight == ["adt"] and len(r.topic.published) == published

            old = await r.sweep(now=lambda: datetime.now(UTC) + timedelta(hours=1))
            assert old["dev"].expired_pending == ["adt"] and old["dev"].repaired == ["adt"]
            assert r.topic.records[("dev", "adt")] == v1  # the last PUBLISHED version, not the orphaned request
            detail = await r.service.channel_deployments("dev", "adt", 10, None)
            assert detail["items"][0]["status"] == "failed" and detail["items"][0]["failure_code"] == "PUBLISH_TIMEOUT"
        finally:
            await r.engine.dispose()

    run(go())


def test_git_managed_refuses_non_git_origins_and_prunes_orphans_ui_managed_only_reports(rig: Callable[[], Rig]) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r, "from-ui", API)
            await two_versions(r, "from-git", GIT)
            async with r.sessions() as s, s.begin():  # a published record the deploy path would have refused
                crepo, drepo = ChannelRepository(s), DeploymentRepository(s)
                env = await EnvironmentRepository(s).get_by_name("prod")
                dep = await drepo.request(
                    await crepo.get_by_name("from-ui"), env, "deploy", await crepo.get_version("from-ui", 1), None, "u"
                )
                await drepo.mark_published(dep)
            assert (await r.service.deploy("prod", "from-git", 1, None, TEST_ACTOR))["status"] == "published"
            await r.drop_topic("prod", "from-git")
            await r.drop_topic("prod", "ghost", b"{}")
            await r.drop_topic("dev", "ghost", b"{}")

            res = await r.sweep()
            assert res["prod"].refused == ["from-ui"] and ("prod", "from-ui", None) not in r.topic.published
            assert res["prod"].repaired == ["from-git"] and res["prod"].tombstoned == ["ghost"]
            assert r.topic.records[("prod", "ghost")] is None
            assert res["dev"].orphans == ["ghost"] and r.topic.records[("dev", "ghost")] == b"{}"
        finally:
            await r.engine.dispose()

    run(go())


def test_an_unreadable_topic_or_a_failed_publish_changes_nothing_and_the_next_pass_retries(
    rig: Callable[[], Rig],
) -> None:
    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            await r.service.deploy("dev", "adt", 1, None, TEST_ACTOR)
            await r.drop_topic("dev", "adt")
            r.topic.unreadable = True
            published = len(r.topic.published)
            assert (await r.sweep())["dev"].errors == ["*topic*"] and len(r.topic.published) == published
            r.topic.unreadable = False
            r.topic.fail = PublishError
            assert (await r.sweep())["dev"].errors == ["adt"] and ("dev", "adt") not in r.topic.records
            r.topic.fail = None
            assert (await r.sweep())["dev"].repaired == ["adt"]
        finally:
            await r.engine.dispose()

    run(go())


def test_cancelling_the_loop_stops_a_blocked_topic_read(rig: Callable[[], Rig]) -> None:
    import threading

    async def go() -> None:
        r = rig()
        entered, release = threading.Event(), threading.Event()

        def blocking(env: str) -> dict[str, bytes | None]:
            entered.set()
            release.wait(10)
            raise TopicReadError

        r.topic.snapshot = blocking  # type: ignore[method-assign]
        r.topic.stop = lambda: release.set()  # type: ignore[method-assign]
        try:
            task = asyncio.create_task(r.reconciler().run_forever(1))
            while not entered.is_set():
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
            assert release.is_set()  # the reader was told to stop, so the thread ends instead of waiting 30 s
        finally:
            release.set()
            await r.engine.dispose()

    run(go())


def test_a_refused_audit_stores_nothing_and_does_not_strand_the_request_in_flight(rig: Callable[[], Rig]) -> None:
    from api_kit import RecordingSender, fake_audit
    from tributary.api.audit import AuditSendError
    from tributary.api.problems import ApiProblem

    class Refusing(RecordingSender):
        async def send(self, env: str, key: bytes, value: bytes) -> None:
            raise AuditSendError

    async def go() -> None:
        r = rig()
        try:
            await two_versions(r)
            await r.service.deploy("dev", "adt", 1, None, TEST_ACTOR)  # A is current and published
            r.service._audit = fake_audit(Refusing())
            with pytest.raises(ApiProblem) as e:
                await r.service.deploy("dev", "adt", 2, 1, TEST_ACTOR)  # B: its audit is refused
            assert e.value.code == "AUDIT_UNAVAILABLE"
            history = await r.service.channel_deployments("dev", "adt", 10, None)
            assert [(i["version"], i["status"]) for i in history["items"]] == [(1, "published")]  # no B row at all
            assert r.topic.records[("dev", "adt")] == await r.spec_bytes("adt", 1)  # and B never reached the topic
        finally:
            await r.engine.dispose()

    run(go())
