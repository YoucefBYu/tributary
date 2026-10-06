"""The routes against real PostgreSQL (testcontainers) with a fake channels publisher; every response is
validated against `contracts/openapi/v0/openapi.json`. Kafka is in `test_deploy_kafka_integration.py`."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from api_kit import REPO, Contract, FakePublisher, Harness, Idp, bearer, run
from sqlalchemy import text
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import EnvironmentRepository
from tributary.api.publisher import PublishError, PublishTimeoutError

pytestmark = pytest.mark.integration

EXAMPLE = REPO / "contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json"
Make = Callable[..., Harness]
GIT = {"kind": "git", "commit": "a" * 40, "path": "channels/adt.yaml"}
API = {"kind": "api"}


def document(name: str = "adt-passthrough", retries: int | None = None) -> dict[str, Any]:
    doc = json.loads(EXAMPLE.read_text())
    doc["metadata"] = {"name": name}
    if retries is not None:
        doc["spec"]["pipeline"]["stepRetries"] = retries
    return doc


@pytest.fixture
def db(pg_url: str) -> str:
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
    return pg_url


class Api:
    def __init__(self, c: httpx.AsyncClient, idp: Idp, contract: Contract) -> None:
        self.c, self.idp, self.contract = c, idp, contract

    async def call(
        self,
        method: str,
        template: str,
        *,
        role: str = "tb-admin",
        expect: int,
        params: dict[str, str] | None = None,
        json_body: Any = None,
        **path: str | int,
    ) -> Any:
        url = template.format(**path)
        r = await self.c.request(
            method, url, json=json_body, headers=bearer(self.idp.mint(roles=[role])), params=params
        )
        assert r.status_code == expect, (method, url, r.status_code, r.text)
        self.contract.check(method, template, r)
        return r.json()

    async def create(self, name: str = "adt-passthrough", origin: dict[str, Any] = API, **kw: Any) -> dict[str, Any]:
        body = {"document": document(name, kw.pop("retries", None)), "origin": origin}
        return await self.call("POST", "/v1/channels", expect=201, json_body=body)  # type: ignore[no-any-return]

    async def add_version(self, name: str, retries: int, origin: dict[str, Any] = API) -> dict[str, Any]:
        body = {"document": document(name, retries), "origin": origin}
        return await self.call("POST", "/v1/channels/{channel}/versions", expect=201, json_body=body, channel=name)  # type: ignore[no-any-return]

    async def deploy(self, env: str, name: str, version: int, *, expect: int = 202, **extra: Any) -> dict[str, Any]:
        return await self.call(  # type: ignore[no-any-return]
            "POST",
            "/v1/environments/{env}/channels/{channel}/deploy",
            expect=expect,
            env=env,
            channel=name,
            json_body={"version": version, **extra},
            role="tb-operator",
        )

    async def rollback(self, env: str, name: str, expected: int, *, expect: int = 202, **extra: Any) -> dict[str, Any]:
        return await self.call(  # type: ignore[no-any-return]
            "POST",
            "/v1/environments/{env}/channels/{channel}/rollback",
            expect=expect,
            env=env,
            channel=name,
            json_body={"expected_current_version": expected, **extra},
            role="tb-operator",
        )

    async def undeploy(self, env: str, name: str, expected: int, *, expect: int = 202) -> dict[str, Any]:
        return await self.call(  # type: ignore[no-any-return]
            "POST",
            "/v1/environments/{env}/channels/{channel}/undeploy",
            expect=expect,
            env=env,
            channel=name,
            json_body={"expected_current_version": expected},
            role="tb-operator",
        )


def scenario(
    make: Make, idp: Idp, contract: Contract, db: str, body: Callable[[Api, FakePublisher], Any], **kw: Any
) -> None:
    async def go() -> None:
        h = make(database_url=db, **kw)
        async with h.running() as c:
            await body(Api(c, idp, contract), h.publisher)

    run(go())


def test_channel_and_version_lifecycle_validates_against_the_contract(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        v1 = await api.create()
        assert v1["version"] == 1 and v1["document"]["metadata"]["version"] == 1
        stored = json.dumps(v1["document"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        assert v1["spec_sha256"] == hashlib.sha256(stored).hexdigest()  # the hash of the stored (published) bytes
        assert v1["bundle"].endswith(v1["document"]["spec"]["pipeline"]["bundle"].split("@")[1])
        assert v1["created_by"] == "user-1"
        await api.call("POST", "/v1/channels", expect=409, json_body={"document": document(), "origin": API})
        v2 = await api.add_version("adt-passthrough", retries=4)
        assert v2["version"] == 2 and v2["document"]["spec"]["pipeline"]["stepRetries"] == 4
        await api.create("second-channel")

        page = await api.call("GET", "/v1/channels", expect=200, params={"limit": "1"})
        assert [i["name"] for i in page["items"]] == ["adt-passthrough"] and page["items"][0]["latest_version"] == 2
        rest = await api.call("GET", "/v1/channels", expect=200, params={"limit": "1", "cursor": page["next_cursor"]})
        assert [i["name"] for i in rest["items"]] == ["second-channel"] and rest["next_cursor"] is None

        versions = await api.call(
            "GET", "/v1/channels/{channel}/versions", expect=200, channel="adt-passthrough", params={"limit": "1"}
        )
        assert [v["version"] for v in versions["items"]] == [2] and versions["next_cursor"]
        assert "document" not in versions["items"][0]
        older = await api.call(
            "GET",
            "/v1/channels/{channel}/versions",
            expect=200,
            channel="adt-passthrough",
            params={"cursor": versions["next_cursor"]},
        )
        assert [v["version"] for v in older["items"]] == [1]

        detail = await api.call(
            "GET", "/v1/channels/{channel}", expect=200, channel="adt-passthrough", role="tb-viewer"
        )
        assert detail["latest_version"] == 2 and detail["deployments"] == []
        one = await api.call(
            "GET",
            "/v1/channels/{channel}/versions/{version}",
            expect=200,
            channel="adt-passthrough",
            version=1,
            role="tb-operator",
        )
        assert one["document"] == v1["document"]
        await api.call("GET", "/v1/channels/{channel}", expect=404, channel="missing")
        await api.call(
            "GET", "/v1/channels/{channel}/versions/{version}", expect=404, channel="adt-passthrough", version=9
        )
        envs = await api.call("GET", "/v1/environments", expect=200, role="tb-auditor")
        assert {e["name"]: (e["management_mode"], e["profile"]) for e in envs["items"]} == {
            "dev": ("ui-managed", "production"),
            "prod": ("git-managed", "production"),
        }
        assert pub.records == []  # nothing is published until a deploy

    scenario(make_harness, idp, contract, db, body)


def test_create_refusals_and_role_checks(make_harness: Make, idp: Idp, contract: Contract, db: str) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        good = {"document": document(), "origin": API}
        with_version = copy.deepcopy(good)
        with_version["document"]["metadata"]["version"] = 7
        r = await api.call("POST", "/v1/channels", expect=422, json_body=with_version)
        assert r["code"] == "VERSION_SET_BY_CLIENT"
        broken = copy.deepcopy(good)
        broken["document"]["spec"]["pipeline"]["bundle"] = "img:latest"  # a tag, not a digest
        r = await api.call("POST", "/v1/channels", expect=422, json_body=broken)
        assert r["code"] == "SPEC_INVALID" and r["errors"] and "img:latest" not in json.dumps(r)
        assert {"json_path": "$.spec.pipeline.bundle", "validator": "pattern"} in r["errors"]
        await api.call(
            "POST", "/v1/channels", expect=400, json_body={"document": document(), "origin": {"kind": "git"}}
        )
        await api.call("POST", "/v1/channels", expect=400, json_body={**good, "extra": 1})
        await api.call("POST", "/v1/channels", expect=400, json_body={"document": [], "origin": API})
        await api.create()
        mismatch = {"document": document("other-name"), "origin": API}
        r = await api.call(
            "POST", "/v1/channels/{channel}/versions", expect=422, json_body=mismatch, channel="adt-passthrough"
        )
        assert r["code"] == "CHANNEL_NAME_MISMATCH"
        await api.call(
            "POST",
            "/v1/channels/{channel}/versions",
            expect=404,
            json_body={"document": document("ghost"), "origin": API},
            channel="ghost",
        )
        for role, template, method, expect in (
            ("tb-viewer", "/v1/channels", "POST", 403),
            ("tb-operator", "/v1/channels", "POST", 403),
            ("tb-auditor", "/v1/channels", "POST", 403),
            ("tb-viewer", "/v1/channels/{channel}/versions/{version}", "GET", 403),
            ("tb-developer", "/v1/environments/{env}/channels/{channel}/deploy", "POST", 403),
            ("tb-viewer", "/v1/environments/{env}/channels/{channel}/undeploy", "POST", 403),
        ):
            await api.call(
                method,
                template,
                role=role,
                expect=expect,
                json_body={},
                channel="adt-passthrough",
                version=1,
                env="dev",
            )
        assert pub.records == []

    scenario(make_harness, idp, contract, db, body)


def test_deploy_rollback_undeploy_with_optimistic_concurrency(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        name = "adt-passthrough"
        v1 = await api.create()
        v2 = await api.add_version(name, retries=4)
        await api.call(
            "GET", "/v1/environments/{env}/channels/{channel}/deployments", expect=200, env="dev", channel=name
        )

        d1 = await api.deploy("dev", name, 1, expected_current_version=None)
        assert (d1["kind"], d1["version"], d1["previous_version"], d1["status"]) == ("deploy", 1, None, "published")
        assert d1["requested_by"] == "user-1" and "published_at" in d1
        r = await api.deploy("dev", name, 2, expect=409, expected_current_version=None)  # something is deployed
        assert r["code"] == "CURRENT_VERSION_MISMATCH"
        d2 = await api.deploy("dev", name, 2, expected_current_version=1)
        assert d2["previous_version"] == 1
        r = await api.deploy("dev", name, 1, expect=409, expected_current_version=1)  # no longer current
        assert r["code"] == "CURRENT_VERSION_MISMATCH"
        r = await api.deploy("dev", name, 9, expect=404)

        # published records: key = name, value = the stored document's bytes, exactly
        assert [(e, ch) for e, ch, _ in pub.records] == [("dev", name), ("dev", name)]
        assert json.loads(pub.records[0][2] or b"") == v1["document"]
        assert hashlib.sha256(pub.records[0][2] or b"").hexdigest() == v1["spec_sha256"]
        assert hashlib.sha256(pub.records[1][2] or b"").hexdigest() == v2["spec_sha256"]

        r = await api.rollback("dev", name, 1, expect=409)
        assert r["code"] == "CURRENT_VERSION_MISMATCH"
        rb = await api.rollback("dev", name, 2)
        assert (rb["kind"], rb["version"], rb["previous_version"], rb["status"]) == ("rollback", 1, 2, "published")
        assert pub.records[-1][2] == pub.records[0][2]  # the old document republished unchanged, version 1 included
        assert json.loads(pub.records[-1][2] or b"")["metadata"]["version"] == 1
        r = await api.rollback("dev", name, 1, expect=422, to_version=1)  # the target equals the current version
        assert r["code"] == "REQUEST_INVALID"
        bounce = await api.rollback("dev", name, 1)  # default: the version published before the current one
        assert bounce["version"] == 2 and bounce["previous_version"] == 1
        explicit = await api.rollback("dev", name, 2, to_version=1)  # an explicit target
        assert explicit["version"] == 1
        await api.rollback("dev", name, 1, to_version=2)
        solo = "solo-channel"
        await api.create(solo)
        await api.deploy("dev", solo, 1)
        assert (await api.rollback("dev", solo, 1, expect=409))["code"] == "NOTHING_TO_ROLL_BACK"

        und = await api.undeploy("dev", name, 2)
        assert (und["kind"], und["version"], und["previous_version"], und["status"]) == (
            "undeploy",
            None,
            2,
            "published",
        )
        assert pub.records[-1] == ("dev", name, None)  # a tombstone
        assert (await api.undeploy("dev", name, 2, expect=409))["code"] == "NOT_DEPLOYED"
        assert (await api.rollback("dev", name, 2, expect=409))["code"] == "NOT_DEPLOYED"
        redo = await api.deploy("dev", name, 2, expected_current_version=None)  # null means "not deployed"
        assert redo["previous_version"] is None

        history = await api.call(
            "GET",
            "/v1/environments/{env}/channels/{channel}/deployments",
            expect=200,
            env="dev",
            channel=name,
            params={"limit": "2"},
        )
        assert [d["kind"] for d in history["items"]] == ["deploy", "undeploy"] and history["next_cursor"]
        rest = await api.call(
            "GET",
            "/v1/environments/{env}/channels/{channel}/deployments",
            expect=200,
            env="dev",
            channel=name,
            params={"cursor": history["next_cursor"], "limit": "50"},
        )
        assert [d["kind"] for d in rest["items"]] == ["rollback"] * 4 + ["deploy", "deploy"] and rest[
            "next_cursor"
        ] is None
        current = await api.call("GET", "/v1/environments/{env}/deployments", expect=200, env="dev", role="tb-viewer")
        assert [(d["channel"], d["version"], d["kind"]) for d in current["items"]] == [
            (name, 2, "deploy"),
            (solo, 1, "deploy"),
        ]
        detail = await api.call("GET", "/v1/channels/{channel}", expect=200, channel=name)
        assert detail["deployments"] == [{"environment": "dev", "version": 2, "status": "published"}]
        assert (await api.call("GET", "/v1/environments/{env}/deployments", expect=404, env="nope"))[
            "code"
        ] == "NOT_FOUND"

    scenario(make_harness, idp, contract, db, body)


def test_git_managed_environment_accepts_only_git_origin_versions(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        name = "adt-passthrough"
        await api.create(origin=API)  # v1 from the API
        await api.add_version(name, retries=3, origin=GIT)  # v2 from git
        r = await api.deploy("prod", name, 1, expect=409)
        assert r["code"] == "MANAGEMENT_MODE" and pub.records == []
        d = await api.deploy("prod", name, 2)
        assert d["status"] == "published"
        r = await api.rollback("prod", name, 2, expect=409, to_version=1)  # the rollback target must be git too
        assert r["code"] == "MANAGEMENT_MODE"
        und = await api.undeploy("prod", name, 2)  # operational actions stay allowed
        assert und["status"] == "published"
        ui = await api.deploy("dev", name, 1)  # ui-managed takes any origin
        assert ui["status"] == "published"

    scenario(make_harness, idp, contract, db, body)


def test_publish_failure_is_a_failed_record_not_an_error_and_never_current(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        name = "adt-passthrough"
        await api.create()
        await api.add_version(name, retries=4)
        await api.deploy("dev", name, 1)
        pub.fail_with = PublishError
        failed = await api.deploy("dev", name, 2)
        assert (failed["status"], failed["failure_code"]) == (
            "failed",
            "PUBLISH_FAILED",
        ) and "published_at" not in failed
        pub.fail_with = PublishTimeoutError
        timed_out = await api.deploy("dev", name, 2)
        assert (timed_out["status"], timed_out["failure_code"]) == ("failed", "PUBLISH_TIMEOUT")
        ready = await api.c.get("/readyz")
        assert ready.json()["degraded"] == ["channels_publisher_unavailable"]
        contract.check("GET", "/readyz", ready)
        # the failed attempts are not "current": the guard still sees v1, and the failure shows on the channel
        pub.fail_with = None
        detail = await api.call("GET", "/v1/channels/{channel}", expect=200, channel=name)
        assert detail["deployments"] == [{"environment": "dev", "version": 2, "status": "failed"}]
        repaired = await api.deploy("dev", name, 2, expected_current_version=1)  # re-deploy is the repair
        assert repaired["status"] == "published" and repaired["previous_version"] == 1
        ready = await api.c.get("/readyz")
        assert ready.status_code == 200 and "degraded" not in ready.json()
        contract.check("GET", "/readyz", ready)

    scenario(make_harness, idp, contract, db, body)


def test_concurrent_deploys_with_the_same_guard_admit_exactly_one(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        name = "adt-passthrough"
        await api.create()
        await api.add_version(name, retries=4)
        results = await asyncio.gather(
            *(
                api.c.post(
                    f"/v1/environments/dev/channels/{name}/deploy",
                    json={"version": 1 + i % 2, "expected_current_version": None},
                    headers=bearer(idp.mint(roles=["tb-operator"])),
                )
                for i in range(6)
            )
        )
        assert sorted(r.status_code for r in results) == [202, 409, 409, 409, 409, 409]
        assert len(pub.records) == 1

    scenario(make_harness, idp, contract, db, body)


def test_a_blocked_publish_serialises_the_next_request_in_order(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    """The channel row lock is held across the publish, so a second request cannot publish first and the
    topic's last record is the newest request (compaction keeps the last)."""

    async def body(api: Api, pub: FakePublisher) -> None:
        name = "adt-passthrough"
        await api.create()
        await api.add_version(name, retries=4)
        pub.gate = asyncio.Event()
        h = bearer(idp.mint(roles=["tb-operator"]))
        first = asyncio.create_task(
            api.c.post(f"/v1/environments/dev/channels/{name}/deploy", json={"version": 1}, headers=h)
        )
        await asyncio.sleep(0.5)
        second = asyncio.create_task(
            api.c.post(f"/v1/environments/dev/channels/{name}/deploy", json={"version": 2}, headers=h)
        )
        await asyncio.sleep(0.5)
        assert not first.done() and not second.done()  # the first waits on the broker, the second on the lock
        pub.gate.set()
        r1, r2 = await asyncio.gather(first, second)
        assert (r1.status_code, r2.status_code) == (202, 202)
        assert [json.loads(v or b"")["metadata"]["version"] for _, _, v in pub.records] == [1, 2]
        assert r2.json()["status"] == "published"

    scenario(make_harness, idp, contract, db, body)


def test_readyz_is_ready_with_the_database_up(make_harness: Make, idp: Idp, contract: Contract, db: str) -> None:
    async def body(api: Api, pub: FakePublisher) -> None:
        r = await api.c.get("/readyz")
        assert r.status_code == 200
        assert r.json()["checks"] == [{"name": "database", "ok": True}, {"name": "oidc_keys", "ok": True}]
        contract.check("GET", "/readyz", r)

    scenario(make_harness, idp, contract, db, body)


def test_deploys_in_flight_are_capped_and_the_excess_is_503(
    make_harness: Make, idp: Idp, contract: Contract, db: str
) -> None:
    """Each deploy holds a pooled connection across its publish: a fifth request beyond a cap of two waits
    briefly, then gets a fixed 503 (UNAVAILABLE) instead of exhausting the pool."""

    async def go() -> None:
        h = make_harness(database_url=db, max_inflight=2, inflight_wait_s=0.3)
        async with h.running() as c:
            api = Api(c, idp, contract)
            for name in ("one-ch", "two-ch", "three-ch"):
                await api.create(name)
            h.publisher.gate = asyncio.Event()
            hdr = bearer(idp.mint(roles=["tb-operator"]))
            tasks = [
                asyncio.create_task(
                    c.post(f"/v1/environments/dev/channels/{n}/deploy", json={"version": 1}, headers=hdr)
                )
                for n in ("one-ch", "two-ch")
            ]
            await asyncio.sleep(0.6)  # both are now blocked on the broker
            refused = await c.post("/v1/environments/dev/channels/three-ch/deploy", json={"version": 1}, headers=hdr)
            assert (refused.status_code, refused.json()["code"]) == (503, "UNAVAILABLE")
            contract.check("POST", "/v1/environments/{env}/channels/{channel}/deploy", refused)
            h.publisher.gate.set()
            done = await asyncio.gather(*tasks)
            assert [r.status_code for r in done] == [202, 202]
            ok = await c.post("/v1/environments/dev/channels/three-ch/deploy", json={"version": 1}, headers=hdr)
            assert ok.status_code == 202  # the slot is free again

    asyncio.run(go())
