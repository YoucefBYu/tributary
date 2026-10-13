"""W3-32 on real Kafka and PostgreSQL: one audit record per audited action in the `audit/v0` shape and no
payload bytes; `requested` fails closed with Kafka down (nothing is published); a retry after an unknown outcome
lands the same `audit_id` and bytes twice, which `audit_log` collapses."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from api_kit import Contract, FakePublisher, Idp, TlsPg, oidc_settings, run
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from test_deploy_kafka_integration import api_environ, read_topic, seed
from test_routes_integration import Api, document
from tributary.api.audit import Actor, AuditHook, AuditSendError, KafkaAuditSender, _validator
from tributary.api.main import build_app
from tributary.runtime.clients import KafkaSettings, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import platform_topic

from tests.support.kafka_helpers import create_topics_with_retry, free_port

pytestmark = pytest.mark.integration


def test_each_deploy_action_lands_exactly_one_valid_record_with_no_payload(
    tls_pg: TlsPg, kafka: str, idp: Idp, contract: Contract, tmp_path: Path
) -> None:
    env = f"au{uuid.uuid4().hex[:6]}"
    seed(tls_pg.plain_url, env)
    topic = platform_topic(env, "audit")
    create_topics_with_retry(kafka, [NewTopic(topic, 1, 1)])
    create_topics_with_retry(kafka, [NewTopic(f"_tb.{env}.channels", 1, 1, config={"cleanup.policy": "compact"})])
    app = build_app(
        api_environ(tls_pg, tls_pg.secrets_dir(tmp_path), idp, kafka, idp.jwks_file(tmp_path / "jwks.json"))
    )

    async def go() -> list[dict[str, Any]]:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                await api.create("adt-audit")
                await api.add_version("adt-audit", retries=4)
                d1 = await api.deploy(env, "adt-audit", 1)
                d2 = await api.deploy(env, "adt-audit", 2, expected_current_version=1)
                rb = await api.rollback(env, "adt-audit", 2)
                und = await api.undeploy(env, "adt-audit", 1)
                return [d1, d2, rb, und]

    deployments = run(go())
    records = [r for r in read_topic(kafka, topic) if json.loads(r.value()).get("channel") == "adt-audit"]
    created = [json.loads(r.value()) for r in records if json.loads(r.value())["action"] == "channel.version.create"]
    assert [(d["environment"], d["version"]) for d in created] == [(env, 1), (env, 2)]  # to the audit home
    records = [r for r in records if json.loads(r.value())["action"].startswith("deployment.")]
    docs = [json.loads(r.value()) for r in records]
    assert [(d["action"], d["phase"]) for d in docs] == [
        ("deployment.deploy", "requested"),
        ("deployment.deploy", "requested"),
        ("deployment.rollback", "requested"),
        ("deployment.undeploy", "requested"),
    ]
    assert [d["deployment_id"] for d in docs] == [d["id"] for d in deployments]  # one per action, joined by id
    assert len({d["audit_id"] for d in docs}) == 4
    assert [d.get("version") for d in docs] == [1, 2, 1, None]
    assert docs[1]["previous_version"] == 1 and "previous_version" not in docs[0]
    for r, d in zip(records, docs, strict=True):
        assert _validator().is_valid(d)
        assert r.key() == d["actor"].encode()  # keyed by actor (README 1)
        assert d["actor_roles"] in (["admin"], ["operator"]) and d["environment"] == env and d["audit_id"][14] == "7"
        assert set(d) <= set(_validator().schema["properties"])
    raw = b"".join(r.value() for r in records)
    assert b"Authorization" not in raw and b"Bearer" not in raw and b"spec" not in raw


def test_requested_fails_closed_when_kafka_is_down_and_nothing_is_published(
    tls_pg: TlsPg, idp: Idp, contract: Contract
) -> None:
    env = f"au{uuid.uuid4().hex[:6]}"
    seed(tls_pg.plain_url, env)
    conf = producer_config(
        KafkaSettings(bootstrap_servers=f"127.0.0.1:{free_port()}"), load_topic_defaults(), client_id="tb-test-audit"
    )
    conf["delivery.timeout.ms"] = 1000
    dead = KafkaAuditSender(Producer(conf), timeout_s=1.0)
    recorder = Recorder()

    class Switch:
        down = False

        async def send(self, env: str, key: bytes, value: bytes) -> None:
            await (dead if self.down else recorder).send(env, key, value)

    switch = Switch()
    app, engine = build_with(tls_pg, idp, switch)

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                await api.create("adt-audit")  # Kafka up
                switch.down = True
                body = await api.call(
                    "POST",
                    "/v1/environments/{env}/channels/{channel}/deploy",
                    expect=503,
                    json_body={"version": 1},
                    env=env,
                    channel="adt-audit",
                    role="tb-operator",
                )
                assert body["code"] == "AUDIT_UNAVAILABLE"
                history = await api.call(
                    "GET",
                    "/v1/environments/{env}/channels/{channel}/deployments",
                    expect=200,
                    env=env,
                    channel="adt-audit",
                )
                assert history["items"] == []  # refused before anything was stored: nothing superseded or stranded
        await engine.dispose()

    run(go())
    assert app.state.fake_publisher.records == []  # the unaudited deploy did not happen


def test_a_retry_after_an_unknown_outcome_lands_the_same_record_twice_on_real_kafka(kafka: str) -> None:
    env = f"au{uuid.uuid4().hex[:6]}"
    topic = platform_topic(env, "audit")
    create_topics_with_retry(kafka, [NewTopic(topic, 1, 1)])
    conf = producer_config(KafkaSettings(bootstrap_servers=kafka), load_topic_defaults(), client_id="tb-test-audit")
    real = KafkaAuditSender(Producer(conf), timeout_s=10.0)

    class DeliversThenReportsFailure:
        calls = 0

        async def send(self, env: str, key: bytes, value: bytes) -> None:
            await real.send(env, key, value)
            self.calls += 1
            if self.calls == 1:
                raise AuditSendError  # the broker has it; the API does not know

    sender = DeliversThenReportsFailure()
    actor = Actor("idp|1", frozenset({"operator"}))
    audit_id = run(
        AuditHook(sender, JsonLogger("api", level="error"), retry_pause_s=0).requested(  # type: ignore[arg-type]
            action="message.content.view",
            environment=env,
            actor=actor,
            channel="adt",
            message_id="0191a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5b",
            stage="received",
        )
    )
    records = read_topic(kafka, topic)
    assert len(records) == 2 and records[0].value() == records[1].value()
    assert json.loads(records[0].value())["audit_id"] == audit_id  # the archive collapses these on audit_id


# -- channel.version.create audits to the audit home environment ----------------------------------------


class Recorder:
    """An `AuditSender` that records instead of producing."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send(self, env: str, key: bytes, value: bytes) -> None:
        self.sent.append((env, key, value))


def build_with(
    tls_pg: TlsPg,
    idp: Idp,
    sender: Any,
    *,
    home: str | None = None,
    retention: Any = None,
    archive: Any = None,
    log_level: str = "error",
) -> tuple[Any, Any]:
    from tributary.api.app import create_app
    from tributary.api.auth.jwks import HttpsJwksFetcher, KeySet
    from tributary.api.config import DatabaseSettings, Settings
    from tributary.api.db.engine import make_engine, make_session_factory

    oidc = oidc_settings(idp)
    keys = KeySet(
        HttpsJwksFetcher(oidc.issuer, jwks_uri=None, timeout_s=oidc.http_timeout_s, ssl_context=idp.client_context()),
        refresh_s=oidc.refresh_s,
        max_stale_s=oidc.max_stale_s,
        timeout_s=oidc.http_timeout_s,
    )
    log = JsonLogger("api", level=log_level)  # type: ignore[arg-type]
    publisher = FakePublisher()
    engine = make_engine(tls_pg.plain_url)
    app = create_app(
        settings=Settings(database=DatabaseSettings("127.0.0.1", "tb", "u", "ref"), oidc=oidc, audit_home_env=home),
        sessions=make_session_factory(engine),
        publisher=publisher,
        keys=keys,
        log=log,
        audit=AuditHook(sender, log, retry_pause_s=0),
        retention=retention,
        archive=archive,
    )
    app.state.fake_publisher = publisher
    return app, engine


def add_environment(pg_url: str, name: str) -> None:
    import asyncio

    from tributary.api.db.engine import make_engine, make_session_factory
    from tributary.api.db.repository import EnvironmentRepository

    async def go() -> None:
        engine = make_engine(pg_url)
        try:
            async with make_session_factory(engine)() as s, s.begin():
                await EnvironmentRepository(s).create(name, "ui-managed")
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_with_one_environment_it_is_the_audit_home_and_each_version_write_is_one_record(
    tls_pg: TlsPg, idp: Idp, contract: Contract
) -> None:
    env = f"au{uuid.uuid4().hex[:6]}"
    seed(tls_pg.plain_url, env)  # exactly one environment
    sender = Recorder()
    app, engine = build_with(tls_pg, idp, sender)

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                await api.create("adt-home")
                await api.add_version("adt-home", retries=4)
        await engine.dispose()

    run(go())
    docs = [json.loads(v) for _, _, v in sender.sent]
    assert [
        (e, d["environment"], d["action"], d["phase"], d["channel"], d["version"])
        for (e, _, _), d in zip(sender.sent, docs, strict=True)
    ] == [
        (env, env, "channel.version.create", "requested", "adt-home", 1),
        (env, env, "channel.version.create", "requested", "adt-home", 2),
    ]
    assert all(_validator().is_valid(d) for d in docs)


def test_with_several_environments_startup_is_refused_unless_a_home_is_set(
    tls_pg: TlsPg, idp: Idp, contract: Contract
) -> None:
    env = f"au{uuid.uuid4().hex[:6]}"
    from tributary.api.errors import ConfigurationError

    seed(tls_pg.plain_url, env)
    add_environment(tls_pg.plain_url, "prod")
    app, engine = build_with(tls_pg, idp, Recorder())

    async def refused() -> None:
        with pytest.raises(ConfigurationError) as e:
            async with app.router.lifespan_context(app):
                pass
        assert e.value.code == "AUDIT_HOME_ENV_REQUIRED" and app.state.startup_error is e.value
        await engine.dispose()

    run(refused())

    sender = Recorder()
    app, engine = build_with(tls_pg, idp, sender, home="prod")

    async def with_home() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                await Api(c, idp, contract).create("adt-home")
        await engine.dispose()

    run(with_home())
    assert [e for e, _, _ in sender.sent] == ["prod"]  # the home, not the first environment

    app, engine = build_with(tls_pg, idp, Recorder(), home="nowhere")

    async def unknown() -> None:
        with pytest.raises(ConfigurationError) as e:
            async with app.router.lifespan_context(app):
                pass
        assert e.value.code == "AUDIT_HOME_ENV_UNKNOWN"
        await engine.dispose()

    run(unknown())


def test_a_version_write_fails_closed_and_stores_nothing_when_the_home_topic_is_down(
    tls_pg: TlsPg, idp: Idp, contract: Contract
) -> None:
    seed(tls_pg.plain_url)
    conf = producer_config(
        KafkaSettings(bootstrap_servers=f"127.0.0.1:{free_port()}"), load_topic_defaults(), client_id="tb-test-audit"
    )
    conf["delivery.timeout.ms"] = 1000
    app, engine = build_with(tls_pg, idp, KafkaAuditSender(Producer(conf), timeout_s=1.0))

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                body = await api.call(
                    "POST",
                    "/v1/channels",
                    expect=503,
                    json_body={"document": document("adt-home"), "origin": {"kind": "api"}},
                )
                assert body["code"] == "AUDIT_UNAVAILABLE"
                await api.call("GET", "/v1/channels/{channel}", expect=404, channel="adt-home")  # rolled back
        await engine.dispose()

    run(go())
