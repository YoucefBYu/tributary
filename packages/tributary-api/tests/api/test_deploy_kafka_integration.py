"""Deploy against real PostgreSQL and a real Kafka broker, through the production wiring (`build_app`:
the runtime's engine-kafka loader and `producer_config`, so `acks=all` and an idempotent producer).

`published` must mean the broker committed the record, and the record must be what channel-spec/v0 section 6
says: key = channel name, value = the stored document's bytes (a tombstone for undeploy).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from api_kit import AUDIENCE, ROLE_MAP, Contract, Idp, TlsPg, run
from confluent_kafka import Consumer
from confluent_kafka.admin import NewTopic
from sqlalchemy import text
from test_routes_integration import Api
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import EnvironmentRepository
from tributary.api.main import build_app
from tributary.runtime.topics import channels_topic

from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import create_topics_with_retry, free_port

pytestmark = pytest.mark.integration

ENV = "dev"


def api_environ(pg: TlsPg, secrets: Path, idp: Idp, bootstrap: str, jwks_file: Path, **extra: str) -> dict[str, str]:
    base = {
        **pg.env(secrets),
        "TB_OIDC_ISSUER": idp.issuer,
        "TB_OIDC_AUDIENCE": AUDIENCE,
        "TB_OIDC_ROLE_MAP": json.dumps(ROLE_MAP),
        "TB_OIDC_JWKS_FILE": str(jwks_file),  # ADR-009 D9 (b): lite profile, loopback bind
        "TB_PROFILE": "lite",
        "TB_API_BIND": "127.0.0.1",
        "PATH": "/usr/bin:/bin",
        **extra,
    }
    return engine_kafka_env(bootstrap, base)


def seed(pg_url: str) -> None:
    upgrade(pg_url)

    async def go() -> None:
        engine = make_engine(pg_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE deployments, channel_versions, channels, environments RESTART IDENTITY CASCADE")
                )
            async with make_session_factory(engine)() as s, s.begin():
                await EnvironmentRepository(s).create(ENV, "ui-managed")
        finally:
            await engine.dispose()

    asyncio.run(go())


def read_topic(bootstrap: str, topic: str) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([topic])
    records: list[Any] = []
    try:
        for _ in range(60):
            record = consumer.poll(1.0)
            if record is None:
                continue
            if record.error() is not None:
                if records:
                    break
                continue
            records.append(record)
    finally:
        consumer.close()
    return records


def test_deploy_rollback_undeploy_publish_to_the_channels_topic(
    tls_pg: TlsPg, kafka: str, idp: Idp, contract: Contract, tmp_path: Path
) -> None:
    seed(tls_pg.plain_url)
    topic = channels_topic(ENV)
    create_topics_with_retry(kafka, [NewTopic(topic, 1, 1, config={"cleanup.policy": "compact"})])
    app = build_app(
        api_environ(tls_pg, tls_pg.secrets_dir(tmp_path), idp, kafka, idp.jwks_file(tmp_path / "jwks.json"))
    )
    name = "adt-passthrough"

    async def go() -> dict[str, Any]:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
                api = Api(c, idp, contract)
                v1 = await api.create(name)
                v2 = await api.add_version(name, retries=4)
                d1 = await api.deploy(ENV, name, 1)
                d2 = await api.deploy(ENV, name, 2, expected_current_version=1)
                rb = await api.rollback(ENV, name, 2)
                und = await api.undeploy(ENV, name, 1)
                ready = await c.get("/readyz")
                assert ready.status_code == 200
                return {"v1": v1, "v2": v2, "d": [d1, d2, rb, und]}

    out = run(go())
    assert [d["status"] for d in out["d"]] == ["published"] * 4  # the broker acknowledged each (acks=all)

    records = read_topic(kafka, topic)
    assert [r.key() for r in records] == [name.encode()] * 4
    values = [r.value() for r in records]
    assert hashlib.sha256(values[0] or b"").hexdigest() == out["v1"]["spec_sha256"]
    assert hashlib.sha256(values[1] or b"").hexdigest() == out["v2"]["spec_sha256"]
    assert values[2] == values[0]  # rollback: the old document, byte for byte
    assert json.loads(values[2] or b"")["metadata"] == {"name": name, "version": 1}
    assert values[3] is None  # undeploy: a tombstone
    assert json.loads(values[1] or b"") == out["v2"]["document"]


def test_unreachable_broker_is_a_failed_record_and_degraded_readiness(
    tls_pg: TlsPg, idp: Idp, contract: Contract, tmp_path: Path
) -> None:
    """Never ACK-and-drop: with no broker the deploy is recorded `failed`, not `published`, and the API says so."""
    seed(tls_pg.plain_url)
    app = build_app(
        api_environ(
            tls_pg,
            tls_pg.secrets_dir(tmp_path),
            idp,
            f"127.0.0.1:{free_port()}",
            idp.jwks_file(tmp_path / "jwks.json"),
            TB_PUBLISH_TIMEOUT_S="1",
        )
    )

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                await api.create("adt-passthrough")
                d = await api.deploy(ENV, "adt-passthrough", 1)
                assert d["status"] == "failed" and d["failure_code"] in ("PUBLISH_TIMEOUT", "PUBLISH_FAILED")
                ready = await c.get("/readyz")
                assert ready.json()["degraded"] == ["channels_publisher_unavailable"]

    run(go())
