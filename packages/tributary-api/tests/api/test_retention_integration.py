"""`GET /v1/environments/{env}/retention` (W3-32's read-only retention surface): what the migrate step wrote to
`archive_settings`, per store, through the API's readonly stdlib ClickHouse client; contract-checked."""

from __future__ import annotations

import urllib.request
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from api_kit import Contract, Idp, TlsPg, run
from test_audit_integration import Recorder, add_environment, build_with
from test_deploy_kafka_integration import seed
from test_routes_integration import Api
from tributary.api.retention import RetentionReader, parse, retention_sql
from tributary.api.tamper import ClickHouseHttp

pytestmark = pytest.mark.integration

ROWS = (
    '{"table_name":"message_events","eff_days":90,"eff_previous":null,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"0.3.0"}\n'
    '{"table_name":"message_content","eff_days":60,"eff_previous":30,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"0.3.0"}\n'
    '{"table_name":"audit_log","eff_days":2190,"eff_previous":null,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"0.3.0"}\n'
)


def test_parse_maps_tables_to_stores_in_order_and_drops_what_the_contract_cannot_carry() -> None:
    out = parse("prod", "".join(reversed(ROWS.splitlines(keepends=True))))
    assert [s["store"] for s in out["stores"]] == ["events", "content", "audit"]
    assert out["stores"][1] == {
        "store": "content",
        "days": 60,
        "previous_days": 30,
        "applied_at": "2026-10-06T07:00:00.000Z",
        "release": "0.3.0",
    }
    junk = parse(
        "prod",
        '{"table_name":"other","eff_days":5,"eff_previous":null,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"x"}\n'
        '{"table_name":"audit_log","eff_days":99999,"eff_previous":null,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"x"}\n'
        '{"table_name":"message_events","eff_days":7,"eff_previous":null,"eff_applied_at":"2026-10-06 07:00:00.000","eff_release":"bad release!"}\n',
    )
    assert junk["stores"] == [
        {"store": "events", "days": 7, "previous_days": None, "applied_at": "2026-10-06T07:00:00.000Z"}
    ]
    with pytest.raises(ValueError):
        retention_sql(database="a`b")


def test_the_endpoint_serves_the_contract_shape_404s_an_unknown_environment_and_503s_without_the_archive(
    tls_pg: TlsPg, idp: Idp, contract: Contract
) -> None:
    env = f"re{uuid.uuid4().hex[:6]}"
    seed(tls_pg.plain_url, env)
    add_environment(tls_pg.plain_url, "other")
    queries: list[str] = []

    def fake(sql: str) -> str:
        queries.append(sql)
        if "boom" in sql:
            raise RuntimeError
        return ROWS

    reader = RetentionReader(fake, {env: "tb_ret"})
    app, engine = build_with(tls_pg, idp, Recorder(), home=env, retention=reader)

    async def go() -> None:
        async with app.router.lifespan_context(app):
            await app.state.keys.refresh()
            import httpx

            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
                api = Api(c, idp, contract)
                body = await api.call("GET", "/v1/environments/{env}/retention", expect=200, env=env, role="tb-viewer")
                assert body["environment"] == env and [s["days"] for s in body["stores"]] == [90, 60, 2190]
                assert queries and "`tb_ret`.archive_settings" in queries[0]
                await api.call("GET", "/v1/environments/{env}/retention", expect=404, env="nosuch")
                unconfigured = await api.call("GET", "/v1/environments/{env}/retention", expect=503, env="other")
                assert unconfigured["code"] == "ARCHIVE_UNAVAILABLE"
        await engine.dispose()

    run(go())


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[tuple[str, str, str]]:
    from testcontainers.community.clickhouse import ClickHouseContainer

    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as c:
        yield f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8123)}", c.username, c.password


def test_it_returns_what_the_migrate_step_wrote_the_latest_row_per_store(clickhouse: tuple[str, str, str]) -> None:
    import base64

    from tributary.archiver.schema import settings_ddl

    url, user, password = clickhouse
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()

    def admin(sql: str) -> None:
        req = urllib.request.Request(url, data=sql.encode(), method="POST", headers={"Authorization": f"Basic {auth}"})  # noqa: S310
        with urllib.request.urlopen(req, timeout=60):  # noqa: S310
            pass

    db = f"t_{uuid.uuid4().hex[:10]}"
    admin(f"CREATE DATABASE {db}")
    admin(settings_ddl(database=db))
    admin(  # content 30 -> 60 (two rows), events once, audit absent
        f"INSERT INTO {db}.archive_settings (table_name, days, previous_days, release, applied_at) VALUES "  # noqa: S608
        "('message_content', 30, NULL, '0.2.0', '2026-10-01 00:00:00.000'), "
        "('message_content', 60, 30, '0.3.0', '2026-10-06 07:00:00.000'), "
        "('message_events', 90, NULL, '0.3.0', '2026-10-06 07:00:00.000')"
    )
    client = ClickHouseHttp(url, user, password)
    got: dict[str, Any] = run(RetentionReader(client.query, {"prod": db}).read("prod"))
    assert got["stores"] == [
        {
            "store": "events",
            "days": 90,
            "previous_days": None,
            "applied_at": "2026-10-06T07:00:00.000Z",
            "release": "0.3.0",
        },
        {
            "store": "content",
            "days": 60,
            "previous_days": 30,
            "applied_at": "2026-10-06T07:00:00.000Z",
            "release": "0.3.0",
        },
    ]  # a store with no row is absent, as the contract says
