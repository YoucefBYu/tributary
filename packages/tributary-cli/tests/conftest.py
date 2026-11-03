"""Fixtures for the CLI smoke tests: the real API app (FastAPI, real PostgreSQL, a recording channels
publisher) served by uvicorn on a loopback port in a thread, so the CLI makes real HTTP calls to it.

The Compose stack is not used: these run wherever Docker (testcontainers) works, and they test the CLI's
contract with the API, which is the same code the stack runs. The API test helpers are reused, not copied."""

from __future__ import annotations

import asyncio
import socket
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
API_TESTS = Path(__file__).resolve().parents[2] / "tributary-api" / "tests" / "api"
sys.path.insert(0, str(API_TESTS))
sys.path.insert(0, str(HERE))

from api_kit import FakePublisher, Idp, fake_audit, oidc_settings  # noqa: E402
from smoke_kit import LiveApi  # noqa: E402
from testcontainers.community.postgres import PostgresContainer  # noqa: E402
from tributary.api.app import create_app  # noqa: E402
from tributary.api.auth.jwks import HttpsJwksFetcher, KeySet  # noqa: E402
from tributary.api.config import DatabaseSettings, Settings  # noqa: E402
from tributary.api.db.engine import make_engine, make_session_factory  # noqa: E402
from tributary.api.db.migrate import upgrade  # noqa: E402
from tributary.api.db.repository import EnvironmentRepository  # noqa: E402
from tributary.runtime.logs import JsonLogger  # noqa: E402


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver=None) as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def idp(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Idp]:
    server = Idp(tmp_path_factory.mktemp("idp"))
    yield server
    server.close()


@pytest.fixture(scope="session")
def live_api(pg_url: str, idp: Idp) -> Iterator[LiveApi]:
    upgrade(pg_url)
    oidc = oidc_settings(idp)
    keys = KeySet(
        HttpsJwksFetcher(oidc.issuer, jwks_uri=None, timeout_s=oidc.http_timeout_s, ssl_context=idp.client_context()),
        refresh_s=oidc.refresh_s,
        max_stale_s=oidc.max_stale_s,
        timeout_s=oidc.http_timeout_s,
    )
    publisher = FakePublisher()
    app = create_app(
        settings=Settings(database=DatabaseSettings("127.0.0.1", "tb", "u", "ref"), oidc=oidc, audit_home_env="dev"),
        sessions=make_session_factory(make_engine(pg_url)),
        publisher=publisher,
        keys=keys,
        log=JsonLogger("api", level="error"),
        audit=fake_audit(),
    )
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, access_log=False, log_config=None, server_header=False)
    )
    thread = threading.Thread(target=server.run, name="tb-cli-smoke-api", daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{url}/readyz", timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    else:
        server.should_exit = True
        raise RuntimeError("the API under test did not become ready")
    try:
        yield LiveApi(url, idp, publisher, pg_url)
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture
def api(live_api: LiveApi) -> LiveApi:
    """The live API with an empty database holding `dev` (ui-managed) and `prod` (git-managed)."""

    async def reset() -> None:
        engine = make_engine(live_api.pg_url)
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
    live_api.publisher.records.clear()
    live_api.publisher.fail_with = None
    return live_api
