"""Fixtures for the API tests. Helpers live in `api_kit.py` (the suite runs in importlib mode, which does
not put this directory on sys.path, so it is added here)."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from api_kit import Contract, FakePublisher, Harness, Idp, TlsPg, oidc_settings
from tributary.api.app import create_app
from tributary.api.auth.jwks import HttpsJwksFetcher, KeySet
from tributary.api.config import DatabaseSettings, OidcSettings, Settings
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.runtime.logs import JsonLogger


@pytest.fixture(scope="session")
def contract() -> Contract:
    return Contract()


@pytest.fixture(scope="session")
def idp(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Idp]:
    server = Idp(tmp_path_factory.mktemp("idp"))
    yield server
    server.close()


@pytest.fixture
def make_harness(idp: Idp) -> Callable[..., Harness]:
    def build(
        *,
        database_url: str | None = None,
        oidc: OidcSettings | None = None,
        publisher: FakePublisher | None = None,
        clock: Callable[[], float] = time.monotonic,
        max_inflight: int = 4,
        inflight_wait_s: float = 5.0,
    ) -> Harness:
        oidc_cfg = oidc or oidc_settings(idp)
        settings = Settings(database=DatabaseSettings("127.0.0.1", "tb", "u", "ref"), oidc=oidc_cfg)
        keys = KeySet(
            HttpsJwksFetcher(
                oidc_cfg.issuer, jwks_uri=None, timeout_s=oidc_cfg.http_timeout_s, ssl_context=idp.client_context()
            ),
            refresh_s=oidc_cfg.refresh_s,
            max_stale_s=oidc_cfg.max_stale_s,
            timeout_s=oidc_cfg.http_timeout_s,
            clock=clock,
        )
        pub = publisher or FakePublisher()
        engine = make_engine(database_url or "postgresql://u:p@127.0.0.1:1/none")
        app = create_app(
            settings=settings,
            sessions=make_session_factory(engine),
            publisher=pub,
            keys=keys,
            log=JsonLogger("api", level="error"),
            max_inflight_deploys=max_inflight,
            inflight_wait_s=inflight_wait_s,
        )
        return Harness(app, keys, pub)

    return build


@pytest.fixture(scope="session")
def kafka() -> Iterator[str]:
    """A single-node KRaft broker on loopback (PLAINTEXT is allowed there, engine-kafka/v0 section 4)."""
    from testcontainers.core.container import DockerContainer

    from tests.support.kafka_helpers import await_broker, free_port

    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "api-it-cluster")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="session")
def tls_pg(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TlsPg]:
    pg = TlsPg(tmp_path_factory.mktemp("tlspg"))
    try:
        yield pg
    finally:
        pg.stop()
