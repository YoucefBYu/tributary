"""`connection_settings_from_config`: TLS on by default with full verification (`verify-full`), a
client certificate loaded only for `auth.type: client-certificate`, and the two secretRefs
(`auth.secretRef`, `tls.secretRef`) kept doing separate jobs — the same rule `httpParams` enforces for
a plaintext `http://` URL, applied here to the loader's outbound database connection."""

from __future__ import annotations

import asyncio
import ssl
from pathlib import Path

import pytest
from tributary.loaders.postgresql import client as client_module
from tributary.loaders.postgresql.client import (
    PgConnectionSettings,
    TransportError,
    connection_settings_from_config,
    create_pool,
)
from tributary.loaders.postgresql.config import (
    CircuitBreakerSpec,
    PgAuthSpec,
    PgTlsSpec,
    PostgresqlDestinationConfig,
)


class FakeSecretResolver:
    def __init__(self, texts: dict[tuple[str, str], str], files: dict[tuple[str, str], Path] | None = None) -> None:
        self.texts = texts
        self.files = files or {}

    def resolve_text(self, ref: str, key: str) -> str:
        try:
            return self.texts[(ref, key)]
        except KeyError:
            raise FileNotFoundError(f"no {key!r} under {ref!r}") from None

    def resolve_file(self, ref: str, key: str) -> Path:
        try:
            return self.files[(ref, key)]
        except KeyError:
            raise FileNotFoundError(f"no {key!r} under {ref!r}") from None


def make_config(**overrides: object) -> PostgresqlDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "warehouse",
        "host": "db.example.org",
        "port": 5432,
        "database": "clinical",
        "table": "observations",
        "id_column": "tb_msg_id",
        "auth": PgAuthSpec("password", "db-creds"),
        "tls": PgTlsSpec("verify-full", None),
        "timeout_s": 5.0,
        "circuit_breaker": None,
        "idempotency_technique": "sink-native",
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return PostgresqlDestinationConfig(**base)  # type: ignore[arg-type]


def test_password_auth_resolves_user_and_password_and_verifies_by_default() -> None:
    resolver = FakeSecretResolver({("db-creds", "user"): "loader", ("db-creds", "password"): "s3cret"})
    settings = connection_settings_from_config(make_config(), resolver)
    assert settings.user == "loader"
    assert settings.password == "s3cret"  # noqa: S105 - asserting the resolved value, not a literal
    assert isinstance(settings.ssl_context, ssl.SSLContext)
    assert settings.ssl_context.verify_mode == ssl.CERT_REQUIRED


def test_tls_disable_returns_no_ssl_context() -> None:
    resolver = FakeSecretResolver({("db-creds", "user"): "loader", ("db-creds", "password"): "s3cret"})
    config = make_config(host="127.0.0.1", tls=PgTlsSpec("disable", None))
    settings = connection_settings_from_config(config, resolver)
    assert settings.ssl_context is None


def test_tls_secret_ref_loads_an_additional_trust_bundle(tmp_path: Path) -> None:
    ca = tmp_path / "ca.crt"
    ca.write_text("not a real cert", encoding="utf-8")
    resolver = FakeSecretResolver(
        {("db-creds", "user"): "loader", ("db-creds", "password"): "s3cret"},
        {},
    )

    # load_verify_locations will raise on a non-PEM file; this proves the path was actually passed
    # through to the SSLContext rather than that ruff/mypy alone). Real certs are exercised by
    # test_integration_postgresql_loader.py against a real server.
    resolver.files[("db-ca", "ca.crt")] = ca
    config = make_config(tls=PgTlsSpec("verify-full", "db-ca"))
    with pytest.raises(ssl.SSLError):
        connection_settings_from_config(config, resolver)


def test_client_certificate_auth_has_no_password_and_loads_a_cert_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proves the plumbing — `auth.secretRef`'s `tls.crt`/`tls.key` paths reach
    `SSLContext.load_cert_chain` — without needing a real certificate/key pair or a `cryptography`
    dependency this package does not otherwise need. `test_integration_postgresql_loader.py` covers
    `password` auth end to end; a real client-certificate round trip needs a PostgreSQL server
    configured for certificate auth, which the default `testcontainers` image is not."""
    cert = tmp_path / "tls.crt"
    key = tmp_path / "tls.key"
    cert.write_text("placeholder", encoding="utf-8")
    key.write_text("placeholder", encoding="utf-8")

    seen: list[tuple[str, str]] = []

    def fake_load_cert_chain(self: ssl.SSLContext, certfile: str, keyfile: str) -> None:
        seen.append((certfile, keyfile))

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", fake_load_cert_chain)

    resolver = FakeSecretResolver(
        {("db-cert", "user"): "loader"}, {("db-cert", "tls.crt"): cert, ("db-cert", "tls.key"): key}
    )
    config = make_config(auth=PgAuthSpec("client-certificate", "db-cert"))
    settings = connection_settings_from_config(config, resolver)
    assert settings.user == "loader"
    assert settings.password is None
    assert isinstance(settings.ssl_context, ssl.SSLContext)
    assert seen == [(str(cert), str(key))]


def test_circuit_breaker_spec_round_trips_through_config() -> None:
    cfg = make_config(circuit_breaker=CircuitBreakerSpec(failure_threshold=3, open_seconds=15.0))
    assert cfg.circuit_breaker is not None
    assert cfg.circuit_breaker.failure_threshold == 3


def test_create_pool_does_not_hang_forever_against_an_unreachable_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: `create_pool` used to await `asyncpg.create_pool(...)` with no bound at all, so an
    unreachable host (or one that accepts the TCP connection but never finishes the Postgres startup
    handshake) hung loader startup forever. No real network needed to prove the bound: a fake
    `asyncpg.create_pool` that never returns stands in for "unreachable", the same way
    `test_a_dropped_connection_is_retried_without_crashing_the_shared_loop`
    (`test_integration_postgresql_loader.py`) needs a real server to prove the *query*-level timeouts
    but this one does not need a real server to prove the *connect*-level one."""

    async def hangs_forever(**kwargs: object) -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr(client_module.asyncpg, "create_pool", hangs_forever)
    settings = PgConnectionSettings(
        host="192.0.2.1",  # TEST-NET-1 (RFC 5737): guaranteed non-routable, not a real host
        port=5432,
        database="d",
        user="u",
        password="p",  # noqa: S106 - a fake pool never connects; there is no real credential here
        ssl_context=None,
    )

    async def run() -> None:
        await create_pool(settings, min_size=1, max_size=1, command_timeout=1.0, connect_timeout=0.05)

    with pytest.raises(TransportError) as exc_info:
        asyncio.run(run())
    # Value-free (README 3.4.3 / invariant 6): the message names the timeout, never the host or
    # credentials `settings` carries.
    assert "0.05" in str(exc_info.value)
    assert "192.0.2.1" not in str(exc_info.value)
