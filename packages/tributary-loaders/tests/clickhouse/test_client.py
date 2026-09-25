"""`transport_from_config`: TLS on by default with full verification, auth resolved into
`X-ClickHouse-User`/`X-ClickHouse-Key` headers -- never the URL -- and a client certificate loaded only
for `auth.type: client-certificate`. Mirrors `tests/postgresql/test_client.py`."""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.loaders.clickhouse.client import transport_from_config
from tributary.loaders.clickhouse.config import (
    ChAuthSpec,
    ChTlsSpec,
    ClickhouseDestinationConfig,
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


def make_config(**overrides: object) -> ClickhouseDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "adt-analytics",
        "host": "clickhouse.example.org",
        "port": 8443,
        "database": "analytics",
        "table": "adt_events",
        "id_column": "tb_msg_id",
        "auth": ChAuthSpec("password", "ch-creds"),
        "tls": ChTlsSpec("verify-full", None),
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
    return ClickhouseDestinationConfig(**base)  # type: ignore[arg-type]


def test_password_auth_resolves_into_headers_not_the_url() -> None:
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport = transport_from_config(make_config(), resolver)
    assert transport.auth_headers["X-ClickHouse-User"] == "loader"
    assert transport.auth_headers["X-ClickHouse-Key"] == "s3cret"
    assert "s3cret" not in transport.base_url
    assert "loader" not in transport.base_url


def test_base_url_carries_only_the_database_query_param() -> None:
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport = transport_from_config(make_config(), resolver)
    assert transport.base_url == "https://clickhouse.example.org:8443/?database=analytics"


def test_tls_disable_uses_http_scheme(tmp_path: Path) -> None:
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    config = make_config(host="127.0.0.1", tls=ChTlsSpec("disable", None))
    transport = transport_from_config(config, resolver)
    assert transport.base_url.startswith("http://127.0.0.1:8443/")


def test_client_certificate_auth_has_no_key_header(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves the plumbing -- `auth.secretRef`'s `tls.crt`/`tls.key` paths reach
    `SSLContext.load_cert_chain` -- without needing a real certificate/key pair, matching
    `tests/postgresql/test_client.py::test_client_certificate_auth_has_no_password_and_loads_a_cert_chain`."""
    import ssl

    cert = tmp_path / "tls.crt"
    key = tmp_path / "tls.key"
    cert.write_text("placeholder", encoding="utf-8")
    key.write_text("placeholder", encoding="utf-8")

    seen: list[tuple[str, str]] = []

    def fake_load_cert_chain(self: ssl.SSLContext, certfile: str, keyfile: str) -> None:
        seen.append((certfile, keyfile))

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", fake_load_cert_chain)

    resolver = FakeSecretResolver(
        {("ch-cert", "user"): "loader"}, {("ch-cert", "tls.crt"): cert, ("ch-cert", "tls.key"): key}
    )
    config = make_config(auth=ChAuthSpec("client-certificate", "ch-cert"))
    transport = transport_from_config(config, resolver)
    assert transport.auth_headers == {"X-ClickHouse-User": "loader"}
    assert seen == [(str(cert), str(key))]


def test_tls_secret_ref_loads_an_additional_trust_bundle(tmp_path: Path) -> None:
    ca = tmp_path / "ca.crt"
    ca.write_text("not a real cert", encoding="utf-8")
    resolver = FakeSecretResolver(
        {("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"}, {("ch-ca", "ca.crt"): ca}
    )
    config = make_config(tls=ChTlsSpec("verify-full", "ch-ca"))
    # load_verify_locations raises on a non-PEM file -- proves the path was actually passed through
    # to the SSLContext, not just accepted by ruff/mypy. Real certs: test_integration_clickhouse_loader.py.
    import ssl

    with pytest.raises(ssl.SSLError):
        transport_from_config(config, resolver)
