"""`transport_from_config`: TLS on by default with full verification, auth resolved into
`X-ClickHouse-User`/`X-ClickHouse-Key` headers -- never the URL -- and a client certificate loaded only
for `auth.type: client-certificate`. Mirrors `tests/postgresql/test_client.py`."""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.loaders.clickhouse.client import TransportTimeout, transport_from_config
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


def test_base_url_carries_the_database_and_wait_end_of_query_params() -> None:
    """`wait_end_of_query=1` (code review, Critical): without it ClickHouse can send `200` before the
    query has actually finished, which is why `loader.py` also scans every response body regardless of
    status -- this only asserts the request side of that fix."""
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport = transport_from_config(make_config(), resolver)
    assert transport.base_url == "https://clickhouse.example.org:8443/?database=analytics&wait_end_of_query=1"


def test_opener_never_reads_an_environment_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Code review, High: `build_opener` otherwise installs a `ProxyHandler` that reads `HTTP_PROXY`/
    `HTTPS_PROXY`, which would send this loader's credentialed, PHI-carrying requests through an
    operator's outbound-internet proxy instead of straight to the destination. Passing `ProxyHandler({})`
    explicitly means no `<scheme>_open` proxy method is ever installed at all (an empty `proxies` dict
    registers nothing -- confirmed against `urllib.request.OpenerDirector.add_handler`), which is the
    strongest form of "never reads the environment": there is no proxy handler on the opener to ask."""
    monkeypatch.setenv("HTTP_PROXY", "http://attacker.example:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://attacker.example:8080")
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport = transport_from_config(make_config(), resolver)
    assert not any(getattr(h, "proxies", None) for h in transport.opener.handlers)


def test_transport_repr_never_prints_the_auth_headers() -> None:
    """Code review, Medium: the default dataclass `repr` would otherwise print `X-ClickHouse-Key`
    (a password) verbatim, e.g. from an uncaught exception's traceback locals."""
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport = transport_from_config(make_config(), resolver)
    assert "s3cret" not in repr(transport)
    assert "loader" not in repr(transport)


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


def test_tls_secret_ref_pins_trust_with_a_bare_ssl_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Code review, Low: `tls.secretRef` is documented as pinning trust to that one bundle, built from
    a fresh `ssl.SSLContext(PROTOCOL_TLS_CLIENT)` -- never `create_default_context()` (which would
    trust the OS store *in addition to* the named bundle)."""
    import ssl

    ca = tmp_path / "ca.crt"
    ca.write_text("not a real cert", encoding="utf-8")

    def fake_load_verify_locations(self: ssl.SSLContext, *, cafile: str) -> None:
        pass  # skip the real PEM parse; this test only cares which constructor path was used

    monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", fake_load_verify_locations)
    monkeypatch.setattr(ssl, "create_default_context", lambda: pytest.fail("must not use the OS trust store"))

    resolver = FakeSecretResolver(
        {("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"}, {("ch-ca", "ca.crt"): ca}
    )
    config = make_config(tls=ChTlsSpec("verify-full", "ch-ca"))
    transport_from_config(config, resolver)  # must not raise / must not call create_default_context


def test_no_tls_secret_ref_uses_the_stdlib_default_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """The complement of the pinning test above: no `tls.secretRef` means the OS trust store, via
    `ssl.create_default_context()` -- `http/client.py`'s existing behaviour for its one, global context."""
    import ssl

    calls = {"default": 0}
    real_default_context = ssl.create_default_context

    def spy_default_context() -> ssl.SSLContext:
        calls["default"] += 1
        return real_default_context()

    monkeypatch.setattr(ssl, "create_default_context", spy_default_context)
    resolver = FakeSecretResolver({("ch-creds", "user"): "loader", ("ch-creds", "password"): "s3cret"})
    transport_from_config(make_config(), resolver)
    assert calls["default"] == 1


def test_tls_verification_failure_is_a_distinct_exception_from_transport_error() -> None:
    """Code review, Low: folding a certificate failure into `TransportError` made `loader.py` classify
    it `CONNECT_FAILED`/retryable instead of `TLS_VERIFY_FAILED`/not-retryable. `SSLCertVerificationError`
    raised directly (not wrapped in `URLError`, security review's later refinement covers that real-world
    path in `tests/clickhouse/test_tls_failures.py`) still reaches `TlsVerificationError` as a defensive
    fallback."""
    import ssl

    from tributary.loaders.clickhouse.client import ClickhouseTransport, TlsVerificationError, TransportError

    class RaisingOpener:
        def open(self, request: object, timeout: float) -> object:
            raise ssl.SSLCertVerificationError("certificate verify failed")

    transport = ClickhouseTransport("https://example.org/?database=d", {}, RaisingOpener())  # type: ignore[arg-type]
    with pytest.raises(TlsVerificationError):
        transport.send(b"SELECT 1", timeout_s=1.0)
    assert not issubclass(TlsVerificationError, TransportError)


def test_an_os_error_reading_an_error_body_is_wrapped_not_left_untyped() -> None:
    """Code review, Low: an `OSError` from `exc.read()` used to escape `send()` untyped."""
    import urllib.error

    from tributary.loaders.clickhouse.client import ClickhouseTransport, TransportError

    class BadFp:
        def read(self, n: int = -1) -> bytes:
            raise OSError("connection reset while reading the error body")

        def close(self) -> None:
            pass

    http_error = urllib.error.HTTPError("https://example.org/", 500, "Internal Server Error", {}, BadFp())  # type: ignore[arg-type]

    class RaisingOpener:
        def open(self, request: object, timeout: float) -> object:
            raise http_error

    transport = ClickhouseTransport("https://example.org/?database=d", {}, RaisingOpener())  # type: ignore[arg-type]
    with pytest.raises(TransportError):
        transport.send(b"SELECT 1", timeout_s=1.0)


def test_read_capped_stops_at_the_byte_limit() -> None:
    import time

    from tributary.loaders.clickhouse.client import _read_capped

    class InfiniteReadable:
        def read(self, n: int) -> bytes:
            return b"x" * n

    body = _read_capped(InfiniteReadable(), limit=100, deadline=time.monotonic() + 10)
    assert len(body) == 100


def test_read_capped_stops_at_the_deadline_even_under_the_byte_limit() -> None:
    import time

    from tributary.loaders.clickhouse.client import _read_capped

    calls = {"n": 0}

    class SlowReadable:
        def read(self, n: int) -> bytes:
            calls["n"] += 1
            return b"x" * min(n, 10)  # always returns data, so only the deadline check can stop this

    deadline = time.monotonic() - 1  # already passed
    with pytest.raises(TransportTimeout):
        _read_capped(SlowReadable(), limit=10_000_000, deadline=deadline)
    assert calls["n"] == 0
