# ruff: noqa: S106  (test settings carry secretRef NAMES, not secrets)
"""W3-37a (ADR-004 C1a): TLS on the API listener from secretRef files, refusal of non-loopback plaintext,
mesh mode, and the optional CA file for the tamper ClickHouse client. A throwaway CA is generated per run."""

from __future__ import annotations

import contextlib
import datetime
import http.client
import http.server
import os
import ssl
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from archive_kit import restore_uvicorn_logging
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
from tributary.api import config, main, tls
from tributary.api.errors import ConfigurationError
from tributary.api.tamper import ClickHouseHttp, TamperQueryError
from tributary.runtime.secrets import DirSecretResolver

BASE = {
    "TB_DATABASE_HOST": "db.example",
    "TB_DATABASE_NAME": "tb",
    "TB_DATABASE_USER": "tbapi",
    "TB_DATABASE_SECRET_REF": "pgcred",
    "TB_OIDC_ISSUER": "https://idp.example/realms/tb",
    "TB_OIDC_AUDIENCE": "tb-api",
    "TB_OIDC_ROLE_MAP": '{"tb-admin": "admin"}',
}


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def __getattr__(self, level: str) -> Any:
        return lambda event, /, **f: self.events.append((level, event, f))


class Pki:
    """A CA and a server certificate for `localhost` (no IP SAN), with the key as PEM."""

    def __init__(self, root: Path, *, kind: str = "ec") -> None:
        self.root = root
        now = datetime.datetime.now(datetime.UTC)
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tb-test-ca")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
            .sign(ca_key, hashes.SHA256())
        )
        self.key: Any = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            if kind == "rsa"
            else ec.generate_private_key(ec.SECP256R1())
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
            .issuer_name(ca_name)
            .public_key(self.key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
            .sign(ca_key, hashes.SHA256())
        )
        self.ca_pem = ca.public_bytes(serialization.Encoding.PEM)
        self.cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        self.key_pem = self.key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
        self.ca_file = root / "ca.pem"
        self.ca_file.write_bytes(self.ca_pem)

    def secret(self, ref: str = "api-tls", key_pem: bytes | None = None) -> DirSecretResolver:
        d = self.root / "secrets" / ref
        d.mkdir(parents=True, exist_ok=True)
        (d / "tls.crt").write_bytes(self.cert_pem)
        (d / "tls.key").write_bytes(key_pem or self.key_pem)
        (d / "tls.key").chmod(0o600)
        return DirSecretResolver(str(self.root / "secrets"))


def settings(**kw: Any) -> config.Settings:
    base = config.from_env({**BASE, **{k: v for k, v in kw.items() if isinstance(v, str)}})
    return base


def refused(code: str, **env: str) -> None:
    with pytest.raises(ConfigurationError) as exc:
        config.from_env({**BASE, **env})
    assert exc.value.code == code


# -- the rules ---------------------------------------------------------------------------------------


def test_plaintext_on_a_non_loopback_bind_is_refused_unless_tls_or_mesh() -> None:
    refused("API_TLS_REQUIRED", TB_API_BIND="0.0.0.0")  # noqa: S104
    refused("API_TLS_REQUIRED", TB_API_BIND="10.1.2.3", TB_API_TLS_MODE="listener")
    refused("API_TLS_REQUIRED", TB_API_BIND="::")
    assert config.from_env({**BASE, "TB_API_BIND": "10.1.2.3", "TB_API_TLS_SECRET_REF": "api-tls"}).tls_secret_ref
    assert config.from_env({**BASE, "TB_API_BIND": "10.1.2.3", "TB_API_TLS_MODE": "mesh"}).tls_mode == "mesh"
    assert config.from_env({**BASE, "TB_API_BIND": "127.0.0.1"}).tls_secret_ref is None  # loopback plaintext starts
    assert config.from_env({**BASE, "TB_API_BIND": "::1"}).tls_mode == "listener"


def test_bad_modes_and_a_mesh_with_a_certificate_are_refused() -> None:
    refused("API_TLS_MODE_INVALID", TB_API_TLS_MODE="off")
    refused("API_TLS_MODE_CONFLICT", TB_API_TLS_MODE="mesh", TB_API_TLS_SECRET_REF="api-tls")
    refused("API_TLS_SECRET_REF_INVALID", TB_API_TLS_SECRET_REF="../etc")
    refused(
        "CLICKHOUSE_CA_SECRET_REF_INVALID",
        TB_CLICKHOUSE_URL="https://ch",
        TB_CLICKHOUSE_USER="u",
        TB_CLICKHOUSE_SECRET_REF="r",
        TB_AUDIT_TAMPER_TARGETS='{"dev": "d"}',
        TB_CLICKHOUSE_CA_SECRET_REF="a b",
    )


def test_main_exits_2_with_the_code_for_non_loopback_plaintext(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for k in list(os.environ):
        if k.startswith("TB_"):
            monkeypatch.delenv(k)
    for k, v in {**BASE, "TB_API_BIND": "0.0.0.0"}.items():  # noqa: S104
        monkeypatch.setenv(k, v)
    assert main.main() == 2
    lines = capsys.readouterr().err
    assert '"event":"startup_refused"' in lines.replace(" ", "") and "API_TLS_REQUIRED" in lines


# -- the listener ------------------------------------------------------------------------------------


def serve(config_: uvicorn.Config) -> tuple[uvicorn.Server, threading.Thread]:
    server = uvicorn.Server(config_)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started
    return server, thread


def free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def app_with_route() -> FastAPI:
    app = FastAPI()

    @app.get("/ping")
    async def ping() -> dict[str, str]:
        return {"ok": "yes"}

    return app


def get(port: int, hostname: str, ca: Path | None) -> int:
    ctx = ssl.create_default_context(cafile=str(ca) if ca else None)
    raw = http.client.HTTPSConnection("127.0.0.1", port, context=ctx, timeout=10)
    raw.connect = lambda: _connect(raw, port, hostname, ctx)  # type: ignore[method-assign]
    try:
        raw.request("GET", "/ping")
        return raw.getresponse().status
    finally:
        raw.close()


def _connect(conn: http.client.HTTPSConnection, port: int, hostname: str, ctx: ssl.SSLContext) -> None:
    import socket

    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    conn.sock = ctx.wrap_socket(sock, server_hostname=hostname)  # verifies the certificate against `hostname`


def test_a_client_verifying_the_ca_connects_and_a_wrong_hostname_is_refused(tmp_path: Path) -> None:
    pki, log, port = Pki(tmp_path), Log(), free_port()
    cfg = tls.server_config(
        app_with_route(),
        config.Settings(**{**vars(_s()), "port": port, "tls_secret_ref": "api-tls"})
        if False
        else _s(port=port, tls_secret_ref="api-tls"),
        pki.secret(),
        log,  # type: ignore[arg-type]
    )
    assert isinstance(cfg.ssl, ssl.SSLContext) and cfg.ssl.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert [e[1] for e in log.events] == ["api_tls_enabled"]
    server, thread = serve(cfg)
    try:
        assert get(port, "localhost", pki.ca_file) == 200
        with pytest.raises(ssl.SSLCertVerificationError):
            get(port, "api.wrong.example", pki.ca_file)  # a name the certificate does not carry
        with pytest.raises(ssl.SSLCertVerificationError):
            get(port, "localhost", None)  # the system trust store does not know the test CA
        plain = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        with pytest.raises((http.client.HTTPException, OSError)):
            plain.request("GET", "/ping")
            plain.getresponse()  # plaintext to the TLS port gets no HTTP answer
    finally:
        server.should_exit = True
        thread.join(10)
        restore_uvicorn_logging()


def _s(**kw: Any) -> config.Settings:
    s = config.from_env(BASE)
    fields = {f: getattr(s, f) for f in s.__dataclass_fields__}
    fields.update(kw)
    return config.Settings(**fields)


def test_a_key_that_does_not_match_the_certificate_or_is_missing_refuses_without_naming_it(tmp_path: Path) -> None:
    pki = Pki(tmp_path)
    other = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    for resolver in (pki.secret(key_pem=other), DirSecretResolver(str(tmp_path / "nowhere"))):
        log = Log()
        with pytest.raises(ConfigurationError) as exc:
            tls.server_config(app_with_route(), _s(tls_secret_ref="api-tls"), resolver, log)  # type: ignore[arg-type]
        assert exc.value.code == "API_TLS_INVALID"
        text = str(exc.value) + repr(log.events)
        assert "BEGIN" not in text and "tls.key" not in text and str(tmp_path) not in text


def test_the_key_is_never_logged_on_a_good_start(tmp_path: Path) -> None:
    pki, log = Pki(tmp_path), Log()
    tls.server_config(app_with_route(), _s(tls_secret_ref="api-tls"), pki.secret(), log)  # type: ignore[arg-type]
    text = repr(log.events)
    assert "BEGIN" not in text and "tls.key" not in text and "api-tls" not in text and str(tmp_path) not in text


def test_mesh_mode_binds_plaintext_and_warns_on_every_start() -> None:
    for _ in range(2):  # every start, not once
        log = Log()
        cfg = tls.server_config(app_with_route(), _s(bind="10.1.2.3", tls_mode="mesh"), None, log)  # type: ignore[arg-type]
        assert cfg.ssl_certfile is None and cfg.host == "10.1.2.3"
        assert log.events == [("warning", "api_tls_delegated_to_mesh", {})]


def test_loopback_plaintext_starts_quietly() -> None:
    log = Log()
    cfg = tls.server_config(app_with_route(), _s(), None, log)  # type: ignore[arg-type]
    assert cfg.ssl_certfile is None and log.events == []


# -- the tamper client's optional CA -----------------------------------------------------------------


def test_the_tamper_client_trusts_exactly_the_mounted_ca_when_one_is_given(tmp_path: Path) -> None:
    pki = Pki(tmp_path)
    (tmp_path / "srv").mkdir()
    (tmp_path / "srv" / "crt").write_bytes(pki.cert_pem)
    (tmp_path / "srv" / "key").write_bytes(pki.key_pem)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(tmp_path / "srv" / "crt", tmp_path / "srv" / "key")

    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"0\n")

        def log_message(self, *_a: Any) -> None:
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), H)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"https://localhost:{server.server_port}"
    try:
        assert ClickHouseHttp(url, "u", "pw-not-real", ca_file=str(pki.ca_file)).query("SELECT 1") == "0\n"
        with pytest.raises(TamperQueryError):  # no CA given: the system store does not know the test CA
            ClickHouseHttp(url, "u", "pw-not-real").query("SELECT 1")
        with pytest.raises(TamperQueryError):  # a name the certificate lacks is refused even with the CA
            ClickHouseHttp(url.replace("localhost", "127.0.0.1"), "u", "pw-not-real", ca_file=str(pki.ca_file)).query(
                "SELECT 1"
            )
    finally:
        server.shutdown()
        server.server_close()
    with pytest.raises((ssl.SSLError, OSError)):
        ClickHouseHttp(url, "u", "p", ca_file=str(tmp_path / "missing.pem"))


def _handshake(port: int, ca: Path, *, ciphers: str | None, tls13: bool) -> str:
    """Connect as a verifying client restricted to TLS 1.2 (or 1.3) and the given suites; return the cipher used."""
    import socket

    ctx = ssl.create_default_context(cafile=str(ca))
    if tls13:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    else:
        ctx.minimum_version = ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    if ciphers is not None:
        ctx.set_ciphers(ciphers)
    with (
        socket.create_connection(("127.0.0.1", port), timeout=10) as raw,
        ctx.wrap_socket(raw, server_hostname="localhost") as tls_sock,
    ):
        return str(tls_sock.cipher()[0])


def test_tls12_allows_only_forward_secret_suites_and_tls13_keeps_its_defaults(tmp_path: Path) -> None:
    pki = Pki(tmp_path, kind="rsa")  # an RSA certificate, so a plain RSA key-exchange suite is usable if offered
    port = free_port()
    cfg = tls.server_config(app_with_route(), _s(port=port, tls_secret_ref="api-tls"), pki.secret(), Log())  # type: ignore[arg-type]
    server, thread = serve(cfg)
    try:
        assert _handshake(port, pki.ca_file, ciphers="ECDHE-RSA-AES256-GCM-SHA384", tls13=False).startswith("ECDHE")
        assert _handshake(port, pki.ca_file, ciphers="ECDHE-RSA-CHACHA20-POLY1305", tls13=False).startswith("ECDHE")
        with pytest.raises(ssl.SSLError):  # no forward secrecy: refused by the listener
            _handshake(port, pki.ca_file, ciphers="AES128-GCM-SHA256", tls13=False)
        assert _handshake(port, pki.ca_file, ciphers=None, tls13=True).startswith("TLS_")  # TLS 1.3 unchanged
    finally:
        server.should_exit = True
        thread.join(10)
        restore_uvicorn_logging()

    # Control: the same client suite succeeds against a listener without the restriction, so the refusal above is
    # the cipher list's doing and not an artefact of the certificate.
    plain_port = free_port()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(pki.root / "secrets" / "api-tls" / "tls.crt", pki.root / "secrets" / "api-tls" / "tls.key")
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.set_ciphers("AES128-GCM-SHA256")
    import socket

    srv = socket.socket()
    srv.bind(("127.0.0.1", plain_port))
    srv.listen(1)

    def accept() -> None:
        conn, _ = srv.accept()
        with contextlib.suppress(OSError), ctx.wrap_socket(conn, server_side=True):
            pass

    threading.Thread(target=accept, daemon=True).start()
    try:
        assert _handshake(plain_port, pki.ca_file, ciphers="AES128-GCM-SHA256", tls13=False) == "AES128-GCM-SHA256"
    finally:
        srv.close()
