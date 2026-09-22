"""TLS and mTLS (gate finding SR-8: the Phase 0 spike terminated TLS but never authenticated the
client). Generates throwaway self-signed certificates with `openssl` and lays them out the way
`DirSecretResolver` expects (`<base_dir>/<ref>/<key>`, mirroring a Kubernetes `Secret` volume mount).
Skipped if `openssl` is not on `PATH`.
"""

from __future__ import annotations

import asyncio
import shutil
import ssl
import subprocess
from pathlib import Path

import pytest
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.tls import TlsSpec, build_ssl_context

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")


def _selfsigned(out_dir: Path, cn: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            shutil.which("openssl"),
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(out_dir / "tls.key"),
            "-out",
            str(out_dir / "tls.crt"),
            "-subj",
            f"/CN={cn}",
        ],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def server_secret(tmp_path: Path) -> DirSecretResolver:
    _selfsigned(tmp_path / "mllp-tls", "localhost")
    return DirSecretResolver(tmp_path)


def test_server_only_tls_builds_a_context_with_no_client_auth(server_secret: DirSecretResolver) -> None:
    ctx = build_ssl_context(TlsSpec("mllp-tls", "none"), server_secret)
    assert ctx.verify_mode == ssl.CERT_NONE


def test_min_version_1_3_is_applied(server_secret: DirSecretResolver) -> None:
    ctx = build_ssl_context(TlsSpec("mllp-tls", "none", "1.3"), server_secret)
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3


def test_client_auth_required_needs_ca_and_sets_cert_required(tmp_path: Path) -> None:
    _selfsigned(tmp_path / "mllp-tls", "localhost")
    (tmp_path / "mllp-tls" / "ca.crt").write_bytes((tmp_path / "mllp-tls" / "tls.crt").read_bytes())
    resolver = DirSecretResolver(tmp_path)
    ctx = build_ssl_context(TlsSpec("mllp-tls", "required"), resolver)
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_client_auth_required_without_ca_raises(server_secret: DirSecretResolver) -> None:
    with pytest.raises(FileNotFoundError):
        build_ssl_context(TlsSpec("mllp-tls", "required"), server_secret)


def test_missing_secret_ref_raises_file_not_found(tmp_path: Path) -> None:
    resolver = DirSecretResolver(tmp_path)
    with pytest.raises(FileNotFoundError):
        build_ssl_context(TlsSpec("does-not-exist", "none"), resolver)


def test_dir_resolver_refuses_to_escape_the_secrets_directory(tmp_path: Path) -> None:
    resolver = DirSecretResolver(tmp_path)
    with pytest.raises(FileNotFoundError):
        resolver.resolve_file("mllp-tls", "../../etc/passwd")


@pytest.mark.parametrize(("client_auth", "min_version"), [("maybe", "1.2"), ("required", "1.1")])
def test_tls_spec_rejects_bad_enum_values(client_auth: str, min_version: str) -> None:
    with pytest.raises(ValueError, match="tls\\."):
        TlsSpec("ref", client_auth, min_version)


def test_handshake_end_to_end_over_real_sockets(server_secret: DirSecretResolver) -> None:
    """A real client actually completes a TLS handshake against the context this module builds."""

    async def go() -> None:
        ctx = build_ssl_context(TlsSpec("mllp-tls", "none"), server_secret)

        async def handle(_r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            w.write(b"ok")
            await w.drain()
            w.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ctx)
        port = server.sockets[0].getsockname()[1]
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)
        assert await r.read(2) == b"ok"
        w.close()
        server.close()
        await server.wait_closed()

    asyncio.run(go())


def _openssl(*args: str) -> None:
    exe = shutil.which("openssl")
    assert exe is not None
    subprocess.run([exe, *args], check=True, capture_output=True)  # noqa: S603 - fixed argv, test fixture only


def _make_ca(out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ca_key, ca_crt = out_dir / "ca.key", out_dir / "ca.crt"
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_crt),
        "-subj",
        "/CN=test-ca",
    )
    return ca_crt, ca_key


def _make_signed_client_cert(out_dir: Path, ca_crt: Path, ca_key: Path, cn: str) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    key, csr, crt = out_dir / f"{cn}.key", out_dir / f"{cn}.csr", out_dir / f"{cn}.crt"
    _openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(csr), "-subj", f"/CN={cn}")
    _openssl(
        "x509",
        "-req",
        "-in",
        str(csr),
        "-CA",
        str(ca_crt),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(crt),
        "-days",
        "1",
    )
    return crt, key


async def _serve_once(ctx: ssl.SSLContext) -> asyncio.base_events.Server:
    async def handle(_r: object, w: object) -> None:
        w.write(b"ok")  # type: ignore[attr-defined]
        await w.drain()  # type: ignore[attr-defined]
        w.close()  # type: ignore[attr-defined]

    return await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ctx)


async def _try_get_ok(port: int, client_ctx: ssl.SSLContext) -> bytes | str:
    """`b"ok"` on a handshake the server actually served; otherwise a short description of the
    failure. A rejected mTLS handshake is not reliably observable as an exception from
    `open_connection` on every asyncio/OpenSSL combination — on this platform the server aborts the
    handshake and the client instead sees a clean, silent EOF on read — so this treats "connected
    without error, then never got `ok`" as rejection too, rather than asserting a specific
    exception type the platform may not raise."""
    try:
        r, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port, ssl=client_ctx), 5)
    except (ssl.SSLError, ConnectionError, OSError, TimeoutError) as exc:
        return f"connect failed: {exc!r}"
    try:
        data = await asyncio.wait_for(r.read(2), 5)
    except (ssl.SSLError, ConnectionError, OSError, TimeoutError) as exc:
        return f"read failed: {exc!r}"
    finally:
        w.close()
    return data


def test_mtls_required_accepts_a_client_certificate_signed_by_the_trusted_ca(tmp_path: Path) -> None:
    """The other half of gate finding SR-8: `clientAuth: required` must not just set
    `CERT_REQUIRED`, it must actually let through a client whose certificate the configured CA
    signed."""
    ca_crt, ca_key = _make_ca(tmp_path / "ca")
    _selfsigned(tmp_path / "mllp-tls", "localhost")
    (tmp_path / "mllp-tls" / "ca.crt").write_bytes(ca_crt.read_bytes())
    client_crt, client_key = _make_signed_client_cert(tmp_path / "client", ca_crt, ca_key, "trusted-client")
    resolver = DirSecretResolver(tmp_path)

    async def go() -> None:
        ctx = build_ssl_context(TlsSpec("mllp-tls", "required"), resolver)
        server = await _serve_once(ctx)
        port = server.sockets[0].getsockname()[1]
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        client_ctx.load_cert_chain(str(client_crt), str(client_key))
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)
        assert await r.read(2) == b"ok"
        w.close()
        server.close()
        await server.wait_closed()

    asyncio.run(go())


def test_mtls_required_rejects_a_client_with_no_certificate(tmp_path: Path) -> None:
    """`clientAuth: required` with a client that presents nothing must fail the handshake, not
    silently downgrade to `clientAuth: none` behaviour."""
    ca_crt, _ca_key = _make_ca(tmp_path / "ca")
    _selfsigned(tmp_path / "mllp-tls", "localhost")
    (tmp_path / "mllp-tls" / "ca.crt").write_bytes(ca_crt.read_bytes())
    resolver = DirSecretResolver(tmp_path)

    async def go() -> None:
        ctx = build_ssl_context(TlsSpec("mllp-tls", "required"), resolver)
        server = await _serve_once(ctx)
        port = server.sockets[0].getsockname()[1]
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE  # this client presents no certificate at all
        try:
            result = await _try_get_ok(port, client_ctx)
            assert result != b"ok", result
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(go())


def test_mtls_required_rejects_a_client_certificate_from_an_untrusted_ca(tmp_path: Path) -> None:
    """A client certificate that is well-formed but signed by the WRONG CA must also be rejected —
    proves the server checks the chain against its configured `ca.crt`, not just "some cert was
    presented"."""
    ca_crt, _ca_key = _make_ca(tmp_path / "ca")
    other_ca_crt, other_ca_key = _make_ca(tmp_path / "other-ca")
    _selfsigned(tmp_path / "mllp-tls", "localhost")
    (tmp_path / "mllp-tls" / "ca.crt").write_bytes(ca_crt.read_bytes())  # server trusts THIS ca
    # client's cert is signed by a DIFFERENT ca
    client_crt, client_key = _make_signed_client_cert(
        tmp_path / "client", other_ca_crt, other_ca_key, "untrusted-client"
    )
    resolver = DirSecretResolver(tmp_path)

    async def go() -> None:
        ctx = build_ssl_context(TlsSpec("mllp-tls", "required"), resolver)
        server = await _serve_once(ctx)
        port = server.sockets[0].getsockname()[1]
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        client_ctx.load_cert_chain(str(client_crt), str(client_key))
        try:
            result = await _try_get_ok(port, client_ctx)
            assert result != b"ok", result
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(go())
