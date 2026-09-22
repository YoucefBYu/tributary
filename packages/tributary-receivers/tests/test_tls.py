"""TLS and mTLS (gate finding SR-8: the Phase 0 spike terminated TLS but never authenticated the
client). Generates throwaway self-signed certificates with `openssl` and lays them out the way
`DirSecretResolver` expects (`<base_dir>/<ref>/<key>`, mirroring a Kubernetes `Secret` volume mount).
Skipped if `openssl` is not on `PATH`.
"""

from __future__ import annotations

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
    import asyncio

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
