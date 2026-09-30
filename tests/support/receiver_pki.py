"""A throwaway CA for receiver mTLS tests: a server secret dir `<base>/srv-tls` (certificate, key, trust bundle)
and a client `SSLContext` whose certificate carries a chosen SAN. Synthetic, one-day certificates; needs `openssl`."""

from __future__ import annotations

import shutil
import ssl
import subprocess
from pathlib import Path

CLIENT_SAN = "sender.example.org"


def openssl_available() -> bool:
    return shutil.which("openssl") is not None


def _openssl(*args: str) -> None:
    exe = shutil.which("openssl")
    assert exe is not None
    subprocess.run([exe, *args], check=True, capture_output=True)  # noqa: S603 - fixed argv, test fixture only


def build_pki(base: Path, *, client_san: str = CLIENT_SAN) -> ssl.SSLContext:
    """Create the CA and the server secret dir under `base`; return a client context whose certificate has
    the SAN `DNS:<client_san>`. The server is `localhost` / `127.0.0.1`; its secretRef is `srv-tls`."""
    ca = base / "ca"
    ca.mkdir()
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", str(ca / "ca.key"), "-out", str(ca / "ca.crt"), "-subj", "/CN=test-ca")  # fmt: skip
    srv = base / "srv-tls"
    srv.mkdir()
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", str(srv / "tls.key"), "-out", str(srv / "tls.crt"), "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1")  # fmt: skip
    (srv / "ca.crt").write_bytes((ca / "ca.crt").read_bytes())
    cli = base / "client"
    cli.mkdir()
    _openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(cli / "k"), "-out", str(cli / "c"), "-subj", "/CN=client")  # fmt: skip
    (cli / "ext").write_text(f"subjectAltName=DNS:{client_san}\n", encoding="utf-8")
    _openssl("x509", "-req", "-in", str(cli / "c"), "-CA", str(ca / "ca.crt"), "-CAkey", str(ca / "ca.key"), "-CAcreateserial", "-out", str(cli / "crt"), "-days", "1", "-extfile", str(cli / "ext"))  # fmt: skip
    ctx = ssl.create_default_context(cafile=str(srv / "tls.crt"))
    ctx.load_cert_chain(str(cli / "crt"), str(cli / "k"))
    return ctx
