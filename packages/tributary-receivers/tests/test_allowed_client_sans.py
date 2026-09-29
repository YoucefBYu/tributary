"""`tls.allowedClientSans` (channel-spec set 1.3, README 3.2.7) on the http and mllp receivers: real mTLS
handshakes against throwaway CA-signed certificates. Match, mismatch, missing SAN, and a wildcard SAN in the
certificate (which must match nothing)."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import ssl
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
from tributary.receivers.http.config import AuthSpec, HttpSettings
from tributary.receivers.http.server import HttpServer
from tributary.receivers.metrics import Metrics
from tributary.receivers.mllp.config import MllpSettings
from tributary.receivers.mllp.server import MllpServer
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.tls import TlsSpec, peer_san_allowed
from tributary.sdk import Message

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
HL7 = b"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|C1|P|2.5\rPID|1||SYNTH0001\r"
BODY = b'{"resourceType": "Patient", "id": "SYNTH1"}'


def _openssl(*args: str) -> None:
    exe = shutil.which("openssl")
    assert exe is not None
    subprocess.run([exe, *args], check=True, capture_output=True)  # noqa: S603 - fixed argv, test fixture only


class Pki:
    def __init__(self, base: Path) -> None:
        self.base = base
        (base / "ca").mkdir()
        _openssl(
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(base / "ca" / "ca.key"), "-out", str(base / "ca" / "ca.crt"), "-subj", "/CN=test-ca",
        )  # fmt: skip
        (base / "srv-tls").mkdir()
        _openssl(
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(base / "srv-tls" / "tls.key"), "-out", str(base / "srv-tls" / "tls.crt"),
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
        )  # fmt: skip
        (base / "srv-tls" / "ca.crt").write_bytes((base / "ca" / "ca.crt").read_bytes())
        self.resolver = DirSecretResolver(base)

    def client_context(self, name: str, san: str | None) -> ssl.SSLContext:
        out = self.base / name
        out.mkdir()
        _openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(out / "k"), "-out", str(out / "c"), "-subj", f"/CN={name}")  # fmt: skip
        args = ["x509", "-req", "-in", str(out / "c"), "-CA", str(self.base / "ca" / "ca.crt")]
        args += ["-CAkey", str(self.base / "ca" / "ca.key"), "-CAcreateserial", "-out", str(out / "crt"), "-days", "1"]
        if san is not None:
            (out / "ext").write_text(f"subjectAltName={san}\n", encoding="utf-8")
            args += ["-extfile", str(out / "ext")]
        _openssl(*args)
        ctx = ssl.create_default_context(cafile=str(self.base / "srv-tls" / "tls.crt"))
        ctx.load_cert_chain(str(out / "crt"), str(out / "k"))
        return ctx


ALLOWED = ("dns:sender.example.org", "uri:spiffe://example.org/ns/prod/sa/sender")

#: (certificate SAN, allow-list entries, expected accepted)
CASES = {
    "dns-match": ("DNS:sender.example.org", ALLOWED, True),
    "dns-match-is-case-insensitive": ("DNS:Sender.Example.ORG", ALLOWED, True),
    "uri-match": ("URI:spiffe://example.org/ns/prod/sa/sender", ALLOWED, True),
    "mismatch": ("DNS:other.example.org", ALLOWED, False),
    "uri-mismatch": ("URI:spiffe://example.org/ns/prod/sa/other", ALLOWED, False),
    "missing-san": (None, ALLOWED, False),
    "wildcard-in-cert-not-matched": ("DNS:*.example.org", ALLOWED, False),
    "type-must-agree": ("DNS:spiffe://example.org/ns/prod/sa/sender", ALLOWED, False),
}


def _http_settings(tls: TlsSpec) -> HttpSettings:
    return HttpSettings(
        channel="fhir-in", channel_version=1, bind="127.0.0.1", port=0, tls=tls, path="/ingest",
        accept=frozenset({"application/fhir+json"}), auth=AuthSpec("mtls"), key_expression=None,
        nack_on=frozenset(), ack_timeout_s=5.0, max_message_bytes=1_000_000, queue_max_bytes=64 * 1024 * 1024,
        queue_max_messages=None, env="dev", idle_timeout_s=2.0,
        request_timeout_s=3.0,
    )  # fmt: skip


def _mllp_settings(tls: TlsSpec) -> MllpSettings:
    return MllpSettings(
        channel="adt-in", channel_version=1, bind="127.0.0.1", port=0, tls=tls, ack_timeout_s=5.0,
        nack_on=frozenset(), max_message_bytes=1_000_000, queue_max_bytes=64 * 1024 * 1024,
        queue_max_messages=None, env="dev", idle_timeout_s=2.0,
    )  # fmt: skip


class Recorder:
    def __init__(self) -> None:
        self.calls: list[Message] = []

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        return True


@pytest.mark.parametrize("case", list(CASES))
def test_http_allowed_client_sans(tmp_path: Path, case: str, caplog: pytest.LogCaptureFixture) -> None:
    san, allowed, accepted = CASES[case]
    pki = Pki(tmp_path)
    client_ctx = pki.client_context("client", san)
    cfg = _http_settings(TlsSpec("srv-tls", "required", allowed_client_sans=allowed))
    pub, metrics = Recorder(), Metrics()

    async def go() -> None:
        srv = HttpServer(cfg, pub, metrics=metrics, secret_resolver=pki.resolver)  # type: ignore[arg-type]
        server = await srv.start()
        port = server.sockets[0].getsockname()[1]
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
            if accepted:
                head = f"POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/fhir+json\r\nContent-Length: {len(BODY)}\r\n\r\n"
                w.write(head.encode() + BODY)
                await w.drain()
            # A refused peer gets its answer without sending a byte: the request is never read.
            raw = await asyncio.wait_for(r.read(65536), 5)
            status = int(raw.split(b" ")[1])
            if accepted:
                assert status == 202 and len(pub.calls) == 1
            else:
                head, _, body = raw.partition(b"\r\n\r\n")
                assert status == 403 and body == b"" and b"connection: close" in head.lower()
                assert pub.calls == [] and metrics.rejected_peer_not_allowed == 1
            w.close()
        finally:
            server.close()
            await server.wait_closed()

    with caplog.at_level(logging.WARNING, logger="tributary.receivers.tls"):
        asyncio.run(go())
    lines = [json.loads(r.getMessage()) for r in caplog.records if r.name == "tributary.receivers.tls"]
    if accepted:
        assert lines == []
    else:
        [line] = lines
        assert line["code"] == "TLS_PEER_NOT_ALLOWED" and line["channel"] == "fhir-in"
        blob = json.dumps(line)
        assert "example.org" not in blob and "client" not in blob and len(line["cert_sha256"]) == 64


@pytest.mark.parametrize("case", list(CASES))
def test_mllp_allowed_client_sans(tmp_path: Path, case: str) -> None:
    san, allowed, accepted = CASES[case]
    pki = Pki(tmp_path)
    client_ctx = pki.client_context("client", san)
    cfg = _mllp_settings(TlsSpec("srv-tls", "required", allowed_client_sans=allowed))
    pub, metrics = Recorder(), Metrics()

    async def go() -> None:
        srv = MllpServer(cfg, pub, metrics=metrics, secret_resolver=pki.resolver)  # type: ignore[arg-type]
        server = await srv.start()
        port = server.sockets[0].getsockname()[1]
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
            w.write(SB + HL7 + EB + CR)
            await w.drain()
            data = await asyncio.wait_for(r.read(65536), 5)
            if accepted:
                assert b"MSA|AA|C1" in data and len(pub.calls) == 1
            else:
                assert data == b"" and pub.calls == [] and metrics.rejected_peer_not_allowed == 1
            w.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(go())


def test_no_allow_list_still_accepts_any_certificate_that_chains(tmp_path: Path) -> None:
    pki = Pki(tmp_path)
    client_ctx = pki.client_context("client", None)
    cfg = _mllp_settings(TlsSpec("srv-tls", "required"))
    pub = Recorder()

    async def go() -> None:
        server = await MllpServer(cfg, pub, secret_resolver=pki.resolver).start()  # type: ignore[arg-type]
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")
        w.write(SB + HL7 + EB + CR)
        await w.drain()
        assert b"MSA|AA|C1" in await asyncio.wait_for(r.read(65536), 5)
        w.close()
        server.close()
        await server.wait_closed()

    asyncio.run(go())


def test_the_mllp_listener_offers_only_aead_ecdhe_ciphers(tmp_path: Path) -> None:
    pki = Pki(tmp_path)
    srv = MllpServer(_mllp_settings(TlsSpec("srv-tls", "none")), Recorder(), secret_resolver=pki.resolver)  # type: ignore[arg-type]
    assert srv._ssl_context is not None
    tls12 = [c["name"] for c in srv._ssl_context.get_ciphers() if c["protocol"] == "TLSv1.2"]
    assert tls12 and all(n.startswith("ECDHE-") and ("GCM" in n or "CHACHA20" in n) for n in tls12)


class _Peer:
    def __init__(self, cert: dict[str, object] | None) -> None:
        self._cert = cert

    def getpeercert(self, binary_form: bool = False) -> dict[str, object] | None:
        return self._cert


def test_no_certificate_with_an_allow_list_fails_closed_and_no_list_allows() -> None:
    assert peer_san_allowed(_Peer(None), ("dns:a.example.org",)) is False  # type: ignore[arg-type]
    assert peer_san_allowed(None, ("dns:a.example.org",)) is False
    assert peer_san_allowed(None, ()) is True


@pytest.mark.parametrize(
    "sans",
    [
        ("dns:*.example.org",),
        ("dns:Upper.example.org",),
        ("email:a@example.org",),
        ("ip:10.0.0.1",),
        ("dns:a.example.org", "dns:a.example.org"),
        tuple(f"dns:h{i}.example.org" for i in range(33)),
    ],
)
def test_tls_spec_rejects_bad_allow_lists(sans: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="allowedClientSans"):
        TlsSpec("x", "required", allowed_client_sans=sans)


def test_tls_spec_rejects_an_allow_list_without_client_auth() -> None:
    with pytest.raises(ValueError, match="clientAuth"):
        TlsSpec("x", "none", allowed_client_sans=("dns:a.example.org",))


def test_the_channel_spec_field_reaches_both_receivers() -> None:
    from tributary.receivers.http.config import http_settings_from_channel_spec  # noqa: F401
    from tributary.receivers.mllp.config import mllp_settings_from_channel_spec

    spec = {
        "metadata": {"name": "adt-in"},
        "spec": {
            "ordering": "none",
            "source": {
                "type": "mllp", "bind": "10.0.0.5", "port": 2575, "ack": {"mode": "after-commit"},
                "tls": {"secretRef": "s", "clientAuth": "required", "allowedClientSans": ["dns:a.example.org"]},
            },
        },
    }  # fmt: skip
    cfg = mllp_settings_from_channel_spec(spec, env="dev")
    assert cfg.tls is not None and cfg.tls.allowed_client_sans == ("dns:a.example.org",)
    assert replace(cfg).tls == cfg.tls
