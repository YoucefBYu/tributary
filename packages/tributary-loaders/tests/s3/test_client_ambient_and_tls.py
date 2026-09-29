"""`build_client`: `ambient` auth is workload identity or an instance profile and nothing else, the shared
config and credentials files and `AWS_PROFILE` have no effect, and TLS is TLS 1.2+ with AEAD suites (security
review, Medium). All against local servers, black-box through `S3Loader.deliver`."""

from __future__ import annotations

import http.server
import json
import logging
import shutil
import socket
import ssl
import subprocess
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import ClassVar

import pytest
from tributary.loaders.s3.client import build_client
from tributary.loaders.s3.loader import S3Loader

from .test_client import KEY_ID_MARKER, _config, _Resolver
from .test_loader import make_config, make_message

CONTAINER_KEY_ID = "SYNTHCONTAINERKEY01"
ENV_KEY_ID = "SYNTHENVKEY0000001"
FILE_KEY_ID = "SYNTHFILEKEY000001"


class _S3Handler(http.server.BaseHTTPRequestHandler):
    seen: ClassVar[list[dict[str, str]]] = []

    def log_message(self, *_args: object) -> None:
        pass

    def do_PUT(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        type(self).seen.append({k.lower(): v for k, v in self.headers.items()} | {"path": self.path})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


class _ContainerCredentialsHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        pass

    def do_GET(self) -> None:
        body = json.dumps(
            {
                "AccessKeyId": CONTAINER_KEY_ID,
                "SecretAccessKey": "synthetic-container-secret-0001",
                "Token": "synthetic-container-token-0001",
                "Expiration": "2099-01-01T00:00:00Z",
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(handler: type[http.server.BaseHTTPRequestHandler]) -> tuple[http.server.HTTPServer, str]:
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


@pytest.fixture
def s3_server() -> Iterator[tuple[str, list[dict[str, str]]]]:
    handler = type("H", (_S3Handler,), {"seen": []})
    httpd, url = _serve(handler)
    try:
        yield url, handler.seen  # type: ignore[attr-defined]
    finally:
        httpd.shutdown()


@pytest.fixture
def hostile_aws_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A shared config and credentials file that would, if read, supply keys, run a `credential_process`
    and turn on the accelerate endpoint; plus the environment variables that name them."""
    marker = tmp_path / "credential-process-ran"
    (tmp_path / "credentials").write_text(
        f"[hostile]\naws_access_key_id = {FILE_KEY_ID}\naws_secret_access_key = synthetic-file-secret\n"
        f"[default]\naws_access_key_id = {FILE_KEY_ID}\naws_secret_access_key = synthetic-file-secret\n"
    )
    (tmp_path / "config").write_text(
        f"[profile hostile]\ncredential_process = touch {marker}\ns3 =\n    use_accelerate_endpoint = true\n"
        f"[default]\ncredential_process = touch {marker}\ns3 =\n    use_accelerate_endpoint = true\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_PROFILE", "hostile")
    monkeypatch.setenv("AWS_DEFAULT_PROFILE", "hostile")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", ENV_KEY_ID)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "synthetic-env-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")  # no IMDS probe in the test
    return marker


def test_ambient_ignores_env_keys_profiles_shared_files_and_credential_process(
    s3_server: tuple[str, list[dict[str, str]]], hostile_aws_files: Path
) -> None:
    endpoint, seen = s3_server
    config = make_config(endpoint=endpoint, auth_type="ambient", auth_secret_ref=None, timeout_s=5.0)
    outcome = S3Loader(config, build_client(config, None)).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "S3_NO_CREDENTIALS", False)
    assert seen == [], "nothing may be sent with credentials from the environment or the shared files"
    assert not hostile_aws_files.exists(), "credential_process must never run"


def test_ambient_uses_container_credentials_the_workload_identity_path(
    s3_server: tuple[str, list[dict[str, str]]], hostile_aws_files: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint, seen = s3_server
    creds_httpd, creds_url = _serve(_ContainerCredentialsHandler)
    try:
        monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", f"{creds_url}/creds")
        config = make_config(endpoint=endpoint, auth_type="ambient", auth_secret_ref=None, timeout_s=5.0)
        outcome = S3Loader(config, build_client(config, None)).deliver(make_message())
    finally:
        creds_httpd.shutdown()
    assert outcome.success, outcome
    (request,) = seen
    assert f"Credential={CONTAINER_KEY_ID}/" in request["authorization"]
    assert ENV_KEY_ID not in request["authorization"] and FILE_KEY_ID not in request["authorization"]


def test_the_shared_config_cannot_turn_on_the_accelerate_endpoint(
    s3_server: tuple[str, list[dict[str, str]]], hostile_aws_files: Path
) -> None:
    endpoint, seen = s3_server
    outcome = S3Loader(
        (config := _config(endpoint)),
        build_client(config, _Resolver()),
    ).deliver(make_message())
    assert outcome.success, outcome
    assert f"Credential={KEY_ID_MARKER}/" in seen[0]["authorization"]  # the secretRef key, not the file's
    assert "accelerate" not in seen[0]["host"]


def test_the_botocore_loggers_are_capped_at_warning() -> None:
    for name in ("botocore", "urllib3"):
        logging.getLogger(name).setLevel(logging.DEBUG)
    build_client(_config("https://s3.example.org"), _Resolver())
    assert logging.getLogger("botocore").level == logging.WARNING
    assert logging.getLogger("urllib3").level == logging.WARNING


def test_the_client_context_has_the_tls_floor() -> None:
    client = build_client(_config("https://s3.example.org"), _Resolver())
    ctx = client._endpoint.http_session._manager.connection_pool_kw["ssl_context"]
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    tls12 = [c["name"] for c in ctx.get_ciphers() if c["protocol"] == "TLSv1.2"]
    assert tls12 and all(n.startswith("ECDHE") and ("GCM" in n or "CHACHA20" in n) for n in tls12)


# -- real handshakes ---------------------------------------------------------------------------------

pytestmark_openssl = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")


def _tls_server(tmp_path: Path, ciphers: str, version: ssl.TLSVersion | None) -> tuple[Callable[[], None], int, Path]:
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            shutil.which("openssl") or "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(tmp_path / "k.pem"), "-out", str(tmp_path / "c.pem"),
            "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "c.pem", tmp_path / "k.pem")
    context.set_ciphers(ciphers)
    if version is not None:
        context.minimum_version = context.maximum_version = version
    handler = type("H", (_S3Handler,), {"seen": []})
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    port = int(listener.getsockname()[1])
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler, bind_and_activate=False)
    httpd.socket.close()
    httpd.socket = context.wrap_socket(listener, server_side=True)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd.shutdown, port, tmp_path / "c.pem"


@pytestmark_openssl
def test_a_cbc_only_endpoint_is_refused_at_the_handshake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stop, port, cert = _tls_server(tmp_path, "AES128-SHA:@SECLEVEL=0", ssl.TLSVersion.TLSv1_2)
    try:
        monkeypatch.setenv("SSL_CERT_FILE", str(cert))  # the certificate is trusted: only the suite can fail
        config = _config(f"https://127.0.0.1:{port}")
        outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message())
    finally:
        stop()
    assert outcome.success is False
    assert outcome.reason != "TLS_VERIFY_FAILED"  # not a certificate problem: a handshake refusal
    assert outcome.reason in ("CONNECT_FAILED", "TRANSPORT_ERROR")


@pytestmark_openssl
def test_an_aead_endpoint_with_a_trusted_certificate_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop, port, cert = _tls_server(tmp_path, "ECDHE+AESGCM", None)
    try:
        monkeypatch.setenv("SSL_CERT_FILE", str(cert))
        config = _config(f"https://127.0.0.1:{port}")
        outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message())
    finally:
        stop()
    assert outcome.success, outcome


def test_the_tls_floor_fails_closed_if_botocore_moves_the_context() -> None:
    from tributary.loaders.errors import ConfigError
    from tributary.loaders.s3.client import _harden_tls

    with pytest.raises(ConfigError):
        _harden_tls(object())
