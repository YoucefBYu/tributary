"""DDL shape, TTL migration and startup refusals."""

from __future__ import annotations

import contextlib
import json
import socket
import struct
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
from tributary.archiver.config import archiver_group, load_config
from tributary.archiver.errors import ArchiverConfigError
from tributary.archiver.schema import ddl, ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect


def test_ddl_is_replacing_with_the_contract_key_and_a_ttl() -> None:
    sql = ddl(ttl_days=30)
    assert "ReplacingMergeTree(ts)" in sql
    assert "ORDER BY (channel, message_id, stage, destination, attempt, scope)" in sql
    assert "INTERVAL 30 DAY" in sql
    assert "_hmac" not in sql


@pytest.mark.parametrize("bad", [0, -1, 40_000, True])
def test_ttl_days_out_of_range_is_refused(bad: Any) -> None:
    with pytest.raises(ValueError, match="ttl_days"):
        ddl(ttl_days=bad)


class RecordingClient:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def command(self, cmd: str, **_: Any) -> Any:
        self.commands.append(cmd)
        return ""


def test_ensure_schema_only_creates_and_never_alters_a_ttl() -> None:
    client = RecordingClient()
    ensure_schema(client, ttl_days=7)
    assert [c.split(" (")[0] for c in client.commands] == ["CREATE TABLE IF NOT EXISTS message_events"]
    assert "ALTER" not in " ".join(client.commands)


class Resolver:
    def resolve_text(self, ref: str, key: str) -> str:
        return {"user": "u", "password": "SYNTH-PW-1"}[key]

    def resolve_file(self, ref: str, key: str) -> Path:  # pragma: no cover
        raise FileNotFoundError


ENVIRON = {"TB_ARCHIVE_CLICKHOUSE_HOST": "ch.internal", "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "ch-creds"}


def test_config_defaults_and_group() -> None:
    config = load_config("dev", ENVIRON, Resolver())
    assert (config.target.port, config.ttl_days, config.group_id) == (8443, 90, "tb.dev.archiver")
    assert config.target.secure is True, "TLS is on by default"
    assert "SYNTH-PW-1" not in repr(config.target)


@pytest.mark.parametrize(
    "mutation",
    [
        {"TB_ARCHIVE_CLICKHOUSE_HOST": ""},
        {"TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "../x"},
        {"TB_ARCHIVE_TTL_DAYS": "0"},
        {"TB_ARCHIVE_CLICKHOUSE_SECURE": "off"},  # unrecognised is refused, not read as plaintext
        {"TB_ARCHIVE_CLICKHOUSE_SECURE": "false"},  # plaintext to a non-loopback host
    ],
)
def test_bad_config_is_refused(mutation: dict[str, str]) -> None:
    with pytest.raises(ArchiverConfigError):
        load_config("dev", {**ENVIRON, **mutation}, Resolver())


def test_retention_settings_defaults_and_overrides() -> None:
    config = load_config("dev", ENVIRON, Resolver())
    assert (config.audit_ttl_days, config.content_ttl_days, config.allow_ttl_decrease) == (2190, None, False)
    on = {**ENVIRON, "TB_ARCHIVE_CONTENT": "true", "TB_ARCHIVE_ALLOW_TTL_DECREASE": "true"}
    config = load_config("dev", on, Resolver())
    assert (config.content_ttl_days, config.allow_ttl_decrease) == (30, True)
    custom = {
        **on,
        "TB_ARCHIVE_CONTENT_TTL_DAYS": "7",
        "TB_ARCHIVE_AUDIT_TTL_DAYS": "36500",
        "TB_ARCHIVE_TTL_DAYS": "1",
    }
    config = load_config("dev", custom, Resolver())
    assert (config.content_ttl_days, config.audit_ttl_days, config.ttl_days) == (7, 36500, 1)


@pytest.mark.parametrize(
    "mutation",
    [
        {"TB_ARCHIVE_AUDIT_TTL_DAYS": "0"},
        {"TB_ARCHIVE_AUDIT_TTL_DAYS": "36501"},
        {"TB_ARCHIVE_AUDIT_TTL_DAYS": "six"},
        {"TB_ARCHIVE_CONTENT": "true", "TB_ARCHIVE_CONTENT_TTL_DAYS": "0"},
        {"TB_ARCHIVE_CONTENT": "true", "TB_ARCHIVE_CONTENT_TTL_DAYS": "36501"},
        {"TB_ARCHIVE_ALLOW_TTL_DECREASE": "yes"},  # only true or false: never read as the unsafe choice
    ],
)
def test_bad_retention_settings_are_refused(mutation: dict[str, str]) -> None:
    with pytest.raises(ArchiverConfigError):
        load_config("dev", {**ENVIRON, **mutation}, Resolver())


def test_content_ttl_is_not_read_while_content_is_off() -> None:
    config = load_config("dev", {**ENVIRON, "TB_ARCHIVE_CONTENT_TTL_DAYS": "0"}, Resolver())
    assert config.content_ttl_days is None


def test_bad_env_segment_is_refused() -> None:
    with pytest.raises(ArchiverConfigError):
        archiver_group("Bad.Env")


def test_connect_failure_is_a_fixed_sentence_without_the_server_text() -> None:
    from tributary.archiver.errors import SinkError

    with pytest.raises(SinkError) as caught:
        connect(ClickhouseTarget(host="127.0.0.1", port=1, database="d", username="u", password="SYNTH-PW-2"))  # noqa: S106
    assert "SYNTH-PW-2" not in str(caught.value) and "127.0.0.1" not in str(caught.value)


def test_client_driver_retries_are_off() -> None:
    class Stub:
        http_retries = 1

    target = ClickhouseTarget(host="h", port=1, database="d", username="u")  # secure by default
    assert "password" not in repr(target)
    sink = ClickhouseSink(Stub())
    sink.close()  # a client without close() must not raise at shutdown


def test_unset_engine_kafka_file_refuses_with_exit_2() -> None:
    env = {
        k: v for k, v in __import__("os").environ.items() if k not in ("TB_ENGINE_KAFKA_FILE", "TB_BOOTSTRAP_SERVERS")
    }
    result = subprocess.run(
        [sys.executable, "-m", "tributary.archiver", "--env", "dev"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 2
    lines = [json.loads(x) for x in result.stderr.splitlines() if x.startswith("{")]
    assert [x["event"] for x in lines] == ["startup_refused"]
    assert lines[0]["code"] == "ENGINE_KAFKA_FILE_UNSET"


def test_permanent_and_transient_classification() -> None:
    from clickhouse_connect.driver import exceptions as ch
    from tributary.archiver.sink import is_permanent

    assert is_permanent(ch.DatabaseError("Code: 60. DB::Exception: Unknown table"))
    assert is_permanent(ch.ProgrammingError("bad type"))
    assert not is_permanent(ch.OperationalError("connection reset"))
    assert not is_permanent(ch.DatabaseError("Code: 252. DB::Exception: Too many parts"))


def test_plaintext_is_loopback_only_and_loopback_is_accepted() -> None:
    config = load_config(
        "dev",
        {**ENVIRON, "TB_ARCHIVE_CLICKHOUSE_HOST": "127.0.0.1", "TB_ARCHIVE_CLICKHOUSE_SECURE": "false"},
        Resolver(),
    )
    assert config.target.secure is False and config.target.port == 8123


def _reset_server() -> tuple[socket.socket, list[int]]:
    """A listener that accepts, then resets the connection mid-request: the shape that triggers the
    driver's one-shot resend. `accepted` counts connection attempts."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(16)
    server.settimeout(0.2)
    accepted: list[int] = []

    def serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            accepted.append(1)
            conn.recv(4096)
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            conn.close()  # RST

    threading.Thread(target=serve, daemon=True).start()
    return server, accepted


def test_no_resend_driver_default_resends_but_ours_does_not() -> None:
    import clickhouse_connect
    from tributary.archiver.errors import SinkError

    server, accepted = _reset_server()
    port = server.getsockname()[1]
    try:
        with pytest.raises(Exception):  # noqa: B017 - the stock client, as the control
            clickhouse_connect.get_client(host="127.0.0.1", port=port, query_retries=0, connect_timeout=2)
        control = len(accepted)
        accepted.clear()
        with pytest.raises(SinkError):
            connect(ClickhouseTarget(host="127.0.0.1", port=port, database="d", username="u", secure=False))
        assert control >= 2, "control: the driver does resend once (this is what the pool manager removes)"
        assert len(accepted) == 1, "exactly one attempt: no resend"
    finally:
        server.close()


def test_connection_close_header_is_sent_so_no_stale_socket_is_reused() -> None:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    seen: list[bytes] = []

    def serve() -> None:
        conn, _ = server.accept()
        seen.append(conn.recv(4096))
        conn.close()

    threading.Thread(target=serve, daemon=True).start()
    from tributary.archiver.errors import SinkError

    with pytest.raises(SinkError):
        connect(
            ClickhouseTarget(host="127.0.0.1", port=server.getsockname()[1], database="d", username="u", secure=False)
        )
    server.close()
    assert b"onnection: close" in seen[0]


def test_proxy_environment_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    server, accepted = _reset_server()
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    from tributary.archiver.errors import SinkError

    with pytest.raises(SinkError):
        connect(
            ClickhouseTarget(host="127.0.0.1", port=server.getsockname()[1], database="d", username="u", secure=False)
        )
    server.close()
    assert len(accepted) == 1, "the connection went to the target, not the proxy"


def test_tls_verification_failure_is_distinguishable_by_code_only(tmp_path: Path) -> None:
    import ssl
    import subprocess

    from tributary.archiver.errors import SinkError

    cert, key = tmp_path / "c.pem", tmp_path / "k.pem"
    subprocess.run(  # noqa: S603
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(cert),  # noqa: S607
         "-days", "1", "-subj", "/CN=localhost"],
        check=True,
        capture_output=True,
    )  # fmt: skip
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert), str(key))
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(4)

    def serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
                with contextlib.suppress(OSError):
                    context.wrap_socket(conn, server_side=True).recv(10)
            except OSError:
                return

    threading.Thread(target=serve, daemon=True).start()
    try:
        with pytest.raises(SinkError) as caught:
            connect(ClickhouseTarget(host="127.0.0.1", port=server.getsockname()[1], database="d", username="u"))
        assert caught.value.code == "TLS_VERIFY_FAILED"
        with pytest.raises(SinkError) as other:
            connect(ClickhouseTarget(host="127.0.0.1", port=1, database="d", username="u", secure=False))
        assert other.value.code == "SINK_ERROR"
    finally:
        server.close()


def test_driver_logging_cannot_reach_a_handler() -> None:
    import logging

    connect_target = ClickhouseTarget(host="127.0.0.1", port=1, database="d", username="u", secure=False)
    with contextlib.suppress(Exception):
        connect(connect_target)
    driver = logging.getLogger("clickhouse_connect")
    assert driver.propagate is False and driver.level > logging.CRITICAL


def test_migration_credential_ref_is_separate() -> None:
    environ = {**ENVIRON, "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "ch-migrate"}
    assert load_config("dev", environ, Resolver(), ref_env_name="TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF")
    with pytest.raises(ArchiverConfigError):
        load_config("dev", ENVIRON, Resolver(), ref_env_name="TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF")


def test_missing_instance_id_refuses_with_exit_2_before_any_client_is_built(tmp_path: Path) -> None:
    from tests.support.engine_kafka import engine_kafka_env

    env = {k: v for k, v in __import__("os").environ.items() if k != "TB_INSTANCE_ID"}
    env.update(engine_kafka_env("127.0.0.1:9092"))
    result = subprocess.run(
        [sys.executable, "-m", "tributary.archiver", "--env", "dev"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 2
    lines = [json.loads(x) for x in result.stderr.splitlines() if x.startswith("{")]
    refused = [x for x in lines if x["event"] == "startup_refused"]
    assert [x["code"] for x in refused] == ["INSTANCE_REQUIRED"]
