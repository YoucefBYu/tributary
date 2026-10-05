"""The receivers' structured lines match `contracts/observability/v0/log-line.schema.json` (section 5, and the
architect's request to move the `print(json.dumps(...))` lines to `JsonLogger`): every line has `ts`, `level`,
`event` and `component`, with the same event names and fields as before."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from tributary.receivers.events import event_logger
from tributary.receivers.lifecycle import json_emitter
from tributary.receivers.mllp.server import emit_lite_profile_warning
from tributary.receivers.tls import log_peer_refused

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[3] / "contracts" / "observability" / "v0" / "log-line.schema.json").read_text(
        encoding="utf-8"
    )
)
MSG_ID = "01a10e47-9041-79e2-8cbc-38d64ead86e3"


def lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().err.splitlines() if line.startswith("{")]


def validate(record: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(SCHEMA).validate(record)


def test_the_event_lines_the_modules_used_to_print_validate_against_the_schema(
    capsys: pytest.CaptureFixture[str],
) -> None:
    emit_lite_profile_warning(profile="lite")
    log_peer_refused("lab-in", None)
    http = event_logger("receiver-http")
    http.info("http_access", channel="lab-in", method="POST", status=202, bytes=71, tb_msg_id=MSG_ID)
    http.info("http_access", channel="lab-in", method="POST", status=404, bytes=0)
    sftp = event_logger("receiver-sftp")
    sftp.info("sftp_committed", channel="lab-in", tb_msg_id=MSG_ID, bytes=10, after="delete")
    sftp.info("sftp_rejected", channel="lab-in", reason="parse-error")
    sftp.info("sftp_connect_failed", channel="lab-in", error="OSError")
    kafka = event_logger("receiver-kafka")
    kafka.info("kafka_source_error", channel="lab-in", code="_TRANSPORT")
    kafka.info(
        "kafka_source_partition_halted", channel="lab-in", topic="up.events", partition=0, offset=8, reason="oversize"
    )
    emitted: list[dict[str, Any]] = []
    json_emitter(event_logger("receiver-mllp"))(
        {
            "event": "listening",
            "channel": "lab-in",
            "channel_version": 1,
            "topic": "dev.lab-in.raw",
            "port": 2575,
            "tls": False,
            "ops_port": 9180,
        }
    )
    seen = lines(capsys)
    assert len(seen) == 10 and emitted == []
    for record in seen:
        validate(record)
        assert {"ts", "level", "event", "component"} <= set(record)
    listening = seen[-1]
    assert listening["event"] == "listening" and listening["port"] == 2575 and listening["ops_port"] == 9180


def test_a_field_named_payload_is_refused_by_the_guard() -> None:
    from tributary.sdk.guards import LogFieldError

    with pytest.raises(LogFieldError):
        event_logger("receiver-http").info("http_access", payload="x")


def test_no_module_prints_a_json_line_any_more() -> None:
    src = Path(__file__).resolve().parents[1] / "src" / "tributary" / "receivers"
    offenders = [
        str(path.relative_to(src))
        for path in src.rglob("*.py")
        if re.search(r"^\s*print\(", path.read_text(encoding="utf-8"), re.MULTILINE)
    ]
    assert offenders == []
