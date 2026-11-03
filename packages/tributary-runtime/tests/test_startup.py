"""W3-28a: the worker's instance id is required (`--instance-id` or `TB_INSTANCE_ID`), never pid-derived.

`pipeline-<pid>` was identical in every container (pid 1), so replicas with the same `group.instance.id`
fenced each other out of the group. Each refusal here is `startup_refused`, exit 2, before any Kafka client is
built, and no refusal echoes the operator's value.
"""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.errors import TopicNameError
from tributary.runtime.startup import (
    InstanceInvalidError,
    InstanceRequiredError,
    resolve_instance_id,
)
from tributary.runtime.worker import WorkerSettings

from .log_line_schema import assert_lines_conform

MARKER = "SYNTH/bad instance"


def test_the_flag_wins_over_the_environment() -> None:
    assert resolve_instance_id("worker-1", {"TB_INSTANCE_ID": "worker-9"}) == "worker-1"
    assert resolve_instance_id(None, {"TB_INSTANCE_ID": "worker-9"}) == "worker-9"


@pytest.mark.parametrize("flag,environ", [(None, {}), (None, {"TB_INSTANCE_ID": ""}), ("", {"TB_INSTANCE_ID": "w-1"})])
def test_a_missing_or_empty_instance_is_refused(flag: str | None, environ: dict[str, str]) -> None:
    with pytest.raises(InstanceRequiredError) as caught:
        resolve_instance_id(flag, environ)
    assert caught.value.code == "INSTANCE_REQUIRED"


def test_an_instance_that_cannot_be_a_group_segment_is_refused_without_echoing_it() -> None:
    with pytest.raises(InstanceInvalidError) as caught:
        resolve_instance_id(MARKER, {})
    assert "SYNTH" not in str(caught.value)


def test_worker_settings_validate_the_instance_before_any_client() -> None:
    with pytest.raises(TopicNameError):
        WorkerSettings(env="dev", channel="adt-passthrough", kafka=KafkaSettings("127.0.0.1:9092"), instance_id=MARKER)


def test_worker_settings_have_no_instance_default() -> None:
    with pytest.raises(TypeError):
        WorkerSettings(env="dev", channel="adt-passthrough", kafka=KafkaSettings("127.0.0.1:9092"))  # type: ignore[call-arg]


# -- main -----------------------------------------------------------------------------------------


@pytest.fixture
def worker_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """A valid loopback PLAINTEXT engine link and a channel, with no instance and no Kafka client possible."""
    import confluent_kafka

    document = tmp_path / "engine-kafka.json"
    document.write_text(
        json.dumps(
            {
                "apiVersion": "tributary.dev/v1",
                "kind": "EngineKafkaConnection",
                "bootstrapServers": ["127.0.0.1:9092"],
                "security": {"protocol": "PLAINTEXT"},
            }
        ),
        encoding="utf-8",
    )
    for name in ("TB_INSTANCE_ID", "TB_BOOTSTRAP_SERVERS", "TB_OPS_BIND", "TB_OPS_PORT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(document))
    monkeypatch.setenv("TB_CHANNEL", "adt-passthrough")

    def no_client(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("a Kafka client was built before the start was refused")

    monkeypatch.setattr(confluent_kafka, "Consumer", no_client)
    monkeypatch.setattr(confluent_kafka, "Producer", no_client)
    yield


def refused(capsys: pytest.CaptureFixture[str], argv: list[str]) -> dict[str, Any]:
    from tributary.runtime import worker

    with pytest.raises(SystemExit) as exited:
        worker.main(argv)
    assert exited.value.code == 2
    text = capsys.readouterr().err
    assert_lines_conform(text)
    lines = [json.loads(line) for line in text.splitlines() if line.strip()]
    (line,) = [entry for entry in lines if entry["event"] == "startup_refused"]
    assert line["component"] == "worker"
    assert "SYNTH" not in text
    return line


@pytest.mark.usefixtures("worker_env")
def test_main_without_an_instance_refuses_with_instance_required(capsys: pytest.CaptureFixture[str]) -> None:
    line = refused(capsys, [])
    assert line["code"] == "INSTANCE_REQUIRED"
    assert "TB_INSTANCE_ID" in line["reason"]


@pytest.mark.usefixtures("worker_env")
def test_main_with_an_empty_environment_instance_refuses(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TB_INSTANCE_ID", "")
    assert refused(capsys, [])["code"] == "INSTANCE_REQUIRED"


@pytest.mark.usefixtures("worker_env")
def test_main_with_a_bad_instance_flag_refuses_without_echoing_it(capsys: pytest.CaptureFixture[str]) -> None:
    assert refused(capsys, ["--instance-id", MARKER])["code"] == "ARGUMENTS_INVALID"


@pytest.mark.usefixtures("worker_env")
def test_main_with_an_unknown_flag_refuses_without_echoing_it(capsys: pytest.CaptureFixture[str]) -> None:
    line = refused(capsys, ["--instance-id", "worker-0", "--SYNTH-flag"])
    assert line["code"] == "ARGUMENTS_INVALID"


@pytest.mark.usefixtures("worker_env")
def test_main_with_a_bad_ops_port_refuses(capsys: pytest.CaptureFixture[str]) -> None:
    assert refused(capsys, ["--instance-id", "worker-0", "--ops-port", "70000"])["code"] == "ARGUMENTS_INVALID"


@pytest.mark.usefixtures("worker_env")
def test_main_with_a_taken_ops_port_refuses_before_any_client(capsys: pytest.CaptureFixture[str]) -> None:
    """O4: the ops listener binds first; a conflict is exit 2 with `OPS_LISTENER_BIND_FAILED` and an errno."""
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        line = refused(capsys, ["--instance-id", "worker-0", "--ops-port", str(port)])
    assert line["code"] == "OPS_LISTENER_BIND_FAILED" and line["errno"] > 0


@pytest.mark.usefixtures("worker_env")
def test_main_with_an_instance_gets_past_the_instance_check(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Control: with `TB_INSTANCE_ID` set the start proceeds to the first Kafka client (which the fixture stops)."""
    from tributary.runtime import worker

    monkeypatch.setenv("TB_INSTANCE_ID", "worker-0")
    with pytest.raises(AssertionError, match="Kafka client was built"):
        worker.main(["--ops-port", "0"])
    text = capsys.readouterr().err
    assert "startup_refused" not in text
    assert '"ops_listening"' in text
