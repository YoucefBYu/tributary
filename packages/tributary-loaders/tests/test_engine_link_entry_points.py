"""Every loader entry point takes its engine Kafka link only from `TB_ENGINE_KAFKA_FILE` (ADR-008 D3):
an unset file, the retired `TB_BOOTSTRAP_SERVERS`, and a non-loopback `PLAINTEXT` document each exit 2 with
one `startup_refused` line, before any Kafka client is built."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tests.support.engine_kafka import write_engine_kafka_file

LOADERS = ["postgresql", "s3", "kafka", "clickhouse", "mllp", "http", "fhir", "sftp"]


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the engine link was accepted")


@pytest.fixture
def entry(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    module = importlib.import_module(f"tributary.loaders.{request.param}.main")
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.delenv("TB_ENGINE_KAFKA_FILE", raising=False)
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("sys.argv", ["prog", "--channel", "c", "--destination", "d", "--env", "dev"])
    return module.main  # type: ignore[no-any-return]


def _refused(exc: pytest.ExceptionInfo[SystemExit], capsys: pytest.CaptureFixture[str], code: str) -> None:
    assert exc.value.code == 2
    lines = [json.loads(x) for x in capsys.readouterr().err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == code


@pytest.mark.parametrize("entry", LOADERS, indirect=True)
def test_missing_engine_kafka_file_exits_2(entry: Callable[[], None], capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        entry()
    _refused(exc, capsys, "ENGINE_KAFKA_FILE_UNSET")


@pytest.mark.parametrize("entry", LOADERS, indirect=True)
def test_non_loopback_plaintext_file_exits_2(
    entry: Callable[[], None], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "kafka:9092")))
    with pytest.raises(SystemExit) as exc:
        entry()
    _refused(exc, capsys, "ENGINE_KAFKA_DOCUMENT_INVALID")


@pytest.mark.parametrize("entry", LOADERS, indirect=True)
def test_retired_bootstrap_environment_exits_2(
    entry: Callable[[], None], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.setenv("TB_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    with pytest.raises(SystemExit) as exc:
        entry()
    _refused(exc, capsys, "ENGINE_KAFKA_RETIRED_INPUT")


RETIRED_FLAG_VALUE = "SYNTH-RETIRED-VALUE-91c2"


@pytest.mark.parametrize("entry", LOADERS, indirect=True)
@pytest.mark.parametrize("flag", ["--bootstrap", "--engine-security"])
@pytest.mark.parametrize("form", ["separate", "equals"])
def test_retired_flag_is_refused_before_argparse_and_not_echoed(
    entry: Callable[[], None],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    flag: str,
    form: str,
) -> None:
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    extra = [f"{flag}={RETIRED_FLAG_VALUE}"] if form == "equals" else [flag, RETIRED_FLAG_VALUE]
    monkeypatch.setattr("sys.argv", ["prog", "--channel", "c", "--destination", "d", "--env", "dev", *extra])
    with pytest.raises(SystemExit) as exc:
        entry()
    err = capsys.readouterr()
    assert RETIRED_FLAG_VALUE not in err.err + err.out
    assert "unrecognized arguments" not in err.err
    assert exc.value.code == 2
    lines = [json.loads(x) for x in err.err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "ENGINE_KAFKA_RETIRED_INPUT"


@pytest.mark.parametrize("entry", LOADERS, indirect=True)
@pytest.mark.parametrize("instance", ["_bad", "has space", "a/b", "x" * 129, "SYNTH-PHI\nMARKER"])
def test_bad_instance_id_exits_2_with_one_phi_free_line(
    entry: Callable[[], None],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    instance: str,
) -> None:
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.setattr(
        "sys.argv", ["prog", "--channel", "c", "--destination", "d", "--env", "dev", "--instance-id", instance]
    )
    with pytest.raises(SystemExit) as exc:
        entry()
    err = capsys.readouterr().err
    assert exc.value.code == 2
    assert "Traceback" not in err and "MARKER" not in err
    lines = [json.loads(x) for x in err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "TOPIC_NAME_INVALID"


def test_spec_reader_group_is_inside_the_acl_granted_prefix() -> None:
    from tributary.loaders.startup import spec_reader_group_or_exit
    from tributary.runtime.logs import JsonLogger

    group = spec_reader_group_or_exit("dev", "adt-passthrough", "loader-http-stub-42", JsonLogger("t"))
    assert group == "tb.dev.adt-passthrough.channels.loader-http-stub-42"
    assert group.startswith("tb.dev.adt-passthrough.channels.")  # ACL PREFIXED grant, topics README 3.4.1
