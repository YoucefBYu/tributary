"""`python -m tributary.receivers.mllp`: the retired `--channel-spec` input (invariant 3), and the parity of
the two ways to build settings from a spec."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.mllp import server
from tributary.receivers.mllp.config import mllp_settings_from_channel_spec, mllp_settings_from_spec
from tributary.runtime.spec import ChannelSpec

from tests.support.channel_spec import walking_skeleton_document
from tests.support.engine_kafka import write_engine_kafka_file

VALUE = "SYNTH-SPEC-PATH-5d1e"


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the flag was refused")


@pytest.mark.parametrize("form", ["separate", "equals"])
def test_channel_spec_flag_is_refused_with_a_stable_code_and_not_echoed(
    form: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    flag = [f"--channel-spec={VALUE}"] if form == "equals" else ["--channel-spec", VALUE]
    monkeypatch.setattr("sys.argv", ["prog", *flag, "--channel", "adt-passthrough", "--env", "dev"])
    with pytest.raises(SystemExit) as exc:
        server.main()
    captured = capsys.readouterr()
    assert exc.value.code == 2
    assert VALUE not in captured.out + captured.err
    assert "unrecognized arguments" not in captured.err
    lines = [json.loads(x) for x in captured.err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "CHANNEL_SPEC_FLAG_RETIRED"


def test_a_bad_instance_id_is_refused_before_any_client_is_built(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr(
        "sys.argv", ["prog", "--channel", "adt-passthrough", "--env", "dev", "--instance", "bad instance!"]
    )
    with pytest.raises(SystemExit) as exc:
        server.main()
    err = capsys.readouterr().err
    assert exc.value.code == 2 and "bad instance" not in err
    assert len([x for x in err.splitlines() if "startup_refused" in x]) == 1


def test_settings_from_the_runtime_spec_equal_settings_from_the_document() -> None:
    document = walking_skeleton_document(mllp_port=6661, http_url="http://127.0.0.1:1/x", version=4)
    document["spec"]["source"]["ack"]["nackOn"] = ["queue-full"]
    document["spec"]["source"]["maxMessageBytes"] = 4096
    assert mllp_settings_from_spec(ChannelSpec.from_document(document), env="dev") == mllp_settings_from_channel_spec(
        document, env="dev"
    )
