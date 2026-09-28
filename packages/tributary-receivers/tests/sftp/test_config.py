"""`sftp_settings_from_channel_spec` against the real contract example: accept what the poller can
serve, refuse (at startup) what it cannot or what would make it unsafe."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.sftp.config import SftpSettings, sftp_settings_from_channel_spec

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
PIN = "SHA256:" + "A" * 43


def load() -> dict[str, Any]:
    spec = json.loads((EXAMPLES / "sftp-drop-to-s3-and-sftp.json").read_text(encoding="utf-8"))
    spec["spec"]["source"]["sftp"]["hostKeys"] = [PIN]
    return copy.deepcopy(spec)


def build(spec: dict[str, Any], **kw: Any) -> SftpSettings:
    return sftp_settings_from_channel_spec(spec, env="dev", bootstrap_servers="k:9092", **kw)


def test_contract_example_is_accepted() -> None:
    cfg = build(load())
    assert cfg.host == "sftp.partner.example.org" and cfg.port == 22
    assert cfg.host_keys == frozenset({PIN})
    assert cfg.after_commit == "move" and cfg.archive_directory == "/outbound/lab/done"
    assert cfg.reject_directory == "/outbound/lab/rejected"
    assert cfg.nack_on == frozenset({"parse-error"})
    assert cfg.key_expression is None


def test_secret_ref_is_not_in_repr() -> None:
    assert "partner-sftp" not in repr(build(load()))


def test_a_malformed_host_key_pin_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["sftp"]["hostKeys"] = ["SHA256:tooshort", "MD5:aa:bb"]
    with pytest.raises(ConfigError, match="hostKeys"):
        build(spec)


def test_missing_host_keys_is_refused_there_is_no_tofu() -> None:
    spec = load()
    del spec["spec"]["source"]["sftp"]["hostKeys"]
    with pytest.raises(ConfigError, match="hostKeys"):
        build(spec)


@pytest.mark.parametrize("nack_on", [["queue-full"], ["spec-invalid"], ["parse-error", "queue-full"]])
def test_nack_conditions_a_poller_cannot_produce_are_refused(nack_on: list[str]) -> None:
    spec = load()
    spec["spec"]["source"]["ack"]["nackOn"] = nack_on
    with pytest.raises(ConfigError, match="nackOn"):
        build(spec)


def test_move_without_archive_directory_is_refused() -> None:
    spec = load()
    del spec["spec"]["source"]["sftp"]["archiveDirectory"]
    with pytest.raises(ConfigError, match="archiveDirectory"):
        build(spec)


@pytest.mark.parametrize("field", ["archiveDirectory", "rejectDirectory"])
def test_output_directory_equal_to_the_source_directory_is_refused(field: str) -> None:
    spec = load()
    spec["spec"]["source"]["sftp"][field] = "/outbound/lab/"
    with pytest.raises(ConfigError, match="differ"):
        build(spec)


def test_archive_equal_to_reject_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["sftp"]["rejectDirectory"] = spec["spec"]["source"]["sftp"]["archiveDirectory"]
    with pytest.raises(ConfigError, match="differ"):
        build(spec)


@pytest.mark.parametrize("bad", ["/a/../b", "/a/./b", "relative/dir"])
def test_directory_traversal_and_relative_paths_are_refused(bad: str) -> None:
    spec = load()
    spec["spec"]["source"]["sftp"]["directory"] = bad
    with pytest.raises(ConfigError, match="absolute"):
        build(spec)


def test_instance_must_not_contain_the_claim_delimiter() -> None:
    with pytest.raises(ConfigError, match="instance"):
        build(load(), instance="sftp.0")


def test_oversize_limit_above_the_inline_limit_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["maxMessageBytes"] = 1_000_001
    with pytest.raises(ConfigError, match="inline limit"):
        build(spec)


def test_per_key_needs_a_scheme_that_applies_to_sftp() -> None:
    spec = load()
    spec["spec"]["ordering"] = "per-key"
    spec["spec"]["partitionKey"] = "header:X-Patient"
    with pytest.raises(ConfigError):
        build(spec)
    spec["spec"]["partitionKey"] = "hl7:PID.3.1"
    assert build(spec).key_expression is not None


def test_source_type_must_be_sftp() -> None:
    spec = load()
    spec["spec"]["source"]["type"] = "mllp"
    with pytest.raises(ConfigError, match="sftp"):
        build(spec)
