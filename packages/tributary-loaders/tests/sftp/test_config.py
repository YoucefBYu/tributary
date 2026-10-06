"""`sftp_destination_config_from_destination` refuses what this loader cannot honour."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.sftp.config import SftpDestinationConfig, sftp_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
PIN = "SHA256:" + "A" * 43


def make_destination(params: dict[str, object] | None = None, technique: str = "deterministic-name") -> Destination:
    base: dict[str, object] = {
        "host": "sftp.example.org",
        "hostKeys": [PIN],
        "auth": {"type": "private-key", "secretRef": "sftp-out"},
        "directory": "/outbound/hl7",
        "timeoutMs": 5000,
    }
    base.update(params or {})
    return Destination(
        name="sftp-out",
        type="sftp",
        params=base,
        idempotency=Idempotency(technique=technique),
        retry=RetryPolicy(policy="blocking", max_attempts=20, on_exhaust="halt-partition"),
    )


def build(params: dict[str, object] | None = None, technique: str = "deterministic-name") -> SftpDestinationConfig:
    return sftp_destination_config_from_destination("c", make_destination(params, technique))


def test_builds_with_defaults() -> None:
    cfg = build()
    assert (cfg.host, cfg.port, cfg.directory) == ("sftp.example.org", 22, "/outbound/hl7")
    assert cfg.host_keys == (PIN,)
    assert cfg.file_name_prefix == "" and cfg.file_extension == ""
    assert cfg.timeout_s == 5.0


def test_a_published_example_with_an_sftp_destination_builds() -> None:
    for path in sorted(EXAMPLES.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        spec["metadata"].setdefault("version", 0)
        for destination in spec.get("spec", {}).get("destinations", []):
            if destination.get("type") == "sftp":
                parsed = ChannelSpec.from_document(spec).destination(destination["name"])
                assert sftp_destination_config_from_destination("c", parsed).host_keys


def test_empty_circuit_breaker_block_means_configured_with_defaults() -> None:
    cfg = build({"circuitBreaker": {}})
    assert (cfg.circuit_breaker.failure_threshold, cfg.circuit_breaker.open_seconds) == (5, 30.0)


def test_directory_is_normalised_without_a_trailing_slash() -> None:
    assert build({"directory": "/out/"}).directory == "/out"
    assert build({"directory": "/"}).directory == "/"


@pytest.mark.parametrize("pins", [None, [], ["SHA256:short"], [PIN, PIN], [PIN] * 5, ["MD5:aa:bb"], "SHA256:x", [1]])
def test_host_key_pins_are_mandatory_and_well_formed(pins: object) -> None:
    params: dict[str, object] = {"hostKeys": pins}
    if pins is None:
        destination = make_destination()
        del destination.params["hostKeys"]
    else:
        destination = make_destination(params)
    with pytest.raises(ConfigError, match=r"hostKeys|missing"):
        sftp_destination_config_from_destination("c", destination)


def test_pin_text_is_not_echoed() -> None:
    with pytest.raises(ConfigError) as exc:
        build({"hostKeys": ["SHA256:" + "Z" * 10]})
    assert "ZZZZ" not in str(exc.value)


@pytest.mark.parametrize("directory", ["out", "/a/../b", "/a/./b", "/a b", "", "/" + "x" * 300, "/a//b"])
def test_unsafe_directories_are_refused(directory: str) -> None:
    with pytest.raises(ConfigError, match="directory"):
        build({"directory": directory})


@pytest.mark.parametrize(
    ("key", "value"),
    [("fileNamePrefix", "../x"), ("fileNamePrefix", "a/b"), ("fileExtension", "hl7"), ("fileExtension", ".H L7")],
)
def test_unsafe_name_parts_are_refused(key: str, value: str) -> None:
    with pytest.raises(ConfigError):
        build({key: value})


@pytest.mark.parametrize("technique", ["sink-native", "none", "conditional-update", "idempotency-key-header"])
def test_only_deterministic_name_is_declarable(technique: str) -> None:
    with pytest.raises(ConfigError, match="not permitted"):
        build(technique=technique)


def test_auth_needs_a_known_type_and_a_secret_ref() -> None:
    with pytest.raises(ConfigError, match="auth"):
        build({"auth": {"type": "none", "secretRef": "x"}})
    with pytest.raises(ConfigError, match="auth"):
        build({"auth": {"type": "password"}})


def test_bad_host_and_port_and_timeout_are_refused() -> None:
    with pytest.raises(ConfigError, match="host"):
        build({"host": "bad host"})
    with pytest.raises(ConfigError, match="port"):
        build({"port": 0})
    with pytest.raises(ConfigError, match="timeoutMs"):
        build({"timeoutMs": 0})
