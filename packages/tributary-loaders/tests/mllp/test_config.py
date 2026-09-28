"""`mllp_destination_config_from_destination`: refuses what this loader cannot honour, mirroring
`tests/clickhouse/test_config.py`'s shape for the `mllpParams` schema instead."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.mllp.config import mllp_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load_example(name: str, destination_name: str) -> Destination:
    spec = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    return ChannelSpec.from_document(spec).destination(destination_name)


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "downstream-lis",
        "type": "mllp",
        "params": {
            "host": "lis.example.org",
            "port": 2575,
            "tls": {"secretRef": "lis-mllp-ca", "minVersion": "1.2"},
            "timeoutMs": 10000,
            "onNegativeAck": "fail",
        },
        "idempotency": Idempotency(technique="none", note="the LIS is idempotent by visit"),
        "retry": RetryPolicy(policy="blocking", max_attempts=10, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def test_builds_config_from_the_published_example() -> None:
    destination = load_example("adt-mllp-to-fhir-clickhouse-mllp.json", "downstream-lis")
    cfg = mllp_destination_config_from_destination("adt-to-fhir-and-analytics", destination)
    assert cfg.host == "lis.example.org"
    assert cfg.port == 2575
    assert cfg.tls is not None
    assert cfg.tls.secret_ref == "lis-mllp-ca"  # noqa: S105 - a secretRef name, not a credential value
    assert cfg.tls.min_version == "1.2"
    assert cfg.timeout_s == 10.0
    assert cfg.on_negative_ack == "fail"
    assert cfg.idempotency_technique == "none"


def test_wrong_type_is_refused() -> None:
    with pytest.raises(ConfigError, match="not 'mllp'"):
        mllp_destination_config_from_destination("c", make_destination(type="http"))


def test_sink_native_is_permitted() -> None:
    cfg = mllp_destination_config_from_destination(
        "c", make_destination(idempotency=Idempotency(technique="sink-native"))
    )
    assert cfg.idempotency_technique == "sink-native"


@pytest.mark.parametrize("technique", ["conditional-update", "deterministic-name", "idempotency-key-header"])
def test_other_techniques_are_refused(technique: str) -> None:
    """README 5.1: `mllp` may only declare `sink-native` or `none` -- an MLLP receiver gives back one
    MSA reply, nothing this loader can attach a second identifier to without rewriting MSH."""
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        mllp_destination_config_from_destination(
            "c",
            make_destination(
                idempotency=Idempotency(technique=technique, header="X-Id" if "header" in technique else None)
            ),
        )


def test_tls_is_required_for_a_non_loopback_host() -> None:
    params = {"host": "lis.example.org", "port": 2575, "timeoutMs": 10000, "onNegativeAck": "retry"}
    with pytest.raises(ConfigError, match=r"with\.tls is required"):
        mllp_destination_config_from_destination("c", make_destination(params=params))


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_tls_is_optional_for_a_loopback_host(host: str) -> None:
    params = {"host": host, "port": 2575, "timeoutMs": 10000, "onNegativeAck": "retry"}
    cfg = mllp_destination_config_from_destination("c", make_destination(params=params))
    assert cfg.tls is None


def test_on_negative_ack_must_be_retry_or_fail() -> None:
    params = {
        "host": "localhost",
        "port": 2575,
        "timeoutMs": 10000,
        "onNegativeAck": "ignore",
    }
    with pytest.raises(ConfigError, match="onNegativeAck"):
        mllp_destination_config_from_destination("c", make_destination(params=params))


def test_circuit_breaker_is_optional() -> None:
    destination = load_example("adt-mllp-to-fhir-clickhouse-mllp.json", "downstream-lis")
    cfg = mllp_destination_config_from_destination("c", destination)
    assert cfg.circuit_breaker is None
