"""`http_destination_config_from_channel_spec` against the real `contracts/channel-spec/v0`
examples: refuses what it cannot honour (unsupported auth, retry-topic) rather than silently
ignoring it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.http.config import http_destination_config_from_channel_spec

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load(name: str) -> dict:
    """The authored examples omit `metadata.version` (the schema allows that: one schema validates
    both the authored and published forms). `ChannelSpec.from_document` parses only the stricter
    published shape (`contracts/channel-spec/v0` README section 6, rule 1), so tests that reuse
    these fixtures stand in for the control plane and stamp a version, the way publishing would."""
    spec = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    return spec


def test_fhir_server_destination_from_the_mtls_example() -> None:
    # the example's own auth (oauth2-client-credentials) is unsupported in v0 (tested below);
    # swapped for bearer here so the rest of the destination's shape can be checked in one place.
    spec = load("adt-to-fhir-per-key-mtls.json")
    spec["spec"]["destinations"][0]["with"]["auth"] = {"type": "bearer", "secretRef": "fhir-oauth"}
    cfg = http_destination_config_from_channel_spec(spec, "fhir-server")
    assert cfg.channel == "adt-to-fhir"
    assert cfg.destination == "fhir-server"
    assert cfg.url == "https://fhir.example.org/fhir"
    assert cfg.method == "POST"
    assert cfg.timeout_s == 8.0
    assert cfg.idempotency_technique == "idempotency-key-header"
    assert cfg.idempotency_header == "Idempotency-Key"
    assert cfg.max_attempts == 10
    assert cfg.on_exhaust == "halt-partition"
    assert cfg.circuit_breaker is not None
    assert cfg.circuit_breaker.failure_threshold == 10


def test_oauth2_auth_is_refused_in_v0() -> None:
    spec = load("adt-to-fhir-per-key-mtls.json")
    assert spec["spec"]["destinations"][0]["with"]["auth"]["type"] == "oauth2-client-credentials"
    with pytest.raises(ConfigError, match=r"auth\.type"):
        http_destination_config_from_channel_spec(spec, "fhir-server")


def test_dwh_destination_sink_native_idempotency_needs_no_header() -> None:
    cfg = http_destination_config_from_channel_spec(load("published-with-version.json"), "dwh")
    assert cfg.idempotency_technique == "sink-native"
    assert cfg.idempotency_header is None
    assert cfg.max_attempts == 20
    assert cfg.on_exhaust == "halt-partition"


def test_fanout_destinations_conditional_update_and_idempotency_key_header() -> None:
    spec = load("two-destinations-fanout.json")
    primary = http_destination_config_from_channel_spec(spec, "primary")
    assert primary.idempotency_technique == "conditional-update"
    assert primary.on_exhaust == "halt-partition"
    analytics = http_destination_config_from_channel_spec(spec, "analytics")
    assert analytics.idempotency_technique == "idempotency-key-header"
    assert analytics.idempotency_header == "X-Idempotency-Key"
    assert analytics.on_exhaust == "dlq-and-continue"


def test_walking_skeleton_none_idempotency() -> None:
    cfg = http_destination_config_from_channel_spec(load("walking-skeleton-loopback.json"), "http-stub")
    assert cfg.idempotency_technique == "none"
    assert cfg.url == "http://127.0.0.1:8080/ingest"


def test_unknown_destination_name_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="no destination"):
        http_destination_config_from_channel_spec(load("walking-skeleton-loopback.json"), "does-not-exist")


def test_retry_topic_is_refused_by_the_shared_channel_spec_parser() -> None:
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["destinations"][0]["retry"]["policy"] = "retry-topic"
    with pytest.raises(ConfigError, match="retry-topic"):
        http_destination_config_from_channel_spec(spec, "http-stub")


def test_mtls_auth_type_is_refused() -> None:
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["destinations"][0]["with"]["auth"] = {"type": "mtls", "secretRef": "x"}
    with pytest.raises(ConfigError, match=r"auth\.type"):
        http_destination_config_from_channel_spec(spec, "http-stub")
