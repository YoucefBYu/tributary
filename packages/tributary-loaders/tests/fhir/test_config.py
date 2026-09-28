"""`fhir_destination_config_from_destination` refuses what this loader cannot honour."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.fhir.config import fhir_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def make_destination(params: dict[str, object] | None = None, technique: str = "conditional-update") -> Destination:
    base: dict[str, object] = {
        "url": "https://fhir.example.org/fhir",
        "fhirVersion": "4.0.1",
        "interaction": "transaction",
        "timeoutMs": 5000,
        "auth": {"type": "bearer", "secretRef": "fhir-token"},
    }
    base.update(params or {})
    return Destination(
        name="fhir-server",
        type="fhir",
        params=base,
        idempotency=Idempotency(technique=technique),
        retry=RetryPolicy(policy="blocking", max_attempts=20, on_exhaust="halt-partition"),
    )


def build(params: dict[str, object] | None = None, technique: str = "conditional-update") -> object:
    return fhir_destination_config_from_destination("c", make_destination(params, technique))


def test_builds_from_a_valid_destination() -> None:
    cfg = fhir_destination_config_from_destination("c", make_destination())
    assert cfg.url == "https://fhir.example.org/fhir"
    assert cfg.timeout_s == 5.0
    assert cfg.auth is not None and cfg.auth.type == "bearer"
    assert cfg.idempotency_technique == "conditional-update"


def test_the_published_example_is_refused_only_for_its_oauth2_auth() -> None:
    spec = json.loads((EXAMPLES / "adt-mllp-to-fhir-clickhouse-mllp.json").read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    destination = ChannelSpec.from_document(spec).destination("fhir-server")
    with pytest.raises(ConfigError, match="oauth2-client-credentials"):
        fhir_destination_config_from_destination("c", destination)


def test_empty_circuit_breaker_block_means_configured_with_defaults() -> None:
    cfg = fhir_destination_config_from_destination("c", make_destination({"circuitBreaker": {}}))
    assert cfg.circuit_breaker.failure_threshold == 5
    assert cfg.circuit_breaker.open_seconds == 30.0


def test_explicit_circuit_breaker_is_honoured() -> None:
    cfg = fhir_destination_config_from_destination(
        "c", make_destination({"circuitBreaker": {"failureThreshold": 2, "openSeconds": 7}})
    )
    assert (cfg.circuit_breaker.failure_threshold, cfg.circuit_breaker.open_seconds) == (2, 7.0)


@pytest.mark.parametrize("host", ["fhir.example.org", "10.0.0.5", "127.0.0.1.evil.example"])
def test_plaintext_http_to_a_non_loopback_host_is_refused_at_runtime(host: str) -> None:
    with pytest.raises(ConfigError, match="plaintext"):
        build({"url": f"http://{host}/fhir"})


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "[::1]"])
def test_plaintext_http_to_loopback_is_allowed(host: str) -> None:
    assert build({"url": f"http://{host}:8080/fhir"}) is not None


@pytest.mark.parametrize(
    "url",
    [
        "https://user:pw@fhir.example.org/fhir",
        "https://fhir.example.org/fhir?x=1",
        "https://fhir.example.org/fhir#frag",
        "https://fhir.example.org/fhir?",
        "ftp://fhir.example.org/fhir",
        "https:///fhir",
    ],
)
def test_bad_urls_are_refused_without_echoing_the_url(url: str) -> None:
    with pytest.raises(ConfigError) as exc:
        build({"url": url})
    assert "pw" not in str(exc.value)


@pytest.mark.parametrize("technique", ["sink-native", "deterministic-name"])
def test_techniques_the_fhir_destination_cannot_honour_are_refused(technique: str) -> None:
    with pytest.raises(ConfigError, match="not permitted"):
        build(technique=technique)


def test_interaction_other_than_transaction_is_refused() -> None:
    with pytest.raises(ConfigError, match="transaction only"):
        build({"interaction": "batch"})


def test_credential_static_headers_are_refused() -> None:
    with pytest.raises(ConfigError, match="credential"):
        build({"headers": {"Authorization": "Bearer x"}})


def test_unsupported_auth_type_is_refused() -> None:
    with pytest.raises(ConfigError, match="not supported"):
        build({"auth": {"type": "mtls", "secretRef": "x"}})


def test_missing_timeout_is_refused() -> None:
    params = {"timeoutMs": 0}
    with pytest.raises(ConfigError, match="timeoutMs"):
        build(params)
