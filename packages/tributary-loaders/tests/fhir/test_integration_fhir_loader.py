"""`FhirLoader` against a real HAPI FHIR server (`hapiproject/hapi`, test-only, pinned), proving invariant 4
and README 3.4.2 for real: a Bundle delivered twice leaves exactly one Patient and one Encounter, because
every entry is conditional. The Bundle is the golden output of the ADT step (`tests/fixtures/fhir/
adt_a01_admit.bundle.json`, `tributary-content-hl7-fhir`), with the MRN made unique per test.

The genuine-redelivery half (a real `ConsumerLoop` replay from an uncommitted offset) is
`tests/integration/test_fhir_loader_duplicate_delivery_uncommitted_redelivery.py`.

`uv run pytest -m integration packages/tributary-loaders/tests/fhir`.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tributary.loaders.fhir.config import FhirDestinationConfig
from tributary.loaders.fhir.loader import FhirLoader
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.fakes import RecordingLogger

pytestmark = pytest.mark.integration

HAPI_IMAGE = "hapiproject/hapi:v7.4.0"
GOLDEN = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "fhir" / "adt_a01_admit.bundle.json"
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))


@pytest.fixture(scope="module")
def hapi() -> Iterator[str]:
    container = (
        DockerContainer(HAPI_IMAGE)
        .with_exposed_ports(8080)
        .waiting_for(LogMessageWaitStrategy("Started Application in").with_startup_timeout(240))
    )
    with container:
        base = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8080)}/fhir"
        yield base


def bundle_for(mrn: str) -> dict[str, Any]:
    """The golden ADT Bundle with a test-unique MRN (Patient) and a test-unique Encounter identifier."""
    text = GOLDEN.read_text(encoding="utf-8").replace("9000001", mrn)
    text = text.replace("0199036e-7a3e-7f00-8c21-abc123456789", f"synth-{mrn}")
    document: dict[str, Any] = json.loads(text)
    return document


def make_message(document: dict[str, Any]) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/fhir+json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "fhir-server",
        },
        payload=json.dumps(document).encode("utf-8"),
    )


def make_config(base: str, **overrides: object) -> FhirDestinationConfig:
    values: dict[str, object] = {
        "channel": "c",
        "destination": "fhir-server",
        "url": base,
        "fhir_version": "4.0.1",
        "timeout_s": 30.0,
        "headers": {},
        "auth": None,
        "circuit_breaker": CircuitBreakerSpec(5, 30.0),
        "idempotency_technique": "conditional-update",
        "idempotency_header": None,
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    values.update(overrides)
    return FhirDestinationConfig(**values)  # type: ignore[arg-type]


def search_total(base: str, query: str) -> int:
    url = f"{base}/{urllib.parse.quote(query, safe='?=&:/')}&_summary=count"  # `|` must be percent-encoded
    with _NO_PROXY.open(url, timeout=30) as resp:
        return int(json.loads(resp.read())["total"])


def post(base: str, document: dict[str, Any]) -> int:
    request = urllib.request.Request(  # noqa: S310
        base,
        data=json.dumps(document).encode(),
        method="POST",
        headers={"Content-Type": "application/fhir+json"},
    )
    with _NO_PROXY.open(request, timeout=30) as resp:
        return int(resp.status)


def mrn_count(base: str, mrn: str) -> int:
    return search_total(base, f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}")


def test_the_same_bundle_delivered_twice_leaves_one_patient_and_one_encounter(hapi: str) -> None:
    mrn = f"DUP{uuid.uuid4().hex[:10]}"
    loader = FhirLoader(make_config(hapi))
    message = make_message(bundle_for(mrn))

    first = loader.deliver(message)
    second = loader.deliver(message)  # a redelivery of the identical record

    assert first.success is True, first
    assert second.success is True, second
    assert mrn_count(hapi, mrn) == 1
    encounters = search_total(
        hapi, f"Encounter?identifier=https://tributary.dev/fhir/identifier/tb-msg-id|synth-{mrn}:encounter"
    )
    assert encounters == 1


def test_a_changed_redelivery_updates_in_place_and_never_duplicates(hapi: str) -> None:
    mrn = f"UPD{uuid.uuid4().hex[:10]}"
    loader = FhirLoader(make_config(hapi))
    document = bundle_for(mrn)
    assert loader.deliver(make_message(document)).success
    document["entry"][0]["resource"]["birthDate"] = "1981-02-03"  # the same patient, corrected
    assert loader.deliver(make_message(document)).success
    assert mrn_count(hapi, mrn) == 1
    assert search_total(hapi, f"Patient?identifier=urn:id:SYNTH-HOSPITAL|{mrn}&birthdate=1981-02-03") == 1


def test_post_with_if_none_exist_is_idempotent_and_never_overwrites(hapi: str) -> None:
    key = f"obs-{uuid.uuid4().hex[:10]}"
    document = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "fullUrl": "urn:uuid:o1",
                "request": {
                    "method": "POST",
                    "url": "Observation",
                    "ifNoneExist": f"identifier=https://tributary.dev/fhir/identifier/tb-msg-id|{key}",
                },
                "resource": {
                    "resourceType": "Observation",
                    "status": "final",
                    "code": {"text": "synthetic"},
                    "identifier": [{"system": "https://tributary.dev/fhir/identifier/tb-msg-id", "value": key}],
                },
            }
        ],
    }
    loader = FhirLoader(make_config(hapi))
    message = make_message(document)
    assert loader.deliver(message).success
    assert loader.deliver(message).success
    total = search_total(hapi, f"Observation?identifier=https://tributary.dev/fhir/identifier/tb-msg-id|{key}")
    assert total == 1


def test_two_different_patients_are_two_patients(hapi: str) -> None:
    loader = FhirLoader(make_config(hapi))
    a, b = (f"TWO{uuid.uuid4().hex[:10]}" for _ in range(2))
    assert loader.deliver(make_message(bundle_for(a))).success
    assert loader.deliver(make_message(bundle_for(b))).success
    assert mrn_count(hapi, a) == 1
    assert mrn_count(hapi, b) == 1


def test_a_condition_matching_two_resources_is_a_non_retryable_412(hapi: str) -> None:
    """The real `412 Precondition Failed`, not a fixture: two Patients already share the identifier the
    Bundle's conditional PUT names, so the server cannot pick one."""
    mrn = f"AMB{uuid.uuid4().hex[:10]}"
    seed = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "request": {"method": "POST", "url": "Patient"},
                "resource": {
                    "resourceType": "Patient",
                    "identifier": [{"system": "urn:id:SYNTH-HOSPITAL", "value": mrn}],
                },
            }
            for _ in range(2)
        ],
    }
    assert post(hapi, seed) == 200
    assert mrn_count(hapi, mrn) == 2
    outcome = FhirLoader(make_config(hapi)).deliver(make_message(bundle_for(mrn)))
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "HTTP_412", False)


def test_a_server_rejection_is_a_non_retryable_4xx_and_leaks_nothing_to_the_log(hapi: str) -> None:
    """HAPI answers a bad conditional URL with a 400 whose `OperationOutcome.diagnostics` quotes the URL,
    which carries the MRN. Nothing of that body may reach a log line (README 3.4 rule 2)."""
    mrn = f"BAD{uuid.uuid4().hex[:10]}"
    document = bundle_for(mrn)
    document["entry"][0]["request"]["url"] = f"Observation?nosuchparam={mrn}"  # not a Patient search param
    log = RecordingLogger()
    outcome = FhirLoader(make_config(hapi), log=log).deliver(make_message(document))
    assert outcome.success is False
    assert outcome.reason.startswith("HTTP_4")
    assert outcome.retryable is False
    assert mrn not in repr(log.records)
    assert mrn not in repr(outcome)
    assert mrn_count(hapi, mrn) == 0  # a transaction is atomic: nothing was written


def test_a_transaction_that_fails_midway_writes_nothing(hapi: str) -> None:
    mrn = f"ATM{uuid.uuid4().hex[:10]}"
    document = bundle_for(mrn)
    document["entry"].append(
        {
            "request": {"method": "PUT", "url": f"Observation?nosuchparam={mrn}"},
            "resource": {"resourceType": "Observation"},
        }
    )
    outcome = FhirLoader(make_config(hapi)).deliver(make_message(document))
    assert outcome.success is False
    assert mrn_count(hapi, mrn) == 0


def test_plain_create_bundle_is_refused_without_a_request(hapi: str) -> None:
    mrn = f"PLN{uuid.uuid4().hex[:10]}"
    document = bundle_for(mrn)
    document["entry"][0]["request"] = {"method": "POST", "url": "Patient"}  # an unconditional create
    outcome = FhirLoader(make_config(hapi)).deliver(make_message(document))
    assert (outcome.reason, outcome.retryable) == ("FHIR_BUNDLE_INVALID", False)
    assert mrn_count(hapi, mrn) == 0


def test_a_refused_connection_is_retryable_connect_failed() -> None:
    outcome = FhirLoader(make_config("http://127.0.0.1:1/fhir", timeout_s=2.0)).deliver(
        make_message(bundle_for("NOPE"))
    )
    assert (outcome.reason, outcome.retryable) == ("CONNECT_FAILED", True)


def test_a_broken_control_unconditional_posts_would_duplicate(hapi: str) -> None:
    """Broken-implementation control: the same delivery-twice shape with a `POST` that has no condition
    (what README 3.4.2 forbids and `check_transaction_bundle` refuses) creates two Patients, so the
    one-Patient assertions above are not vacuous."""
    mrn = f"CTL{uuid.uuid4().hex[:10]}"
    document = bundle_for(mrn)
    document["entry"] = [
        {"request": {"method": "POST", "url": "Patient"}, "resource": document["entry"][0]["resource"]}
    ]
    post(hapi, document)
    post(hapi, document)
    assert mrn_count(hapi, mrn) == 2
