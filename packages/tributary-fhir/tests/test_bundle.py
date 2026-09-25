"""`LocalFhir`: conditional Bundle-entry construction, no network, deterministic with an injected id
factory."""

from __future__ import annotations

import pytest
from fhir.resources.R4B.patient import Patient
from tributary.fhir.bundle import (
    TB_MESSAGE_SYSTEM,
    Identifier,
    LocalFhir,
    message_id_factory,
    message_identifier,
    sequential_id_factory,
    uuid_id_factory,
)
from tributary.fhir.errors import BundleEntryError

SYNTH_IDENTIFIER = Identifier(system="urn:synth:mrn", value="9000001")


def _patient(identifier: Identifier | None = SYNTH_IDENTIFIER) -> Patient:
    return Patient(identifier=[identifier] if identifier else None)


def test_upsert_builds_a_conditional_put() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.upsert(_patient())
    assert entry.request.method == "PUT"
    assert entry.request.url == "Patient?identifier=urn:synth:mrn|9000001"
    assert entry.request.ifNoneExist == "identifier=urn:synth:mrn|9000001"
    assert entry.resource is not None


def test_upsert_uses_an_explicit_identifier_override() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    override = Identifier(system=TB_MESSAGE_SYSTEM, value="0199-uuid:obx-1")
    entry = fhir.upsert(_patient(), identifier=override)
    assert entry.request.url == f"Patient?identifier={TB_MESSAGE_SYSTEM}|0199-uuid:obx-1"


def test_upsert_raises_without_any_identifier() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    with pytest.raises(BundleEntryError):
        fhir.upsert(_patient(identifier=None))


def test_create_builds_a_conditional_post() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.create(_patient())
    assert entry.request.method == "POST"
    assert entry.request.url == "Patient"
    assert entry.request.ifNoneExist == "identifier=urn:synth:mrn|9000001"


def test_delete_builds_a_conditional_delete() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory())
    entry = fhir.delete("Patient", identifier=SYNTH_IDENTIFIER)
    assert entry.request.method == "DELETE"
    assert entry.request.url == "Patient?identifier=urn:synth:mrn|9000001"
    assert entry.resource is None


def test_id_factory_is_injectable_and_deterministic() -> None:
    fhir = LocalFhir(id_factory=sequential_id_factory("urn:uuid:golden-"))
    first = fhir.upsert(_patient())
    second = fhir.upsert(_patient())
    assert first.fullUrl == "urn:uuid:golden-1"
    assert second.fullUrl == "urn:uuid:golden-2"


def test_default_id_factory_produces_a_urn_uuid() -> None:
    fhir = LocalFhir()
    entry = fhir.upsert(_patient())
    assert entry.fullUrl is not None
    assert entry.fullUrl.startswith("urn:uuid:")


def test_uuid_id_factory_produces_distinct_values() -> None:
    factory = uuid_id_factory()
    assert factory() != factory()


def test_message_identifier_uses_the_tributary_system() -> None:
    identifier = message_identifier("0199-abc-uuid")
    assert identifier.system == TB_MESSAGE_SYSTEM
    assert identifier.value == "0199-abc-uuid"


def test_message_identifier_with_discriminator() -> None:
    identifier = message_identifier("0199-abc-uuid", discriminator="obx-2")
    assert identifier.value == "0199-abc-uuid:obx-2"


def test_message_identifier_never_equals_a_patient_identifier_system() -> None:
    identifier = message_identifier("0199-abc-uuid")
    assert identifier.system != SYNTH_IDENTIFIER.system


def test_message_id_factory_is_deterministic_and_not_uuid4() -> None:
    """W2-2 open item 3: production `fullUrl`s come from `tb-msg-id`, so a reprocess of the same message
    reproduces the same `fullUrl`s rather than a fresh `uuid4` each run."""
    first_run = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    second_run = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    assert first_run() == second_run()
    assert first_run() == second_run()  # the second call in each run also lines up (per-call sequence)


def test_message_id_factory_differs_across_messages() -> None:
    a = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    b = message_id_factory("0199036e-7a3e-7f00-8c21-000000000000")
    assert a() != b()


def test_message_id_factory_produces_a_urn_uuid() -> None:
    factory = message_id_factory("0199036e-7a3e-7f00-8c21-abc123456789")
    assert factory().startswith("urn:uuid:")
