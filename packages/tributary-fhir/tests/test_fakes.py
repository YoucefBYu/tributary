"""`fake_fhir_context`: the one-call fake for a mapping step's tests."""

from __future__ import annotations

from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from tributary.fhir.context import FhirContext
from tributary.fhir.fakes import fake_fhir_context
from tributary.fhir.terminology import CodeLookup, LocalTerminology, load_table
from tributary.sdk.fakes import FakeContext


def test_default_fake_is_deterministic_across_calls() -> None:
    ctx = fake_fhir_context()
    patient = Patient(identifier=[Identifier(system="urn:synth:mrn", value="9000001")])
    first = ctx.fhir.upsert(patient)
    second = ctx.fhir.upsert(patient)
    assert first.fullUrl == "urn:uuid:test-1"
    assert second.fullUrl == "urn:uuid:test-2"


def test_fake_is_a_fhir_context() -> None:
    assert isinstance(fake_fhir_context(), FhirContext)


def test_terminology_can_be_preloaded() -> None:
    table = load_table([CodeLookup(system="http://loinc.org", code="8867-4", display="Heart rate")])
    ctx = fake_fhir_context(terminology=LocalTerminology(table))
    assert ctx.terminology.lookup("http://loinc.org", "8867-4") is not None


def test_base_context_can_be_supplied_and_inspected_afterwards() -> None:
    base = FakeContext(channel="adt-to-fhir")
    ctx = fake_fhir_context(base=base)
    assert ctx.channel == "adt-to-fhir"
    ctx.log.info("mapped")
    assert base.log.events() == ["mapped"]  # same underlying logger, reachable from the original base
