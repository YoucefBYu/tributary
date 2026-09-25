"""Golden-file tests: ADT A01 (admit), A03 (discharge) and A08 (update) to FHIR R4.

Each case parses a synthetic `tests/fixtures/hl7/*.hl7` fixture (MSH-3 `TRIBUTARY-SYNTH`), runs
`adt_to_fhir`, and compares the resulting `Bundle` JSON to a golden file under `tests/fixtures/fhir/`.
The mapping-source citation for every rule these golden files exercise lives in `adt.py`'s module
docstring and inline comments -- not repeated per test, so a rule change has one place to update.
"""

from __future__ import annotations

import pytest
from tributary.content_hl7_fhir import UnsupportedTriggerError, adt_to_fhir
from tributary.content_hl7_fhir.errors import RequiredFieldMissingError
from tributary.fhir.fakes import fake_fhir_context
from tributary.hl7v2.step import parse
from tributary.sdk.testing import make_message, run_step

from .conftest import GOLDEN_MSG_ID, load_golden, map_fixture


@pytest.mark.parametrize(
    ("hl7_fixture", "golden_fixture"),
    [
        # ADT^A01 (admit): Encounter.status "in-progress", period.start from EVN-2 (PV1-44 blank).
        ("adt_a01_admit.hl7", "adt_a01_admit.bundle.json"),
        # ADT^A03 (discharge): Encounter.status "finished", period.end from EVN-2 (PV1-45 blank);
        # period.start is left unset rather than reusing the discharge EVN-2 as an admission time.
        ("adt_a03_discharge.hl7", "adt_a03_discharge.bundle.json"),
        # ADT^A08 (update), no PV1 at all: only a Patient entry, no Encounter -- a pure demographic
        # update has nothing PV1-shaped to map, and this mapping does not fabricate one.
        ("adt_a08_update_no_pv1.hl7", "adt_a08_update_no_pv1.bundle.json"),
        # ADT^A01 with two NK1 repetitions and an outpatient PV1-2 ("O" -> Encounter.class AMB): NK1 is
        # out of this reference set's resource scope (RelatedPerson is a natural follow-on, not built
        # here), so the golden file asserts the NK1 repetitions are simply *not* reflected -- proving
        # the mapping does not choke on them rather than silently mis-mapping one.
        ("adt_a01_two_nk1.hl7", "adt_a01_two_nk1.bundle.json"),
    ],
)
def test_adt_mapping_matches_golden_bundle(hl7_fixture: str, golden_fixture: str) -> None:
    assert map_fixture(hl7_fixture, adt_to_fhir) == load_golden(golden_fixture)


def test_patient_entry_is_conditional_on_pid3() -> None:
    """Channel-spec README 3.4.2: every Bundle entry is conditional, keyed on exactly one identifier
    system. The `Patient` entry's is PID-3's own assigning-authority-derived system -- a real business
    identifier, never `tb-msg-id` (that fallback is for resources with no identifier of their own)."""
    bundle = map_fixture("adt_a01_admit.hl7", adt_to_fhir)
    patient_entry = bundle["entry"][0]  # type: ignore[index]
    assert patient_entry["request"]["method"] == "PUT"  # type: ignore[index]
    assert patient_entry["request"]["url"] == "Patient?identifier=urn:id:SYNTH-HOSPITAL|9000001"  # type: ignore[index]


def test_encounter_entry_is_conditional_on_message_identifier_when_no_visit_number() -> None:
    """No PV1-19 in this fixture set, so `Encounter` falls back to the message-scoped identifier
    (`tributary.fhir.message_identifier`) -- still exactly one identifier system, never unconditional."""
    bundle = map_fixture("adt_a01_admit.hl7", adt_to_fhir)
    encounter_entry = bundle["entry"][1]  # type: ignore[index]
    assert encounter_entry["request"]["method"] == "PUT"  # type: ignore[index]
    assert f"tb-msg-id|{GOLDEN_MSG_ID}:encounter" in encounter_entry["request"]["url"]  # type: ignore[index]


def test_unsupported_trigger_raises_a_typed_error() -> None:
    raw = (
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
        b"20260101120000||ADT^A02^ADT_A02|SYNTHMSG00099|P|2.5.1\r"
        b"EVN|A02|20260101120000\r"
        b"PID|1||9000099^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^X||19800101|M\r"
    )
    ctx = fake_fhir_context()
    parsed = run_step(parse, make_message(raw, content_type="application/hl7-v2+er7"), ctx=ctx)
    with pytest.raises(UnsupportedTriggerError):
        run_step(adt_to_fhir, parsed, ctx=ctx)


def test_missing_pid3_raises_a_typed_error_not_a_crash() -> None:
    raw = (
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
        b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00098|P|2.5.1\r"
        b"EVN|A01|20260101120000\r"
        b"PID|1|||SYNTHPATIENT^X||19800101|M\r"
    )
    ctx = fake_fhir_context()
    parsed = run_step(parse, make_message(raw, content_type="application/hl7-v2+er7"), ctx=ctx)
    with pytest.raises(RequiredFieldMissingError) as excinfo:
        run_step(adt_to_fhir, parsed, ctx=ctx)
    assert excinfo.value.path == "PID.3"


def test_repeated_run_produces_a_byte_identical_bundle() -> None:
    """Invariant 4 in miniature: the same message mapped twice (same `tb-msg-id`, same fixture) yields
    the same Bundle -- no `uuid4` fullUrl or non-deterministic ordering has crept in."""
    first = map_fixture("adt_a01_admit.hl7", adt_to_fhir)
    second = map_fixture("adt_a01_admit.hl7", adt_to_fhir)
    assert first == second
