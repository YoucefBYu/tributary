"""Golden-file tests: ADT A01 (admit), A03 (discharge) and A08 (update) to FHIR R4.

Each case parses a synthetic `tests/fixtures/hl7/*.hl7` fixture (MSH-3 `TRIBUTARY-SYNTH`), runs
`adt_to_fhir`, and compares the resulting `Bundle` JSON to a golden file under `tests/fixtures/fhir/`.
The mapping-source citation for every rule these golden files exercise lives in `adt.py`'s module
docstring and inline comments -- not repeated per test, so a rule change has one place to update.
"""

from __future__ import annotations

import json

import pytest
from tributary.content_hl7_fhir import UnsupportedTriggerError, adt_to_fhir
from tributary.content_hl7_fhir.errors import RequiredFieldMissingError
from tributary.fhir.fakes import fake_fhir_context
from tributary.hl7v2.step import parse
from tributary.sdk import Message
from tributary.sdk.testing import make_message, run_step

from .conftest import GOLDEN_MSG_ID, load_golden, map_fixture


@pytest.mark.parametrize(
    ("hl7_fixture", "golden_fixture"),
    [
        # ADT^A01 (admit): Encounter.status "in-progress", period.start from EVN-2 (PV1-44 blank).
        ("adt_a01_admit.hl7", "adt_a01_admit.bundle.json"),
        # ADT^A03 (discharge), no PV1-19: no `Encounter` entry at all -- a message-scoped identifier
        # here would create a second, wrong Encounter rather than converge with the one A01 created
        # (see adt.py's module docstring and `_encounter_identifier`). Only `Patient` is produced.
        ("adt_a03_discharge.hl7", "adt_a03_discharge.bundle.json"),
        # ADT^A01 with PV1-19 (Visit Number) populated: `Encounter` is conditional on it, not on
        # `tb-msg-id` -- this is the case that converges across a clinical flow.
        ("adt_a01_admit_with_visit_number.hl7", "adt_a01_admit_with_visit_number.bundle.json"),
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


def test_encounter_entry_is_conditional_on_visit_number_when_present() -> None:
    """PV1-19 (Visit Number), when present, is a real per-visit business identifier -- `Encounter`'s
    conditional PUT is keyed on it, not on `tb-msg-id` (channel-spec README 3.4.2, and the resource-
    convergence rule in `adt.py`'s module docstring)."""
    bundle = map_fixture("adt_a01_admit_with_visit_number.hl7", adt_to_fhir)
    encounter_entry = bundle["entry"][1]  # type: ignore[index]
    assert encounter_entry["request"]["url"] == "Encounter?identifier=urn:id:SYNTH-HOSPITAL|VN20260108-00001"  # type: ignore[index]
    assert GOLDEN_MSG_ID not in encounter_entry["request"]["url"]  # type: ignore[index]


@pytest.mark.parametrize("hl7_fixture", ["adt_a03_discharge.hl7", "adt_a08_update_no_pv1.hl7"])
def test_encounter_entry_is_omitted_without_a_visit_number_for_a03_and_a08(hl7_fixture: str) -> None:
    """A03/A08 with no PV1-19 must not fall back to a message-scoped `Encounter` identifier: that would
    create a second, wrong `Encounter` next to whatever A01 already created, instead of converging on
    it (the CRITICAL finding on `4998f33`). Only `Patient` is produced."""
    bundle = map_fixture(hl7_fixture, adt_to_fhir)
    resource_types = [entry["resource"]["resourceType"] for entry in bundle["entry"]]  # type: ignore[index]
    assert resource_types == ["Patient"]


def _adt_message(*, trigger: str, msg_id: str, visit_number: str, control_id: str) -> Message:
    raw = (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
        f"20260101120000||ADT^{trigger}^ADT_{trigger}|{control_id}|P|2.5.1\r"
        f"EVN|{trigger}|20260101120000\r"
        f"PID|1||9000077^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^CONVERGE||19800101|M\r"
        f"PV1|1|I|SYNTHUNIT^101^A^SYNTH-HOSPITAL||||9001^SYNTHDOC^ATTENDING^^^^MD|||SUR||||ADM|A0||||"
        f"{visit_number}^^^SYNTH-HOSPITAL^VN\r"
    ).encode()
    ctx = fake_fhir_context()
    message = make_message(raw, content_type="application/hl7-v2+er7", msg_id=msg_id)
    parsed = run_step(parse, message, ctx=ctx)
    mapped = run_step(adt_to_fhir, parsed, ctx=ctx)
    assert isinstance(mapped, Message)
    return mapped


def test_a01_then_a03_then_a08_for_the_same_visit_converge_on_one_encounter_url() -> None:
    """Regression for the CRITICAL finding on `4998f33`: three different messages (different `tb-msg-
    id`, different trigger events) for the *same* visit (same PV1-19) must resolve the same conditional
    `Encounter` URL, so a real FHIR server updates one `Encounter` instead of creating three."""
    visit_number = "VN20260109-00002"
    urls = set()
    for trigger, msg_id, control_id in (
        ("A01", "0199036e-0000-7000-8000-000000000001", "SYNTHMSGCONV01"),
        ("A03", "0199036e-0000-7000-8000-000000000002", "SYNTHMSGCONV02"),
        ("A08", "0199036e-0000-7000-8000-000000000003", "SYNTHMSGCONV03"),
    ):
        mapped = _adt_message(trigger=trigger, msg_id=msg_id, visit_number=visit_number, control_id=control_id)
        bundle = json.loads(mapped.payload)
        encounter_entry = next(e for e in bundle["entry"] if e["resource"]["resourceType"] == "Encounter")
        urls.add(encounter_entry["request"]["url"])
    assert urls == {"Encounter?identifier=urn:id:SYNTH-HOSPITAL|VN20260109-00002"}


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
