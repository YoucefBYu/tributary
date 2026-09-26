"""Golden-file test: ORU R01 (unsolicited observation result) to FHIR R4.

Mapping-source citations for every rule this exercises are in `oru.py`'s module docstring and inline
comments.
"""

from __future__ import annotations

import json

import pytest
from tributary.content_hl7_fhir import UnsupportedTriggerError, oru_r01_to_fhir
from tributary.content_hl7_fhir.errors import InvalidFieldError, RequiredFieldMissingError
from tributary.fhir.fakes import fake_fhir_context
from tributary.hl7v2.step import parse
from tributary.sdk import Message
from tributary.sdk.testing import make_message, run_step

from .conftest import GOLDEN_MSG_ID, load_golden, map_fixture


def _oru_message(obx_segments: str, *, control_id: str = "SYNTHMSG00099") -> Message:
    raw = (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-LAB|SYNTH-ORU-RECV|SYNTH-RECV-FACILITY|"
        f"20260105101500||ORU^R01^ORU_R01|{control_id}|P|2.5.1\r"
        f"PID|1||9000099^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^X||19900101|M\r"
        f"OBR|1|SYNTHORD0099|SYNTHFILL0099|CBC^COMPLETE BLOOD COUNT^L|||20260105100000\r"
        f"{obx_segments}"
    ).encode()
    return make_message(raw, content_type="application/hl7-v2+er7")


def _mapped_observation(obx_segments: str) -> dict[str, object]:
    ctx = fake_fhir_context()
    parsed = run_step(parse, _oru_message(obx_segments), ctx=ctx)
    mapped = run_step(oru_r01_to_fhir, parsed, ctx=ctx)
    assert isinstance(mapped, Message)
    assert mapped.payload is not None
    bundle = json.loads(mapped.payload)
    result: dict[str, object] = next(
        entry["resource"] for entry in bundle["entry"] if entry["resource"]["resourceType"] == "Observation"
    )
    return result


def test_oru_r01_mapping_matches_golden_bundle() -> None:
    assert map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir) == load_golden("oru_r01_repeating_obx.bundle.json")


def test_diagnostic_report_entry_is_conditional_on_filler_order_number() -> None:
    """Channel-spec README 3.4.2: `DiagnosticReport` is conditional on OBR-3 (Filler Order Number), the
    lab's own business identifier for this order -- not `tb-msg-id`, which is reserved for a resource
    that genuinely has no business identifier of its own (`Observation`, here). Treated as a state
    resource (`PUT`), so a corrected result for the same order converges on the one `DiagnosticReport`
    rather than creating a duplicate (`docs/MAPPING.md`)."""
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    report_entry = bundle["entry"][-1]  # type: ignore[index]
    assert report_entry["request"]["method"] == "PUT"  # type: ignore[index]
    assert report_entry["resource"]["resourceType"] == "DiagnosticReport"  # type: ignore[index]
    assert report_entry["request"]["url"] == "DiagnosticReport?identifier=urn:id:SYNTH-LAB|SYNTHFILL0001"  # type: ignore[index]


def test_diagnostic_report_body_carries_its_own_conditional_identifier() -> None:
    """Security review MEDIUM 4: every resource must include the identifier its entry is conditional
    on, so a redelivery's conditional search actually matches what a previous delivery wrote."""
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    report_entry = bundle["entry"][-1]  # type: ignore[index]
    identifiers = report_entry["resource"]["identifier"]  # type: ignore[index]
    assert identifiers == [{"system": "urn:id:SYNTH-LAB", "value": "SYNTHFILL0001"}]


def test_each_observation_entry_is_a_conditional_post_on_a_distinct_message_identifier() -> None:
    """One `Observation` per `OBX`, each an event-resource `POST` + `ifNoneExist` (channel-spec README
    3.4.2, security review MEDIUM 4 -- not a `PUT`, which would let a corrected later message overwrite
    a result in place), keyed on `message_identifier(msg_id, discriminator=f"obx-{OBX-1}")` -- distinct
    per OBX set id, stable across a redelivery of the same message."""
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    observation_entries = [
        entry
        for entry in bundle["entry"]
        if entry["resource"]["resourceType"] == "Observation"  # type: ignore[index]
    ]
    assert len(observation_entries) == 3
    assert all(entry["request"]["method"] == "POST" for entry in observation_entries)  # type: ignore[index]
    assert all(entry["request"]["url"] == "Observation" for entry in observation_entries)  # type: ignore[index]
    if_none_exist = {entry["request"]["ifNoneExist"] for entry in observation_entries}  # type: ignore[index]
    assert if_none_exist == {
        f"identifier=https://tributary.dev/fhir/identifier/tb-msg-id|{GOLDEN_MSG_ID}:obx-{n}" for n in (1, 2, 3)
    }
    # Security review MEDIUM 4: the resource body itself carries the identifier its entry is conditional
    # on, so a redelivery's conditional search actually matches what a previous delivery wrote.
    for entry in observation_entries:
        identifiers = entry["resource"]["identifier"]  # type: ignore[index]
        assert len(identifiers) == 1
        assert f"identifier={identifiers[0]['system']}|{identifiers[0]['value']}" in entry["request"]["ifNoneExist"]  # type: ignore[index]


def test_diagnostic_report_result_references_the_observation_full_urls() -> None:
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    observation_full_urls = {
        entry["fullUrl"]
        for entry in bundle["entry"]
        if entry["resource"]["resourceType"] == "Observation"  # type: ignore[index]
    }
    report_entry = bundle["entry"][-1]  # type: ignore[index]
    result_refs = {ref["reference"] for ref in report_entry["resource"]["result"]}  # type: ignore[index]
    assert result_refs == observation_full_urls


def test_repeated_run_produces_a_byte_identical_bundle() -> None:
    first = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    second = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    assert first == second


# -- MEDIUM 2: MSH-9 must actually be checked, not just claimed to be -----------------------------------


def test_wrong_message_type_raises_a_typed_error() -> None:
    ctx = fake_fhir_context()
    raw = (
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
        b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00097|P|2.5.1\r"
        b"PID|1||9000097^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^X||19800101|M\r"
    )
    parsed = run_step(parse, make_message(raw, content_type="application/hl7-v2+er7"), ctx=ctx)
    with pytest.raises(UnsupportedTriggerError):
        run_step(oru_r01_to_fhir, parsed, ctx=ctx)


def test_non_structural_msh9_never_reaches_the_exception() -> None:
    """Security review, MEDIUM 2: a malformed MSH-9 is replaced with a fixed placeholder, never echoed."""
    ctx = fake_fhir_context()
    raw = (
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
        b"20260101120000||not a valid code^R01^ORU_R01|SYNTHMSG00096|P|2.5.1\r"
        b"PID|1||9000096^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^X||19800101|M\r"
    )
    parsed = run_step(parse, make_message(raw, content_type="application/hl7-v2+er7"), ctx=ctx)
    with pytest.raises(UnsupportedTriggerError) as excinfo:
        run_step(oru_r01_to_fhir, parsed, ctx=ctx)
    assert excinfo.value.message_type == "<non-structural>"
    assert "not a valid code" not in str(excinfo.value)


# -- MEDIUM 4: a blank or repeated OBX-1 must not collide -----------------------------------------------


def test_blank_obx1_raises_a_typed_error() -> None:
    with pytest.raises(RequiredFieldMissingError) as excinfo:
        _mapped_observation("OBX||NM|8867-4^Heart rate^LN||72|/min|60-100|N|||F\r")
    assert excinfo.value.path == "OBX.1"


def test_repeated_obx1_raises_a_typed_error() -> None:
    with pytest.raises(InvalidFieldError) as excinfo:
        _mapped_observation(
            "OBX|1|NM|8867-4^Heart rate^LN||72|/min|60-100|N|||F\r"
            "OBX|1|NM|8480-6^Systolic^LN||120|mm[Hg]|90-120|N|||F\r"
        )
    assert excinfo.value.path == "OBX.1"


# -- HIGH 1: unbounded CPU/memory from an unbounded exponent --------------------------------------------


def test_huge_exponent_falls_back_to_value_string_quickly() -> None:
    import time

    start = time.perf_counter()
    observation = _mapped_observation("OBX|1|NM|8867-4^Heart rate^LN||1E+999999999|/min|60-100|N|||F\r")
    elapsed = time.perf_counter() - start
    assert observation["valueString"] == "1E+999999999"
    assert "valueQuantity" not in observation
    assert elapsed < 1.0


@pytest.mark.parametrize("bad_value", ["NOT-A-NUMBER", "NaN", "Infinity", "-Infinity"])
def test_unparseable_or_non_finite_numeric_value_falls_back_to_value_string(bad_value: str) -> None:
    observation = _mapped_observation(f"OBX|1|NM|8867-4^Heart rate^LN||{bad_value}|/min|60-100|N|||F\r")
    assert observation["valueString"] == bad_value
    assert "valueQuantity" not in observation


# -- MEDIUM 4: numeric precision --------------------------------------------------------------------


def test_trailing_zero_precision_falls_back_to_value_string() -> None:
    """`"72.10"` would serialise as `72.1` through `fhir_core`'s float-based JSON encoding of `Decimal` --
    documented, not silently lossy: `valueString` preserves the sender's own text exactly."""
    observation = _mapped_observation("OBX|1|NM|8867-4^Heart rate^LN||72.10|/min|60-100|N|||F\r")
    assert observation["valueString"] == "72.10"
    assert "valueQuantity" not in observation


def test_beyond_float_precision_falls_back_to_value_string() -> None:
    observation = _mapped_observation("OBX|1|NM|8867-4^Heart rate^LN||1.00000000000000001|/min|60-100|N|||F\r")
    assert observation["valueString"] == "1.00000000000000001"
    assert "valueQuantity" not in observation


def test_exactly_representable_value_still_uses_value_quantity() -> None:
    """Not every fractional value is penalised -- only ones `fhir_core`'s float round-trip actually
    changes (this keeps the existing golden fixtures' plain-integer OBX-5 values on `valueQuantity`)."""
    observation = _mapped_observation("OBX|1|NM|8867-4^Heart rate^LN||72.5|/min|60-100|N|||F\r")
    assert observation["valueQuantity"]["value"] == 72.5
    assert "valueString" not in observation


# -- HIGH 2 / MEDIUM 2: no PHI in an exception, even via __context__ -------------------------------------


def test_leading_trailing_space_in_obx3_code_does_not_crash_or_leak() -> None:
    """Security review, HIGH 2: a leading/trailing space in OBX-3.1 must not reach `Coding.code`'s own
    validator, whose `ValidationError` embeds the rejected text verbatim."""
    observation = _mapped_observation("OBX|1|NM|  8867-4  ^Heart rate^LN||72|/min|60-100|N|||F\r")
    assert observation["code"]["coding"][0].get("code") is None or "8867-4" in observation["code"]["coding"][0]["code"]


# -- LOW: fhir.resources is an explicit dependency of this package (checked in test_import.py's sibling) -


def test_dependency_pyproject_declares_fhir_resources() -> None:
    import pathlib

    pyproject = pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"
    assert "fhir.resources" in pyproject.read_text()
