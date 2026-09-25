"""Golden-file test: ORU R01 (unsolicited observation result) to FHIR R4.

Mapping-source citations for every rule this exercises are in `oru.py`'s module docstring and inline
comments.
"""

from __future__ import annotations

import pytest
from tributary.content_hl7_fhir import oru_r01_to_fhir

from .conftest import GOLDEN_MSG_ID, load_golden, map_fixture


def test_oru_r01_mapping_matches_golden_bundle() -> None:
    assert map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir) == load_golden("oru_r01_repeating_obx.bundle.json")


def test_diagnostic_report_entry_is_conditional_on_filler_order_number() -> None:
    """Channel-spec README 3.4.2: `DiagnosticReport` is conditional on OBR-3 (Filler Order Number), the
    lab's own business identifier for this order -- not `tb-msg-id`, which is reserved for a resource
    that genuinely has no business identifier of its own (`Observation`, here)."""
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    report_entry = bundle["entry"][-1]  # type: ignore[index]
    assert report_entry["resource"]["resourceType"] == "DiagnosticReport"  # type: ignore[index]
    assert report_entry["request"]["url"] == "DiagnosticReport?identifier=urn:id:SYNTH-LAB|SYNTHFILL0001"  # type: ignore[index]


def test_each_observation_entry_is_conditional_on_a_distinct_message_identifier() -> None:
    """One `Observation` per `OBX`, each keyed on `message_identifier(msg_id, discriminator=f"obx-
    {OBX-1}")` -- distinct per OBX set id, stable across a redelivery of the same message."""
    bundle = map_fixture("oru_r01_repeating_obx.hl7", oru_r01_to_fhir)
    observation_entries = [
        entry
        for entry in bundle["entry"]
        if entry["resource"]["resourceType"] == "Observation"  # type: ignore[index]
    ]
    assert len(observation_entries) == 3
    urls = {entry["request"]["url"] for entry in observation_entries}  # type: ignore[index]
    assert urls == {
        f"Observation?identifier=https://tributary.dev/fhir/identifier/tb-msg-id|{GOLDEN_MSG_ID}:obx-{n}"
        for n in (1, 2, 3)
    }


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


@pytest.mark.parametrize("bad_value", ["NOT-A-NUMBER"])
def test_unparseable_numeric_value_falls_back_to_value_string_not_a_crash(bad_value: str) -> None:
    from tributary.fhir.fakes import fake_fhir_context
    from tributary.hl7v2.step import parse
    from tributary.sdk.testing import make_message, run_step

    raw = (
        b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-LAB|SYNTH-ORU-RECV|SYNTH-RECV-FACILITY|"
        b"20260105101500||ORU^R01^ORU_R01|SYNTHMSG00099|P|2.5.1\r"
        b"PID|1||9000099^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^X||19900101|M\r"
        b"OBR|1|SYNTHORD0099|SYNTHFILL0099|CBC^COMPLETE BLOOD COUNT^L|||20260105100000\r"
        b"OBX|1|NM|8867-4^Heart rate^LN||" + bad_value.encode() + b"|/min|60-100|N|||F\r"
    )
    ctx = fake_fhir_context()
    parsed = run_step(parse, make_message(raw, content_type="application/hl7-v2+er7"), ctx=ctx)
    mapped = run_step(oru_r01_to_fhir, parsed, ctx=ctx)
    import json

    bundle = json.loads(mapped.payload)
    observation = next(
        entry["resource"] for entry in bundle["entry"] if entry["resource"]["resourceType"] == "Observation"
    )
    assert observation["valueString"] == bad_value
