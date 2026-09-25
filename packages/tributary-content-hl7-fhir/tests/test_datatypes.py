"""Unit tests for the small, independently-testable v2-datatype-to-FHIR conversions."""

from __future__ import annotations

from tributary.content_hl7_fhir.datatypes import (
    administrative_sex_to_gender,
    cx_to_identifier,
    dt8_to_fhir_date,
    local_identifier_system,
    patient_class_to_encounter_class,
    ts_to_fhir_datetime,
    xad_to_address,
    xpn_to_human_name,
    xtn_to_contact_point,
)


def test_cx_to_identifier_with_assigning_authority() -> None:
    identifier = cx_to_identifier("9000001^^^SYNTH-HOSPITAL^MR")
    assert identifier is not None
    assert identifier.value == "9000001"
    assert identifier.system == "urn:id:SYNTH-HOSPITAL"
    assert identifier.type.coding[0].code == "MR"  # type: ignore[union-attr]


def test_cx_to_identifier_falls_back_to_the_given_system() -> None:
    identifier = cx_to_identifier("9000001-ACC", fallback_system=local_identifier_system("SYNTH-HOSPITAL"))
    assert identifier is not None
    assert identifier.system == "urn:id:SYNTH-HOSPITAL"


def test_cx_to_identifier_returns_none_for_empty_input() -> None:
    assert cx_to_identifier("") is None


def test_xpn_to_human_name() -> None:
    name = xpn_to_human_name("SYNTHPATIENT^ALPHA^Q")
    assert name is not None
    assert name.family == "SYNTHPATIENT"
    assert name.given == ["ALPHA", "Q"]


def test_xad_to_address() -> None:
    address = xad_to_address("100 SYNTH ST^^SYNTHVILLE^ZZ^00000^US")
    assert address is not None
    assert address.line == ["100 SYNTH ST"]
    assert address.city == "SYNTHVILLE"
    assert address.state == "ZZ"
    assert address.postalCode == "00000"
    assert address.country == "US"


def test_xtn_to_contact_point() -> None:
    point = xtn_to_contact_point("(000)555-0100")
    assert point is not None
    assert point.system == "phone"
    assert point.value == "(000)555-0100"


def test_dt8_to_fhir_date() -> None:
    assert dt8_to_fhir_date("19800115") == "1980-01-15"
    assert dt8_to_fhir_date("") is None
    assert dt8_to_fhir_date("1980") is None


def test_ts_to_fhir_datetime_requires_a_timezone_once_time_is_present() -> None:
    assert ts_to_fhir_datetime("20260101120000") == "2026-01-01T12:00:00+00:00"
    assert ts_to_fhir_datetime("20260101") == "2026-01-01"
    assert ts_to_fhir_datetime("") is None


def test_administrative_sex_to_gender() -> None:
    assert administrative_sex_to_gender("M") == "male"
    assert administrative_sex_to_gender("F") == "female"
    assert administrative_sex_to_gender("U") == "unknown"
    assert administrative_sex_to_gender("Q") is None


def test_patient_class_to_encounter_class_known_code() -> None:
    coding = patient_class_to_encounter_class("I")
    assert coding.system == "http://terminology.hl7.org/CodeSystem/v3-ActCode"
    assert coding.code == "IMP"


def test_patient_class_to_encounter_class_unknown_code_falls_back_to_v2_table() -> None:
    coding = patient_class_to_encounter_class("Z")
    assert coding.system == "http://terminology.hl7.org/CodeSystem/v2-0004"
    assert coding.code == "Z"
