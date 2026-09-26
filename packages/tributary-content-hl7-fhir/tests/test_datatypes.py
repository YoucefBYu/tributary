"""Unit tests for the small, independently-testable v2-datatype-to-FHIR conversions."""

from __future__ import annotations

from decimal import Decimal

from tributary.content_hl7_fhir.datatypes import (
    administrative_sex_to_gender,
    bounded_decimal,
    cwe_components,
    cx_identifier,
    decimal_preserves_precision,
    dt8_to_fhir_date,
    local_identifier_system,
    patient_class_to_encounter_class,
    pl_display,
    sanitize_fhir_code,
    ts_to_fhir_datetime,
    xad_address,
    xcn_display,
    xpn_human_name,
)
from tributary.hl7v2 import Hl7Message, Segment

_MSH = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
)
#: A message using custom delimiters (`!` field, `@` component, `$` repetition, `\` escape, `*`
#: subcomponent) -- the exact shape `tests/fixtures/hl7/custom_delimiters.hl7` uses, so a component
#: accessor that (wrongly) hard-codes `"^"` misparses this and one that reads the real delimiters does not.
_CUSTOM_DELIM_MSH = b"MSH!@$\\*!TRIBUTARY-SYNTH!SYNTH-HOSPITAL!SYNTH-ADT-RECV!SYNTH-RECV-FACILITY!20260106140000!!ADT@A01@ADT_A01!SYNTHMSG00005!P!2.5.1\r"


def _segment(name: str, field_text: str, *, msh: bytes = _MSH) -> Segment:
    message = Hl7Message.parse(msh + f"{name}|{field_text}\r".encode())
    return message.segment(name)


def _custom_delim_segment(name: str, field_text: str) -> Segment:
    message = Hl7Message.parse(_CUSTOM_DELIM_MSH + f"{name}!{field_text}\r".encode())
    return message.segment(name)


def test_cx_identifier_with_assigning_authority() -> None:
    pid = _segment("PID", "1||9000001^^^SYNTH-HOSPITAL^MR")
    identifier = cx_identifier(pid, 3)
    assert identifier is not None
    assert identifier.value == "9000001"
    assert identifier.system == "urn:id:SYNTH-HOSPITAL"
    assert identifier.type.coding[0].code == "MR"  # type: ignore[union-attr]


def test_cx_identifier_falls_back_to_the_given_system() -> None:
    pid = _segment("PID", "1" + "|" * 17 + "9000001-ACC")  # "1" (field 1), 16 empty fields, then field 18
    identifier = cx_identifier(pid, 18, fallback_system=local_identifier_system("SYNTH-HOSPITAL"))
    assert identifier is not None
    assert identifier.system == "urn:id:SYNTH-HOSPITAL"


def test_cx_identifier_returns_none_for_empty_field() -> None:
    pid = _segment("PID", "1")
    assert cx_identifier(pid, 3) is None


def test_cx_identifier_does_not_split_an_escaped_component_separator() -> None:
    """Security review, MEDIUM 3: an escaped `\\S\\` in a component is a literal caret in the sender's
    data, not a real component boundary. `PID.3.4` (assigning authority) here contains one; the
    identifier's `system` must be the whole literal string, not truncated at the escaped caret as a
    naive post-unescape `"^".split()` would do."""
    pid = _segment("PID", r"1||9000001^^^SYNTH\S\HOSPITAL^MR")
    identifier = cx_identifier(pid, 3)
    assert identifier is not None
    assert identifier.system == "urn:id:SYNTH^HOSPITAL"
    assert identifier.value == "9000001"


def test_cx_identifier_honours_custom_delimiters() -> None:
    """Security review, MEDIUM 3: `tests/fixtures/hl7/custom_delimiters.hl7`'s own shape -- `^` is not
    this message's component separator, so a literal `^` inside PID-3 is just data, not four components."""
    pid = _custom_delim_segment("PID", "1!!9000004^^^SYNTH-HOSPITAL^MR")
    identifier = cx_identifier(pid, 3)
    assert identifier is not None
    assert identifier.value == "9000004^^^SYNTH-HOSPITAL^MR"
    assert identifier.system is None


def test_xpn_human_name() -> None:
    pid = _segment("PID", "1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q")
    name = xpn_human_name(pid, 5)
    assert name is not None
    assert name.family == "SYNTHPATIENT"
    assert name.given == ["ALPHA", "Q"]


def test_xpn_human_name_honours_custom_delimiters() -> None:
    pid = _custom_delim_segment("PID", "1!!9000004^^^SYNTH-HOSPITAL^MR!!SYNTHPATIENT@EPSILON")
    name = xpn_human_name(pid, 5)
    assert name is not None
    assert name.family == "SYNTHPATIENT"
    assert name.given == ["EPSILON"]


def test_xad_address() -> None:
    pid = _segment(
        "PID", "1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M|||100 SYNTH ST^^SYNTHVILLE^ZZ^00000^US"
    )
    address = xad_address(pid, 11)
    assert address is not None
    assert address.line == ["100 SYNTH ST"]
    assert address.city == "SYNTHVILLE"
    assert address.state == "ZZ"
    assert address.postalCode == "00000"
    assert address.country == "US"


def test_cwe_components() -> None:
    obx = _segment("OBX", "1|NM|8867-4^Heart rate^LN", msh=_MSH)
    assert cwe_components(obx, 3) == ("8867-4", "Heart rate", "LN")


def test_pl_display() -> None:
    pv1 = _segment("PV1", "1|I|SYNTHUNIT^101^A^SYNTH-HOSPITAL")
    assert pl_display(pv1, 3) == "SYNTHUNIT/101/A"


def test_pl_display_none_when_absent() -> None:
    pv1 = _segment("PV1", "1|I")
    assert pl_display(pv1, 3) is None


def test_xcn_display_prefers_name_over_id() -> None:
    pv1 = _segment("PV1", "1|I|||||9001^SYNTHDOC^ATTENDING^^^^MD")
    assert xcn_display(pv1, 7) == "SYNTHDOC ATTENDING"


def test_xcn_display_falls_back_to_id_with_no_name() -> None:
    pv1 = _segment("PV1", "1|I|||||9001")
    assert xcn_display(pv1, 7) == "9001"


def test_dt8_to_fhir_date() -> None:
    assert dt8_to_fhir_date("19800115") == "1980-01-15"
    assert dt8_to_fhir_date("") is None
    assert dt8_to_fhir_date("1980") is None


def test_dt8_to_fhir_date_rejects_a_calendar_invalid_date() -> None:
    """Security review, HIGH 2: `19800231` (there is no 31st of February) must not reach `Patient.
    birthDate`'s own validator, whose `ValidationError` would embed it verbatim."""
    assert dt8_to_fhir_date("19800231") is None


def test_dt8_to_fhir_date_rejects_non_digits() -> None:
    assert dt8_to_fhir_date("1980AB15") is None


def test_ts_to_fhir_datetime_requires_a_timezone_once_time_is_present() -> None:
    assert ts_to_fhir_datetime("20260101120000") == "2026-01-01T12:00:00+00:00"
    assert ts_to_fhir_datetime("20260101") == "2026-01-01"
    assert ts_to_fhir_datetime("") is None


def test_ts_to_fhir_datetime_rejects_invalid_clock_values() -> None:
    assert ts_to_fhir_datetime("202601019965") is None  # minute 65
    assert ts_to_fhir_datetime("20260230120000") is None  # 30 February


def test_ts_to_fhir_datetime_rejects_non_digits() -> None:
    assert ts_to_fhir_datetime("2026AB01120000") is None


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


def test_patient_class_to_encounter_class_strips_surrounding_whitespace() -> None:
    """Security review, HIGH 2: a leading/trailing space in PV1-2 must not reach `Coding.code`'s own
    validator, whose `ValidationError` would embed it verbatim."""
    coding = patient_class_to_encounter_class(" I ")
    assert coding.code == "IMP"


def test_sanitize_fhir_code() -> None:
    assert sanitize_fhir_code(" I ") == "I"
    assert sanitize_fhir_code("normal code") == "normal code"
    assert sanitize_fhir_code("   ") is None
    assert sanitize_fhir_code("") is None


# -- bounded_decimal / decimal_preserves_precision (security review HIGH 1 / MEDIUM 4) -----------------


def test_bounded_decimal_accepts_an_ordinary_value() -> None:
    assert bounded_decimal("72") == Decimal("72")
    assert bounded_decimal("72.10") == Decimal("72.10")


def test_bounded_decimal_rejects_non_decimal_text() -> None:
    assert bounded_decimal("not-a-number") is None


def test_bounded_decimal_rejects_nan_and_infinity() -> None:
    assert bounded_decimal("NaN") is None
    assert bounded_decimal("Infinity") is None
    assert bounded_decimal("-Infinity") is None


def test_bounded_decimal_rejects_an_oversized_exponent_cheaply() -> None:
    import time

    start = time.perf_counter()
    result = bounded_decimal("1E+999999999")
    elapsed = time.perf_counter() - start
    assert result is None
    assert elapsed < 1.0


def test_bounded_decimal_rejects_too_many_significant_digits() -> None:
    assert bounded_decimal("1" * 40) is None


def test_decimal_preserves_precision_for_whole_numbers() -> None:
    assert decimal_preserves_precision(Decimal("72")) is True
    assert decimal_preserves_precision(Decimal("120")) is True


def test_decimal_preserves_precision_rejects_a_dropped_trailing_zero() -> None:
    assert decimal_preserves_precision(Decimal("72.10")) is False


def test_decimal_preserves_precision_rejects_beyond_float_precision() -> None:
    assert decimal_preserves_precision(Decimal("1.00000000000000001")) is False


def test_decimal_preserves_precision_accepts_an_exact_ordinary_decimal() -> None:
    assert decimal_preserves_precision(Decimal("72.5")) is True
