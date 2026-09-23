"""`ack.build_ack`: MSH/MSA construction, app/facility swap, and that PID never leaks into the ACK."""

from __future__ import annotations

import pytest
from tributary.hl7v2.ack import build_ack
from tributary.hl7v2.errors import AckBuildError
from tributary.hl7v2.message import Hl7Message

ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
    b"PID|1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M\r"
)


def test_application_and_facility_are_swapped() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00001", timestamp="20260101120005")
    assert ack["MSH.3"] == "SYNTH-ADT-RECV"  # was MSH-5 on the source
    assert ack["MSH.4"] == "SYNTH-RECV-FACILITY"  # was MSH-6
    assert ack["MSH.5"] == "TRIBUTARY-SYNTH"  # was MSH-3
    assert ack["MSH.6"] == "SYNTH-HOSPITAL"  # was MSH-4


def test_delimiters_are_echoed_from_the_source() -> None:
    raw = ADT_A01.replace(b"|^~\\&|", b"!@$\\*!").replace(b"|", b"!")
    source = Hl7Message.parse(raw)
    ack = build_ack(source, code="AA", control_id="ACK00002", timestamp="20260101120010")
    assert ack.delimiters.field == "!"
    assert ack.delimiters.component == "@"


def test_msa_carries_the_code_and_echoes_the_original_control_id() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AE", control_id="ACK00003", timestamp="20260101120015", text="parse failed")
    assert ack["MSA.1"] == "AE"
    assert ack["MSA.2"] == "SYNTHMSG00001"
    assert ack["MSA.3"] == "parse failed"


def test_ack_type_mirrors_the_source_trigger_event() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00004", timestamp="20260101120020")
    assert ack["MSH.9.1"] == "ACK"
    assert ack["MSH.9.2"] == "A01"


def test_ack_never_contains_pid_content() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="ACK00005", timestamp="20260101120025")
    assert ack.segment_count("PID") == 0
    assert b"SYNTHPATIENT" not in ack.to_bytes()
    assert b"9000001" not in ack.to_bytes()


def test_processing_id_defaults_to_source_msh11_else_p() -> None:
    source = Hl7Message.parse(ADT_A01)  # this fixture has no MSH-11 set
    ack = build_ack(source, code="AA", control_id="ACK00006", timestamp="20260101120030")
    assert ack["MSH.11"] == "P"

    with_processing_id = ADT_A01.replace(b"SYNTHMSG00001|P|2.5.1", b"SYNTHMSG00001|T|2.5.1")
    source2 = Hl7Message.parse(with_processing_id)
    ack2 = build_ack(source2, code="AA", control_id="ACK00007", timestamp="20260101120035")
    assert ack2["MSH.11"] == "T"


def test_ack_control_id_and_timestamp_are_caller_supplied() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="MY-OWN-ID", timestamp="20991231235959")
    assert ack["MSH.10"] == "MY-OWN-ID"
    assert ack["MSH.7"] == "20991231235959"


def test_ack_control_id_containing_reserved_characters_is_escaped() -> None:
    source = Hl7Message.parse(ADT_A01)
    ack = build_ack(source, code="AA", control_id="weird|id^value", timestamp="20260101120040")
    assert ack["MSH.10"] == "weird|id^value"  # round-trips through escape/unescape


def test_invalid_ack_code_is_rejected() -> None:
    source = Hl7Message.parse(ADT_A01)
    with pytest.raises(AckBuildError):
        build_ack(source, code="NOPE", control_id="ACK00008", timestamp="20260101120045")  # type: ignore[arg-type]
