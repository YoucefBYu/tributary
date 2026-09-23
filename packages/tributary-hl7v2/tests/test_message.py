"""`Hl7Message`: parsing, accessors, repetitions, mutation and the round-trip guarantee."""

from __future__ import annotations

import pytest
from tributary.hl7v2.errors import FieldPathError, MalformedMessageError, PayloadDecodeError, SegmentNotFoundError
from tributary.hl7v2.message import Hl7Message

ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-HOSPITAL|SYNTH-ADT-RECV|SYNTH-RECV-FACILITY|"
    b"20260101120000||ADT^A01^ADT_A01|SYNTHMSG00001|P|2.5.1\r"
    b"EVN|A01|20260101120000\r"
    b"PID|1||9000001^^^SYNTH-HOSPITAL^MR||SYNTHPATIENT^ALPHA^Q||19800115|M\r"
    b"NK1|1|SYNTHCONTACT^ONE|SPO\r"
    b"NK1|2|SYNTHCONTACT^TWO|CHD\r"
    b"PV1|1|I|SYNTHUNIT^101^A^SYNTH-HOSPITAL||||9001^SYNTHDOC^ATTENDING\r"
)


def test_msh_field_offset_msh1_is_the_separator_itself() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.1"] == "|"


def test_msh_field_offset_msh2_is_the_encoding_characters() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.2"] == "^~\\&"


def test_msh_field_offset_msh3_is_the_first_real_field() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["MSH.3"] == "TRIBUTARY-SYNTH"


def test_component_access() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5.1"] == "SYNTHPATIENT"
    assert msg["PID.5.2"] == "ALPHA"
    assert msg["PID.5.3"] == "Q"


def test_missing_trailing_component_is_empty_string_not_an_error() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5.4"] == ""


def test_missing_trailing_field_is_empty_string() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.29"] == ""  # far beyond what this fixture sends


def test_whole_field_with_no_component_index() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["PID.5"] == "SYNTHPATIENT^ALPHA^Q"


def test_second_segment_occurrence() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg["NK1[1].2.1"] == "SYNTHCONTACT"
    assert msg["NK1[1].2.2"] == "ONE"
    assert msg["NK1[2].2.2"] == "TWO"


def test_segment_count() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.segment_count("NK1") == 2
    assert msg.segment_count("OBX") == 0


def test_segments_returns_every_occurrence_in_order() -> None:
    msg = Hl7Message.parse(ADT_A01)
    names = [seg.field(2, component=2) for seg in msg.segments("NK1")]
    assert names == ["ONE", "TWO"]


def test_segment_field_convenience_method() -> None:
    msg = Hl7Message.parse(ADT_A01)
    pid = msg.segment("PID")
    assert pid.field(5, component=1) == "SYNTHPATIENT"
    assert pid.field(5) == "SYNTHPATIENT^ALPHA^Q"


def test_get_returns_default_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.get("OBX.5") is None
    assert msg.get("OBX.5", "fallback") == "fallback"


def test_require_raises_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.require("OBX.5")


def test_getitem_raises_when_segment_absent() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        _ = msg["OBX.5"]


def test_segment_raises_for_occurrence_beyond_what_exists() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.segment("NK1", 3)


def test_message_type() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.message_type() == ("ADT", "A01", "ADT_A01")


def test_control_id_and_version() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.control_id() == "SYNTHMSG00001"
    assert msg.version() == "2.5.1"


# -- round trip -----------------------------------------------------------------------------------


def test_to_bytes_is_identical_for_an_untouched_message() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert msg.to_bytes() == ADT_A01


def test_repr_never_contains_field_content() -> None:
    msg = Hl7Message.parse(ADT_A01)
    text = repr(msg)
    assert "SYNTHPATIENT" not in text
    assert "9000001" not in text


def test_len_counts_segments() -> None:
    msg = Hl7Message.parse(ADT_A01)
    assert len(msg) == 6  # MSH, EVN, PID, NK1, NK1, PV1


# -- repetitions ------------------------------------------------------------------------------------


def test_repetitions_of_a_repeating_field() -> None:
    raw = ADT_A01.replace(b"9001^SYNTHDOC^ATTENDING", b"9001^SYNTHDOC^ATTENDING~9002^SYNTHDOC2^COATTENDING")
    msg = Hl7Message.parse(raw)
    reps = msg.repetitions("PV1.7.2")
    assert reps == ("SYNTHDOC", "SYNTHDOC2")


def test_repetitions_needs_a_field_index() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.repetitions("PID")


# -- custom delimiters ------------------------------------------------------------------------------


def test_custom_delimiters_round_trip_and_are_used_for_field_access() -> None:
    raw = (
        b"MSH!@$\\*!TRIBUTARY-SYNTH!SYNTH-HOSPITAL!SYNTH-ADT-RECV!SYNTH-RECV-FACILITY!"
        b"20260106140000!!ADT@A01@ADT_A01!SYNTHMSG00005!P!2.5.1\r"
        b"PID!1!!9000004^^^SYNTH-HOSPITAL^MR!!SYNTHPATIENT@EPSILON!!19850707!M\r"
    )
    msg = Hl7Message.parse(raw)
    assert msg.delimiters.field == "!"
    assert msg.delimiters.component == "@"
    assert msg["PID.5.1"] == "SYNTHPATIENT"
    assert msg["PID.5.2"] == "EPSILON"
    assert msg.to_bytes() == raw


# -- escaping on read --------------------------------------------------------------------------------


def test_field_access_unescapes_reserved_characters() -> None:
    raw = ADT_A01.replace(b"SYNTHPATIENT^ALPHA^Q", rb"SYNTHPATIENT\S\ALPHA^Q")
    msg = Hl7Message.parse(raw)
    assert msg["PID.5.1"] == "SYNTHPATIENT^ALPHA"  # \S\ decoded to a literal component separator


# -- malformed input: typed errors, never a crash ------------------------------------------------


def test_empty_message_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"")


def test_non_msh_start_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"NOTHL7 this is not a message at all")


def test_truncated_msh_raises_typed_error() -> None:
    with pytest.raises(MalformedMessageError):
        Hl7Message.parse(b"MS")


def test_bad_encoding_raises_typed_error_not_unicode_decode_error() -> None:
    # A field containing a byte invalid in the *requested* encoding raises on access, typed, not raw.
    raw = ADT_A01.replace(b"SYNTHPATIENT", b"SYNTHPATIENT\xff\xfe")
    bad = Hl7Message.parse(raw, encoding="ascii")
    with pytest.raises(PayloadDecodeError):
        _ = bad["PID.5.1"]


def test_malformed_message_error_never_contains_message_bytes() -> None:
    with pytest.raises(MalformedMessageError) as caught:
        Hl7Message.parse(b"")
    assert "TRIBUTARY-SYNTH" not in str(caught.value)


# -- mutation (copy-on-write) ------------------------------------------------------------------------


def test_with_field_replaces_only_the_addressed_component() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.2", "BETA")
    assert updated["PID.5.1"] == "SYNTHPATIENT"
    assert updated["PID.5.2"] == "BETA"
    assert updated["PID.5.3"] == "Q"
    # Every other segment, byte-identical.
    assert updated.segment("EVN").raw_bytes() == msg.segment("EVN").raw_bytes()
    assert updated.segment("NK1", 1).raw_bytes() == msg.segment("NK1", 1).raw_bytes()


def test_with_field_escapes_reserved_characters_in_the_new_value() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.2", "A^B")
    assert updated["PID.5.2"] == "A^B"  # round-trips through escape/unescape
    assert b"A\\S\\B" in updated.segment("PID").raw_bytes()


def test_with_field_does_not_mutate_the_original() -> None:
    msg = Hl7Message.parse(ADT_A01)
    msg.with_field("PID.5.2", "CHANGED")
    assert msg["PID.5.2"] == "ALPHA"
    assert msg.to_bytes() == ADT_A01


def test_with_field_on_a_new_component_pads_missing_ones() -> None:
    msg = Hl7Message.parse(ADT_A01)
    updated = msg.with_field("PID.5.5", "NEWCOMP")
    assert updated["PID.5.5"] == "NEWCOMP"
    assert updated["PID.5.4"] == ""


def test_with_field_cannot_set_msh1() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.with_field("MSH.1", "#")


def test_with_field_cannot_set_msh2() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(FieldPathError):
        msg.with_field("MSH.2", "@$\\*")


def test_with_field_on_a_missing_segment_occurrence_raises() -> None:
    msg = Hl7Message.parse(ADT_A01)
    with pytest.raises(SegmentNotFoundError):
        msg.with_field("OBX.5", "1")
