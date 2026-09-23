"""`delimiters.extract`: reading `MSH-1`/`MSH-2`, including tolerant fallback."""

from __future__ import annotations

import pytest
from tributary.hl7v2.delimiters import Delimiters, extract
from tributary.hl7v2.errors import MalformedMessageError


def test_standard_delimiters() -> None:
    d = extract(b"MSH|^~\\&|TRIBUTARY-SYNTH", encoding="utf-8")
    assert d == Delimiters(field="|", component="^", repetition="~", escape="\\", subcomponent="&")


def test_custom_delimiters() -> None:
    d = extract(b"MSH!@$\\*!TRIBUTARY-SYNTH", encoding="utf-8")
    assert d == Delimiters(field="!", component="@", repetition="$", escape="\\", subcomponent="*")


def test_short_encoding_characters_fall_back_to_standard_for_the_rest() -> None:
    # Only component and repetition declared; escape and subcomponent fall back.
    d = extract(b"MSH|^~|TRIBUTARY-SYNTH", encoding="utf-8")
    assert d.component == "^"
    assert d.repetition == "~"
    assert d.escape == "\\"
    assert d.subcomponent == "&"


def test_single_encoding_character() -> None:
    d = extract(b"MSH|^|TRIBUTARY-SYNTH", encoding="utf-8")
    assert d.component == "^"
    assert d.repetition == "~"


def test_bhs_and_fhs_are_accepted_as_the_first_segment() -> None:
    assert extract(b"BHS|^~\\&|TRIBUTARY-SYNTH", encoding="utf-8").field == "|"
    assert extract(b"FHS|^~\\&|TRIBUTARY-SYNTH", encoding="utf-8").field == "|"


@pytest.mark.parametrize(
    "raw",
    [
        b"XYZ|^~\\&|TRIBUTARY-SYNTH",  # not MSH/BHS/FHS
        b"MS",  # too short for MSH-1
        b"MSH|",  # MSH-2 empty
        b"MSH|^~\\&12|TRIBUTARY-SYNTH",  # MSH-2 too long (5 chars before the next field sep)
    ],
)
def test_malformed_delimiters_raise_typed_error(raw: bytes) -> None:
    with pytest.raises(MalformedMessageError):
        extract(raw, encoding="utf-8")


def test_error_message_never_contains_field_content() -> None:
    with pytest.raises(MalformedMessageError) as caught:
        extract(b"MSH|", encoding="utf-8")
    assert "TRIBUTARY-SYNTH" not in str(caught.value)


def test_delimiters_must_be_five_distinct_characters() -> None:
    with pytest.raises(MalformedMessageError):
        Delimiters(field="|", component="|", repetition="~", escape="\\", subcomponent="&")
