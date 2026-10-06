"""`escaping.escape`/`unescape`: the standard escape sequences, and that they are exact inverses."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st
from tributary.hl7v2.delimiters import Delimiters
from tributary.hl7v2.escaping import escape, unescape

D = Delimiters()


def test_unescape_decodes_the_five_named_sequences() -> None:
    assert unescape("a\\F\\b", D) == "a|b"
    assert unescape("a\\S\\b", D) == "a^b"
    assert unescape("a\\T\\b", D) == "a&b"
    assert unescape("a\\R\\b", D) == "a~b"
    assert unescape("a\\E\\b", D) == "a\\b"


def test_unescape_decodes_hex() -> None:
    assert unescape("a\\X4869\\b", D) == "aHib"


def test_unescape_strips_highlighting_markers() -> None:
    assert unescape("a\\H\\bold\\N\\c", D) == "aboldc"


def test_unescape_passes_through_unterminated_escape() -> None:
    assert unescape("abc\\F", D) == "abc\\F"


def test_unescape_preserves_unknown_local_sequences() -> None:
    # A vendor Z-escape this module does not define is kept, not dropped.
    assert unescape("a\\Zfoo\\b", D) == "a\\Zfoo\\b"


def test_text_with_no_escape_character_is_returned_unchanged() -> None:
    assert unescape("plain text", D) == "plain text"


def test_escape_is_the_inverse_for_the_five_reserved_characters() -> None:
    text = "a|b^c&d~e\\f"
    assert unescape(escape(text, D), D) == text


def test_escape_handles_carriage_return() -> None:
    escaped = escape("a\rb", D)
    assert "\r" not in escaped
    assert unescape(escaped, D) == "a\rb"


@given(st.text(alphabet="ab|^&~\\\r", max_size=40))
def test_escape_then_unescape_round_trips_for_reserved_characters(text: str) -> None:
    assert unescape(escape(text, D), D) == text


@given(st.text(min_size=0, max_size=60))
def test_escape_output_never_contains_a_bare_delimiter(text: str) -> None:
    escaped = escape(text, D)
    # `escape` substitutes every occurrence of each delimiter, so none of them can survive unescaped --
    # the property that makes it safe to splice `escaped` into a field without corrupting structure.
    for delimiter in (D.field, D.component, D.subcomponent, D.repetition):
        assert delimiter not in escaped


# -- security regression: every C0 control character and DEL, not only CR -------------------------


def test_escape_hex_escapes_bare_lf() -> None:
    """A security review found LF left raw: `with_field("PID.5.1", "EVIL\\nZZZ|1|INJECTED")` on an
    LF-only message injected a `ZZZ` segment. `escape` must never let a bare LF back out."""
    escaped = escape("a\nb", D)
    assert "\n" not in escaped
    assert unescape(escaped, D) == "a\nb"


@given(st.integers(min_value=0x00, max_value=0x1F) | st.just(0x7F))
def test_escape_hex_escapes_every_c0_control_character_and_del(codepoint: int) -> None:
    char = chr(codepoint)
    escaped = escape(char, D)
    assert char not in escaped
    assert unescape(escaped, D) == char


@given(st.text(alphabet=st.characters(min_codepoint=0x00, max_codepoint=0x1F), max_size=20))
def test_escape_then_unescape_round_trips_for_c0_control_characters(text: str) -> None:
    assert unescape(escape(text, D), D) == text
