"""HL7 v2 delimiters: self-describing per message, from `MSH-1` and `MSH-2`.

Every other segment in the message is split using these, never a hardcoded `|^~\\&`. A sender is free to
choose different characters (rare in practice, but legal, and real-world feeds do vary this), so treating
the standard characters as constants would silently corrupt any message that does not use them.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import MalformedMessageError

#: The characters the standard recommends, used only as the fallback for a missing `MSH-2` component.
_DEFAULT_COMPONENT = "^"
_DEFAULT_REPETITION = "~"
_DEFAULT_ESCAPE = "\\"
_DEFAULT_SUBCOMPONENT = "&"


@dataclass(frozen=True, slots=True)
class Delimiters:
    """The five characters that punctuate one HL7 v2 message. Each is exactly one character."""

    field: str = "|"
    component: str = _DEFAULT_COMPONENT
    repetition: str = _DEFAULT_REPETITION
    escape: str = _DEFAULT_ESCAPE
    subcomponent: str = _DEFAULT_SUBCOMPONENT

    def __post_init__(self) -> None:
        values = (self.field, self.component, self.repetition, self.escape, self.subcomponent)
        for value in values:
            if not isinstance(value, str) or len(value) != 1:
                raise MalformedMessageError("each delimiter must be exactly one character")
        if len(set(values)) != len(values):
            raise MalformedMessageError("the five delimiters must be distinct characters")


def extract(first_segment: bytes, *, encoding: str) -> Delimiters:
    """Read `MSH-1` and `MSH-2` from the first segment's raw bytes.

    `MSH-1` is the field separator itself: the byte immediately after the literal `MSH`. `MSH-2` is the
    run of bytes between that separator and the next occurrence of it (standard: `^~\\&`, in that order
    -- component, repetition, escape, subcomponent). A message that supplies fewer than four is tolerated:
    the missing trailing ones fall back to the standard characters, which is the most common real-world
    deviation (a sender that only bothers to declare `^~\\&` up to the characters it actually uses).
    """
    if (
        not first_segment.startswith(b"MSH")
        and not first_segment.startswith(b"BHS")
        and not first_segment.startswith(b"FHS")
    ):
        raise MalformedMessageError("first segment is not MSH, BHS or FHS")
    if len(first_segment) < 4:
        raise MalformedMessageError("first segment is too short to carry MSH-1 and MSH-2")
    try:
        field_sep = first_segment[3:4].decode(encoding)
    except (UnicodeDecodeError, LookupError) as exc:
        raise MalformedMessageError("MSH-1 is not decodable in the given encoding") from exc

    field_sep_byte = first_segment[3:4]
    rest = first_segment[4:]
    end = rest.find(field_sep_byte)
    encoding_chars_bytes = rest if end == -1 else rest[:end]
    try:
        encoding_chars = encoding_chars_bytes.decode(encoding)
    except (UnicodeDecodeError, LookupError) as exc:
        raise MalformedMessageError("MSH-2 is not decodable in the given encoding") from exc

    if not encoding_chars:
        raise MalformedMessageError("MSH-2 (encoding characters) is empty")
    if len(encoding_chars) > 4:
        raise MalformedMessageError("MSH-2 (encoding characters) has more than four characters")

    component = encoding_chars[0]
    repetition = encoding_chars[1] if len(encoding_chars) >= 2 else _DEFAULT_REPETITION
    escape = encoding_chars[2] if len(encoding_chars) >= 3 else _DEFAULT_ESCAPE
    subcomponent = encoding_chars[3] if len(encoding_chars) >= 4 else _DEFAULT_SUBCOMPONENT

    return Delimiters(
        field=field_sep, component=component, repetition=repetition, escape=escape, subcomponent=subcomponent
    )
