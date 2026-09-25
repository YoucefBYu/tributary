"""Accessor path grammar: `msg["PID.5.1"]` and friends.

    SEG[occurrence].field[repetition].component.subcomponent

`SEG` is the segment name (`PID`, `OBX`, a 2-4 character Z-segment like `ZPD`). Everything else is
optional and 1-based, matching HL7 v2's own numbering so a path reads the same as the implementation
guide that names the field. `[occurrence]` selects which repeat of a repeating segment (default 1,
i.e. the first); `[repetition]` selects which repeat of a repeating field (default 1). Examples:

    "PID.5.1"        -> PID segment, first occurrence, field 5, component 1
    "PID[2].5"       -> second PID occurrence, field 5
    "PID.3[2].1"     -> field 3, repetition 2, component 1
    "OBX.5.1.2"      -> field 5, component 1, subcomponent 2
    "MSH.9"          -> whole of MSH-9 (message type), unsplit

No other syntax is supported in v0 (no `PID-5-1` dashes, no 0-based indexing): one grammar, documented
once, rather than several equivalent spellings that a reader has to recognise.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import FieldPathError

#: The largest index any of the five numeric groups below may hold. `with_field`'s padding loops
#: (`message.py`) grow a Python list to this many entries in the worst case, so this bounds that work as
#: well as the value itself -- an unbounded `\d+` here was a code-review finding (an index of, say,
#: 999999999 would try to append that many empty strings to a component list).
MAX_INDEX = 9999

_PATH = re.compile(
    r"^(?P<segment>[A-Za-z][A-Za-z0-9]{1,3})"
    r"(?:\[(?P<segocc>\d{1,4})\])?"
    r"(?:\.(?P<field>\d{1,4})"
    r"(?:\[(?P<rep>\d{1,4})\])?"
    r"(?:\.(?P<component>\d{1,4})"
    r"(?:\.(?P<subcomponent>\d{1,4}))?)?)?$"
)


@dataclass(frozen=True, slots=True)
class FieldPath:
    """A parsed accessor path. Every index is 1-based and `None` means "not addressed"."""

    segment: str
    segment_occurrence: int
    field: int | None
    repetition: int
    component: int | None
    subcomponent: int | None


def parse(path: str) -> FieldPath:
    """Parse an accessor path string. Raises `FieldPathError` for anything malformed, including a
    zero or negative index -- HL7 v2 addressing starts at 1, so `"PID.0"` is a path bug, not field 1."""
    if not isinstance(path, str) or not path:
        raise FieldPathError(str(path), "path must be a non-empty string")
    match = _PATH.match(path)
    if match is None:
        raise FieldPathError(path, "does not match SEG[occ].field[rep].component.subcomponent")

    def _int(name: str, *, default: int) -> int:
        text = match.group(name)
        if text is None:
            return default
        value = int(text)
        if value < 1:
            raise FieldPathError(path, f"{name} must be 1 or greater, got {value}")
        if value > MAX_INDEX:
            raise FieldPathError(path, f"{name} must be {MAX_INDEX} or less, got {value}")
        return value

    field_text = match.group("field")
    return FieldPath(
        segment=match.group("segment").upper(),
        segment_occurrence=_int("segocc", default=1),
        field=None if field_text is None else _int("field", default=1),
        repetition=_int("rep", default=1),
        component=(_int("component", default=1) if match.group("component") is not None else None),
        subcomponent=(_int("subcomponent", default=1) if match.group("subcomponent") is not None else None),
    )
