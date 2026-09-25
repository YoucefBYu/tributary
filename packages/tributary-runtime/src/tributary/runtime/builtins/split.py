"""`tributary.core.split`: the engine's splitter (channel-spec README 3.3.2).

    - name: unbatch
      uses: tributary.core.split
      with: {by: hl7-batch}

    - name: entries
      uses: tributary.core.split
      with: {by: json-array, pointer: /entry}

Every split rule of README 3.3.2 applies, because this is a `@splitter` like any other: deterministic
child ids, `tb-parent-id`, the parent's key, refused on a `per-key` channel, at most 10,000 children,
and an empty split is `Drop("EMPTY_SPLIT")`.

* `hl7-batch` - one child per `MSH` segment group. The batch envelope (`FHS`, `BHS`, `BTS`, `FTS`) is
  not part of any child. Segments are re-joined with CR, the HL7 segment terminator; field bytes are
  untouched.
* `json-array` - one child per element of the array at `pointer` (RFC 6901; empty means the document
  itself). **Each child is the element's exact source text**, not a re-serialisation: parsing and
  re-dumping would turn a lab value written `1.50` into `1.5`, and significant figures are clinical data.

Input the splitter cannot read raises `Reject("SPLIT_INPUT_INVALID")`: a malformed batch does not become
well-formed on the fourth attempt.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Final, Literal

from tributary.sdk import Message, Reject
from tributary.sdk import headers as h
from tributary.sdk.codes import SPLIT_INPUT_INVALID
from tributary.sdk.keys import JSON_CONTENT_TYPES

from .base import BuiltinSplitter, hl7_segments, is_hl7, refuse, segment_id

NAME: Final = "tributary.core.split"
BATCH_ENVELOPE: Final = frozenset({b"FHS", b"BHS", b"BTS", b"FTS"})
POINTER: Final = re.compile(r"^(/[A-Za-z0-9._:-]+)*$")
_WHITESPACE: Final = " \t\n\r"

SplitBy = Literal["hl7-batch", "json-array"]


def _check(step: str, values: Mapping[str, object]) -> None:
    if values.get("by") != "json-array" and "pointer" in values:
        raise refuse(step, "`pointer` applies to by: json-array only")
    if not POINTER.match(str(values.get("pointer", ""))):
        raise refuse(step, "`pointer` must be an RFC 6901 pointer such as /entry, or empty for the document")


def _split(message: Message, *, by: SplitBy, pointer: str = "") -> list[Message]:
    if by == "hl7-batch":
        return [message.with_payload(part) for part in _hl7_batch(message)]
    return [message.with_payload(part) for part in _json_array(message, pointer)]


def _hl7_batch(message: Message) -> list[bytes]:
    if not is_hl7(message):
        raise Reject.engine(SPLIT_INPUT_INVALID)
    groups: list[list[bytes]] = []
    current: list[bytes] | None = None
    for segment in hl7_segments(message.payload or b""):
        identifier = segment_id(segment)
        if identifier is None:
            raise Reject.engine(SPLIT_INPUT_INVALID)
        if identifier in BATCH_ENVELOPE:
            current = None
            continue
        if identifier == b"MSH":
            current = [segment]
            groups.append(current)
            continue
        if current is None:  # a segment outside any message: the batch is not what it claims
            raise Reject.engine(SPLIT_INPUT_INVALID)
        current.append(segment)
    return [b"\r".join(group) + b"\r" for group in groups]


def _json_array(message: Message, pointer: str) -> list[bytes]:
    content_type = (message.get(h.TB_CONTENT_TYPE) or "").split(";", 1)[0].strip().lower()
    if content_type not in JSON_CONTENT_TYPES or message.payload is None:
        raise Reject.engine(SPLIT_INPUT_INVALID)
    try:
        text = message.payload.decode("utf-8")
        json.loads(text, parse_constant=_no_constants)  # the whole document, strictly, before walking it
        spans = _array_spans(text, tuple(pointer[1:].split("/")) if pointer else ())
    except (ValueError, RecursionError, IndexError):
        raise Reject.engine(SPLIT_INPUT_INVALID) from None
    if spans is None:
        raise Reject.engine(SPLIT_INPUT_INVALID)
    return [span.encode("utf-8") for span in spans]


def _no_constants(_name: str) -> object:
    raise ValueError("NaN and Infinity are not JSON")


def _skip(text: str, index: int) -> int:
    while index < len(text) and text[index] in _WHITESPACE:
        index += 1
    return index


def _array_spans(text: str, tokens: tuple[str, ...]) -> list[str] | None:
    """The exact source text of each element of the array at `tokens`, or `None` when the pointer is
    absent, ambiguous (a duplicated member name) or does not name an array. `text` is valid JSON."""
    decoder = json.JSONDecoder()
    index = _skip(text, 0)
    for token in tokens:
        if text[index] == "{":
            found: int | None = None
            index = _skip(text, index + 1)
            while text[index] != "}":
                name, index = decoder.raw_decode(text, index)
                index = _skip(text, _skip(text, index) + 1)  # past the colon
                if name == token:
                    if found is not None:
                        return None
                    found = index
                _value, index = decoder.raw_decode(text, index)
                index = _skip(text, index)
                if text[index] == ",":
                    index = _skip(text, index + 1)
            if found is None:
                return None
            index = found
        elif text[index] == "[":
            if not token.isdigit() or (len(token) > 1 and token[0] == "0"):
                return None
            wanted, position = int(token), 0
            index = _skip(text, index + 1)
            while text[index] != "]" and position < wanted:
                _value, index = decoder.raw_decode(text, index)
                index = _skip(text, index)
                if text[index] == ",":
                    index = _skip(text, index + 1)
                position += 1
            if text[index] == "]":
                return None
        else:
            return None
    if text[index] != "[":
        return None
    spans: list[str] = []
    index = _skip(text, index + 1)
    while text[index] != "]":
        _value, end = decoder.raw_decode(text, index)
        spans.append(text[index:end])
        index = _skip(text, end)
        if text[index] == ",":
            index = _skip(text, index + 1)
    return spans


split = BuiltinSplitter(_split, name=NAME, check=_check)
