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
  not part of any child. Segments are re-joined with CR, the HL7 segment terminator, and empty lines
  are dropped; field bytes are untouched. **A child never gains a byte its parent did not have**: the
  last segment gets a CR only if the parent terminated it. Otherwise a 1,000,000-byte message with no
  final CR would become a 1,000,001-byte child that no topic accepts (security review, W2-5).
* `json-array` - one child per element of the array at `pointer` (RFC 6901; empty means the document
  itself). **Each child is the element's exact source text**, not a re-serialisation: parsing and
  re-dumping would turn a lab value written `1.50` into `1.5`, and significant figures are clinical data.

Input the splitter cannot read raises `Reject("SPLIT_INPUT_INVALID")`: a malformed batch does not become
well-formed on the fourth attempt.

**Both splitters are generators.** The SDK's 10,000-child cap counts children as they are pulled, so
a payload that would split into a million children stops costing work at child 10,001 instead of
building every child first. Input is validated as it is walked, to the end: a defect after the last
child still rejects the parent, because the SDK drains the generator before anything is produced.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping
from typing import Final, Literal

from tributary.sdk import Message, Reject
from tributary.sdk import headers as h
from tributary.sdk.codes import SPLIT_INPUT_INVALID
from tributary.sdk.keys import JSON_CONTENT_TYPES

from .base import BuiltinSplitter, is_hl7, refuse, segment_id

NAME: Final = "tributary.core.split"
BATCH_ENVELOPE: Final = frozenset({b"FHS", b"BHS", b"BTS", b"FTS"})
POINTER: Final = re.compile(r"^(/[A-Za-z0-9._:-]+)*$")
_WHITESPACE: Final = " \t\n\r"
_LINE: Final = re.compile(rb"([^\r\n]*)(\r\n|\r|\n)?")

SplitBy = Literal["hl7-batch", "json-array"]


def _check(step: str, values: Mapping[str, object]) -> None:
    if values.get("by") != "json-array" and "pointer" in values:
        raise refuse(step, "`pointer` applies to by: json-array only")
    if not POINTER.match(str(values.get("pointer", ""))):
        raise refuse(step, "`pointer` must be an RFC 6901 pointer such as /entry, or empty for the document")


def _split(message: Message, *, by: SplitBy, pointer: str = "") -> Iterator[Message]:
    parts = _hl7_batch(message) if by == "hl7-batch" else _json_array(message, pointer)
    return (message.with_payload(part) for part in parts)


# -- hl7-batch ------------------------------------------------------------------------------------


def _hl7_batch(message: Message) -> Iterator[bytes]:
    if not is_hl7(message):
        raise Reject.engine(SPLIT_INPUT_INVALID)
    group: list[bytes] = []
    terminated = False  # whether the group's last segment was terminated in the parent
    for match in _LINE.finditer(message.payload or b""):
        segment, terminator = match.group(1), match.group(2)
        if not segment:
            continue  # an empty line, or the empty match at the end of the payload
        identifier = segment_id(segment)
        if identifier is None:
            raise Reject.engine(SPLIT_INPUT_INVALID)
        if identifier == b"MSH" or identifier in BATCH_ENVELOPE:
            if group:
                yield _joined(group, terminated)
            group = [segment] if identifier == b"MSH" else []
        elif not group:  # a segment outside any message: the batch is not what it claims
            raise Reject.engine(SPLIT_INPUT_INVALID)
        else:
            group.append(segment)
        terminated = terminator is not None
    if group:
        yield _joined(group, terminated)


def _joined(group: list[bytes], terminated: bool) -> bytes:
    return b"\r".join(group) + (b"\r" if terminated else b"")


# -- json-array -----------------------------------------------------------------------------------


class _Invalid(ValueError):
    """The document, the pointer or the target is not what a json-array split needs."""


def _json_array(message: Message, pointer: str) -> Iterator[bytes]:
    content_type = (message.get(h.TB_CONTENT_TYPE) or "").split(";", 1)[0].strip().lower()
    if content_type not in JSON_CONTENT_TYPES or message.payload is None:
        raise Reject.engine(SPLIT_INPUT_INVALID)
    try:
        text = message.payload.decode("utf-8")
        for span in _array_spans(text, tuple(pointer[1:].split("/")) if pointer else ()):
            yield span.encode("utf-8")
    except (ValueError, RecursionError, IndexError):
        # ValueError covers JSONDecodeError, UnicodeDecodeError and _Invalid.
        raise Reject.engine(SPLIT_INPUT_INVALID) from None


def _no_constants(_name: str) -> object:
    raise _Invalid("NaN and Infinity are not JSON")


def _skip(text: str, index: int) -> int:
    while index < len(text) and text[index] in _WHITESPACE:
        index += 1
    return index


def _separator(text: str, index: int, close: str) -> int:
    """After a member or element: past a `,` to the next one, or stopped at `close`."""
    index = _skip(text, index)
    if text[index] == ",":
        index = _skip(text, index + 1)
        if text[index] == close:
            raise _Invalid("trailing comma")
        return index
    if text[index] != close:
        raise _Invalid("expected , or a closing bracket")
    return index


def _member_name(decoder: json.JSONDecoder, text: str, index: int) -> tuple[str, int]:
    """An object member name and the index of its value."""
    if text[index] != '"':
        raise _Invalid("member name is not a string")
    name, index = decoder.raw_decode(text, index)
    index = _skip(text, index)
    if text[index] != ":":
        raise _Invalid("expected :")
    return name, _skip(text, index + 1)


def _array_spans(text: str, tokens: tuple[str, ...]) -> Iterator[str]:
    """The exact source text of each element of the array at `tokens`, validating the whole document.

    One pass, holding one element at a time: the path is walked down, the target array's elements are
    yielded as they are read, and then every enclosing container is read to its end - which is where a
    duplicated member name on the path (ambiguous, README 3.3.2) or trailing garbage is caught.
    """
    decoder = json.JSONDecoder(parse_constant=_no_constants)
    index = _skip(text, 0)
    path: list[tuple[str, str]] = []  # (container, member name on the path) for the closing pass
    for token in tokens:
        if text[index] == "{":
            index = _skip(text, index + 1)
            while True:
                if text[index] == "}":
                    raise _Invalid("pointer absent")
                name, index = _member_name(decoder, text, index)
                if name == token:
                    break
                _value, index = decoder.raw_decode(text, index)
                index = _separator(text, index, "}")
            path.append(("}", token))
        elif text[index] == "[":
            if not token.isdigit() or (len(token) > 1 and token[0] == "0"):
                raise _Invalid("not an array index")
            index = _skip(text, index + 1)
            for _position in range(int(token)):
                if text[index] == "]":
                    raise _Invalid("index out of range")
                _value, index = decoder.raw_decode(text, index)
                index = _separator(text, index, "]")
            if text[index] == "]":
                raise _Invalid("index out of range")
            path.append(("]", ""))
        else:
            raise _Invalid("pointer passes through a scalar")

    if text[index] != "[":
        raise _Invalid("pointer does not name an array")
    index = _skip(text, index + 1)
    while text[index] != "]":
        _value, end = decoder.raw_decode(text, index)
        yield text[index:end]
        index = _separator(text, end, "]")
    index += 1

    for close, token in reversed(path):
        index = _separator(text, index, close)
        while text[index] != close:
            if close == "}":
                name, index = _member_name(decoder, text, index)
                if name == token:
                    raise _Invalid("duplicated member name on the pointer path")
            _value, index = decoder.raw_decode(text, index)
            index = _separator(text, index, close)
        index += 1
    if _skip(text, index) != len(text):
        raise _Invalid("trailing data after the document")


split = BuiltinSplitter(_split, name=NAME, check=_check)
