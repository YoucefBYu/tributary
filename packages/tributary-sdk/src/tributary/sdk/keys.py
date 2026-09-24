"""The key-expression evaluator: `spec.partitionKey` in, Kafka record key bytes (or "unresolved") out.

`contracts/channel-spec/v0` README 4.3 fixes the semantics; this module is the **one** implementation,
used by every receiver when it produces to `raw` and by the CLI and control plane at deploy. Two copies
of this function would put two messages of one patient on different partitions, which is invariant 5
broken with nothing in any log to show for it.

Pure: no I/O, stdlib only, and deterministic in its inputs. Two rules shape the API:

* **The result is a value, not an exception.** An unresolved key comes back as `UnresolvedKey` with a
  reason code, so there is no path by which the value it failed on reaches an exception message.
* **The key is PHI** (envelope 4). `ResolvedKey` redacts itself in `repr` and `str`; the bytes are
  reachable only through `.key`, which is what the producer needs and nothing else should touch.

What "the value's bytes exactly as they appear" means per scheme:

* `hl7:` - the byte slice between delimiters, taken from the payload as sent: no HL7 unescaping, no
  trimming, no case folding. HL7's explicit null `""` is treated as empty, because it *is* the empty
  value in HL7, not a two-character identifier. The field is its first repetition - or, with a
  selector `SEG.F[c:VALUE]`, the first repetition whose component `c` equals `VALUE` byte for byte - and
  a component or subcomponent is taken from that repetition. PID-3 repeats and senders order it
  differently, so the selector (`hl7:PID.3[5:MR].1`) is how a key names one identifier (README 4.3).
* `header:` - matched case-insensitively for an `http` source and exactly for a `kafka` source; a
  header present more than once is unresolved, never "the first one".
* `json:` - a string target is the decoded string, UTF-8 encoded (`"12\\u0033"` and `"123"` are the
  same JSON string); a number is its text exactly as written (`012` is not valid JSON, `1.50` stays
  `1.50`); `true`/`false` are their text; `null`, objects and arrays are not scalar keys.
* `header:` / `record:key` - the bytes as received.
"""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from .errors import TributaryError, safe_token

KeyScheme = Literal["hl7", "json", "header", "record"]

#: `contracts/envelope/v0` section 4: a record key is at most 256 bytes.
KEY_MAX_BYTES: Final = 256

HL7_CONTENT_TYPE: Final = "application/hl7-v2+er7"
JSON_CONTENT_TYPES: Final = frozenset({"application/json", "application/fhir+json"})

# Unresolved reason codes. Stable: receivers put them in a NACK and in events.
KEY_NOT_HL7: Final = "KEY_NOT_HL7"
KEY_NOT_JSON: Final = "KEY_NOT_JSON"
KEY_CONTENT_TYPE_MISMATCH: Final = "KEY_CONTENT_TYPE_MISMATCH"
KEY_SEGMENT_ABSENT: Final = "KEY_SEGMENT_ABSENT"
KEY_FIELD_ABSENT: Final = "KEY_FIELD_ABSENT"
KEY_REPETITION_ABSENT: Final = "KEY_REPETITION_ABSENT"
KEY_POINTER_ABSENT: Final = "KEY_POINTER_ABSENT"
KEY_HEADER_ABSENT: Final = "KEY_HEADER_ABSENT"
KEY_HEADER_AMBIGUOUS: Final = "KEY_HEADER_AMBIGUOUS"
KEY_JSON_AMBIGUOUS: Final = "KEY_JSON_AMBIGUOUS"
KEY_RECORD_KEY_ABSENT: Final = "KEY_RECORD_KEY_ABSENT"
KEY_EMPTY: Final = "KEY_EMPTY"
KEY_NOT_SCALAR: Final = "KEY_NOT_SCALAR"
KEY_TOO_LONG: Final = "KEY_TOO_LONG"

_SCHEMES: Final = ("hl7", "json", "header", "record")
_HL7_LOCATOR = re.compile(
    r"^([A-Z][A-Z0-9]{2})\.([1-9][0-9]{0,2})"
    r"(?:\[([1-9][0-9]{0,2}):([A-Za-z0-9._:/-]{1,64})\])?"
    r"(?:\.([1-9][0-9]{0,2}))?(?:\.([1-9][0-9]{0,2}))?$"
)
_HL7_NUMERIC_INDEX = re.compile(r"\[[0-9]+\]")
_JSON_TOKEN = re.compile(r"^[A-Za-z0-9._:\[\]-]*$")
_HEADER_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SOURCES_FOR: Final[Mapping[str, frozenset[str]]] = {
    "hl7": frozenset({"mllp", "http", "sftp", "kafka"}),
    "json": frozenset({"http", "sftp", "kafka"}),
    "header": frozenset({"http", "kafka"}),
    "record": frozenset({"kafka"}),
}


class KeyExpressionError(TributaryError):
    """A `partitionKey` expression that cannot be evaluated. Raised at deploy and at spec load, never
    per message. `reason_code` is stable; the message names the expression only when it is safe to."""

    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class KeyExpression:
    """A parsed `partitionKey`. The expression is spec-author text, not message data, so it may be shown."""

    scheme: KeyScheme
    locator: str
    #: hl7 only: (segment, field, component, subcomponent), 1-based, None when not given.
    hl7_path: tuple[str, int, int | None, int | None] | None = None
    #: hl7 only: the repetition selector `[c:VALUE]` as (component, value bytes), or None.
    hl7_selector: tuple[int, bytes] | None = None
    #: json only: the decoded pointer tokens.
    json_path: tuple[str, ...] | None = None

    @property
    def text(self) -> str:
        return "record:key" if self.scheme == "record" else f"{self.scheme}:{self.locator}"


@dataclass(frozen=True, slots=True)
class ResolvedKey:
    """The record key. Redacted in `repr`/`str` because a key is usually a patient identifier."""

    key: bytes

    def __repr__(self) -> str:
        return f"ResolvedKey(<{len(self.key)} bytes>)"

    __str__ = __repr__


@dataclass(frozen=True, slots=True)
class UnresolvedKey:
    """No key. `reason` is a code from this module; the value that failed is deliberately not kept."""

    reason: str


KeyResult = ResolvedKey | UnresolvedKey
Headers = Mapping[str, bytes | None] | Iterable[tuple[str, bytes | None]]


def parse_key_expression(text: str) -> KeyExpression:
    """Parse and validate `spec.partitionKey`. Raises `KeyExpressionError`; `xpath:` is refused."""
    if not isinstance(text, str):
        raise KeyExpressionError("KEY_EXPRESSION_INVALID", "partitionKey must be a string")
    if text == "record:key":
        return KeyExpression("record", "key")
    scheme, sep, locator = text.partition(":")
    if not sep or not locator:
        raise KeyExpressionError("KEY_EXPRESSION_INVALID", f"{safe_token(text)} is not scheme:locator")
    if scheme == "xpath":
        raise KeyExpressionError("KEY_SCHEME_RESERVED", "xpath: is reserved and has no evaluator (README 4.3)")
    if scheme not in _SCHEMES or scheme == "record":
        raise KeyExpressionError("KEY_SCHEME_UNKNOWN", f"unknown key scheme {safe_token(scheme)}")
    if scheme == "hl7":
        if _HL7_NUMERIC_INDEX.search(locator):
            raise KeyExpressionError(
                "KEY_LOCATOR_RESERVED", "hl7 numeric repetition index [n] is reserved (README 4.3)"
            )
        match = _HL7_LOCATOR.match(locator)
        if match is None:
            raise KeyExpressionError(
                "KEY_LOCATOR_INVALID", f"hl7 locator {safe_token(locator)} is not SEG.F[c:VALUE][.C[.S]]"
            )
        seg, field_, sel_component, sel_value, comp, sub = match.groups()
        if sel_component and seg == "MSH" and int(field_) <= 2:
            raise KeyExpressionError("KEY_LOCATOR_INVALID", "MSH-1 and MSH-2 are delimiters and do not repeat")
        return KeyExpression(
            "hl7",
            locator,
            hl7_path=(seg, int(field_), int(comp) if comp else None, int(sub) if sub else None),
            hl7_selector=(int(sel_component), sel_value.encode("ascii")) if sel_component else None,
        )
    if scheme == "json":
        if not locator.startswith("/"):
            raise KeyExpressionError("KEY_LOCATOR_INVALID", "a json locator is an RFC 6901 pointer starting with /")
        tokens = tuple(locator[1:].split("/"))
        if any(not _JSON_TOKEN.match(token) for token in tokens):
            raise KeyExpressionError(
                "KEY_LOCATOR_INVALID", f"json pointer {safe_token(locator)} has an unsupported token"
            )
        return KeyExpression("json", locator, json_path=tokens)
    if not _HEADER_NAME.match(locator):
        raise KeyExpressionError("KEY_LOCATOR_INVALID", f"header name {safe_token(locator)} is not a header token")
    return KeyExpression("header", locator)


def check_key_source(
    expression: KeyExpression, source_type: str, *, content_types: Collection[str] | None = None
) -> None:
    """Deploy-time refusal of a scheme that cannot apply to the source (README 4.3).

    `content_types`, when the source declares which payloads it accepts (an `http` source), also refuses
    `hl7:` on a JSON-only source and `json:` on an HL7-only one.
    """
    allowed = _SOURCES_FOR[expression.scheme]
    if source_type not in allowed:
        raise KeyExpressionError(
            "KEY_SCHEME_SOURCE_MISMATCH",
            f"{expression.scheme}: keys cannot be evaluated on a {safe_token(source_type)} source",
        )
    if content_types is not None:
        types = {_media_type(value) for value in content_types}
        if expression.scheme == "hl7" and HL7_CONTENT_TYPE not in types:
            raise KeyExpressionError("KEY_SCHEME_SOURCE_MISMATCH", "hl7: key on a source that accepts no HL7 v2")
        if expression.scheme == "json" and not types & JSON_CONTENT_TYPES:
            raise KeyExpressionError("KEY_SCHEME_SOURCE_MISMATCH", "json: key on a source that accepts no JSON")


def evaluate_key(
    expression: KeyExpression,
    *,
    payload: bytes,
    content_type: str,
    headers: Headers | None = None,
    record_key: bytes | None = None,
    header_source: Literal["http", "kafka"] = "kafka",
) -> KeyResult:
    """Evaluate one message's key. Never raises for anything the message contains.

    `header_source` decides how `header:` names match: case-insensitively for `http`, exactly for
    `kafka` (README 4.3). It is ignored by every other scheme.
    """
    if expression.scheme == "hl7":
        if _media_type(content_type) != HL7_CONTENT_TYPE:
            return UnresolvedKey(KEY_CONTENT_TYPE_MISMATCH)
        if expression.hl7_path is None:  # a KeyExpression built by hand rather than parsed
            raise KeyExpressionError("KEY_EXPRESSION_INVALID", "use parse_key_expression")
        return _checked(_hl7(payload, expression.hl7_path, expression.hl7_selector))
    if expression.scheme == "json":
        if _media_type(content_type) not in JSON_CONTENT_TYPES:
            return UnresolvedKey(KEY_CONTENT_TYPE_MISMATCH)
        if expression.json_path is None:
            raise KeyExpressionError("KEY_EXPRESSION_INVALID", "use parse_key_expression")
        return _checked(_json(payload, expression.json_path))
    if expression.scheme == "header":
        return _checked(_header(headers, expression.locator, case_insensitive=header_source == "http"))
    if record_key is None:
        return UnresolvedKey(KEY_RECORD_KEY_ABSENT)
    return _checked(record_key)


def _media_type(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


def _checked(value: bytes | UnresolvedKey) -> KeyResult:
    if isinstance(value, UnresolvedKey):
        return value
    if not value:
        return UnresolvedKey(KEY_EMPTY)
    if len(value) > KEY_MAX_BYTES:
        return UnresolvedKey(KEY_TOO_LONG)
    return ResolvedKey(value)


# -- hl7 ------------------------------------------------------------------------------------------


def _hl7(
    payload: bytes, path: tuple[str, int, int | None, int | None], selector: tuple[int, bytes] | None = None
) -> bytes | UnresolvedKey:
    if len(payload) < 8 or not payload.startswith(b"MSH"):
        return UnresolvedKey(KEY_NOT_HL7)
    field_sep = payload[3:4]
    if field_sep.isalnum() or field_sep in (b"\r", b"\n"):
        return UnresolvedKey(KEY_NOT_HL7)
    encoding = payload[4:].split(field_sep, 1)[0].split(b"\r", 1)[0].split(b"\n", 1)[0]
    if len(encoding) < 2:
        return UnresolvedKey(KEY_NOT_HL7)
    component_sep, repetition_sep = encoding[0:1], encoding[1:2]
    subcomponent_sep = encoding[3:4] if len(encoding) >= 4 else b""

    segment_id, field_no, component_no, subcomponent_no = path
    wanted = segment_id.encode("ascii")
    segment = next(
        (
            line
            for line in payload.replace(b"\r\n", b"\r").replace(b"\n", b"\r").split(b"\r")
            if line[:3] == wanted and line[3:4] == field_sep
        ),
        None,
    )
    if segment is None:
        return UnresolvedKey(KEY_SEGMENT_ABSENT)

    parts = segment.split(field_sep)
    if segment_id == "MSH":
        # HL7 numbering: MSH-1 is the field separator itself, so MSH-n is parts[n - 1] for n >= 2.
        if field_no == 1:
            value = field_sep
        elif field_no == 2:
            value = encoding  # the delimiters themselves: never split
            return value if component_no is None else UnresolvedKey(KEY_FIELD_ABSENT)
        else:
            if field_no - 1 >= len(parts):
                return UnresolvedKey(KEY_FIELD_ABSENT)
            value = parts[field_no - 1]
    else:
        if field_no >= len(parts):
            return UnresolvedKey(KEY_FIELD_ABSENT)
        value = parts[field_no]

    repetitions = value.split(repetition_sep)
    if selector is None:
        value = repetitions[0]
    else:
        wanted_component, wanted_value = selector
        chosen = next(
            (
                repetition
                for repetition in repetitions
                if (parts_ := repetition.split(component_sep))
                and len(parts_) >= wanted_component
                and parts_[wanted_component - 1] == wanted_value
            ),
            None,
        )
        if chosen is None:
            return UnresolvedKey(KEY_REPETITION_ABSENT)
        value = chosen
    if component_no is not None:
        components = value.split(component_sep)
        if component_no > len(components):
            return UnresolvedKey(KEY_FIELD_ABSENT)
        value = components[component_no - 1]
        if subcomponent_no is not None:
            subcomponents = value.split(subcomponent_sep) if subcomponent_sep else [value]
            if subcomponent_no > len(subcomponents):
                return UnresolvedKey(KEY_FIELD_ABSENT)
            value = subcomponents[subcomponent_no - 1]
    if value == b'""':
        return UnresolvedKey(KEY_EMPTY)  # HL7's explicit null
    return value


# -- json -----------------------------------------------------------------------------------------


class _Number(str):
    """A JSON number kept as the text it was written as."""


class _Ambiguous:
    """An object member name that appeared more than once: which value is "the" key is undefined."""


_AMBIGUOUS: Final = _Ambiguous()


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        result[name] = _AMBIGUOUS if name in result else value
    return result


def _json(payload: bytes, tokens: tuple[str, ...]) -> bytes | UnresolvedKey:
    try:
        document = json.loads(
            payload,
            parse_int=_Number,
            parse_float=_Number,
            parse_constant=lambda _name: _AMBIGUOUS,  # NaN/Infinity are not JSON
            object_pairs_hook=_pairs,
        )
    except (ValueError, RecursionError):
        return UnresolvedKey(KEY_NOT_JSON)
    node: Any = document
    for token in tokens:
        if isinstance(node, dict):
            if token not in node:
                return UnresolvedKey(KEY_POINTER_ABSENT)
            node = node[token]
        elif isinstance(node, list):
            if not token.isdigit() or (len(token) > 1 and token[0] == "0") or int(token) >= len(node):
                return UnresolvedKey(KEY_POINTER_ABSENT)
            node = node[int(token)]
        else:
            return UnresolvedKey(KEY_POINTER_ABSENT)
        if node is _AMBIGUOUS:
            return UnresolvedKey(KEY_JSON_AMBIGUOUS)
    if node is None:
        return UnresolvedKey(KEY_EMPTY)
    if node is True or node is False:
        return b"true" if node else b"false"
    if isinstance(node, str):  # includes _Number
        return node.encode("utf-8")
    return UnresolvedKey(KEY_NOT_SCALAR)


# -- header ---------------------------------------------------------------------------------------


def _header(headers: Headers | None, name: str, *, case_insensitive: bool) -> bytes | UnresolvedKey:
    """HTTP names match case-insensitively, Kafka names exactly. More than one match is ambiguous."""
    if headers is None:
        return UnresolvedKey(KEY_HEADER_ABSENT)
    items = list(headers.items()) if isinstance(headers, Mapping) else list(headers)
    wanted = name.lower() if case_insensitive else name
    matches = [value for header, value in items if (header.lower() if case_insensitive else header) == wanted]
    if not matches:
        return UnresolvedKey(KEY_HEADER_ABSENT)
    if len(matches) > 1:
        return UnresolvedKey(KEY_HEADER_AMBIGUOUS)
    return matches[0] if matches[0] is not None else UnresolvedKey(KEY_EMPTY)
