"""`tributary.core.filter`: keep or drop a message on one field (channel-spec README 3.3.2).

    - name: only-adt
      uses: tributary.core.filter
      with: {field: "hl7:MSH.9.1", in: ["ADT"], onUnresolved: reject}

`field` plus exactly one of `in`, `notIn`, `present`, and a required `onUnresolved: drop | keep | reject`.

**What "unresolved" means.** For `in`/`notIn`, any locator result that is not a value: the segment,
field or pointer is absent, the payload is not the locator's format, the value is empty or longer than
the 256-byte evaluator bound. Silently dropping a clinical message because a field was absent is a
choice the author writes down, which is why `onUnresolved` has no default. For `present`, an absent
field is the answer ("not present"), so only a payload the locator cannot read at all - wrong format,
wrong content type, an ambiguous duplicate JSON member - is unresolved.

**Which fields may be filtered on: an allow-list** (security review of W2-5, pending the architect's
ratification in README 3.3.2). The `in` values are written in the spec, which is published on
`_tb.<env>.channels` and shown in the UI, so a filter on an identifying field puts PHI in a spec. The
decision of 2026-09-24 is that filter "may not filter on identifier fields", and a deny-list of
segments could not deliver it: `PV1.19` (visit number), `ORC.3`/`OBR.3` (filler order numbers), `OBX.5`
(a result value), any Z-segment and `json:/subject/reference` all passed. So `field` must be **exactly**
one of `FILTER_FIELDS` - message typing and status codes, the same kind of field enrich may copy - and
the ratified deny-list (identifying segments; every JSON token) is still checked as a second line.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping, Sequence
from typing import Final, Literal

from tributary.sdk import Context, Drop, Message, Reject
from tributary.sdk import headers as h
from tributary.sdk.codes import FILTER_UNRESOLVED
from tributary.sdk.keys import (
    KEY_EMPTY,
    KEY_FIELD_ABSENT,
    KEY_NOT_SCALAR,
    KEY_POINTER_ABSENT,
    KEY_REPETITION_ABSENT,
    KEY_SEGMENT_ABSENT,
    KEY_TOO_LONG,
    ResolvedKey,
    UnresolvedKey,
    evaluate_key,
)

from .base import BuiltinStep, locator, parse_locator, refuse

NAME: Final = "tributary.core.filter"
FILTERED: Final = "FILTERED"
#: `onUnresolved: drop`. Distinct from `FILTERED` so the browser shows *why* a message went nowhere.
FILTERED_UNRESOLVED: Final = "FILTERED_UNRESOLVED"

#: Exact locator text. Feed and message typing (as enrich's `attrFrom`), plus status-style codes with
#: small, published vocabularies: patient class, admission type, order control and status, result
#: status, value type; FHIR `status`, `intent` and Bundle `type`. Nothing here can hold an identifier.
FILTER_FIELDS: Final = frozenset(
    {
        "hl7:MSH.3",
        "hl7:MSH.4",
        "hl7:MSH.5",
        "hl7:MSH.6",
        "hl7:MSH.9.1",
        "hl7:MSH.9.2",
        "hl7:MSH.9.3",
        "hl7:MSH.11",
        "hl7:MSH.12",
        "hl7:EVN.1",
        "hl7:PV1.2",
        "hl7:PV1.4",
        "hl7:ORC.1",
        "hl7:ORC.5",
        "hl7:OBR.25",
        "hl7:OBX.2",
        "hl7:OBX.11",
        "json:/resourceType",
        "json:/status",
        "json:/intent",
        "json:/type",
    }
)
IDENTIFYING_SEGMENTS: Final = frozenset({"PID", "PD1", "NK1", "GT1", "IN1", "IN2", "MRG"})
IDENTIFYING_JSON_TOKENS: Final = frozenset({"identifier", "name", "birthDate", "telecom", "address"})

#: For `present`: the field is not there, which answers the question rather than failing to.
_ABSENT: Final = frozenset({KEY_SEGMENT_ABSENT, KEY_FIELD_ABSENT, KEY_REPETITION_ABSENT, KEY_POINTER_ABSENT, KEY_EMPTY})
#: For `present`: a value exists even though it is not a usable key.
_PRESENT: Final = frozenset({KEY_TOO_LONG, KEY_NOT_SCALAR})

OnUnresolved = Literal["drop", "keep", "reject"]


def _check(step: str, values: Mapping[str, object]) -> None:
    field = str(values.get("field", ""))
    expression = parse_locator(step, "field", field)
    if expression.hl7_path is not None and expression.hl7_path[0] in IDENTIFYING_SEGMENTS:
        raise refuse(
            step,
            f"`field` points into {expression.hl7_path[0]}, a patient-identifying segment: its values would be "
            "written in the spec, which is published and shown in the UI (README 3.3.2)",
        )
    if expression.scheme == "json" and expression.json_path is not None:
        denied = sorted(set(expression.json_path) & IDENTIFYING_JSON_TOKENS)
        if denied:
            raise refuse(step, f"`field` passes through {denied[0]}, which identifies a patient (README 3.3.2)")
    if field not in FILTER_FIELDS:
        raise refuse(
            step,
            "`field` may only be one of " + ", ".join(sorted(FILTER_FIELDS)) + ": filter values are published "
            "in the spec, so other fields could put identifiers there",
        )
    chosen = [key for key in ("in", "notIn", "present") if key in values]
    if len(chosen) != 1:
        raise refuse(step, "exactly one of `in`, `notIn`, `present` is required")
    listed = values.get(chosen[0])
    if chosen[0] != "present" and (not isinstance(listed, Sequence) or not listed or any(not item for item in listed)):
        raise refuse(step, f"`{chosen[0]}` must be a non-empty array of non-empty strings")


@functools.lru_cache(maxsize=256)
def _encoded(values: tuple[str, ...]) -> frozenset[bytes]:
    return frozenset(value.encode("utf-8") for value in values)


def _filter(
    message: Message,
    ctx: Context,
    *,
    field: str,
    onUnresolved: OnUnresolved,  # camelCase: the contract's key, bound by exact name
    in_: Sequence[str] = (),
    notIn: Sequence[str] = (),
    present: bool | None = None,
) -> Message | Drop:
    result = evaluate_key(
        locator(field),
        payload=message.payload or b"",
        content_type=message.get(h.TB_CONTENT_TYPE) or "",
    )
    if present is not None:
        if isinstance(result, ResolvedKey) or result.reason in _PRESENT:
            found = True
        elif result.reason in _ABSENT:
            found = False
        else:
            return _unresolved(message, ctx, result, onUnresolved)
        return message if found is present else Drop(reason=FILTERED)
    if isinstance(result, UnresolvedKey):
        return _unresolved(message, ctx, result, onUnresolved)
    if in_:
        return message if result.key in _encoded(tuple(in_)) else Drop(reason=FILTERED)
    return Drop(reason=FILTERED) if result.key in _encoded(tuple(notIn)) else message


def _unresolved(message: Message, ctx: Context, result: UnresolvedKey, choice: OnUnresolved) -> Message | Drop:
    # The reason is an evaluator code (KEY_SEGMENT_ABSENT, ...), never the value.
    ctx.log.debug("filter_unresolved", message_id=message.get(h.TB_MSG_ID) or "", reason=result.reason, choice=choice)
    if choice == "keep":
        return message
    if choice == "drop":
        return Drop(reason=FILTERED_UNRESOLVED)
    raise Reject.engine(FILTER_UNRESOLVED)


filter_step = BuiltinStep(_filter, name=NAME, check=_check)
