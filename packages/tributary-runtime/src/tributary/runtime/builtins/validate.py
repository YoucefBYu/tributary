"""`tributary.core.validate`: structural checks on an HL7 v2 message (channel-spec README 3.3.2).

    - name: check
      uses: tributary.core.validate
      with: {requireSegments: ["MSH", "EVN", "PID", "PV1"], version: ["2.5", "2.5.1"]}

A failure raises `Reject(code)`, so the message is dead-lettered at once with a code an operator can
alert on, skipping `stepRetries`: a missing segment does not appear on the fourth attempt. The codes are
engine codes (`tributary.sdk.codes`):

* `VALIDATE_NOT_HL7` - the content type is not `application/hl7-v2+er7`, the payload is claim-checked,
  or it does not start with an `MSH` segment;
* `VALIDATE_SEGMENT_MISSING` - a segment in `requireSegments` does not occur;
* `VALIDATE_VERSION_MISMATCH` - `MSH-12.1` is absent or not one of `version`.

JSON-schema validation of FHIR payloads comes later (README 3.3.2).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Final

from tributary.sdk import Message, Reject
from tributary.sdk.codes import VALIDATE_NOT_HL7, VALIDATE_SEGMENT_MISSING, VALIDATE_VERSION_MISMATCH
from tributary.sdk.keys import ResolvedKey, evaluate_key

from .base import SEGMENT_NAME, BuiltinStep, hl7_segments, is_hl7, locator, refuse, segment_id

NAME: Final = "tributary.core.validate"
VERSION: Final = re.compile(r"^[0-9]{1,2}(\.[0-9]{1,2}){0,3}$")
_MSH_12_1: Final = "hl7:MSH.12.1"


def _versions(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value) if isinstance(value, Sequence) else ()


def _check(step: str, values: Mapping[str, object]) -> None:
    segments = values.get("requireSegments") or ()
    versions = _versions(values.get("version"))
    if not segments and not versions:
        raise refuse(step, "nothing to check: set `requireSegments`, `version` or both")
    if not isinstance(segments, Sequence) or any(not SEGMENT_NAME.match(str(item)) for item in segments):
        raise refuse(step, "`requireSegments` must be segment ids such as MSH, PID, PV1")
    if any(not VERSION.match(version) for version in versions):
        raise refuse(step, "`version` must be HL7 version ids such as 2.5.1 (a YAML number loses the text)")


def _validate(
    message: Message,
    *,
    requireSegments: Sequence[str] = (),  # camelCase: the contract's key, bound by exact name
    version: str | Sequence[str] | None = None,
) -> None:
    if not is_hl7(message):
        raise Reject.engine(VALIDATE_NOT_HL7)
    segments = hl7_segments(message.payload or b"")
    if not segments or segment_id(segments[0]) != b"MSH":
        raise Reject.engine(VALIDATE_NOT_HL7)
    if requireSegments:
        present = {segment_id(segment) for segment in segments}
        if any(name.encode("ascii") not in present for name in requireSegments):
            raise Reject.engine(VALIDATE_SEGMENT_MISSING)
    versions = _versions(version)
    if versions:
        found = evaluate_key(locator(_MSH_12_1), payload=message.payload or b"", content_type=message.content_type)
        if not isinstance(found, ResolvedKey) or found.key not in {item.encode("ascii") for item in versions}:
            raise Reject.engine(VALIDATE_VERSION_MISMATCH)


validate = BuiltinStep(_validate, name=NAME, check=_check)
