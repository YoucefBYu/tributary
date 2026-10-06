"""Typed exceptions for `tributary-hl7v2`.

Same rule as the SDK (`tributary.sdk.errors`) and `tributary-receivers` (`tributary.receivers.errors`):
no PHI in an exception message (invariant 6). A field, component or subcomponent *value* is sender data
and never appears here; a segment name, an occurrence number and an accessor path like `"PID.5.1"` are
structural addresses the engine defines, not data the sender sent, so they are safe to include verbatim.

"Malformed input raises a typed exception whose message never contains segment content" (W2-1): every
raise below is checked against that at review time, not just at write time, because the failure mode is
a segment string slipped into an f-string six months from now.
"""

from __future__ import annotations


class Hl7Error(Exception):
    """Base class for every error this package raises."""


class MalformedMessageError(Hl7Error):
    """The message cannot be parsed at all: empty, no `MSH` segment, or unreadable encoding characters.

    Never carries the message bytes. `reason` is a short, static, engine-authored phrase.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(f"malformed HL7 v2 message: {reason}")
        self.reason = reason


class SegmentNotFoundError(Hl7Error):
    """The requested segment (or occurrence of it) is not present.

    Carries the segment name and the requested occurrence, both structural, never the segments that
    *are* present (which would leak the message's shape and, transitively, values elsewhere in it).
    """

    def __init__(self, segment: str, occurrence: int) -> None:
        super().__init__(f"segment {segment!r} occurrence {occurrence} not found")
        self.segment = segment
        self.occurrence = occurrence


class FieldPathError(Hl7Error):
    """An accessor path is not syntactically valid, or addresses a repetition/component/subcomponent
    index of zero or below (HL7 v2 addressing is 1-based).

    The path text itself (`"PID.5.1"`) is an engine-defined address, not sender data, and is included
    verbatim so a caller can see exactly what was rejected.
    """

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"invalid HL7 path {path!r}: {reason}")
        self.path = path
        self.reason = reason


class PayloadDecodeError(Hl7Error):
    """The payload bytes are not decodable as the requested encoding.

    Never carries the bytes or the offending byte position's neighbourhood; only the encoding name,
    which is channel configuration, not sender data.
    """

    def __init__(self, encoding: str) -> None:
        super().__init__(f"payload is not decodable as {encoding!r}")
        self.encoding = encoding


class AckBuildError(Hl7Error):
    """An ACK cannot be built from the given source message or arguments (structural reasons only)."""
