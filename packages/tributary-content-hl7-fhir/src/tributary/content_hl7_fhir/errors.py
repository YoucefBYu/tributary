"""Typed exceptions for `tributary-content-hl7-fhir`.

Same invariant-6 rule as every other package here: no field value ever reaches a message. A segment name
and an HL7 field path (`"PID.3"`) are structural, not sender data, and are included where useful; the
text found at that path never is.
"""

from __future__ import annotations


class MappingError(Exception):
    """Base class for a mapping step's own errors (distinct from `tributary.hl7v2.errors.Hl7Error`,
    which is a parse-time failure, and `tributary.fhir.errors.BundleEntryError`, which is a
    Bundle-construction failure this module also lets propagate unchanged)."""


class RequiredFieldMissingError(MappingError):
    """A mapping rule needs a field the message does not carry, and has no fallback for it.

    Carries the HL7 path only, e.g. `"PID.3.1"` -- never what (if anything) was actually there.
    """

    def __init__(self, path: str, *, reason: str) -> None:
        super().__init__(f"cannot map: {path} is required and {reason}")
        self.path = path
        self.reason = reason


class UnsupportedTriggerError(MappingError):
    """A step was handed a message type/trigger event pair it does not implement a mapping for.

    Carries the structural MSH-9 codes (three-letter, allow-listed by `tributary.hl7v2.message.
    STRUCTURAL_CODE` before this ever sees them), never any other field.
    """

    def __init__(self, message_type: str, trigger_event: str) -> None:
        super().__init__(f"no mapping registered for {message_type}^{trigger_event}")
        self.message_type = message_type
        self.trigger_event = trigger_event
