"""Typed exceptions for `tributary-content-hl7-fhir`.

Same invariant-6 rule as every other package here: no field value ever reaches a message. A segment name
and an HL7 field path (`"PID.3"`) are structural, not sender data, and are included where useful; the
text found at that path never is.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable

#: Distinguishes "the factory raised" from "the factory returned this exact object" -- `None` or `False`
#: are both legitimate return values `safe_build` must not confuse with failure.
_FAILED = object()


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


class InvalidFieldError(MappingError):
    """A field could not be turned into a valid FHIR value, or building a FHIR resource around it failed.

    Carries the HL7 path and a fixed, non-sender-controlled reason string only -- never the field's own
    text and never a third-party exception's own message. That second part matters specifically for a
    `pydantic`/`fhir.resources` `ValidationError`: its message embeds the *rejected value itself*
    (`input_value=...`), which is exactly the field text invariant 6 forbids putting in an exception.
    See `safe_build` below for how a caller gets this instead of that.
    """

    def __init__(self, path: str, *, reason: str) -> None:
        super().__init__(f"cannot map {path}: {reason}")
        self.path = path
        self.reason = reason


class UnsupportedTriggerError(MappingError):
    """A step was handed a message type/trigger event pair it does not implement a mapping for.

    `message_type`/`trigger_event` are expected to already be checked against `tributary.hl7v2.message.
    STRUCTURAL_CODE` (a three-character `[A-Z0-9]` code) by the caller before this is raised -- this
    class itself does not check, so a caller that skips that step puts raw MSH-9 text in the message and
    reintroduces the exact injection a security review found (a fixed placeholder like `"<non-
    structural>"` is what a caller should pass instead when the check fails).
    """

    def __init__(self, message_type: str, trigger_event: str) -> None:
        super().__init__(f"no mapping registered for {message_type}^{trigger_event}")
        self.message_type = message_type
        self.trigger_event = trigger_event


def safe_build[T](factory: Callable[[], T], *, path: str, reason: str = "could not build a valid FHIR value") -> T:
    """Call `factory` and return its result. Any exception raised while building it -- above all a
    `pydantic`/`fhir.resources` `ValidationError`, which embeds the offending field's own text in
    `input_value=...` -- is discarded and replaced with `InvalidFieldError(path)`, naming only the field
    path (invariant 6).

    **Why the raise happens after the `try`/`except` has already finished, not inside the `except`
    suite.** Python sets a newly-raised exception's `__context__` to whatever exception is currently
    being handled *at the point of the `raise` statement*, regardless of a `from` clause -- `raise
    InvalidFieldError(...) from None` only clears `__cause__` and suppresses the chain's *display*; the
    original, PHI-bearing exception would still be reachable through `__context__` on the new one. Using
    a sentinel to flag failure and raising once the `try`/`except` statement itself has completed means
    there is no exception being handled at that point, so `__context__` comes back `None` too -- checked
    directly in this package's tests, not just asserted in this docstring. This is also why the failure
    path below is a bare `contextlib.suppress(Exception)` rather than a caught, inspected or logged
    exception (invariant 6 again -- there is deliberately nothing here to log): anything that reaches
    this point is, by construction, a value this function must never let escape in any form.
    """
    result: object = _FAILED
    with contextlib.suppress(Exception):
        result = factory()
    if result is _FAILED:
        raise InvalidFieldError(path, reason=reason)
    return result  # type: ignore[return-value]
