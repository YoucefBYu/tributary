"""Typed exceptions for the SDK, and the one rule they all obey.

Invariant 6: no PHI in exception messages. A record key is frequently a patient identifier
(`contracts/envelope/v0/README.md` section 4) and a header value can carry anything a sender sent, so
no exception raised here embeds a header value, a key or payload bytes. Where naming the offending
token helps, it goes through `safe_token`, which only lets a short, boring identifier through.
"""

from __future__ import annotations

import re

_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:@/\[\]-]{1,64}$")
REDACTED = "<redacted>"


def safe_token(value: object) -> str:
    """A token safe to put in an exception message, or `<redacted>`.

    Names that the contracts constrain (header names, attribute names, step names) pass through; a
    value that fails the charset or length test is redacted rather than truncated, because a truncated
    patient name is still a patient name.
    """
    if isinstance(value, str) and _SAFE_TOKEN.match(value):
        return value
    return REDACTED


class TributaryError(Exception):
    """Base class for every error the SDK raises."""


class HeaderError(TributaryError):
    """A `tb-*` header is missing, malformed, or not representable as text."""


class MissingHeaderError(HeaderError):
    """A required header is absent."""

    def __init__(self, name: str) -> None:
        super().__init__(f"missing header {safe_token(name)}")
        self.name = name


class HeaderValueError(HeaderError):
    """A header is present but its value does not match the envelope contract.

    Carries the header name and the reason, never the value (gate requirement 13: a malformed header
    is a reject, and the reject must not leak what was in it).
    """

    def __init__(self, name: str, reason: str, *, value_length: int | None = None) -> None:
        detail = f"header {safe_token(name)}: {reason}"
        if value_length is not None:
            detail = f"{detail} (length {value_length})"
        super().__init__(detail)
        self.name = name
        self.reason = reason
        self.value_length = value_length


class PayloadError(TributaryError):
    """The payload is absent or is not decodable as the requested text encoding.

    Never carries the bytes or the offending character: a payload is PHI by default.
    """


class EnvelopeError(TributaryError):
    """A message violates an envelope rule that JSON Schema cannot express (byte budgets, class rules)."""


class StepError(TributaryError):
    """Base class for a step that does not honour the SDK contract."""


class StepDefinitionError(StepError):
    """`@step` was applied to something it cannot call: wrong arity, `*args`, or a missing context."""


class StepReturnError(StepError):
    """A step returned something other than `Message`, `Drop` or `None`."""


class DropReasonError(TributaryError):
    """A `Drop` reason is empty, too long, or contains characters that must not reach a log or an event."""


class AttrError(TributaryError):
    """An `_tb.events` `attrs` entry breaks the events contract (section 3)."""


class UsrHeaderError(TributaryError):
    """A `tb-usr-*` header name or value breaks the envelope contract (section 2.1)."""


class LogFieldError(TributaryError):
    """A structured-log field is not a scalar, is too long, or uses a name that denotes an identifier."""


class SecretNotFoundError(TributaryError):
    """No secret is bound to the given `secretRef`."""

    def __init__(self, ref: str) -> None:
        super().__init__(f"no secret for ref {safe_token(ref)}")
        self.ref = ref
