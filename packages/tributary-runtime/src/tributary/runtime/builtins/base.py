"""What the built-in steps share: bind-time checks, `name=value` pairs, and HL7 segment splitting.

A built-in's `with` shape is public surface (channel-spec README 3.3.2), so every rule about it is
checked **once, at bind**, and refused with a `StepConfigError` the runtime turns into a refused spec.
A rule checked per message instead would dead-letter every message of a misconfigured channel, which
is the same outage delivered more slowly.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Final, Self

from tributary.sdk import Message, Splitter, Step, StepConfigError
from tributary.sdk import headers as h
from tributary.sdk.errors import safe_token
from tributary.sdk.keys import HL7_CONTENT_TYPE, KeyExpression, KeyExpressionError, parse_key_expression

Check = Callable[[str, Mapping[str, object]], None]

SEGMENT_ID: Final = re.compile(rb"^[A-Z][A-Z0-9]{2}$")
SEGMENT_NAME: Final = re.compile(r"^[A-Z][A-Z0-9]{2}$")


class BuiltinStep(Step):
    """A `Step` whose bound `with` values are checked as a whole when bound."""

    def __init__(self, func: Callable[..., object], *, name: str, check: Check) -> None:
        super().__init__(func, name=name)
        self._check = check

    def bind(self, values: Mapping[str, object] | None) -> Self:
        clone = super().bind(values)
        self._check(self.name, dict(values or {}))
        return clone


class BuiltinSplitter(Splitter):
    """A `Splitter` whose bound `with` values are checked as a whole when bound."""

    def __init__(self, func: Callable[..., object], *, name: str, check: Check) -> None:
        super().__init__(func, name=name)
        self._check = check

    def bind(self, values: Mapping[str, object] | None) -> Self:
        clone = super().bind(values)
        self._check(self.name, dict(values or {}))
        return clone


def refuse(step: str, reason: str) -> StepConfigError:
    """A bind-time refusal. `reason` is written by this package and never contains a `with` value."""
    return StepConfigError(f"step {safe_token(step)}: {reason}")


@functools.lru_cache(maxsize=256)
def locator(text: str) -> KeyExpression:
    """Parse a locator with the shared `partitionKey` evaluator's grammar (README 4.3). Cached: the
    runtime evaluates the same few locators on every message."""
    return parse_key_expression(text)


def parse_locator(step: str, key: str, text: str) -> KeyExpression:
    try:
        expression = locator(text)
    except KeyExpressionError as exc:
        raise refuse(step, f"`{key}` is not a locator ({exc.reason_code})") from None
    if expression.scheme not in ("hl7", "json"):
        raise refuse(step, f"`{key}` must be an hl7: or json: locator")
    return expression


@functools.lru_cache(maxsize=256)
def pairs(values: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    """`("trigger=a01", ...)` as `(("trigger", "a01"), ...)`. `with` admits no nested objects, so a map
    is written as a flat array of `name=value` strings. Validated at bind; cached for the hot path."""
    parsed = []
    for item in values:
        name, sep, value = item.partition("=")
        if not sep or not name or not value:
            raise ValueError("not name=value")
        parsed.append((name, value))
    return tuple(parsed)


def check_pairs(step: str, key: str, values: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(values, Sequence) or isinstance(values, str):
        raise refuse(step, f"`{key}` must be an array of name=value strings")
    try:
        parsed = pairs(tuple(str(item) for item in values))
    except ValueError:
        raise refuse(step, f"every `{key}` entry must be name=value with both sides non-empty") from None
    names = [name for name, _value in parsed]
    if len(set(names)) != len(names):
        raise refuse(step, f"`{key}` names a key more than once")
    return parsed


def is_hl7(message: Message) -> bool:
    content_type = (message.get(h.TB_CONTENT_TYPE) or "").split(";", 1)[0].strip().lower()
    return content_type == HL7_CONTENT_TYPE and message.payload is not None


def hl7_segments(payload: bytes) -> list[bytes]:
    """Segments of an ER7 payload: split on CR, LF or CRLF, empty lines skipped. No unescaping, no
    trimming - the bytes as sent (invariant 8)."""
    return [line for line in payload.replace(b"\r\n", b"\r").replace(b"\n", b"\r").split(b"\r") if line]


def segment_id(segment: bytes) -> bytes | None:
    """The three-character segment id, or `None` when the segment does not start with one."""
    candidate = segment[:3]
    if not SEGMENT_ID.match(candidate):
        return None
    if len(segment) > 3 and segment[3:4].isalnum():
        return None  # "MSHX|..." is not an MSH segment
    return candidate
