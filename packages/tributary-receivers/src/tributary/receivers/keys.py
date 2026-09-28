"""Wiring `spec.ordering`/`spec.partitionKey` to the shared evaluator (`tributary.sdk.keys`) for the
`http`, `sftp` and `kafka` receivers.

`tributary.sdk.keys` is the **one** implementation of what a `partitionKey` expression means
(`contracts/channel-spec/v0` README 4.3); this module is only the receiver-side plumbing around it:
parse and validate at config time (so a malformed or source-incompatible expression is a startup
`ConfigError`, never a per-message surprise), then evaluate per message at runtime. The MLLP receiver
(wave 1) does not use this yet -- it refuses anything but `ordering: none` (`mllp/config.py`) because
the evaluator did not exist when it was built; extending it is `platform-engineer`'s W2-4 follow-up,
not this module's job.

Per README 4.3: on a per-key channel, an unresolved key is a `parse-error` at the receiver -- never a
null key, a default, or round-robin, any of which would silently drop a patient out of order
(invariant 5). Callers translate `KeyOutcome.reason` into their own NACK/reject shape.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Literal

from tributary.sdk.keys import (
    Headers,
    KeyExpression,
    KeyExpressionError,
    ResolvedKey,
    check_key_source,
    evaluate_key,
    parse_key_expression,
)

from .errors import ConfigError

__all__ = ["KeyOutcome", "key_expression_from_spec", "resolve_key"]


def key_expression_from_spec(
    body: Mapping[str, object],
    *,
    channel: str,
    source_type: str,
    content_types: Collection[str] | None = None,
) -> KeyExpression | None:
    """`None` for `ordering: none`; the parsed, source-checked expression for `ordering: per-key`.
    Raises `ConfigError` for anything the schema would allow but this expression cannot honour
    (an unknown scheme, a scheme that cannot apply to `source_type`, a malformed locator)."""
    ordering = body.get("ordering")
    if ordering not in ("none", "per-key"):
        raise ConfigError(f"channel {channel!r}: spec.ordering must be 'none' or 'per-key', not {ordering!r}")
    has_key = "partitionKey" in body and body["partitionKey"] is not None
    if ordering == "none":
        if has_key:
            raise ConfigError(f"channel {channel!r}: partitionKey is forbidden when ordering: none")
        return None
    if not has_key:
        raise ConfigError(f"channel {channel!r}: partitionKey is required when ordering: per-key")
    text = body["partitionKey"]
    if not isinstance(text, str):
        raise ConfigError(f"channel {channel!r}: spec.partitionKey must be a string")
    try:
        expression = parse_key_expression(text)
        check_key_source(expression, source_type, content_types=content_types)
    except KeyExpressionError as exc:
        raise ConfigError(f"channel {channel!r}: {exc}") from exc
    return expression


@dataclass(frozen=True, slots=True)
class KeyOutcome:
    """Either a resolved key (`key` set, `reason` `None`) or an unresolved one (`key` `None`,
    `reason` a stable code from `tributary.sdk.keys`). Never both."""

    key: bytes | None
    reason: str | None


def resolve_key(
    expression: KeyExpression | None,
    *,
    payload: bytes,
    content_type: str,
    headers: Headers | None = None,
    record_key: bytes | None = None,
    header_source: Literal["http", "kafka"] = "kafka",
) -> KeyOutcome:
    """`KeyOutcome(None, None)` for `ordering: none` (`expression is None`). Otherwise the shared
    evaluator's result, translated: `ResolvedKey` -> the key bytes, `UnresolvedKey` -> its reason code."""
    if expression is None:
        return KeyOutcome(None, None)
    result = evaluate_key(
        expression,
        payload=payload,
        content_type=content_type,
        headers=headers,
        record_key=record_key,
        header_source=header_source,
    )
    if isinstance(result, ResolvedKey):
        return KeyOutcome(result.key, None)
    return KeyOutcome(None, result.reason)
