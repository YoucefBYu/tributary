"""Structured JSON logging with the PHI guard applied at the call site.

One line of JSON per record, fields validated by `tributary.sdk.guards` before anything is written:
scalars only, bounded length, no CR/LF, and no field name that denotes a direct identifier. Invariant 6
names log lines first, and the record key — usually `PID.3.1` — is the value most likely to be reached
for while debugging, so the guard refuses `bytes` outright and refuses a field called `key`.

**A refused line never takes down the caller (lead ruling, set 1.8 review).** By default a refusal
writes one fixed `log_refused` line instead — the original event name (already a checked token) and a
closed reason code, **no field name or value from the refused call** — increments `stats.refused`, and
returns. The refused line is not written. A log call sits on every failure path, so raising there
would stop a worker loop over a debugging line or replace the exception being reported. **Strict
mode** (`strict=True`, or `TB_LOG_STRICT=1`, which the SDK's pytest plugin sets for test runs) raises
`LogFieldError` instead, so tests still fail loudly on the bug.

Validation runs **before** the level filter, so a bad `debug` call is caught at `info` too: otherwise
it would surface only when someone turns on debug logging in production, which is the worst time.

The line format is `contracts/observability/v0` section 4 (`log-line.schema.json`): the four envelope
fields (L1) cannot be overridden by a caller's field, a line has at most 32 fields (L2), and the
identifier and content deny-lists (L3, L4) are enforced by the guard. The message id is `message_id`
(L7); `tb_msg_id` and `msg_id` are deprecated aliases new code does not write.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, Any, Final, Literal

import orjson
from tributary.sdk.errors import LogFieldError, safe_token
from tributary.sdk.guards import validate_log_event, validate_log_fields

LEVELS: Final[dict[str, int]] = {"debug": 10, "info": 20, "warning": 30, "error": 40}
DEFAULT_LEVEL = "info"
#: The fields every line carries (observability/v0 L1). A caller's field of the same name would replace
#: the validated value (`info("x", event="...")` would bypass `validate_log_event`), so it is refused.
RESERVED_FIELDS: Final = frozenset({"ts", "level", "event", "component"})
#: observability/v0 L2, counting the four reserved fields.
MAX_FIELDS: Final = 32
#: `TB_LOG_STRICT=1` makes refusals raise. For test suites only; never set it on a data-plane process.
STRICT_ENV: Final = "TB_LOG_STRICT"
REFUSED_EVENT: Final = "log_refused"
_CHANNEL: Final = re.compile(r"^[a-z][a-z0-9-]{0,63}$")  # log-line.schema.json `channel`

#: The closed list of `log_refused` reasons. A fixed code, never the guard's message (which names the field).
RefusalReason = Literal[
    "event_refused",  # the event name itself is not a snake_case code
    "field_refused",  # a field failed the guard (deny-listed name, bytes, CR/LF, too long, not a scalar)
    "reserved_field",  # a field named ts, level, event or component
    "too_many_fields",  # more than 32 fields with the bound ones (L2)
    "unserializable",  # orjson refused a value the guard let through (an int beyond 64 bits)
    "bound_field_refused",  # `bind(...)` was refused; the child keeps the parent's fields only
    "base_field_refused",  # the constructor's `base` was refused; the logger starts with no base fields
]


@dataclass(slots=True)
class LogStats:
    """Shared by a logger and every logger `bind` derives from it, so one counter covers a component."""

    refused: int = 0
    write_failed: int = 0


class _Refused(Exception):
    """Internal: a refusal with its fixed reason. Never leaves this module."""

    def __init__(self, reason: RefusalReason, error: LogFieldError) -> None:
        super().__init__(reason)
        self.reason: RefusalReason = reason
        self.error = error


def _check_reserved(fields: Mapping[str, object]) -> None:
    for name in fields:
        if name in RESERVED_FIELDS:
            raise _Refused(
                "reserved_field",
                LogFieldError(f"log field {safe_token(name)} is set by the logger and cannot be passed (L1)"),
            )


def _check_fields(fields: Mapping[str, object]) -> None:
    _check_reserved(fields)
    try:
        validate_log_fields(fields)
    except LogFieldError as exc:
        raise _Refused("field_refused", exc) from None


def _strict_from_env() -> bool:
    return os.environ.get(STRICT_ENV, "").lower() in ("1", "true")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonLogger:
    """A `tributary.sdk.Logger` that writes single-line JSON.

    `bind` returns a child logger carrying extra base fields (channel, destination, partition), so a
    message id is attached once per message instead of at every call site, where it gets forgotten.
    """

    __slots__ = ("_base", "_component", "_stream", "_strict", "_threshold", "stats")

    def __init__(
        self,
        component: str,
        *,
        stream: IO[str] | None = None,
        level: str | None = None,
        base: Mapping[str, object] | None = None,
        strict: bool | None = None,
        stats: LogStats | None = None,
    ) -> None:
        self._component = component
        self._stream = stream if stream is not None else sys.stderr
        chosen = (level or os.environ.get("TB_LOG_LEVEL") or DEFAULT_LEVEL).lower()
        self._threshold = LEVELS.get(chosen, LEVELS[DEFAULT_LEVEL])
        self._strict = _strict_from_env() if strict is None else strict
        self.stats = stats if stats is not None else LogStats()
        self._base: dict[str, object] = {}
        base_fields = dict(base or {})
        try:
            _check_fields(base_fields)
        except _Refused as refused:
            self._refuse("base_field_refused", refused.error, event=None)
        else:
            self._base = base_fields

    @property
    def strict(self) -> bool:
        return self._strict

    def bind(self, **fields: object) -> JsonLogger:
        merged: dict[str, object] = {**self._base, **fields}
        try:
            _check_fields(fields)
        except _Refused as refused:
            self._refuse("bound_field_refused", refused.error, event=None)
            merged = dict(self._base)
        return JsonLogger(
            self._component,
            stream=self._stream,
            level=next(name for name, value in LEVELS.items() if value == self._threshold),
            base=merged,
            strict=self._strict,
            stats=self.stats,
        )

    def _emit(self, level: str, event: str, fields: Mapping[str, object]) -> None:
        # Validate first, whatever the level (module docstring).
        try:
            validate_log_event(event)
        except LogFieldError as exc:
            self._refuse("event_refused", exc, event=None)
            return
        try:
            _check_fields(fields)
            count = len(RESERVED_FIELDS) + len(self._base.keys() | fields.keys())
            if count > MAX_FIELDS:
                raise _Refused(
                    "too_many_fields",
                    LogFieldError(f"log line {safe_token(event)} has {count} fields, at most {MAX_FIELDS} (L2)"),
                )
            if LEVELS[level] < self._threshold:
                return
            record: dict[str, Any] = {
                "ts": _now(),
                "level": level,
                "event": event,
                "component": self._component,
                **self._base,
                **fields,
            }
            try:
                line = orjson.dumps(record).decode("utf-8")
            except orjson.JSONEncodeError:
                raise _Refused(
                    "unserializable", LogFieldError(f"log line {safe_token(event)} has a value JSON cannot hold")
                ) from None
        except _Refused as refused:
            self._refuse(refused.reason, refused.error, event=event)
            return
        self._write(line)

    def _refuse(self, reason: RefusalReason, error: LogFieldError, *, event: str | None) -> None:
        """Strict: raise. Otherwise one fixed line, nothing from the refused call but its checked event name."""
        if self._strict:
            raise error from None
        self.stats.refused += 1
        record: dict[str, Any] = {"ts": _now(), "level": "error", "event": REFUSED_EVENT, "component": self._component}
        if event is not None:
            record["refused_event"] = event
        record["reason"] = reason
        channel = self._base.get("channel")
        if isinstance(channel, str) and _CHANNEL.fullmatch(channel):
            record["channel"] = channel  # bound earlier, not from this call; tells the operator where to look
        self._write(orjson.dumps(record).decode("utf-8"))

    def _write(self, line: str) -> None:
        try:
            self._stream.write(line + "\n")
            self._stream.flush()
        except (OSError, ValueError):  # stderr closed or gone: logging must not be what stops the process
            self.stats.write_failed += 1
            if self._strict:
                raise

    def debug(self, event: str, /, **fields: object) -> None:
        self._emit("debug", event, fields)

    def info(self, event: str, /, **fields: object) -> None:
        self._emit("info", event, fields)

    def warning(self, event: str, /, **fields: object) -> None:
        self._emit("warning", event, fields)

    def error(self, event: str, /, **fields: object) -> None:
        self._emit("error", event, fields)
