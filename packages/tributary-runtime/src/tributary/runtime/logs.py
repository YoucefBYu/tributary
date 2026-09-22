"""Structured JSON logging with the PHI guard applied at the call site.

One line of JSON per record, fields validated by `tributary.sdk.guards` before anything is written:
scalars only, bounded length, no CR/LF, and no field name that denotes a direct identifier. Invariant 6
names log lines first, and the record key — usually `PID.3.1` — is the value most likely to be reached
for while debugging, so the guard refuses `bytes` outright and refuses a field called `key`.

A `LogFieldError` is raised rather than swallowed: a log call that would leak PHI is a bug in the
caller, and failing the write loudly in a test is the whole point of guarding at the call site.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import IO, Any, Final

import orjson
from tributary.sdk.guards import validate_log_event, validate_log_fields

LEVELS: Final[dict[str, int]] = {"debug": 10, "info": 20, "warning": 30, "error": 40}
DEFAULT_LEVEL = "info"


class JsonLogger:
    """A `tributary.sdk.Logger` that writes single-line JSON.

    `bind` returns a child logger carrying extra base fields (channel, destination, partition), so a
    message id is attached once per message instead of at every call site, where it gets forgotten.
    """

    __slots__ = ("_base", "_component", "_stream", "_threshold")

    def __init__(
        self,
        component: str,
        *,
        stream: IO[str] | None = None,
        level: str | None = None,
        base: Mapping[str, object] | None = None,
    ) -> None:
        self._component = component
        self._stream = stream if stream is not None else sys.stderr
        chosen = (level or os.environ.get("TB_LOG_LEVEL") or DEFAULT_LEVEL).lower()
        self._threshold = LEVELS.get(chosen, LEVELS[DEFAULT_LEVEL])
        base_fields = dict(base or {})
        validate_log_fields(base_fields)
        self._base = base_fields

    def bind(self, **fields: object) -> JsonLogger:
        validate_log_fields(fields)
        return JsonLogger(
            self._component,
            stream=self._stream,
            level=next(name for name, value in LEVELS.items() if value == self._threshold),
            base={**self._base, **fields},
        )

    def _emit(self, level: str, event: str, fields: Mapping[str, object]) -> None:
        if LEVELS[level] < self._threshold:
            return
        validate_log_event(event)
        validate_log_fields(fields)
        record: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": level,
            "event": event,
            "component": self._component,
            **self._base,
            **fields,
        }
        self._stream.write(orjson.dumps(record).decode("utf-8") + "\n")
        self._stream.flush()

    def debug(self, event: str, /, **fields: object) -> None:
        self._emit("debug", event, fields)

    def info(self, event: str, /, **fields: object) -> None:
        self._emit("info", event, fields)

    def warning(self, event: str, /, **fields: object) -> None:
        self._emit("warning", event, fields)

    def error(self, event: str, /, **fields: object) -> None:
        self._emit("error", event, fields)
