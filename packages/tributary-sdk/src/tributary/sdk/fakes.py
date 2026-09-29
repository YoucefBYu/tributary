"""In-memory implementations of the `ctx` protocols, for testing a step with no Kafka and no network.

The fakes enforce **the same rules as the real runtime** — the attrs charset, the identifier deny-list,
the `tb-usr-*` limits, the log-field guard. A permissive fake is worse than no fake: it turns a test
into evidence that the step is fine, and the rule then fires for the first time in production, where
the failure is an invariant-6 incident rather than a red test.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType

from .errors import KeyValueConflictError, SecretNotFoundError
from .guards import validate_attr, validate_attrs, validate_log_event, validate_log_fields, validate_usr
from .reply import ReplyBody, check_once, check_reply

#: A fixed instant, so a step that stamps a time produces the same output on every run.
DEFAULT_NOW = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class LogRecord:
    """One captured log call: level, event name and validated fields."""

    level: str
    event: str
    fields: Mapping[str, object]


class RecordingLogger:
    """Captures log calls instead of writing them, and validates fields exactly as the runtime does."""

    def __init__(self) -> None:
        self.records: list[LogRecord] = []

    def _record(self, level: str, event: str, fields: Mapping[str, object]) -> None:
        validate_log_event(event)
        validate_log_fields(fields)
        self.records.append(LogRecord(level=level, event=event, fields=MappingProxyType(dict(fields))))

    def debug(self, event: str, /, **fields: object) -> None:
        self._record("debug", event, fields)

    def info(self, event: str, /, **fields: object) -> None:
        self._record("info", event, fields)

    def warning(self, event: str, /, **fields: object) -> None:
        self._record("warning", event, fields)

    def error(self, event: str, /, **fields: object) -> None:
        self._record("error", event, fields)

    def events(self) -> list[str]:
        """Just the event names, which is what most assertions want."""
        return [record.event for record in self.records]


class FakeSecrets:
    """A `secretRef` to value mapping. Values are never logged or put in an event."""

    def __init__(self, secrets: Mapping[str, str] | None = None) -> None:
        self._secrets = dict(secrets or {})

    def get(self, ref: str) -> str:
        try:
            return self._secrets[ref]
        except KeyError:
            raise SecretNotFoundError(ref) from None


class FakeKeyValueStore:
    """An in-memory versioned key-value store with the same optimistic-concurrency contract."""

    def __init__(self, initial: Mapping[str, str] | None = None) -> None:
        self._items: dict[str, tuple[str, int]] = {key: (value, 1) for key, value in (initial or {}).items()}

    def get(self, key: str) -> tuple[str, int] | None:
        return self._items.get(key)

    def put(self, key: str, value: str, *, expected_version: int | None = None) -> int:
        current = self._items.get(key)
        current_version = 0 if current is None else current[1]
        if expected_version is not None and expected_version != current_version:
            raise KeyValueConflictError(key, expected=expected_version, actual=current_version)
        version = current_version + 1
        self._items[key] = (value, version)
        return version


@dataclass(slots=True)
class FakeContext:
    """An in-memory `Context`. Everything a step did through it is inspectable afterwards.

    `now()` is a fixed instant by default rather than the wall clock, because a step that stamps a
    timestamp should produce a comparable result on every run. Pass a `datetime` or a callable to
    control it.

    `ctx.reply` is recorded in `replied` with the runtime's rules (`tributary.sdk.reply`): set
    `destination` to test a destination step (the call raises there) and `source_type="mllp"` to hold
    the body to the MLLP content type. The recorded body never appears in `repr`.
    """

    channel: str = "test-channel"
    channel_version: int = 1
    instance: str = "test:step-runner@localhost"
    destination: str | None = None
    attempt: int = 0
    traceparent: str | None = None
    vars: MutableMapping[str, object] = field(default_factory=dict)
    log: RecordingLogger = field(default_factory=RecordingLogger)
    secrets: FakeSecrets = field(default_factory=FakeSecrets)
    kv: FakeKeyValueStore = field(default_factory=FakeKeyValueStore)
    clock: datetime | Callable[[], datetime] = DEFAULT_NOW
    source_type: str | None = None
    replied: ReplyBody | None = field(default=None, init=False, repr=False)
    _attrs: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _usr: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    def now(self) -> datetime:
        return self.clock() if callable(self.clock) else self.clock

    def set_attr(self, name: str, value: str) -> None:
        validate_attr(name, value)
        candidate = {**self._attrs, name: value}
        validate_attrs(candidate)  # the 16-entry and 1024-byte caps are properties of the whole object
        self._attrs = candidate

    def attrs(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._attrs))

    def set_usr(self, name: str, value: str) -> None:
        validate_usr(name, value)
        self._usr[name] = value

    def usr_headers(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._usr))

    def reply(self, body: bytes, *, content_type: str) -> None:
        check_once(self.replied)
        self.replied = check_reply(
            body, content_type=content_type, destination=self.destination, source_type=self.source_type
        )
