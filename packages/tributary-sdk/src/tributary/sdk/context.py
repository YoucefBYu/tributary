"""`ctx`: the only way a step reaches outside itself.

Invariant 7 makes a step a function of `(message, ctx)` that must be unit-testable without Kafka, so
every side effect — logging, event metadata, secrets, shared state, the clock — is a method on this
protocol and nothing else. **No network I/O is implemented in the SDK.** These are `Protocol`s: the
runtime supplies the real objects, `fakes.FakeContext` supplies in-memory ones, and a step cannot tell
the difference. That is what makes "test a step with no Kafka" true rather than aspirational.

`ctx.fhir`, `ctx.terminology` and `ctx.ids` from ADR-001 6.2 are deliberately absent: they belong to
`tributary-fhir` (W2-2) and are not the platform's to define.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Logger(Protocol):
    """Structured logging. Fields are validated by `guards.validate_log_fields` before they are emitted.

    There is no `%s`-style formatting and no free-text message argument beyond a short event name,
    because invariant 6 names log lines first and an f-string is how a payload gets into one.
    """

    def debug(self, event: str, /, **fields: object) -> None: ...

    def info(self, event: str, /, **fields: object) -> None: ...

    def warning(self, event: str, /, **fields: object) -> None: ...

    def error(self, event: str, /, **fields: object) -> None: ...


@runtime_checkable
class SecretStore(Protocol):
    """Resolves a channel spec's `secretRef` to its value. Never logged, never put in an event."""

    def get(self, ref: str) -> str: ...


@runtime_checkable
class KeyValueStore(Protocol):
    """Long-lived shared state (ADR-001 6.2), versioned so a concurrent writer is detected, not lost.

    v0 ships the protocol and an in-memory fake only. The PostgreSQL-backed implementation with a local
    cache belongs to the control plane in Wave 3, and invariant 3 means a step that depends on it must
    still behave sanely when PostgreSQL is down.
    """

    def get(self, key: str) -> tuple[str, int] | None: ...

    def put(self, key: str, value: str, *, expected_version: int | None = None) -> int: ...


@runtime_checkable
class Context(Protocol):
    """Everything a step may touch besides its `Message`."""

    @property
    def channel(self) -> str: ...

    @property
    def channel_version(self) -> int: ...

    @property
    def instance(self) -> str:
        """The emitting component instance, e.g. `worker:pipeline-2@worker-4` (events contract 2)."""

    @property
    def destination(self) -> str | None:
        """The destination being processed, or `None` for pre-routing stages."""

    @property
    def attempt(self) -> int:
        """Delivery attempt within the current stage. `0` on the first try."""

    @property
    def traceparent(self) -> str | None:
        """The current hop's W3C trace context, for a step that calls an instrumented client."""

    @property
    def vars(self) -> MutableMapping[str, object]:
        """Message-scoped variables: the analogue of Mirth's channelMap. Discarded after the message."""

    @property
    def log(self) -> Logger: ...

    @property
    def secrets(self) -> SecretStore: ...

    @property
    def kv(self) -> KeyValueStore: ...

    def now(self) -> datetime:
        """Timezone-aware current time. Injected so a step that stamps a timestamp can be tested."""

    def set_attr(self, name: str, value: str) -> None:
        """Add one searchable `attrs` entry to this message's lifecycle events.

        Validated at the call site against the events contract: short codes only, identifier names
        rejected, and identifier-derived `_hmac` values refused in v0 (events contract 3.1).
        """

    def attrs(self) -> Mapping[str, str]:
        """The `attrs` accumulated for this message so far."""

    def set_usr(self, name: str, value: str) -> None:
        """Set a channel-defined `tb-usr-<name>` header, propagated to the next hop but never interpreted.

        Not projected into `_tb.events` (envelope contract 2.2): use `set_attr` for searchable metadata.
        """

    def usr_headers(self) -> Mapping[str, str]:
        """The `tb-usr-*` values set by steps so far, without the prefix."""


@runtime_checkable
class ReplyContext(Context, Protocol):
    """A `Context` that can supply a synchronous reply body (set 1.2, channel-spec README 3.2.6).

    A separate protocol so that adding it was additive: a wrapper written against `Context` (for
    example one that adds `.fhir`) stays a valid `Context`, and gains `reply` by forwarding it. The
    runtime's context and `FakeContext` implement it; a step that answers annotates `ctx: ReplyContext`.
    """

    def reply(self, body: bytes, *, content_type: str) -> None:
        """Supply the synchronous reply body for this message (set 1.2, channel-spec README 3.2.6).

        At most once per message; pipeline steps only (a destination step raises); `content_type` must
        be a registered envelope type, `application/hl7-v2+er7` for an `mllp` source; at most 1,000,000
        bytes. Violations raise `ReplyError`, whose message never quotes the body.

        Always legal otherwise: the body is used only when the request is synchronous and the channel
        says `reply.body: step`, and is discarded (and counted) in every other case.
        """
