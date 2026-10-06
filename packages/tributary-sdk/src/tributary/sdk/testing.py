"""Test helpers: build a synthetic `Message`, run a step, run a pipeline. No Kafka, no network.

This is the module that makes invariant 7 checkable rather than aspirational:

    def test_filters_unsupported_triggers():
        out = run_step(only_admit_transfer_discharge, make_message(b"..."))
        assert isinstance(out, Drop)

`make_message` produces a contract-shaped envelope with synthetic values — the source is
`sdk-test:run-step@localhost` and the payload defaults to a JSON marker, never anything HL7-shaped, so
nothing here can be mistaken for clinical data (CLAUDE.md: synthetic data only).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path

from . import headers as h
from .context import Context
from .fakes import DEFAULT_NOW, FakeContext
from .ids import new_traceparent, uuid7
from .message import Message
from .outcome import Drop
from .step import SplitResult, Splitter, Step, StepResult

DEFAULT_PAYLOAD = b'{"synthetic": true}'
DEFAULT_SOURCE = "sdk-test:run-step@localhost"
DEFAULT_CONTENT_TYPE = "application/json"


def make_message(
    payload: bytes | str | None = DEFAULT_PAYLOAD,
    *,
    key: bytes | str | None = None,
    headers: Mapping[str, str] | None = None,
    channel: str = "test-channel",
    channel_version: int = 1,
    msg_id: str | None = None,
    source: str = DEFAULT_SOURCE,
    received_at: datetime | str | None = None,
    content_type: str = DEFAULT_CONTENT_TYPE,
    attempt: int = 0,
    traceparent: str | None = None,
    correlation_id: str | None = None,
    destination: str | None = None,
) -> Message:
    """A synthetic `Message` with every header the `raw` class requires.

    `headers=` is applied last, so a test can override or add anything, including the headers of another
    topic class. Explicit `None` for the payload builds a claim-checked (null-value) record.
    """
    built: dict[str, str] = {
        h.TB_MSG_ID: msg_id or uuid7(),
        h.TB_CHANNEL: channel,
        h.TB_CHANNEL_VERSION: str(channel_version),
        h.TB_SOURCE: source,
        h.TB_RECEIVED_AT: _received_at(received_at),
        h.TB_CONTENT_TYPE: content_type,
        h.TB_ATTEMPT: str(attempt),
        h.TRACEPARENT: traceparent or new_traceparent(),
    }
    if correlation_id is not None:
        built[h.TB_CORRELATION_ID] = correlation_id
    if destination is not None:
        built[h.TB_DESTINATION] = destination
    built.update(headers or {})
    return Message(
        headers=built,
        payload=payload.encode("utf-8") if isinstance(payload, str) else payload,
        key=key.encode("utf-8") if isinstance(key, str) else key,
    )


def _received_at(value: datetime | str | None) -> str:
    if value is None:
        return h.format_timestamp(DEFAULT_NOW)
    if isinstance(value, datetime):
        return h.format_timestamp(value)
    return value


def run_step(
    step: Step,
    message: Message | bytes | str,
    *,
    ctx: Context | None = None,
    with_: Mapping[str, object] | None = None,
) -> StepResult:
    """Run one step against a message (or raw payload bytes) and return `Message` or `Drop`.

    `with_` is the step's `with` block exactly as a channel spec would write it (`{"in": ["A01"]}`), bound
    through the same binder the runtime uses, so a wrong key fails here with the same `StepConfigError`
    it would raise at deploy (channel-spec README 3.3). `Reject` propagates: assert it with
    `pytest.raises(Reject)`.

    A `FakeContext` is supplied when none is given. To assert on what the step did through `ctx` —
    attrs, `tb-usr-*` headers, log calls — build the `FakeContext` yourself and pass it in.
    """
    if not isinstance(step, Step):
        raise TypeError("run_step takes a @step-decorated function; use run_splitter for a @splitter")
    bound = step if with_ is None else step.bind(with_)
    subject = message if isinstance(message, Message) else make_message(message)
    return bound(subject, ctx if ctx is not None else FakeContext())


def run_splitter(
    splitter: Splitter,
    message: Message | bytes | str,
    *,
    ctx: Context | None = None,
    with_: Mapping[str, object] | None = None,
) -> SplitResult:
    """Run one `@splitter` and return its stamped children (or a `Drop`), exactly as the runtime sees them.

    Children carry their deterministic `tb-msg-id`, `tb-parent-id` and the parent's key, so a test can
    assert that splitting the same parent twice yields the same ids (channel-spec README 3.3.2).
    """
    if not isinstance(splitter, Splitter):
        raise TypeError("run_splitter takes a @splitter-decorated function")
    bound = splitter if with_ is None else splitter.bind(with_)
    subject = message if isinstance(message, Message) else make_message(message)
    return bound(subject, ctx if ctx is not None else FakeContext())


def run_pipeline(
    steps: Iterable[Step],
    message: Message | bytes | str,
    *,
    ctx: Context | None = None,
) -> StepResult:
    """Run steps in order, stopping at the first `Drop`, exactly as the runtime's executor does.

    One `ctx` spans the whole pipeline: `ctx.vars` is message-scoped, so a value set by an early step is
    visible to a later one, and that is a behaviour a channel's tests should be able to reproduce.
    """
    context = ctx if ctx is not None else FakeContext()
    steps = tuple(steps)
    if any(isinstance(node, Splitter) for node in steps):
        raise TypeError("run_pipeline runs one-to-one steps; test a @splitter with run_splitter")
    current: StepResult = message if isinstance(message, Message) else make_message(message)
    for step in steps:
        if isinstance(current, Drop):
            return current
        current = step(current, context)
    return current


def fixture_loader(directory: str | Path) -> Callable[[str], bytes]:
    """A loader that reads a fixture file as bytes, for a bundle's own `tests/fixtures/` directory.

    Bytes, not text: a payload is opaque (invariant 8), and HL7 v2 in the field is frequently not UTF-8,
    so decoding here would corrupt the fixture before the step ever saw it.
    """
    root = Path(directory)

    def load(name: str) -> bytes:
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("fixture name escapes the fixture directory")
        return path.read_bytes()

    return load


def utc_now() -> datetime:
    """Timezone-aware wall clock, for a test that deliberately wants real time."""
    return datetime.now(UTC)
