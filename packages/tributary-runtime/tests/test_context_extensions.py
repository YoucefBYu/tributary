"""Context extensions (`tributary.runtime.extensions`): how a worker step gets `ctx.fhir`, with no Kafka.

The regression this file exists for: the W2-3 mapping step `tributary.content_hl7_fhir.adt` needs
`ctx.fhir`, the worker's `WorkerContext` has none, so every ADT message was dead-lettered as
`TRANSFORM_ERROR` (qa-engineer, W2 exit criterion 1). The runtime does not import `tributary-fhir`
(W2-2 approval: optional attachment, not a hard import); `tributary-fhir` registers a `tributary.context`
entry point and the worker applies it. `fhir_extension` below is exactly what that entry point's factory
returns, so the worker path under test is the production one apart from discovery, which has its own
tests further down.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Any, cast

import orjson
import pytest
from tributary.fhir import LocalFhir, LocalTerminology, attach, message_id_factory
from tributary.runtime.context import ContextExtension, WorkerContext
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.executor import PipelineExecutor, StepFailure
from tributary.runtime.extensions import (
    ENTRY_POINT_GROUP,
    ContextExtensionError,
    ContextResolver,
    LoadedExtension,
    verify_extension,
)
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec
from tributary.runtime.worker import PipelineHandler
from tributary.sdk import Message, ReplyContext, step
from tributary.sdk.codes import TRANSFORM_ERROR
from tributary.sdk.context import Context, KeyValueStore, Logger, SecretStore
from tributary.sdk.testing import make_message

from .fakes import FakeProducer

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
FIXTURE = ROOT / "tests" / "fixtures" / "hl7" / "adt_a01_admit_with_visit_number.hl7"
HL7 = "application/hl7-v2+er7"
PARSE = {"name": "parse", "uses": "tributary.hl7v2.parse", "with": {"version": "2.5.1"}}
ADT = {"name": "to-fhir", "uses": "tributary.content_hl7_fhir.adt"}
#: Values from the synthetic fixture that stand for a patient's identity. A log line carrying one would
#: be a PHI leak by invariant 6, whatever the data's provenance.
PHI_MARKERS = ("SYNTHPATIENT", "ALPHA", "SYNTHCONTACT", "100 SYNTH ST", "555-0100", "19800115", "9000001")


def fhir_extension(base: Context, message: Message) -> Context:
    """What `tributary-fhir`'s `tributary.context` factory returns (the ask in the handoff): the
    production `fullUrl` source, `message_id_factory(tb-msg-id)`, and the local, no-I/O terminology."""
    return attach(base, fhir=LocalFhir(id_factory=message_id_factory(message.msg_id)), terminology=LocalTerminology())


def channel(
    *,
    pipeline: list[dict[str, Any]],
    destination_steps: list[dict[str, Any]] | None = None,
    context: list[str] | None = None,
    version: int = 4,
) -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": version}
    document["spec"]["pipeline"]["steps"] = pipeline
    if context is not None:
        document["spec"]["pipeline"]["context"] = context
    if destination_steps is not None:
        document["spec"]["destinations"][0]["steps"] = destination_steps
    return ChannelSpec.from_document(document)


def adt_record() -> ConsumedRecord:
    message = make_message(FIXTURE.read_bytes(), key=b"SYNTH-KEY-1", content_type=HL7, channel="adt-passthrough")
    return ConsumedRecord(topic="dev.adt-passthrough.raw", partition=0, offset=3, message=message)


def handler(
    producer: FakeProducer,
    emitter: RecordingEmitter,
    spec: ChannelSpec,
    *,
    extensions: tuple[ContextExtension, ...] | None = None,
    stream: io.StringIO,
) -> PipelineHandler:
    return PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=RecordProducer(producer, log=JsonLogger("test", stream=stream)),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream, level="debug"),
        instance="worker:test@localhost",
        context_extensions=extensions,
    )


def produced_bundle(producer: FakeProducer) -> tuple[dict[str, Any], dict[str, str]]:
    assert producer.topics() == ["dev.adt-passthrough.out.http-stub"]
    _topic, _key, value, headers = producer.produced[0]
    assert value is not None
    return orjson.loads(value), {name: blob.decode() for name, blob in headers or []}


# -- the worker path, with the real W2-3 step ------------------------------------------------------


def test_the_real_adt_step_produces_a_bundle_through_the_worker() -> None:
    """Exit criterion 1's worker half, on the production path: the spec declares `context: [fhir]`, the
    worker resolves `tributary-fhir`'s real entry point, `uses:` resolves the real steps, and the `out`
    record is a FHIR transaction Bundle rather than a dead letter."""
    producer, emitter, stream = FakeProducer(), RecordingEmitter(), io.StringIO()
    record = adt_record()

    handler(producer, emitter, channel(pipeline=[PARSE, ADT], context=["fhir"]), stream=stream).handle(record)

    bundle, headers = produced_bundle(producer)
    assert bundle["resourceType"] == "Bundle"
    assert bundle["type"] == "transaction"
    assert [entry["resource"]["resourceType"] for entry in bundle["entry"]] == ["Patient", "Encounter"]
    assert headers["tb-content-type"] == "application/fhir+json"
    assert emitter.stages() == ["transformed", "routed"]
    # The production fullUrl source (W2-2 open item 3): derived from tb-msg-id, so a redelivery of the
    # same record produces the same Bundle.
    expected = message_id_factory(record.message.msg_id)
    assert [entry["fullUrl"] for entry in bundle["entry"]] == [expected(), expected()]
    logged = stream.getvalue()
    assert not [marker for marker in PHI_MARKERS if marker in logged], "invariant 6: no PHI in worker logs"
    loaded = [line for line in map(orjson.loads, logged.splitlines()) if line["event"] == "context_extension_loaded"]
    assert [(line["extension"], line["distribution"], line["channel_version"]) for line in loaded] == [
        ("fhir", "tributary-fhir", 4)
    ]
    assert loaded[0]["channel"] == "adt-passthrough" and loaded[0]["version"]


def test_a_redelivered_record_produces_a_byte_identical_bundle() -> None:
    """At-least-once (invariant 4) means the same record can be handled twice; the FHIR loader's
    conditional entries converge only if the Bundle is the same, fullUrls included."""
    record = adt_record()
    payloads = []
    for _ in range(2):
        producer = FakeProducer()
        handler(
            producer,
            RecordingEmitter(),
            channel(pipeline=[PARSE, ADT], context=["fhir"]),
            stream=io.StringIO(),
        ).handle(record)
        payloads.append(producer.produced[0][2])
    assert payloads[0] == payloads[1]


def test_destination_steps_get_ctx_fhir_too() -> None:
    """The exit-criterion channel maps in `destinations[].steps`, on the destination's own fork."""
    producer, emitter = FakeProducer(), RecordingEmitter()
    spec = channel(
        pipeline=[{"name": "passthrough", "uses": "tributary.core.passthrough"}],
        destination_steps=[PARSE, ADT],
        context=["fhir"],
    )

    handler(producer, emitter, spec, stream=io.StringIO()).handle(adt_record())

    bundle, headers = produced_bundle(producer)
    assert bundle["type"] == "transaction"
    assert headers["tb-destination"] == "http-stub"


def test_a_channel_that_does_not_declare_fhir_does_not_get_it() -> None:
    """L1, and README 3.3.4's known failure mode: `tributary-fhir` is installed, but a spec without
    `context: [fhir]` loads nothing, so the step raises `AttributeError` on every attempt and the loop
    dead-letters the record as TRANSFORM_ERROR. Only the exception's type name survives, never its text."""
    stream = io.StringIO()
    with pytest.raises(StepFailure) as failure:
        handler(FakeProducer(), RecordingEmitter(), channel(pipeline=[PARSE, ADT]), stream=stream).handle(adt_record())
    skipped = [
        line for line in map(orjson.loads, stream.getvalue().splitlines()) if line["event"].startswith("context_")
    ]
    assert [(line["event"], line["extension"], line["reason"]) for line in skipped] == [
        ("context_extension_skipped", "fhir", "not-declared")
    ]
    assert failure.value.error_code == TRANSFORM_ERROR
    assert failure.value.exception_type == "AttributeError"
    assert failure.value.step_name == "to-fhir"


# -- WorkerContext.step_view ----------------------------------------------------------------------


def worker_ctx(
    *, extensions: tuple[ContextExtension, ...] = (fhir_extension,), destination: str | None = None
) -> WorkerContext:
    return WorkerContext(
        channel="adt-passthrough",
        channel_version=4,
        instance="worker:test@localhost",
        log=JsonLogger("t", stream=io.StringIO()),
        source_type="mllp",
        destination=destination,
        extensions=extensions,
        subject=make_message(b"{}"),
    )


def test_the_view_is_built_once_and_shared_by_every_step_on_a_context() -> None:
    """`attach()` once per message, the same `ctx.fhir` for every step, so fullUrls never repeat."""
    calls: list[str] = []

    def counting(base: Context, message: Message) -> Context:
        calls.append(message.msg_id)
        return fhir_extension(base, message)

    ctx = worker_ctx(extensions=(counting,))
    assert ctx.step_view is ctx.step_view
    assert len(calls) == 1


def test_no_extension_means_the_step_gets_the_worker_context_itself() -> None:
    ctx = worker_ctx(extensions=())
    assert ctx.step_view is ctx


def test_extensions_without_a_subject_are_refused() -> None:
    with pytest.raises(ValueError, match="needs the message"):
        WorkerContext(
            channel="c", channel_version=1, instance="worker:t@h", log=JsonLogger("t"), extensions=(fhir_extension,)
        )


def test_a_fork_gets_its_own_view_over_itself_for_its_own_message() -> None:
    """A destination's attrs and tb-usr-* must land on the destination's fork, not the parent's."""
    parent = worker_ctx()
    child_message = make_message(b"{}")
    fork = parent.fork(destination="http-stub", subject=child_message)
    view = cast(Any, fork.step_view)

    view.set_attr("trigger", "a01")
    view.set_usr("batch-id", "b-1")

    assert fork.attrs() == {"trigger": "a01"} and parent.attrs() == {}
    assert fork.usr_headers() == {"batch-id": "b-1"} and parent.usr_headers() == {}
    assert view is not parent.step_view
    assert view.fhir is not cast(Any, parent.step_view).fhir, "the fork's own ctx.fhir, for its own message"


def test_the_extended_view_forwards_reply_to_the_worker_context() -> None:
    """Commit 215bec3: `attach()` of a ReplyContext stays one, and the body lands in the worker's cell."""
    ctx = worker_ctx()
    view = ctx.step_view
    assert isinstance(view, ReplyContext)
    view.reply(b"MSH|^~\\&|TRIBUTARY-SYNTH|X\r", content_type=HL7)
    assert ctx.reply_body is not None


def test_a_reply_from_a_failed_attempt_is_discarded_before_the_retry_through_the_view() -> None:
    """The executor's per-attempt restore acts on the WorkerContext under the view (channel-spec 3.2.6),
    so a retried step can reply again instead of hitting "at most once"."""
    attempts: list[int] = []

    @step
    def answer_then_fail_once(message: Message, ctx: ReplyContext) -> Message:
        ctx.reply(b"MSH|^~\\&|TRIBUTARY-SYNTH|X\r", content_type=HL7)
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient")
        return message

    ctx = worker_ctx()
    PipelineExecutor([answer_then_fail_once], step_retries=1).run(make_message(b"{}"), ctx)
    assert len(attempts) == 2
    assert ctx.reply_body is not None


# -- resolution: only declared, registered, correctly distributed extensions (context-extension/v0 L1-L5) --


@dataclass
class FakeDist:
    name: str
    version: str = "0.0.0"


@dataclass
class FakePoint:
    """Duck-types `importlib.metadata.EntryPoint`: `name`, `value`, `dist` and `load()`, counting loads."""

    name: str
    target: Callable[[], Any]
    dist: FakeDist | None = None
    value: str = "pkg.module:factory"
    loads: int = 0

    def load(self) -> Any:
        self.loads += 1
        return self.target()


def factory_of(extension: ContextExtension) -> Callable[[], Any]:
    return lambda: lambda: extension


def _raise(exc: BaseException) -> Callable[[], Any]:
    def boom() -> Any:
        raise exc

    return boom


def fhir_point(target: Callable[[], Any] | None = None, *, dist: str = "tributary-fhir") -> FakePoint:
    return FakePoint("fhir", target or factory_of(fhir_extension), FakeDist(dist, "1.2.3"))


def resolver(*installed: FakePoint, stream: io.StringIO | None = None) -> ContextResolver:
    return ContextResolver(
        log=JsonLogger("worker", stream=stream if stream is not None else io.StringIO(), level="debug"),
        points=lambda: cast("list[EntryPoint]", list(installed)),
    )


def lines(stream: io.StringIO) -> list[dict[str, Any]]:
    return [orjson.loads(line) for line in stream.getvalue().splitlines()]


def declaring_fhir(version: int = 4) -> ChannelSpec:
    return channel(pipeline=[PARSE, ADT], context=["fhir"], version=version)


def test_a_declared_capability_is_loaded_from_its_registered_distribution_and_logged() -> None:
    stream = io.StringIO()
    point = fhir_point()
    extensions = resolver(point, stream=stream).resolve(declaring_fhir())
    assert extensions == (fhir_extension,) and point.loads == 1
    assert lines(stream) == [
        {
            **lines(stream)[0],
            "event": "context_extension_loaded",
            "extension": "fhir",
            "distribution": "tributary-fhir",
            "version": "1.2.3",
            "channel": "adt-passthrough",
            "channel_version": 4,
        }
    ]


def test_an_undeclared_registrant_is_never_imported() -> None:
    """L1: a transitive dependency registering the group runs nothing, even if importing it would raise."""
    stream = io.StringIO()
    rogue = FakePoint("telemetry", _raise(RuntimeError("would run at import")), FakeDist("some-transitive-dep"))
    installed_fhir = fhir_point(_raise(RuntimeError("must not be imported either")))
    assert resolver(rogue, installed_fhir, stream=stream).resolve(channel(pipeline=[PARSE])) == ()
    assert rogue.loads == 0 and installed_fhir.loads == 0
    assert sorted((line["extension"], line["reason"], line["level"]) for line in lines(stream)) == [
        ("fhir", "not-declared", "info"),
        ("telemetry", "unregistered", "warning"),
    ]
    assert all(line["event"] == "context_extension_skipped" for line in lines(stream))


def test_an_unregistered_name_in_a_spec_is_refused() -> None:
    """L2 at parse time: the runtime is lenient about unknown fields, never about a capability."""
    with pytest.raises(ChannelSpecError, match="not a registered capability"):
        channel(pipeline=[PARSE], context=["dicom"])


@pytest.mark.parametrize(
    ("context", "match"), [([], "non-empty array"), (["fhir", "fhir"], "twice"), (["FHIR"], "names")]
)
def test_malformed_pipeline_context_is_refused(context: list[str], match: str) -> None:
    with pytest.raises(ChannelSpecError, match=match):
        channel(pipeline=[PARSE], context=context)


def test_a_non_list_pipeline_context_is_refused() -> None:
    with pytest.raises(ChannelSpecError, match="non-empty array"):
        channel(pipeline=[PARSE], context=cast(Any, "fhir"))


def test_a_declared_capability_from_the_wrong_distribution_is_refused_without_importing_it() -> None:
    """A typosquat named `fhir` while `tributary-fhir` is absent (context-extension/v0 section 5)."""
    stream = io.StringIO()
    squat = fhir_point(dist="tributary_fhirr")
    with pytest.raises(ContextExtensionError, match="registered to tributary-fhir"):
        resolver(squat, stream=stream).resolve(declaring_fhir())
    assert squat.loads == 0
    assert [(line["reason"], line["level"]) for line in lines(stream)] == [("distribution-mismatch", "warning")]


def test_a_declared_capability_registered_twice_is_refused() -> None:
    """A typosquat named `fhir` alongside the real `tributary-fhir`: ambiguous, so neither is imported."""
    real, squat = fhir_point(), fhir_point(dist="fhir-tools")
    with pytest.raises(ContextExtensionError, match="more than one distribution"):
        resolver(real, squat).resolve(declaring_fhir())
    assert real.loads == 0 and squat.loads == 0


def test_a_declared_capability_that_is_not_installed_is_refused() -> None:
    with pytest.raises(ContextExtensionError, match="not installed"):
        resolver().resolve(declaring_fhir())


def test_the_distribution_name_is_compared_pep503_normalised() -> None:
    assert resolver(fhir_point(dist="Tributary_FHIR")).resolve(declaring_fhir()) == (fhir_extension,)


def test_the_factory_runs_once_per_process_and_loaded_is_logged_per_version() -> None:
    stream = io.StringIO()
    point = fhir_point()
    subject = resolver(point, stream=stream)
    subject.resolve(declaring_fhir(4))
    subject.resolve(declaring_fhir(5))
    assert point.loads == 1
    assert [line["channel_version"] for line in lines(stream)] == [4, 5]


@pytest.mark.parametrize(
    ("target", "match"),
    [
        (_raise(ImportError("SYNTHPATIENT^ALPHA")), "cannot load .* ImportError"),
        (_raise(SystemExit(3)), "cannot load .* SystemExit"),
        (lambda: "not callable", "not a callable factory"),
        (lambda: _raise(RuntimeError("SYNTHPATIENT^ALPHA")), "failed to start: RuntimeError"),
        (lambda: _raise(SystemExit("SYNTHPATIENT^ALPHA")), "failed to start: SystemExit"),
        (lambda: lambda: 42, "did not return a callable"),
    ],
)
def test_a_declared_extension_that_cannot_be_built_refuses_the_spec(target: Callable[[], Any], match: str) -> None:
    """Never skipped: a skipped extension is the original bug again, one AttributeError per message."""
    with pytest.raises(ContextExtensionError, match=match) as refused:
        resolver(fhir_point(target)).resolve(declaring_fhir())
    assert isinstance(refused.value, ChannelSpecError), "refused like an unresolvable `uses` step"
    assert "SYNTHPATIENT" not in str(refused.value), "the exception's own text is never repeated"


def test_a_refused_context_on_hot_reload_keeps_the_previous_version_running() -> None:
    """L6: resolved with the steps, all or nothing; the running plan survives a refused version."""
    specs = [declaring_fhir(4)]
    stream = io.StringIO()
    subject = PipelineHandler(
        env="dev",
        spec_provider=lambda: specs[-1],
        producer=RecordProducer(FakeProducer(), log=JsonLogger("test", stream=stream)),
        emitter=RecordingEmitter(),
        log=JsonLogger("worker", stream=stream),
        instance="worker:test@localhost",
        context_resolver=resolver(fhir_point(), stream=stream),
    )
    subject.prepare(specs[0])
    specs.append(channel(pipeline=[PARSE, ADT], context=["fhir"], version=5))
    subject._context_resolver = resolver(stream=stream)  # the capability vanished from the image
    subject.handle(adt_record())  # still version 4, still with ctx.fhir
    assert [line["event"] for line in lines(stream)].count("channel_spec_refused") == 1


def _exits(base: Context, message: Message) -> Context:
    raise SystemExit("SYNTHPATIENT^ALPHA")


def reloading_handler(
    specs: list[ChannelSpec],
    *installed: FakePoint,
    producer: FakeProducer,
    emitter: RecordingEmitter,
    stream: io.StringIO,
) -> PipelineHandler:
    return PipelineHandler(
        env="dev",
        spec_provider=lambda: specs[-1],
        producer=RecordProducer(producer, log=JsonLogger("test", stream=stream)),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream),
        instance="worker:test@localhost",
        context_resolver=resolver(*installed, stream=stream),
    )


@pytest.mark.parametrize(
    ("target", "match"),
    [
        (lambda: _raise(SystemExit("SYNTHPATIENT^ALPHA")), "failed to start: SystemExit"),
        (factory_of(_exits), "failed its probe: SystemExit"),
    ],
)
def test_a_factory_or_probe_raising_systemexit_on_hot_reload_keeps_the_previous_version(
    target: Callable[[], Any], match: str
) -> None:
    """Security re-review Low: `sys.exit()` in plugin code is a refused version, not a stopped worker."""
    specs = [channel(pipeline=[{"name": "passthrough", "uses": "tributary.core.passthrough"}], version=4)]
    producer, emitter, stream = FakeProducer(), RecordingEmitter(), io.StringIO()
    subject = reloading_handler(specs, fhir_point(target), producer=producer, emitter=emitter, stream=stream)
    subject.prepare(specs[0])
    specs.append(declaring_fhir(5))
    subject.handle(adt_record())  # must not raise SystemExit
    assert {event.channel_version for event in emitter.events if hasattr(event, "channel_version")} == {4}
    refused = [line for line in lines(stream) if line["event"] == "channel_spec_refused"]
    assert [(line["channel_version"], line["running_version"]) for line in refused] == [(5, 4)]
    assert re.search(match, refused[0]["reason"])
    assert "SYNTHPATIENT" not in stream.getvalue()


def test_the_worker_refuses_to_start_on_an_unresolvable_context() -> None:
    """At startup there is no previous version: `prepare` raises and the worker does not consume."""
    subject = PipelineHandler(
        env="dev",
        spec_provider=declaring_fhir,
        producer=RecordProducer(FakeProducer(), log=JsonLogger("test", stream=io.StringIO())),
        emitter=RecordingEmitter(),
        log=JsonLogger("worker", stream=io.StringIO()),
        instance="worker:test@localhost",
        context_resolver=resolver(),
    )
    with pytest.raises(ContextExtensionError):
        subject.prepare(declaring_fhir())


def test_the_real_tributary_fhir_registrant_resolves_and_passes_the_probe() -> None:
    """fb644c3: `tributary-fhir` registers `fhir`; resolution through real entry-point metadata."""
    registered = {point.name: point for point in entry_points(group=ENTRY_POINT_GROUP)}
    assert "fhir" in registered
    (extension,) = ContextResolver(log=JsonLogger("t", stream=io.StringIO())).resolve(declaring_fhir())
    view = extension(worker_ctx(extensions=()), make_message(b"{}"))
    assert hasattr(view, "fhir") and hasattr(view, "terminology")


# -- the probe (L3) ---------------------------------------------------------------------------------


class _Delegating:
    """A correct wrapper, written against `ReplyContext`. The subclasses below each break one rule."""

    def __init__(self, base: Context) -> None:
        self._base = cast(ReplyContext, base)

    channel = property(lambda self: self._base.channel)
    channel_version = property(lambda self: self._base.channel_version)
    instance = property(lambda self: self._base.instance)
    destination = property(lambda self: self._base.destination)
    attempt = property(lambda self: self._base.attempt)
    traceparent = property(lambda self: self._base.traceparent)

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._base.vars

    @property
    def log(self) -> Logger:
        return self._base.log

    @property
    def secrets(self) -> SecretStore:
        return self._base.secrets

    @property
    def kv(self) -> KeyValueStore:
        return self._base.kv

    def now(self) -> datetime:
        return self._base.now()

    def set_attr(self, name: str, value: str) -> None:
        self._base.set_attr(name, value)

    def attrs(self) -> Mapping[str, str]:
        return self._base.attrs()

    def set_usr(self, name: str, value: str) -> None:
        self._base.set_usr(name, value)

    def usr_headers(self) -> Mapping[str, str]:
        return self._base.usr_headers()

    def reply(self, body: bytes, *, content_type: str) -> None:
        self._base.reply(body, content_type=content_type)


class _NoReply(_Delegating):
    reply = None  # type: ignore[assignment]


class _ReplyReturnsNone(_Delegating):
    """The security review's Medium: `isinstance` passes, but the body goes nowhere."""

    def reply(self, body: bytes, *, content_type: str) -> None:
        return None


class _ReplySwallowsErrors(_Delegating):
    """Forwards, but hides the destination refusal, so N destinations could each answer."""

    def reply(self, body: bytes, *, content_type: str) -> None:
        try:
            self._base.reply(body, content_type=content_type)
        except Exception:
            return None


class _OwnLog(_Delegating):
    @property
    def log(self) -> Logger:
        return JsonLogger("plugin", stream=io.StringIO())


class _OwnSecrets(_Delegating):
    @property
    def secrets(self) -> SecretStore:
        return self._base.secrets.__class__()  # type: ignore[call-arg]


class _OwnVars(_Delegating):
    def __init__(self, base: Context) -> None:
        super().__init__(base)
        self._own: dict[str, object] = {}

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._own


class _SwallowsAttrs(_Delegating):
    def set_attr(self, name: str, value: str) -> None:
        return None


class _SwallowsUsr(_Delegating):
    def set_usr(self, name: str, value: str) -> None:
        return None


def _probe_raises(base: Context, message: Message) -> Context:
    raise KeyError("SYNTHPATIENT^ALPHA")


@pytest.mark.parametrize(
    ("wrapper", "match"),
    [
        (lambda base, message: object(), "not a Context"),
        (lambda base, message: _NoReply(base), "dropped `reply`"),
        (lambda base, message: _ReplyReturnsNone(base), "does not forward `reply`"),
        (lambda base, message: _ReplySwallowsErrors(base), "destination context, which must raise"),
        (lambda base, message: _OwnLog(base), "own `log`"),
        (lambda base, message: _OwnSecrets(base), "own `secrets`"),
        (lambda base, message: _OwnVars(base), "own `vars`"),
        (lambda base, message: _SwallowsAttrs(base), "set_attr"),
        (lambda base, message: _SwallowsUsr(base), "set_usr"),
        (_probe_raises, "failed its probe: KeyError"),
        (_exits, "failed its probe: SystemExit"),
    ],
)
def test_the_probe_refuses_a_wrapper_that_breaks_what_the_worker_reads(wrapper: Any, match: str) -> None:
    """attrs feed events, tb-usr-* feed record headers, vars carry the parsed message between steps,
    reply is the synchronous answer, and log/secrets must be the worker's own objects (P1, L3)."""
    with pytest.raises(ContextExtensionError, match=match) as refused:
        verify_extension(LoadedExtension(name="bad", distribution="pkg", version="1", extend=wrapper))
    assert "SYNTHPATIENT" not in str(refused.value)


def test_a_correct_wrapper_and_the_real_attach_pass_the_probe() -> None:
    for extend in (lambda base, message: _Delegating(base), fhir_extension):
        verify_extension(LoadedExtension(name="ok", distribution="pkg", version="1", extend=extend))


def test_a_probe_failure_refuses_the_spec_through_the_resolver() -> None:
    with pytest.raises(ContextExtensionError, match="does not forward `reply`"):
        resolver(fhir_point(factory_of(lambda base, message: _ReplyReturnsNone(base)))).resolve(declaring_fhir())
