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
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Any, cast

import orjson
import pytest
from tributary.fhir import LocalFhir, LocalTerminology, attach, message_id_factory
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.context import ContextExtension, WorkerContext
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.executor import PipelineExecutor, StepFailure
from tributary.runtime.extensions import (
    ENTRY_POINT_GROUP,
    ContextExtensionError,
    LoadedExtension,
    load_context_extensions,
    verify_extension,
)
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec
from tributary.runtime.worker import PipelineHandler, PipelineWorker, WorkerSettings
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


def channel(*, pipeline: list[dict[str, Any]], destination_steps: list[dict[str, Any]] | None = None) -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": 4}
    document["spec"]["pipeline"]["steps"] = pipeline
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
    extensions: tuple[ContextExtension, ...],
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
    """Exit criterion 1's worker half: `uses:` resolves the real entry points, the step gets `ctx.fhir`
    from the extension, and the `out` record is a FHIR transaction Bundle rather than a dead letter."""
    producer, emitter, stream = FakeProducer(), RecordingEmitter(), io.StringIO()
    record = adt_record()

    handler(producer, emitter, channel(pipeline=[PARSE, ADT]), extensions=(fhir_extension,), stream=stream).handle(
        record
    )

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
            channel(pipeline=[PARSE, ADT]),
            extensions=(fhir_extension,),
            stream=io.StringIO(),
        ).handle(record)
        payloads.append(producer.produced[0][2])
    assert payloads[0] == payloads[1]


def test_destination_steps_get_ctx_fhir_too() -> None:
    """The exit-criterion channel maps in `destinations[].steps`, on the destination's own fork."""
    producer, emitter = FakeProducer(), RecordingEmitter()
    spec = channel(
        pipeline=[{"name": "passthrough", "uses": "tributary.core.passthrough"}], destination_steps=[PARSE, ADT]
    )

    handler(producer, emitter, spec, extensions=(fhir_extension,), stream=io.StringIO()).handle(adt_record())

    bundle, headers = produced_bundle(producer)
    assert bundle["type"] == "transaction"
    assert headers["tb-destination"] == "http-stub"


def test_without_an_extension_the_adt_step_is_dead_lettered_as_before() -> None:
    """The bug, pinned: with no `ctx.fhir` the step raises `AttributeError` on every attempt and the loop
    dead-letters the record as TRANSFORM_ERROR. Only the exception's type name survives, never its text."""
    with pytest.raises(StepFailure) as failure:
        handler(
            FakeProducer(), RecordingEmitter(), channel(pipeline=[PARSE, ADT]), extensions=(), stream=io.StringIO()
        ).handle(adt_record())
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


# -- discovery and the startup probe ---------------------------------------------------------------


@dataclass
class FakeDist:
    name: str


@dataclass
class FakePoint:
    """Duck-types `importlib.metadata.EntryPoint`: `name`, `dist`, `load()`."""

    name: str
    target: Callable[[], Any]
    dist: FakeDist | None = None

    def load(self) -> Any:
        return self.target()


def points(*items: FakePoint) -> list[EntryPoint]:
    return cast("list[EntryPoint]", list(items))


def factory_of(extension: ContextExtension) -> Callable[[], Any]:
    return lambda: lambda: extension


def test_discovery_loads_verifies_and_logs_every_extension_in_name_order() -> None:
    stream = io.StringIO()
    loaded = load_context_extensions(
        log=JsonLogger("worker", stream=stream),
        points=points(
            FakePoint("zeta", factory_of(fhir_extension), FakeDist("pkg-z")),
            FakePoint("fhir", factory_of(fhir_extension), FakeDist("tributary-fhir")),
        ),
    )
    assert [extension.name for extension in loaded] == ["fhir", "zeta"]
    lines = [orjson.loads(line) for line in stream.getvalue().splitlines()]
    assert [(line["event"], line["extension"], line["distribution"]) for line in lines] == [
        ("context_extension_loaded", "fhir", "tributary-fhir"),
        ("context_extension_loaded", "zeta", "pkg-z"),
    ]


def test_no_installed_extension_is_not_an_error() -> None:
    assert load_context_extensions(points=[]) == ()


def _raise(exc: BaseException) -> Callable[[], Any]:
    def boom() -> Any:
        raise exc

    return boom


@pytest.mark.parametrize(
    ("point", "match"),
    [
        (FakePoint("fhir", _raise(ImportError("SYNTHPATIENT^ALPHA"))), "cannot load .* ImportError"),
        (FakePoint("fhir", _raise(SystemExit(3))), "cannot load .* SystemExit"),
        (FakePoint("fhir", lambda: "not callable"), "not a callable factory"),
        (FakePoint("fhir", lambda: _raise(RuntimeError("SYNTHPATIENT^ALPHA"))), "failed to start: RuntimeError"),
        (FakePoint("fhir", lambda: lambda: 42), "did not return a callable"),
    ],
)
def test_an_extension_that_cannot_be_built_refuses_the_worker(point: FakePoint, match: str) -> None:
    """Never skipped: a skipped extension is the original bug again, one AttributeError per message."""
    with pytest.raises(ContextExtensionError, match=match) as refused:
        load_context_extensions(points=points(point))
    assert "SYNTHPATIENT" not in str(refused.value), "the exception's own text is never repeated"


def test_two_distributions_registering_one_name_are_refused() -> None:
    with pytest.raises(ContextExtensionError, match="more than one distribution"):
        load_context_extensions(
            points=points(FakePoint("fhir", factory_of(fhir_extension)), FakePoint("fhir", factory_of(fhir_extension)))
        )


class _NoReply:
    """A wrapper written against `Context` only: delegates everything except `reply`."""

    def __init__(self, base: Context) -> None:
        self._base = base

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


class _SwallowsAttrs(_NoReply):
    def set_attr(self, name: str, value: str) -> None:
        return None

    def reply(self, body: bytes, *, content_type: str) -> None:
        return None


class _OwnVars(_SwallowsAttrs):
    def __init__(self, base: Context) -> None:
        super().__init__(base)
        self._own: dict[str, object] = {}

    def set_attr(self, name: str, value: str) -> None:
        self._base.set_attr(name, value)

    def set_usr(self, name: str, value: str) -> None:
        self._base.set_usr(name, value)

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._own


class _SwallowsUsr(_OwnVars):
    def set_usr(self, name: str, value: str) -> None:
        return None

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._base.vars


def _probe_raises(base: Context, message: Message) -> Context:
    raise KeyError("SYNTHPATIENT^ALPHA")


@pytest.mark.parametrize(
    ("extension", "match"),
    [
        (lambda base, message: object(), "not a Context"),
        (lambda base, message: _NoReply(base), "dropped `reply`"),
        (lambda base, message: _SwallowsAttrs(base), "set_attr"),
        (lambda base, message: _SwallowsUsr(base), "set_usr"),
        (lambda base, message: _OwnVars(base), "vars"),
        (_probe_raises, "failed its probe: KeyError"),
    ],
)
def test_the_probe_refuses_a_wrapper_that_breaks_what_the_worker_reads(extension: Any, match: str) -> None:
    """attrs feed events, tb-usr-* feed record headers, vars carry the parsed message between steps, and
    reply is the synchronous answer. A wrapper that loses any of them is refused before consuming."""
    with pytest.raises(ContextExtensionError, match=match) as refused:
        verify_extension(LoadedExtension(name="bad", distribution="pkg", extend=extension))
    assert "SYNTHPATIENT" not in str(refused.value)


def test_the_real_attach_passes_the_probe() -> None:
    verify_extension(LoadedExtension(name="fhir", distribution="tributary-fhir", extend=fhir_extension))


def test_a_refused_extension_stops_the_worker_before_any_kafka_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discovery runs in `PipelineWorker.build` right after the profile warning, before the channels
    reader: this test has no broker, so reaching a client constructor would fail differently."""

    def refuse(**_: object) -> tuple[LoadedExtension, ...]:
        raise ContextExtensionError("context extension fhir failed to start: RuntimeError")

    monkeypatch.setattr("tributary.runtime.worker.load_context_extensions", refuse)
    worker = PipelineWorker(
        WorkerSettings(
            env="dev",
            channel="adt-passthrough",
            kafka=KafkaSettings(bootstrap_servers="127.0.0.1:9092"),
            instance_id="pipeline-1",
            profile="production",
        ),
        log=JsonLogger("worker", stream=io.StringIO()),
    )
    with pytest.raises(ContextExtensionError):
        worker.build()


@pytest.mark.xfail(
    strict=True,
    reason="ask to healthcare-standards-engineer: tributary-fhir registers `fhir` in the tributary.context "
    "entry-point group (platform-engineer handoff, 2026-09-29). Remove this marker when it lands.",
)
def test_tributary_fhir_registers_its_context_extension() -> None:
    registered = {point.name: point for point in entry_points(group=ENTRY_POINT_GROUP)}
    assert "fhir" in registered
    extension = load_context_extensions(points=[registered["fhir"]])[0]
    view = extension.extend(worker_ctx(extensions=()), make_message(b"{}"))
    assert hasattr(view, "fhir") and hasattr(view, "terminology")
