"""The executor tree: destination steps (README 3.5), split (3.3.2) and `Reject`, with no Kafka.

What each test pins is a delivery or ordering property, not an implementation detail:

* a destination step's failure or `Drop` touches that destination only, and the others still get their
  record in the same flush — so the raw offset commits only when every branch is produced or dead-lettered;
* a destination-scoped dead letter carries `tb-destination` and the destination step's *input*;
* split children have deterministic ids, so a redelivered parent produces byte-identical records;
* a splitter is refused on a per-key channel, and a refused hot reload keeps the previous plan running;
* no exception text reaches a log line, an event or a record (invariant 6).
"""

from __future__ import annotations

import copy
import io
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.events import MessageEvent, RecordingEmitter, serialize
from tributary.runtime.executor import PIPELINE_SCOPE, StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec, StepRef
from tributary.runtime.steps import Node, build_plan
from tributary.runtime.worker import PipelineHandler
from tributary.sdk import Context, Drop, Message, Reject, splitter, step
from tributary.sdk import headers as h
from tributary.sdk.ids import derived_uuid7
from tributary.sdk.testing import make_message

from .fakes import FakeProducer

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
PHI_MARKER = "SYNTH^JANE"


# -- steps used by the channels below -------------------------------------------------------------


@step
def passthrough(msg: Message) -> None:
    return None


@step
def to_row(msg: Message, ctx: Context) -> Message:
    ctx.set_attr("shape", "row")
    ctx.set_usr("sink", ctx.destination or "none")
    return msg.with_payload(b'{"row": true}', content_type="application/json")


@step
def only_rows(msg: Message) -> Drop:
    return Drop(reason="not for this sink")


CALLS: dict[str, int] = {}


@step
def explode(msg: Message) -> Message:
    CALLS["explode"] = CALLS.get("explode", 0) + 1
    raise ValueError(f"{PHI_MARKER} mrn 12345")


@step
def refuse(msg: Message) -> Message:
    CALLS["refuse"] = CALLS.get("refuse", 0) + 1
    raise Reject("NOT_FOR_THIS_SINK")


@splitter
def by_line(msg: Message) -> list[Message]:
    return [msg.with_payload(line) for line in (msg.payload or b"").split(b"\n") if line]


@step
def fail_on_b(msg: Message) -> None:
    if msg.payload == b"b":
        raise Reject("CHILD_B_REFUSED")


@step
def drop_c(msg: Message) -> Drop | None:
    return Drop(reason="c is not wanted") if msg.payload == b"c" else None


REGISTRY: dict[str, Node] = {
    "t.passthrough": passthrough,
    "t.to_row": to_row,
    "t.only_rows": only_rows,
    "t.explode": explode,
    "t.refuse": refuse,
    "t.by_line": by_line,
    "t.fail_on_b": fail_on_b,
    "t.drop_c": drop_c,
}


def resolve(reference: StepRef) -> Node:
    node = REGISTRY[reference.uses or ""]
    return node.bind(reference.params)


# -- channel builders ----------------------------------------------------------------------------


def document(
    *,
    pipeline: list[str],
    destinations: Mapping[str, list[str]],
    ordering: str = "none",
    version: int = 1,
    step_retries: int = 2,
) -> dict[str, Any]:
    base = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    template = base["spec"]["destinations"][0]
    base["metadata"]["version"] = version
    base["spec"]["ordering"] = ordering
    if ordering == "per-key":
        base["spec"]["partitionKey"] = "hl7:MSH.10"
    base["spec"]["pipeline"]["steps"] = [{"name": f"p{i}", "uses": uses} for i, uses in enumerate(pipeline)]
    base["spec"]["pipeline"]["stepRetries"] = step_retries
    built = []
    for name, steps in destinations.items():
        destination = copy.deepcopy(template)
        destination["name"] = name
        if steps:
            destination["steps"] = [{"name": f"{name}-{i}", "uses": uses} for i, uses in enumerate(steps)]
        built.append(destination)
    base["spec"]["destinations"] = built
    return base


def channel(**kwargs: Any) -> ChannelSpec:
    return ChannelSpec.from_document(document(**kwargs))


def record(payload: bytes = b"synthetic", *, key: bytes | None = None) -> ConsumedRecord:
    return ConsumedRecord(
        topic="dev.adt-passthrough.raw",
        partition=0,
        offset=11,
        message=make_message(payload, key=key, channel="adt-passthrough"),
    )


class Harness:
    def __init__(self, spec: ChannelSpec | Callable[[], ChannelSpec], *, transactional: bool = False) -> None:
        self.producer = FakeProducer()
        self.emitter = RecordingEmitter()
        self.stream = io.StringIO()
        provider = spec if callable(spec) else (lambda: spec)
        self.handler = PipelineHandler(
            env="dev",
            spec_provider=provider,
            producer=RecordProducer(self.producer, log=JsonLogger("test", stream=self.stream)),
            emitter=self.emitter,
            log=JsonLogger("worker", stream=self.stream),
            instance="worker:test@localhost",
            resolve=resolve,
            transactional=transactional,
        )

    def out(self, destination: str) -> list[tuple[bytes | None, bytes | None, dict[str, str]]]:
        topic = f"dev.adt-passthrough.out.{destination}"
        return [
            (key, value, {name: blob.decode() for name, blob in headers or []})
            for produced_topic, key, value, headers in self.producer.produced
            if produced_topic == topic
        ]

    def dlq(self) -> list[tuple[bytes | None, bytes | None, dict[str, str]]]:
        return [
            (key, value, {name: blob.decode() for name, blob in headers or []})
            for topic, key, value, headers in self.producer.produced
            if topic == "dev.adt-passthrough.dlq"
        ]

    def events(self, stage: str) -> list[MessageEvent]:
        return [event for event in self.emitter.events if isinstance(event, MessageEvent) and event.stage == stage]


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


# -- destination steps ---------------------------------------------------------------------------


def test_each_destination_gets_its_own_copy_and_its_own_steps() -> None:
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"fhir": [], "rows": ["t.to_row"]}))
    harness.handler.handle(record(b"original"))

    [(_, fhir_value, fhir_headers)] = harness.out("fhir")
    [(_, rows_value, rows_headers)] = harness.out("rows")
    assert fhir_value == b"original"
    assert rows_value == b'{"row": true}'
    assert rows_headers[h.TB_CONTENT_TYPE] == "application/json"
    assert rows_headers["tb-usr-sink"] == "rows", "ctx.destination is the destination's name"
    assert "tb-usr-sink" not in fhir_headers, "a destination step's tb-usr-* stays on that destination"
    routed = {event.destination: event.attrs for event in harness.events("routed")}
    assert routed == {"fhir": None, "rows": {"shape": "row"}}, "attrs a destination step set stay on its events"


def test_a_drop_in_a_destination_step_filters_that_destination_only() -> None:
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"fhir": [], "rows": ["t.only_rows"]}))
    harness.handler.handle(record())

    assert len(harness.out("fhir")) == 1
    assert harness.out("rows") == []
    [filtered] = harness.events("filtered")
    assert (filtered.destination, filtered.status, filtered.error_message) == ("rows", "dropped", "not for this sink")
    assert [event.destination for event in harness.events("routed")] == ["fhir"]


def test_a_failing_destination_step_is_retried_then_dead_lettered_for_that_destination_only() -> None:
    harness = Harness(channel(pipeline=["t.to_row"], destinations={"fhir": [], "rows": ["t.explode"]}, step_retries=2))
    source = record(b"original")
    harness.handler.handle(source)  # returns normally: the loop may commit

    assert CALLS["explode"] == 3, "stepRetries=2 means three attempts"
    assert len(harness.out("fhir")) == 1, "the other destination still gets its record"
    [(dlq_key, dlq_value, dlq_headers)] = harness.dlq()
    assert dlq_headers[h.TB_DESTINATION] == "rows", "destination-scoped reprocess needs tb-destination"
    assert dlq_headers[h.TB_ERROR_CODE] == "TRANSFORM_ERROR"
    assert dlq_value == b'{"row": true}', "the dlq payload is the destination step's input, not the raw payload"
    assert dlq_key == source.message.msg_id.encode()
    assert dlq_headers[h.TB_MSG_ID] == source.message.msg_id
    assert harness.producer.flushes == 1, "out and dlq records are flushed together before the commit"
    [dead] = harness.events("dlq")
    assert (dead.destination, dead.error_code) == ("rows", "TRANSFORM_ERROR")


def test_reject_in_a_destination_step_skips_the_retries() -> None:
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"rows": ["t.refuse"]}, step_retries=5))
    harness.handler.handle(record())

    assert CALLS["refuse"] == 1, "Reject is non-retryable: stepRetries is skipped"
    [(_, _, headers)] = harness.dlq()
    assert headers[h.TB_ERROR_CODE] == "NOT_FOR_THIS_SINK"


def test_reject_in_a_pipeline_step_fails_the_record_with_its_code_at_once() -> None:
    harness = Harness(channel(pipeline=["t.refuse"], destinations={"fhir": []}, step_retries=5))
    with pytest.raises(StepFailure) as caught:
        harness.handler.handle(record())
    assert caught.value.error_code == "NOT_FOR_THIS_SINK"
    assert caught.value.attempts == 1
    assert caught.value.rejected
    assert harness.producer.produced == []


def test_no_exception_text_reaches_a_log_an_event_or_a_record() -> None:
    """Invariant 6: the step's exception quotes a synthetic name and MRN; none of it may escape."""
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"rows": ["t.explode"]}))
    harness.handler.handle(record(b"MSH|^~\\&|TRIBUTARY-SYNTH"))
    harness.handler.close()

    assert PHI_MARKER not in harness.stream.getvalue()
    assert "12345" not in harness.stream.getvalue()
    for event in harness.emitter.events:
        assert PHI_MARKER.encode() not in serialize(event)
    for _topic, _key, _value, headers in harness.producer.produced:
        assert all(PHI_MARKER not in blob.decode() for _name, blob in headers or [])


def test_transactional_mode_leaves_the_flush_to_the_commit() -> None:
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"rows": ["t.explode"]}), transactional=True)
    harness.handler.handle(record())
    assert harness.producer.flushes == 0
    assert len(harness.dlq()) == 1, "the dead letter joins the open transaction with the out records"


# -- split ---------------------------------------------------------------------------------------


def test_split_children_are_routed_with_deterministic_ids_and_the_parent_key() -> None:
    harness = Harness(channel(pipeline=["t.passthrough", "t.by_line"], destinations={"fhir": [], "rows": []}))
    source = record(b"a\nb\nc")
    harness.handler.handle(source)

    parent_id = source.message.msg_id
    for destination in ("fhir", "rows"):
        out = harness.out(destination)
        assert [value for _key, value, _headers in out] == [b"a", b"b", b"c"], "children stay in split order"
        assert [headers[h.TB_MSG_ID] for _k, _v, headers in out] == [derived_uuid7(parent_id, i) for i in range(3)]
        assert {headers[h.TB_PARENT_ID] for _k, _v, headers in out} == {parent_id}
        assert {key for key, _v, _h in out} == {None}, "ordering none: the parent's (null) key"
    assert harness.producer.flushes == 1
    transformed = harness.events("transformed")
    assert [event.message_id for event in transformed] == [parent_id, *(derived_uuid7(parent_id, i) for i in range(3))]
    assert all(event.parent_id == parent_id for event in transformed[1:])


def test_a_redelivered_parent_produces_identical_children() -> None:
    """Invariant 4: the second delivery is a duplicate the loaders absorb, not a new set of messages."""
    spec = channel(pipeline=["t.by_line"], destinations={"fhir": []})
    source = record(b"a\nb")
    first, second = Harness(spec), Harness(spec)
    first.handler.handle(source)
    second.handler.handle(source)
    ids = [[headers[h.TB_MSG_ID] for _k, _v, headers in harness.out("fhir")] for harness in (first, second)]
    assert ids[0] == ids[1]
    values = [[value for _k, value, _h in harness.out("fhir")] for harness in (first, second)]
    assert values[0] == values[1]


def test_a_failing_child_is_dead_lettered_alone_and_the_others_proceed() -> None:
    harness = Harness(channel(pipeline=["t.by_line", "t.fail_on_b", "t.drop_c"], destinations={"fhir": []}))
    source = record(b"a\nb\nc")
    harness.handler.handle(source)

    assert [value for _k, value, _h in harness.out("fhir")] == [b"a"]
    [(dlq_key, dlq_value, dlq_headers)] = harness.dlq()
    child_b = derived_uuid7(source.message.msg_id, 1)
    assert dlq_value == b"b", "the child as the splitter produced it"
    assert dlq_key == child_b.encode()
    assert dlq_headers[h.TB_PARENT_ID] == source.message.msg_id
    assert dlq_headers[h.TB_ERROR_CODE] == "CHILD_B_REFUSED"
    assert h.TB_DESTINATION not in dlq_headers
    [filtered] = harness.events("filtered")
    assert filtered.message_id == derived_uuid7(source.message.msg_id, 2)


def test_an_empty_split_filters_the_parent() -> None:
    harness = Harness(channel(pipeline=["t.by_line"], destinations={"fhir": []}))
    harness.handler.handle(record(b""))
    assert harness.producer.produced == []
    [filtered] = harness.events("filtered")
    assert filtered.error_message == "EMPTY_SPLIT"


def test_a_splitter_on_a_per_key_channel_is_refused_at_load() -> None:
    """README 3.3.2 rule 3: children would inherit one patient's key; invariant 5 broken silently."""
    spec = channel(pipeline=["t.by_line"], destinations={"fhir": []}, ordering="per-key")
    with pytest.raises(ChannelSpecError, match="per-key"):
        build_plan(spec, resolve=resolve)
    with pytest.raises(ChannelSpecError, match="per-key"):
        Harness(spec).handler.prepare(spec)


def test_a_splitter_in_destination_steps_or_twice_in_the_pipeline_is_refused() -> None:
    with pytest.raises(ChannelSpecError, match="destination"):
        build_plan(channel(pipeline=["t.passthrough"], destinations={"fhir": ["t.by_line"]}), resolve=resolve)
    with pytest.raises(ChannelSpecError, match="at most one splitter"):
        build_plan(channel(pipeline=["t.by_line", "t.by_line"], destinations={"fhir": []}), resolve=resolve)


def test_a_refused_hot_reload_keeps_the_previous_plan_running() -> None:
    """README 3.3: refused loudly at load, and the previous spec keeps running."""
    current = {"spec": channel(pipeline=["t.by_line"], destinations={"fhir": []}, version=1)}
    harness = Harness(lambda: current["spec"])
    harness.handler.handle(record(b"a\nb"))
    assert len(harness.out("fhir")) == 2

    current["spec"] = channel(pipeline=["t.by_line"], destinations={"fhir": []}, ordering="per-key", version=2)
    harness.handler.handle(record(b"c\nd"))
    harness.handler.handle(record(b"e"))

    assert [value for _k, value, _h in harness.out("fhir")] == [b"a", b"b", b"c", b"d", b"e"]
    refusals = [line for line in harness.stream.getvalue().splitlines() if "channel_spec_refused" in line]
    assert len(refusals) == 1, "logged once per refused version, not once per message"
    assert json.loads(refusals[0])["channel_version"] == 2


def test_a_bad_with_on_hot_reload_is_refused_the_same_way() -> None:
    current = {"spec": channel(pipeline=["t.passthrough"], destinations={"fhir": []}, version=1)}
    harness = Harness(lambda: current["spec"])
    harness.handler.handle(record())
    bad = document(pipeline=["t.passthrough"], destinations={"fhir": []}, version=2)
    bad["spec"]["pipeline"]["steps"][0]["with"] = {"verison": "2.5"}
    current["spec"] = ChannelSpec.from_document(bad)
    harness.handler.handle(record())
    assert len(harness.out("fhir")) == 2
    assert "verison" in harness.stream.getvalue()


# -- timing ------------------------------------------------------------------------------------


def test_per_step_timing_is_kept_per_destination_and_logged_without_message_data() -> None:
    harness = Harness(channel(pipeline=["t.passthrough"], destinations={"fhir": [], "rows": ["t.to_row"]}))
    for _ in range(3):
        harness.handler.handle(record(PHI_MARKER.encode()))
    harness.handler.close()

    pipeline_stat = harness.handler.stats.get("p0")
    rows_stat = harness.handler.stats.get("rows-0", "rows")
    assert pipeline_stat is not None and pipeline_stat.calls == 3 and pipeline_stat.ok == 3
    assert rows_stat is not None and rows_stat.calls == 3
    assert rows_stat.max_ms >= rows_stat.mean_ms >= 0
    lines = [json.loads(line) for line in harness.stream.getvalue().splitlines() if "step_stats" in line]
    assert {(line["scope"], line["step"]) for line in lines} == {(PIPELINE_SCOPE, "p0"), ("rows", "rows-0")}
    assert PHI_MARKER not in harness.stream.getvalue()


# -- spec parsing and the loader ---------------------------------------------------------------


def test_destination_steps_are_parsed_in_order() -> None:
    spec = channel(pipeline=["t.passthrough"], destinations={"fhir": [], "rows": ["t.to_row", "t.only_rows"]})
    assert spec.destination("fhir").steps == ()
    assert [ref.uses for ref in spec.destination("rows").steps] == ["t.to_row", "t.only_rows"]


@pytest.mark.parametrize("steps", ["t.to_row", ["t.to_row", 7]])
def test_malformed_destination_steps_are_refused_rather_than_silently_shortened(steps: object) -> None:
    bad = document(pipeline=["t.passthrough"], destinations={"rows": []})
    bad["spec"]["destinations"][0]["steps"] = (
        steps if isinstance(steps, str) else [{"name": "a", "uses": steps[0]}, steps[1]]  # type: ignore[index]
    )
    with pytest.raises(ChannelSpecError):
        ChannelSpec.from_document(bad)


def test_the_shipping_loader_binds_with_and_refuses_an_unknown_key() -> None:
    """`load_step` is where README 3.3's refusal happens for real specs, not just in the harness."""
    from tributary.runtime.steps import load_step

    with pytest.raises(ChannelSpecError, match="unknown `with` key"):
        load_step(StepRef(name="p", uses="tributary.core.passthrough", params={"version": "2.5.1"}))
    loaded = load_step(StepRef(name="p", uses="tributary.core.passthrough", params={}))
    assert loaded.bound == {}


# -- properties: ordering and no loss across destination steps -----------------------------------


@step
def by_marker(msg: Message) -> Message | Drop:
    """Drops on `d`, rejects on `r`, passes anything else: the payload's first byte chooses."""
    marker = (msg.payload or b"-")[:1]
    if marker == b"d":
        return Drop(reason="marker d")
    if marker == b"r":
        raise Reject("MARKER_R")
    return msg


REGISTRY["t.by_marker"] = by_marker


@given(st.lists(st.tuples(st.sampled_from(["k1", "k2", "k3"]), st.sampled_from(["-", "d", "r"])), max_size=30))
@settings(max_examples=60, deadline=None)
def test_destination_steps_never_reorder_or_lose_a_message(stream: list[tuple[str, str]]) -> None:
    """Invariant 5 and invariant 4 on a per-key channel with destination steps.

    For every destination, each consumed message ends as exactly one of: an `out` record, a `filtered`
    event, or a destination-scoped dead letter - and the `out` records of one key appear in the order
    the messages were consumed, with the key unchanged.
    """
    spec = channel(
        pipeline=["t.passthrough"],
        destinations={"plain": [], "gated": ["t.by_marker"]},
        ordering="per-key",
    )
    harness = Harness(spec)
    consumed_ids: list[tuple[str, str, str]] = []
    for position, (key, marker) in enumerate(stream):
        source = ConsumedRecord(
            topic="dev.adt-passthrough.raw",
            partition=0,
            offset=position,
            message=make_message(f"{marker}{position}".encode(), key=key.encode(), channel="adt-passthrough"),
        )
        harness.handler.handle(source)
        consumed_ids.append((source.message.msg_id, key, marker))

    plain = [(headers[h.TB_MSG_ID], out_key) for out_key, _v, headers in harness.out("plain")]
    assert plain == [(msg_id, key.encode()) for msg_id, key, _m in consumed_ids], "plain gets every message in order"

    gated_out = [headers[h.TB_MSG_ID] for _k, _v, headers in harness.out("gated")]
    gated_dlq = [headers[h.TB_MSG_ID] for _k, _v, headers in harness.dlq()]
    gated_filtered = [event.message_id for event in harness.events("filtered") if event.destination == "gated"]
    assert gated_out == [msg_id for msg_id, _k, marker in consumed_ids if marker == "-"]
    assert gated_dlq == [msg_id for msg_id, _k, marker in consumed_ids if marker == "r"]
    assert gated_filtered == [msg_id for msg_id, _k, marker in consumed_ids if marker == "d"]
    assert all(headers[h.TB_DESTINATION] == "gated" for _k, _v, headers in harness.dlq())


def test_an_exception_merely_named_reject_is_retried_like_any_other() -> None:
    """Only the SDK's `Reject` skips the retries; a class that shares its name does not."""

    class Reject(Exception):  # the point is the name collision
        pass

    @step
    def impostor(msg: Message) -> None:
        CALLS["impostor"] = CALLS.get("impostor", 0) + 1
        raise Reject

    REGISTRY["t.impostor"] = impostor
    harness = Harness(channel(pipeline=["t.impostor"], destinations={"fhir": []}, step_retries=2))
    with pytest.raises(StepFailure) as caught:
        harness.handler.handle(record())
    assert CALLS["impostor"] == 3
    assert caught.value.error_code == "TRANSFORM_ERROR"
    assert not caught.value.rejected


# -- security review LOWs: import-time failures and what `ref` may name -----------------------------


def test_a_resolver_that_raises_anything_on_hot_reload_keeps_the_previous_plan() -> None:
    """A KeyError, RuntimeError or SyntaxError while resolving must refuse the spec, not escape
    `handle` on every record."""
    current = {"spec": channel(pipeline=["t.passthrough"], destinations={"fhir": []}, version=1)}
    harness = Harness(lambda: current["spec"])
    harness.handler.handle(record())
    current["spec"] = channel(pipeline=["t.not-registered"], destinations={"fhir": []}, version=2)
    harness.handler.handle(record())  # the test resolver raises KeyError for this name
    assert len(harness.out("fhir")) == 2
    assert "channel_spec_refused" in harness.stream.getvalue()


@pytest.mark.parametrize(
    ("source", "error"),
    [("raise RuntimeError('SYNTH^JANE at import')\n", "RuntimeError"), ("def broken(:\n", "SyntaxError")],
)
def test_a_ref_module_that_fails_at_import_is_a_channel_spec_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str, error: str
) -> None:
    from tributary.runtime.steps import load_step

    name = f"synth_bundle_{error.lower()}"  # one package per case: sys.modules caches packages
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "steps.py").write_text(source, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(ChannelSpecError) as caught:
        load_step(StepRef(name="s", ref=f"{name}.steps.step"))
    assert error in str(caught.value)
    assert PHI_MARKER not in str(caught.value), "only the exception's type name is kept"


class _Dist:
    """What `EntryPoint.dist` offers the resolver, read without importing: a name, a version, the install
    folder (`_path`) and the `RECORD` (`files`), which lists the target module."""

    name = "tributary-runtime"
    version = "0.0.0"
    files = ("synth_steps/module.py",)
    _path = Path("/site-packages/tributary_runtime-0.0.0.dist-info")


def _raising_point(name: str, raised: BaseException) -> object:
    """An entry point from the registered distribution of `tributary.core`, which raises on `load()`.

    Through the resolver's test seam (README 3.3.5 N6), with a registered three-segment name, so the
    tests below reach the import-failure path rather than the namespace refusal in front of it.
    """

    class Raising:
        value = "synth_steps.module:step"
        dist = _Dist()

        def __init__(self) -> None:
            self.name = name

        def load(self) -> object:
            raise raised

    return Raising()


def test_an_entry_point_that_fails_at_import_is_a_channel_spec_error() -> None:
    from tributary.runtime.steps import StepResolver

    broken = _raising_point("tributary.core.broken", RuntimeError("import-time failure"))
    resolver = StepResolver(points=lambda: [broken])  # type: ignore[list-item]
    with pytest.raises(ChannelSpecError, match=r"cannot load step .*: RuntimeError"):
        resolver.load(StepRef(name="s", uses="tributary.core.broken"))


@pytest.mark.parametrize(
    "ref", ["os.system", "subprocess.run", "json.loads", "tributary.runtime.core_steps.passthrough"]
)
def test_a_ref_into_the_standard_library_or_the_engine_is_refused_before_import(ref: str) -> None:
    from tributary.runtime.steps import load_step

    with pytest.raises(ChannelSpecError, match="standard library or the engine"):
        load_step(StepRef(name="s", ref=ref))


def test_sys_exit_at_import_refuses_the_spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.runtime.steps import load_step

    package = tmp_path / "synth_bundle_exits"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "steps.py").write_text("import sys\nsys.exit(3)\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(ChannelSpecError, match="SystemExit"):
        load_step(StepRef(name="s", ref="synth_bundle_exits.steps.step"))


@pytest.mark.parametrize(("raised", "refused"), [(SystemExit(3), True), (KeyboardInterrupt(), False)])
def test_an_entry_point_exiting_is_refused_but_an_interrupt_propagates(raised: BaseException, refused: bool) -> None:
    from tributary.runtime.steps import StepResolver

    exiting = _raising_point("tributary.core.exiting", raised)
    resolver = StepResolver(points=lambda: [exiting])  # type: ignore[list-item]
    if refused:
        with pytest.raises(ChannelSpecError, match=r"cannot load step .*: SystemExit"):
            resolver.load(StepRef(name="s", uses="tributary.core.exiting"))
    else:
        with pytest.raises(KeyboardInterrupt):
            resolver.load(StepRef(name="s", uses="tributary.core.exiting"))
