"""The built-in steps of channel-spec README 3.3.2, unit-tested with no Kafka (invariant 7).

Each built-in is run exactly as a bundle author would test their own step - `run_step(..., with_=...)`
- which also proves the binder is what enforces the approved `with` shapes. All HL7 here is synthetic:
MSH-3 is TRIBUTARY-SYNTH and every name and number is invented.
"""

from __future__ import annotations

import json
from importlib.metadata import entry_points

import pytest
from tributary.runtime.core_steps import enrich, filter_step, split, validate
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import StepRef
from tributary.runtime.steps import ENTRY_POINT_GROUP, load_step
from tributary.sdk import Drop, Message, Reject, StepConfigError
from tributary.sdk import headers as h
from tributary.sdk.fakes import FakeContext
from tributary.sdk.ids import derived_uuid7
from tributary.sdk.testing import make_message, run_splitter, run_step

HL7 = "application/hl7-v2+er7"
ADT_A01 = (
    b"MSH|^~\\&|TRIBUTARY-SYNTH|SYNTH-FAC|RCV|RCV-FAC|20260101000000||ADT^A01^ADT_A01|MSG0001|P|2.5.1\r"
    b"EVN|A01|20260101000000\r"
    b"PID|1||SYNTH-0001^^^SYNTH^MR||SYNTH^JANE||19700101|F\r"
    b"PV1|1|I|WARD^1^1\r"
)
PHI_MARKER = "SYNTH^JANE"


def hl7(payload: bytes = ADT_A01) -> Message:
    return make_message(payload, content_type=HL7)


def fhir(document: object) -> Message:
    return make_message(json.dumps(document).encode(), content_type="application/fhir+json")


def test_every_builtin_is_registered_under_its_contract_name() -> None:
    registered = {point.name: point.value for point in entry_points(group=ENTRY_POINT_GROUP)}
    assert registered["tributary.core.filter"] == "tributary.runtime.core_steps:filter_step"
    assert registered["tributary.core.enrich"] == "tributary.runtime.core_steps:enrich"
    assert registered["tributary.core.validate"] == "tributary.runtime.core_steps:validate"
    assert registered["tributary.core.split"] == "tributary.runtime.core_steps:split"


# -- filter --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("with_", "kept"),
    [
        ({"field": "hl7:MSH.9.2", "in": ["A01", "A04"], "onUnresolved": "reject"}, True),
        ({"field": "hl7:MSH.9.2", "in": ["A08"], "onUnresolved": "reject"}, False),
        ({"field": "hl7:MSH.9.2", "notIn": ["A08"], "onUnresolved": "reject"}, True),
        ({"field": "hl7:MSH.9.2", "notIn": ["A01"], "onUnresolved": "reject"}, False),
        ({"field": "hl7:PV1.2", "present": True, "onUnresolved": "reject"}, True),
        ({"field": "hl7:PV2.1", "present": True, "onUnresolved": "reject"}, False),
        ({"field": "hl7:PV2.1", "present": False, "onUnresolved": "reject"}, True),
    ],
)
def test_filter_keeps_or_drops_on_the_field(with_: dict[str, object], kept: bool) -> None:
    message = hl7()
    outcome = run_step(filter_step, message, with_=with_)
    if kept:
        assert outcome is message, "a kept message passes through untouched"
    else:
        assert outcome == Drop(reason="FILTERED")


@pytest.mark.parametrize(
    ("choice", "expected"),
    [("keep", Message), ("drop", Drop)],
)
def test_filter_on_unresolved_does_what_the_author_wrote(choice: str, expected: type) -> None:
    """A missing field is not silently a drop: the author chose, and `onUnresolved` has no default."""
    with_ = {"field": "hl7:ZZZ.1", "in": ["X"], "onUnresolved": choice}
    ctx = FakeContext()
    outcome = run_step(filter_step, hl7(), with_=with_, ctx=ctx)
    assert isinstance(outcome, expected)
    if isinstance(outcome, Drop):
        assert outcome.reason == "FILTERED_UNRESOLVED"
    [logged] = ctx.log.records
    assert logged.fields["reason"] == "KEY_SEGMENT_ABSENT"


def test_filter_on_unresolved_reject_dead_letters_with_an_engine_code() -> None:
    with pytest.raises(Reject) as caught:
        run_step(
            filter_step,
            fhir({"resourceType": "Patient"}),
            with_={"field": "hl7:MSH.9.2", "in": ["A01"], "onUnresolved": "reject"},
        )
    assert caught.value.code == "FILTER_UNRESOLVED"


def test_filter_present_treats_a_wrong_format_as_unresolved_not_absent() -> None:
    outcome = run_step(
        filter_step, fhir({"a": 1}), with_={"field": "hl7:PV1.2", "present": False, "onUnresolved": "drop"}
    )
    assert outcome == Drop(reason="FILTERED_UNRESOLVED")


def test_filter_reads_json_with_the_shared_evaluator() -> None:
    with_ = {"field": "json:/resourceType", "in": ["Observation"], "onUnresolved": "drop"}
    assert isinstance(run_step(filter_step, fhir({"resourceType": "Observation"}), with_=with_), Message)
    assert run_step(filter_step, fhir({"resourceType": "Patient"}), with_=with_) == Drop(reason="FILTERED")


@pytest.mark.parametrize(
    "field", ["hl7:PID.3.1", "hl7:PID.5", "hl7:NK1.2", "hl7:GT1.3", "hl7:IN1.2", "hl7:IN2.1", "hl7:MRG.1", "hl7:PD1.4"]
)
def test_filter_refuses_patient_identifying_segments(field: str) -> None:
    """The `in` values are published in the spec, so a filter on PID would put PHI on `_tb.channels`."""
    with pytest.raises(StepConfigError, match="patient-identifying"):
        filter_step.bind({"field": field, "in": ["X"], "onUnresolved": "drop"})


@pytest.mark.parametrize(
    "field",
    [
        "json:/identifier",
        "json:/name",
        "json:/birthDate",
        "json:/telecom",
        "json:/address",
        "json:/subject/identifier/value",
        "json:/contact/0/name/family",
    ],
)
def test_filter_refuses_identifying_json_tokens_anywhere_in_the_pointer(field: str) -> None:
    with pytest.raises(StepConfigError, match="identifies a patient"):
        filter_step.bind({"field": field, "in": ["X"], "onUnresolved": "drop"})


@pytest.mark.parametrize(
    "with_",
    [
        {"field": "hl7:MSH.9.2", "onUnresolved": "drop"},  # none of in/notIn/present
        {"field": "hl7:MSH.9.2", "in": ["A01"], "notIn": ["A02"], "onUnresolved": "drop"},  # two
        {"field": "hl7:MSH.9.2", "in": [], "onUnresolved": "drop"},  # empty list
        {"field": "hl7:MSH.9.2", "in": ["A01"]},  # onUnresolved missing: no default
        {"field": "hl7:MSH.9.2", "in": ["A01"], "onUnresolved": "ignore"},  # not one of drop|keep|reject
        {"field": "hl7:MSH.9.2", "in": [1], "onUnresolved": "drop"},  # a number would never match bytes
        {"field": "record:key", "in": ["A01"], "onUnresolved": "drop"},  # the key is a patient identifier
        {"field": "xpath://x", "in": ["A01"], "onUnresolved": "drop"},
        {"field": "MSH.9.2", "in": ["A01"], "onUnresolved": "drop"},  # no scheme
    ],
)
def test_filter_with_shapes_outside_the_contract_are_refused_at_bind(with_: dict[str, object]) -> None:
    with pytest.raises(StepConfigError):
        filter_step.bind(with_)


def test_a_refused_filter_is_a_channel_spec_error_through_the_shipping_loader() -> None:
    with pytest.raises(ChannelSpecError, match="patient-identifying"):
        load_step(
            StepRef(
                name="f",
                uses="tributary.core.filter",
                params={"field": "hl7:PID.3.1", "in": ["X"], "onUnresolved": "drop"},
            )
        )


# -- enrich --------------------------------------------------------------------------------------


def test_enrich_sets_static_attrs_usr_and_allow_listed_fields() -> None:
    ctx = FakeContext()
    with_ = {
        "attrs": ["feed=adt-main"],
        "usr": ["route=primary"],
        "attrFrom": ["sender=hl7:MSH.3", "trigger=hl7:MSH.9.2", "hl7_version=hl7:MSH.12"],
    }
    outcome = run_step(enrich, hl7(), with_=with_, ctx=ctx)
    assert isinstance(outcome, Message)
    assert dict(ctx.attrs()) == {
        "feed": "adt-main",
        "sender": "TRIBUTARY-SYNTH",
        "trigger": "A01",
        "hl7_version": "2.5.1",
    }
    assert dict(ctx.usr_headers()) == {"route": "primary"}


def test_enrich_reads_the_fhir_resource_type() -> None:
    ctx = FakeContext()
    run_step(
        enrich, fhir({"resourceType": "Observation"}), with_={"attrFrom": ["resource=json:/resourceType"]}, ctx=ctx
    )
    assert dict(ctx.attrs()) == {"resource": "Observation"}


@pytest.mark.parametrize(
    "locator",
    ["hl7:PID.3.1", "hl7:PID.5", "hl7:MSH.10", "hl7:PV1.19", "hl7:OBX.5", "json:/id", "json:/subject/reference"],
)
def test_enrich_attr_from_is_limited_to_the_allow_list(locator: str) -> None:
    with pytest.raises(StepConfigError, match="may only read"):
        enrich.bind({"attrFrom": [f"field={locator}"]})


def test_enrich_skips_a_value_that_fails_the_attrs_guard_and_logs_no_value() -> None:
    """A sending application with a space is not an attr; the message still flows (metadata never
    fails a clinical message), and the log names the attr and a reason code only."""
    payload = ADT_A01.replace(b"TRIBUTARY-SYNTH", b"TRIBUTARY SYNTH APP")
    ctx = FakeContext()
    outcome = run_step(enrich, hl7(payload), with_={"attrFrom": ["sender=hl7:MSH.3"]}, ctx=ctx)
    assert isinstance(outcome, Message)
    assert dict(ctx.attrs()) == {}
    [record] = ctx.log.records
    assert record.event == "enrich_attr_skipped"
    assert record.fields["reason"] == "ATTR_VALUE_INVALID"
    assert "SYNTH APP" not in json.dumps(dict(record.fields))


def test_enrich_reads_the_other_allow_listed_feed_fields() -> None:
    ctx = FakeContext()
    run_step(enrich, hl7(), with_={"attrFrom": ["event=hl7:EVN.1", "facility=hl7:MSH.4", "x=hl7:MSH.11"]}, ctx=ctx)
    assert dict(ctx.attrs()) == {"event": "A01", "facility": "SYNTH-FAC", "x": "P"}


def test_enrich_skips_an_absent_field_by_reason_code() -> None:
    ctx = FakeContext()
    without_evn = ADT_A01.replace(b"EVN|A01|20260101000000\r", b"")
    outcome = run_step(enrich, hl7(without_evn), with_={"attrFrom": ["event=hl7:EVN.1"]}, ctx=ctx)
    assert isinstance(outcome, Message)
    assert dict(ctx.attrs()) == {}
    [record] = ctx.log.records
    assert record.fields["reason"] == "KEY_SEGMENT_ABSENT"


@pytest.mark.parametrize(
    "with_",
    [
        {},  # nothing to do
        {"attrs": ["no-equals-sign"]},
        {"attrs": ["=value"]},
        {"attrs": ["mrn=x"]},  # an identifier-shaped attr name (events/v0 3)
        {"attrs": ["feed=has space"]},
        {"attrs": ["feed=a", "feed=b"]},
        {"attrs": ["feed=a"], "attrFrom": ["feed=hl7:MSH.3"]},
        {"usr": ["Route=primary"]},  # tb-usr names are lower-case
        {"attrFrom": ["patient=hl7:MSH.3"]},  # identifier-shaped name even from an allowed field
        {"attrs": [f"a{i}=v" for i in range(17)]},  # over the 16-attr cap
    ],
)
def test_enrich_with_shapes_outside_the_contract_are_refused_at_bind(with_: dict[str, object]) -> None:
    with pytest.raises(StepConfigError):
        enrich.bind(with_)


# -- validate ------------------------------------------------------------------------------------


def test_validate_passes_a_conforming_message_through() -> None:
    outcome = run_step(
        validate, hl7(), with_={"requireSegments": ["MSH", "EVN", "PID", "PV1"], "version": ["2.5", "2.5.1"]}
    )
    assert isinstance(outcome, Message)
    assert outcome.payload == ADT_A01


@pytest.mark.parametrize(
    ("message", "with_", "code"),
    [
        (hl7(), {"requireSegments": ["PV2"]}, "VALIDATE_SEGMENT_MISSING"),
        (hl7(), {"version": "2.3"}, "VALIDATE_VERSION_MISMATCH"),
        (hl7(ADT_A01.replace(b"|P|2.5.1", b"|P|")), {"version": "2.5.1"}, "VALIDATE_VERSION_MISMATCH"),
        (fhir({"resourceType": "Patient"}), {"requireSegments": ["MSH"]}, "VALIDATE_NOT_HL7"),
        (hl7(b"EVN|A01\rMSH|^~\\&|TRIBUTARY-SYNTH\r"), {"requireSegments": ["MSH"]}, "VALIDATE_NOT_HL7"),
        (make_message(None, content_type=HL7), {"requireSegments": ["MSH"]}, "VALIDATE_NOT_HL7"),
    ],
)
def test_validate_rejects_with_a_stable_code_and_no_payload_text(
    message: Message, with_: dict[str, object], code: str
) -> None:
    with pytest.raises(Reject) as caught:
        run_step(validate, message, with_=with_)
    assert caught.value.code == code
    assert PHI_MARKER not in str(caught.value)


@pytest.mark.parametrize(
    "with_",
    [{}, {"requireSegments": ["pid"]}, {"requireSegments": ["PIDX"]}, {"version": "two"}, {"version": 2.5}],
)
def test_validate_with_shapes_outside_the_contract_are_refused_at_bind(with_: dict[str, object]) -> None:
    with pytest.raises(StepConfigError):
        validate.bind(with_)


# -- split ---------------------------------------------------------------------------------------

BATCH = (
    b"FHS|^~\\&|TRIBUTARY-SYNTH\r"
    b"BHS|^~\\&|TRIBUTARY-SYNTH\r"
    b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A01|M1|P|2.5.1\r"
    b"PID|1||SYNTH-0001^^^SYNTH^MR\r"
    b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A08|M2|P|2.5.1\n"
    b"PID|1||SYNTH-0002^^^SYNTH^MR\n"
    b"BTS|2\r"
    b"FTS|1\r"
)


def test_split_hl7_batch_yields_one_child_per_message_without_the_envelope() -> None:
    parent = hl7(BATCH)
    children = run_splitter(split, parent, with_={"by": "hl7-batch"})
    assert isinstance(children, tuple)
    assert [child.payload for child in children] == [
        b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A01|M1|P|2.5.1\rPID|1||SYNTH-0001^^^SYNTH^MR\r",
        b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A08|M2|P|2.5.1\rPID|1||SYNTH-0002^^^SYNTH^MR\r",
    ]
    assert [child.msg_id for child in children] == [derived_uuid7(parent.msg_id, i) for i in range(2)]
    assert all(child.content_type == HL7 for child in children)


@pytest.mark.parametrize(
    "payload",
    [b"PID|1||x\rMSH|^~\\&|TRIBUTARY-SYNTH\r", b"MSH|^~\\&|TRIBUTARY-SYNTH\rnot a segment\r"],
)
def test_split_hl7_batch_rejects_input_it_cannot_read(payload: bytes) -> None:
    with pytest.raises(Reject) as caught:
        run_splitter(split, hl7(payload), with_={"by": "hl7-batch"})
    assert caught.value.code == "SPLIT_INPUT_INVALID"


def test_split_json_array_keeps_each_element_s_exact_source_text() -> None:
    """`1.50` stays `1.50`: significant figures are clinical data, so children are source slices."""
    text = b'{"resourceType": "Bundle", "entry": [ {"v": 1.50, "u": "mmol/L"} ,{"v":2.0} , "s\\u00e9"] }'
    parent = make_message(text, content_type="application/fhir+json")
    children = run_splitter(split, parent, with_={"by": "json-array", "pointer": "/entry"})
    assert isinstance(children, tuple)
    assert [child.payload for child in children] == [b'{"v": 1.50, "u": "mmol/L"}', b'{"v":2.0}', b'"s\\u00e9"']


def test_split_json_array_at_the_document_root_and_by_index() -> None:
    root = make_message(b"[1, [2, 3]]", content_type="application/json")
    children = run_splitter(split, root, with_={"by": "json-array"})
    assert isinstance(children, tuple) and [child.payload for child in children] == [b"1", b"[2, 3]"]
    nested = run_splitter(split, root, with_={"by": "json-array", "pointer": "/1"})
    assert isinstance(nested, tuple) and [child.payload for child in nested] == [b"2", b"3"]


def test_an_empty_array_is_an_empty_split() -> None:
    parent = make_message(b'{"entry": []}', content_type="application/json")
    assert run_splitter(split, parent, with_={"by": "json-array", "pointer": "/entry"}) == Drop(reason="EMPTY_SPLIT")


@pytest.mark.parametrize(
    ("payload", "pointer"),
    [
        (b'{"entry": {"a": 1}}', "/entry"),  # not an array
        (b'{"other": []}', "/entry"),  # absent
        (b'{"entry": [1], "entry": [2]}', "/entry"),  # ambiguous duplicate member
        (b"[1, 2", ""),  # not JSON
        (b"[NaN]", ""),  # not JSON either
        (b"[1]", "/5"),  # index out of range
        (b"[1]", "/01"),  # leading zero is not an array index
    ],
)
def test_split_json_array_rejects_input_it_cannot_read(payload: bytes, pointer: str) -> None:
    with pytest.raises(Reject) as caught:
        run_splitter(
            split,
            make_message(payload, content_type="application/json"),
            with_={"by": "json-array", "pointer": pointer},
        )
    assert caught.value.code == "SPLIT_INPUT_INVALID"


def test_split_json_array_rejects_a_non_json_content_type() -> None:
    with pytest.raises(Reject):
        run_splitter(split, hl7(), with_={"by": "json-array"})


@pytest.mark.parametrize(
    "with_",
    [{}, {"by": "lines"}, {"by": "hl7-batch", "pointer": "/entry"}, {"by": "json-array", "pointer": "entry"}],
)
def test_split_with_shapes_outside_the_contract_are_refused_at_bind(with_: dict[str, object]) -> None:
    with pytest.raises(StepConfigError):
        split.bind(with_)


def test_the_builtin_splitter_is_refused_on_a_per_key_channel_through_the_shipping_loader() -> None:
    from tributary.runtime.spec import ChannelSpec
    from tributary.runtime.steps import build_plan

    from .test_tree_execution import document

    spec_document = document(pipeline=["t.passthrough"], destinations={"fhir": []}, ordering="per-key")
    spec_document["spec"]["pipeline"]["steps"] = [
        {"name": "unbatch", "uses": "tributary.core.split", "with": {"by": "hl7-batch"}}
    ]
    with pytest.raises(ChannelSpecError, match="per-key"):
        build_plan(ChannelSpec.from_document(spec_document))


def test_a_channel_of_builtins_runs_end_to_end_through_the_worker_handler() -> None:
    """validate -> split -> filter -> enrich, then a destination-only enrich, resolved by entry point."""
    import io

    from tributary.runtime.events import MessageEvent, RecordingEmitter
    from tributary.runtime.logs import JsonLogger
    from tributary.runtime.producer import RecordProducer
    from tributary.runtime.records import ConsumedRecord
    from tributary.runtime.spec import ChannelSpec
    from tributary.runtime.worker import PipelineHandler

    from .fakes import FakeProducer
    from .test_tree_execution import document

    spec_document = document(pipeline=["t.passthrough"], destinations={"fhir": [], "rows": []})
    spec_document["spec"]["pipeline"]["steps"] = [
        {"name": "check", "uses": "tributary.core.validate", "with": {"requireSegments": ["FHS"]}},
        {"name": "unbatch", "uses": "tributary.core.split", "with": {"by": "hl7-batch"}},
        {
            "name": "admits",
            "uses": "tributary.core.filter",
            "with": {"field": "hl7:MSH.9.2", "in": ["A01"], "onUnresolved": "reject"},
        },
        {"name": "tag", "uses": "tributary.core.enrich", "with": {"attrFrom": ["trigger=hl7:MSH.9.2"]}},
    ]
    spec_document["spec"]["destinations"][1]["steps"] = [
        {"name": "route", "uses": "tributary.core.enrich", "with": {"usr": ["sink=rows"]}}
    ]
    batch = BATCH.replace(b"FHS|", b"MSH|^~\\&|TRIBUTARY-SYNTH|F|R|R|20260101||ADT^A01|M0|P|2.5.1\rFHS|", 1)
    spec = ChannelSpec.from_document(spec_document)
    producer, emitter, stream = FakeProducer(), RecordingEmitter(), io.StringIO()
    handler = PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=RecordProducer(producer, log=JsonLogger("test", stream=stream)),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream),
        instance="worker:test@localhost",
    )
    handler.prepare(spec)
    source = make_message(batch, content_type=HL7, channel="adt-passthrough")
    handler.handle(ConsumedRecord(topic="dev.adt-passthrough.raw", partition=0, offset=0, message=source))

    out = {topic.rsplit(".", 1)[1]: [] for topic, *_rest in producer.produced}
    for topic, _key, value, headers in producer.produced:
        out[topic.rsplit(".", 1)[1]].append((value, {name: blob.decode() for name, blob in headers or []}))
    # Children 0 (M0, A01) and 1 (M1, A01) pass the filter; child 2 (M2, A08) is filtered.
    assert sorted(out) == ["fhir", "rows"]
    assert [headers[h.TB_MSG_ID] for _v, headers in out["fhir"]] == [derived_uuid7(source.msg_id, i) for i in (0, 1)]
    assert all(headers.get("tb-usr-sink") == "rows" for _v, headers in out["rows"])
    assert all("tb-usr-sink" not in headers for _v, headers in out["fhir"])
    routed = [event for event in emitter.events if isinstance(event, MessageEvent) and event.stage == "routed"]
    assert {event.attrs["trigger"] for event in routed if event.attrs} == {"A01"}
    filtered = [event for event in emitter.events if isinstance(event, MessageEvent) and event.stage == "filtered"]
    assert [event.message_id for event in filtered] == [derived_uuid7(source.msg_id, 2)]
    assert PHI_MARKER not in stream.getvalue() and "SYNTH-0001" not in stream.getvalue()
