"""Audit records into `audit_log` rows: the contract's examples, the schema's conditionals, the loop's routing."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import re
import sys
import uuid
from pathlib import Path
from typing import Any

import orjson
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator
from tributary.archiver.archiver import Archiver, ArchiverSettings, parse_event_record
from tributary.archiver.audit_record import _KNOWN as KNOWN_FIELDS
from tributary.archiver.audit_record import COLUMNS, MAX_RECORD_BYTES, AuditRow, parse_audit_record
from tributary.archiver.errors import AuditRejected, EventRejected, SinkError, UnsupportedVersion
from tributary.archiver.main import build_record_parser, subscription_topics
from tributary.archiver.schema import audit_ddl
from tributary.runtime.logs import JsonLogger


def _fakes() -> Any:
    """The loop test's fakes (importlib import mode: sibling test modules are not importable by name)."""
    path = Path(__file__).with_name("test_archiver_loop.py")
    spec = importlib.util.spec_from_file_location("archiver_loop_fakes", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["archiver_loop_fakes"] = module
    spec.loader.exec_module(module)
    return module


_loop = _fakes()
FakeConsumer, FakeKafkaError, FakeMessage, FakeSink, base = (
    _loop.FakeConsumer,
    _loop.FakeKafkaError,
    _loop.FakeMessage,
    _loop.FakeSink,
    _loop.base,
)

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "audit" / "v0" / "examples"
MANIFEST = json.loads((EXAMPLES / "manifest.json").read_text())
VALID = [e["file"] for e in MANIFEST["valid"]]
#: Under the lenient consumer (audit/v0 README 4) an unlisted field is dropped, so the one example that is invalid
#: only by `additionalProperties` is accepted by the archiver (and tested as such below).
INVALID = [e["file"] for e in MANIFEST["invalid"] if e["file"] != "invalid/content-view-with-payload.json"]


def load(name: str) -> dict[str, Any]:
    return json.loads((EXAMPLES / name).read_text())  # type: ignore[no-any-return]


def parse(record: dict[str, Any] | bytes, *, env: str = "prod") -> AuditRow:
    value = record if isinstance(record, bytes) else orjson.dumps(record)
    return parse_audit_record(value, env=env, partition=3, offset=11)


def named(row: AuditRow) -> dict[str, Any]:
    return dict(zip(COLUMNS, row.values, strict=True))


def test_columns_are_the_tables_columns_without_the_server_set_one() -> None:
    ddl_columns = re.findall(r"^  `?(\w+)`?\s", audit_ddl().split("(\n", 1)[1], flags=re.M)
    assert ddl_columns[-1] == "inserted_at" and list(COLUMNS) == ddl_columns[:-1]


@pytest.mark.parametrize("name", VALID)
def test_every_valid_example_becomes_a_row(name: str) -> None:
    row = parse(load(name))
    values = named(row)
    assert len(row.values) == len(COLUMNS) and values["source_partition"] == 3 and values["source_offset"] == 11
    assert (
        values["audit_id"] == uuid.UUID(load(name)["audit_id"]) and values["actor_roles"] == load(name)["actor_roles"]
    )
    assert row.table == "audit_log" and "inserted_at" not in values


def test_a_row_carries_the_contracts_fields_and_nothing_else() -> None:
    values = named(parse(load("valid/replay-completed-reorder.json")))
    assert (values["action"], values["phase"], values["count"], values["allow_reorder"]) == (
        "channel.replay",
        "completed",
        41873,
        True,
    )
    assert values["target_topic"] == "prod.adt-fanout.raw" and values["range_end"].year == 2026
    assert values["message_id"] is None and values["channel"] == "adt-fanout" and values["error_code"] == ""
    failed = named(parse(load("valid/reprocess-failed.json")))
    assert failed["phase"] == "failed" and failed["error_code"] and failed["count"] == 0


@pytest.mark.parametrize("name", INVALID)
def test_every_invalid_example_is_rejected_with_a_fixed_code_and_not_quoted(name: str) -> None:
    with pytest.raises(EventRejected) as raised:
        parse(load(name))
    assert re.fullmatch(r"AUDIT_[A-Z_]+", raised.value.code)
    assert "synth" not in str(raised.value)


def test_unknown_fields_are_dropped_and_counted_and_the_known_fields_are_stored() -> None:
    """audit/v0 README 4: lenient consumer. The example that carries content is stored WITHOUT it."""
    record = {**load("invalid/content-view-with-payload.json"), "future_field": "SYNTH-NEW-FIELD-VALUE"}
    row = parse(record)
    assert row.unknown_fields == len(set(record) - set(load("valid/content-view-requested.json"))) >= 1
    flattened = repr(row.values)
    assert "SYNTH-NEW-FIELD-VALUE" not in flattened and record["action"] in flattened
    assert named(row)["action"] == "message.content.view"


@pytest.mark.parametrize("version", [1, 2, 99, -1, 1.0])
def test_a_version_other_than_zero_is_not_stored_and_is_its_own_rejection(version: Any) -> None:
    with pytest.raises(UnsupportedVersion) as raised:
        parse({**load("valid/content-view-requested.json"), "v": version})
    assert raised.value.code == "AUDIT_VERSION_UNSUPPORTED" and isinstance(raised.value, AuditRejected)


@pytest.mark.parametrize("version", ["0", True, False, None, [], {}])
def test_a_version_that_is_not_a_number_is_an_ordinary_rejection(version: Any) -> None:
    with pytest.raises(AuditRejected) as raised:
        parse({**load("valid/content-view-requested.json"), "v": version})
    assert not isinstance(raised.value, UnsupportedVersion) and raised.value.code == "AUDIT_FIELD_INVALID"


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ({"environment": "dev"}, "AUDIT_ENVIRONMENT_MISMATCH"),
        ({"audit_id": "not-a-uuid"}, "AUDIT_FIELD_INVALID"),
        ({"audit_id": str(uuid.uuid4())}, "AUDIT_FIELD_INVALID"),  # the contract wants a UUIDv7
        ({"ts": "2026-10-06T09:35:00Z"}, "AUDIT_FIELD_INVALID"),
        ({"phase": "pending"}, "AUDIT_FIELD_INVALID"),
        ({"actor": "has space"}, "AUDIT_FIELD_INVALID"),
        ({"actor_roles": []}, "AUDIT_FIELD_INVALID"),
        ({"actor_roles": ["operator", "operator"]}, "AUDIT_FIELD_INVALID"),
        ({"actor_roles": ["root"]}, "AUDIT_FIELD_INVALID"),
        ({"stage": "transformed"}, "AUDIT_FIELD_INVALID"),
        ({"phase": "completed"}, "AUDIT_FIELD_INVALID"),  # a content view is `requested` only
        ({"message_id": None}, "AUDIT_FIELD_INVALID"),  # present but null: the schema has no nullable field
    ],
)
def test_schema_violations_are_rejected_by_code(mutation: dict[str, Any], code: str) -> None:
    record = {**load("valid/content-view-requested.json"), **mutation}
    with pytest.raises(EventRejected) as raised:
        parse(record)
    assert raised.value.code == code


def test_actor_accepts_an_oidc_subject_and_the_per_action_conditionals_hold() -> None:
    assert named(parse({**load("valid/content-view-requested.json"), "actor": "idp|1234"}))["actor"] == "idp|1234"
    undeploy = {
        **load("valid/deployment-rollback-requested.json"),
        "action": "deployment.undeploy",
    }
    with pytest.raises(EventRejected):  # undeploy has no `version`
        parse(undeploy)
    undeploy.pop("version")
    undeploy.pop("previous_version", None)
    assert named(parse(undeploy))["action"] == "deployment.undeploy"
    requested = {k: v for k, v in load("valid/reprocess-requested.json").items() if k != "allow_reorder"}
    with pytest.raises(EventRejected):  # invariant 5: the reorder choice is on the record
        parse(requested)


@pytest.mark.parametrize("value", [None, b"", b"{not json", b"[]", b"\xff\xfe"])
def test_unparseable_values_are_rejected(value: bytes | None) -> None:
    with pytest.raises(EventRejected) as raised:
        parse_audit_record(value, env="prod", partition=0, offset=0)
    assert raised.value.code in {"AUDIT_EMPTY", "AUDIT_NOT_JSON", "AUDIT_NOT_OBJECT"}


def test_an_oversize_record_is_rejected() -> None:
    padded = orjson.dumps({**load("valid/content-view-requested.json"), "actor": "a" * 255}) + b" " * MAX_RECORD_BYTES
    with pytest.raises(EventRejected) as raised:
        parse_audit_record(padded, env="prod", partition=0, offset=0)
    assert raised.value.code == "AUDIT_TOO_LARGE"


# ---- the loop: events and audit records in one consumer, one commit ------------------------------------------


class Topic(FakeMessage):
    def __init__(self, topic: str, *a: Any, **k: Any) -> None:
        super().__init__(*a, **k)
        self._t = topic

    def topic(self) -> str:
        return self._t


def build(consumer: FakeConsumer, sink: FakeSink, audit: FakeSink) -> Archiver:
    def parser(message: Any) -> Any:
        if message.topic() == "_tb.prod.audit":
            return parse_audit_record(
                message.value(), env="prod", partition=message.partition(), offset=message.offset()
            )
        return parse_event_record(message)

    return Archiver(
        consumer,  # type: ignore[arg-type]
        sink,
        log=JsonLogger("archiver-test", stream=io.StringIO()),
        settings=ArchiverSettings(backoff_initial_s=0.01, backoff_max_s=0.02),
        sleep=lambda _s: None,
        parser=parser,
        side_sinks={"audit_log": audit},
    )


def audit_msg(offset: int, name: str = "valid/content-view-requested.json") -> Topic:
    return Topic("_tb.prod.audit", offset, orjson.dumps(load(name)))


def test_audit_rows_go_to_the_audit_sink_events_to_the_main_sink_and_one_commit_covers_both() -> None:
    consumer = FakeConsumer([[Topic("_tb.prod.events", 0, orjson.dumps(base())), audit_msg(1)]])
    sink, audit = FakeSink(), FakeSink()
    archiver = build(consumer, sink, audit)
    assert archiver.run_once() == 2
    assert len(sink.inserted) == 1 and len(audit.inserted) == 1
    assert len(audit.inserted[0]) == len(COLUMNS)
    assert consumer.commits and archiver.metrics.rows_written == 2


def test_a_malformed_audit_record_is_skipped_counted_and_committed_while_the_rest_lands() -> None:
    bad = Topic("_tb.prod.audit", 1, b"{broken")
    consumer = FakeConsumer([[audit_msg(0), bad, audit_msg(2, "valid/reprocess-requested.json")]])
    sink, audit = FakeSink(), FakeSink()
    archiver = build(consumer, sink, audit)
    archiver.run_once()
    assert len(audit.inserted) == 2 and sink.inserted == []
    assert archiver.metrics.audit_invalid == 1 and archiver.metrics.events_rejected == 0
    assert consumer.commits[-1] == [(0, 3)], "the poison record is committed past, not retried"


def test_a_failed_main_insert_retries_without_inserting_the_audit_rows_again() -> None:
    consumer = FakeConsumer([[Topic("_tb.prod.events", 0, orjson.dumps(base())), audit_msg(1)]])
    sink, audit = FakeSink(fail_times=2), FakeSink()
    archiver = build(consumer, sink, audit)
    archiver.run_once()
    assert audit.calls == 1 and len(audit.inserted) == 1, "append-only: a retry must not add a second copy"
    assert len(sink.inserted) == 1 and sink.calls == 3


def test_an_audit_insert_failure_commits_nothing_until_it_succeeds() -> None:
    consumer = FakeConsumer([[audit_msg(0)]])
    sink, audit = FakeSink(), FakeSink(fail_times=1)
    archiver = build(consumer, sink, audit)
    archiver.run_once()
    assert audit.calls == 2 and len(audit.inserted) == 1 and consumer.commits
    assert SinkError and FakeKafkaError


# ---- robustness: whatever a record holds, the parser returns a row or raises AuditRejected --------------------

UNHASHABLE = [[], ["x"], {}, {"k": "v"}, [[]], 1.5, True, None, "", 0]


@pytest.mark.parametrize("field", ["action", "phase", "stage", "actor_roles"])
@pytest.mark.parametrize("bad", [[], ["x"], {}, {"k": "v"}, [[]], [{}]])
def test_a_list_or_dict_in_a_membership_checked_field_is_a_rejection_not_a_crash(field: str, bad: Any) -> None:
    """Regression (security review H1): an unhashable value made `value in frozenset` raise TypeError and the
    archiver crash-loop on the record."""
    record = {**load("valid/content-view-requested.json"), field: bad}
    with pytest.raises(AuditRejected) as raised:
        parse(record)
    assert raised.value.code == "AUDIT_FIELD_INVALID"


def test_a_list_or_dict_inside_actor_roles_is_a_rejection() -> None:
    for roles in ([[]], [{}], ["operator", []], [["operator"]]):
        with pytest.raises(AuditRejected):
            parse({**load("valid/content-view-requested.json"), "actor_roles": roles})


json_values = st.recursive(
    st.none() | st.booleans() | st.integers(-(2**70), 2**70) | st.floats() | st.text(max_size=20),
    lambda children: st.lists(children, max_size=3) | st.dictionaries(st.text(max_size=8), children, max_size=3),
    max_leaves=6,
)


@settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
@given(
    field=st.sampled_from(sorted({*KNOWN_FIELDS, "extra"})),
    value=json_values,
    example=st.sampled_from(VALID),
    drop=st.sets(st.sampled_from(sorted(KNOWN_FIELDS)), max_size=3),
)
def test_arbitrary_json_in_any_field_never_escapes_as_anything_but_a_rejection(
    field: str, value: Any, example: str, drop: set[str]
) -> None:
    record = {k: v for k, v in load(example).items() if k not in drop}
    record[field] = value
    try:
        row = parse(orjson_safe(record))
    except AuditRejected:
        return
    assert len(row.values) == len(COLUMNS)


@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
@given(blob=st.binary(max_size=300) | json_values.map(lambda v: json.dumps(v, allow_nan=True).encode()))
def test_arbitrary_bytes_and_json_documents_are_rejected_or_parsed_never_raised(blob: bytes) -> None:
    with contextlib.suppress(AuditRejected):
        parse_audit_record(blob, env="prod", partition=0, offset=0)


def orjson_safe(record: dict[str, Any]) -> bytes:
    """Serialise with the stdlib (NaN and huge ints are legal inputs for the property, orjson refuses them)."""
    return json.dumps(record, allow_nan=True).encode()


# ---- differential: the hand validator and the contract's schema agree --------------------------------------

SCHEMA = json.loads((EXAMPLES.parent / "audit-event.schema.json").read_text())
LENIENT = {k: v for k, v in SCHEMA.items() if k != "additionalProperties"}  # README 4: unknown fields are dropped
VALIDATOR = Draft202012Validator(LENIENT, format_checker=None)

#: Per field: values that mix the valid and the invalid, drawn from what a producer bug could plausibly emit.
POOL: dict[str, list[Any]] = {
    "v": [0, 0.0, "0", True, None],
    "audit_id": ["0192f3f0-0005-7abc-8def-0123456789ab", "0192f3f0-0005-4abc-8def-0123456789ab", "x", 1, None, []],
    "ts": ["2026-10-06T09:34:10.000Z", "2026-10-06T09:34:10Z", "2026-10-06T09:34:10.000+00:00", "", 5, None, {}],
    "environment": ["prod", "PROD", "", 5, None],
    "action": [
        "message.content.view",
        "message.reprocess",
        "channel.replay",
        "channel.version.create",
        "deployment.deploy",
        "deployment.rollback",
        "deployment.undeploy",
        "other",
        "",
        5,
        None,
        [],
        {},
    ],
    "phase": ["requested", "completed", "failed", "done", 5, None, []],
    "actor": ["synth-operator-0002", "idp|1234", "has space", "", "a" * 256, 5, None, []],
    "actor_roles": [["operator"], ["admin", "viewer"], [], ["operator", "operator"], ["root"], "operator", None, [[]]],
    "request_id": ["0192f3f0-0000-7abc-8def-0123456789ab", "x", 5, None],
    "operation_id": ["0192f3d2-5555-7abc-8def-0123456789ab", "0192f3d2-5555-4abc-8def-0123456789ab", "x", None],
    "channel": ["adt-fanout", "Bad", "", 5, None],
    "message_id": ["0192f3b0-1a2b-7c3d-8e4f-0123456789ab", "x", 5, None],
    "stage": ["received", "routed", "dlq", "transformed", 5, None, []],
    "destination": ["fhir-store", "Bad", 5, None],
    "deployment_id": ["0192f3b0-1a2b-7c3d-8e4f-0123456789ab", "x", None],
    "version": [1, 7, 0, -1, 2**31, 1.0, 1.5, "1", True, None],
    "previous_version": [1, 0, True, None],
    "allow_reorder": [True, False, 1, "true", None],
    "target_topic": ["prod.adt-fanout.raw", "prod.adt-fanout.out.x", "x", None],
    "range_start": ["2026-10-06T06:00:00.000Z", "2026-10-06T06:00:00Z", None],
    "range_end": ["2026-10-06T07:00:00.000Z", "x", None],
    "count": [0, 5, -1, 1.0, 2.5, True, "5", None],
    "error_code": ["NO_PAYLOAD_COORDINATES", "lower", "", 5, None],
}


@settings(max_examples=600, suppress_health_check=[HealthCheck.too_slow])
@given(
    example=st.sampled_from(VALID),
    picks=st.dictionaries(st.sampled_from(sorted(POOL)), st.integers(0, 20), max_size=4),
    drop=st.sets(st.sampled_from(sorted(POOL)), max_size=3),
    unknown=st.booleans(),
)
def test_the_hand_validator_agrees_with_the_contracts_schema(
    example: str, picks: dict[str, int], drop: set[str], unknown: bool
) -> None:
    record = {k: v for k, v in load(example).items() if k not in drop}
    for field, pick in picks.items():
        record[field] = POOL[field][pick % len(POOL[field])]
    if unknown:
        record["future_field"] = {"nested": ["SYNTH"]}
    schema_ok = VALIDATOR.is_valid(record)
    try:
        parse(orjson_safe(record))
        mine_ok = True
    except AuditRejected:
        mine_ok = False
    assert mine_ok == schema_ok, "the hand validator and audit-event.schema.json disagree"


def test_a_missing_required_or_conditional_field_is_a_rejection_by_code() -> None:
    record = load("valid/content-view-requested.json")
    for name in ("audit_id", "ts", "actor", "message_id", "stage"):
        with pytest.raises(AuditRejected) as raised:
            parse({k: v for k, v in record.items() if k != name})
        assert raised.value.code == "AUDIT_FIELD_MISSING"


# ---- the loop around the parser ------------------------------------------------------------------------------


def lines(stream: io.StringIO, event_name: str) -> list[dict[str, Any]]:
    return [json.loads(x) for x in stream.getvalue().splitlines() if f'"{event_name}"' in x]


def build_logged(consumer: Any, sink: Any, audit: Any, stream: io.StringIO, parser: Any = None) -> Archiver:
    archiver = build(consumer, sink, audit)
    archiver._log = JsonLogger("archiver-test", stream=stream)  # type: ignore[attr-defined]
    if parser is not None:
        archiver._parse = parser  # type: ignore[attr-defined]
    return archiver


def test_unknown_fields_are_logged_by_count_and_offset_never_by_name_or_value() -> None:
    record = {**load("valid/content-view-requested.json"), "payload": "SYNTH-PAYLOAD-VALUE"}
    consumer = FakeConsumer([[Topic("_tb.prod.audit", 4, orjson.dumps(record))]])
    stream = io.StringIO()
    archiver = build_logged(consumer, FakeSink(), audit := FakeSink(), stream)
    archiver.run_once()
    (entry,) = lines(stream, "audit_unknown_fields_ignored")
    assert (entry["count"], entry["offset"], entry["topic"]) == (1, 4, "_tb.prod.audit")
    assert "payload" not in json.dumps(entry) and "SYNTH-PAYLOAD-VALUE" not in stream.getvalue()
    assert len(audit.inserted) == 1 and "SYNTH-PAYLOAD-VALUE" not in repr(audit.inserted)


def test_a_version_above_zero_is_counted_apart_logged_by_offset_and_committed_past() -> None:
    v1 = {**load("valid/content-view-requested.json"), "v": 1}
    consumer = FakeConsumer([[Topic("_tb.prod.audit", 0, orjson.dumps(v1)), audit_msg(1)]])
    stream = io.StringIO()
    archiver = build_logged(consumer, FakeSink(), audit := FakeSink(), stream)
    archiver.run_once()
    assert archiver.metrics.audit_unsupported_version == 1 and archiver.metrics.audit_invalid == 0
    assert archiver.metrics.events_rejected == 0 and len(audit.inserted) == 1
    (entry,) = lines(stream, "event_rejected")
    assert (entry["code"], entry["offset"]) == ("AUDIT_VERSION_UNSUPPORTED", 0)
    assert consumer.commits[-1] == [(0, 2)], "committed past the record"


def test_a_parser_bug_is_one_skipped_record_not_a_dead_loop() -> None:
    def broken(message: Any) -> Any:
        if message.offset() == 0:
            raise TypeError("unhashable type: 'list'")  # what the H1 bug looked like
        return parse_audit_record(message.value(), env="prod", partition=0, offset=message.offset())

    consumer = FakeConsumer([[audit_msg(0), audit_msg(1)]])
    stream = io.StringIO()
    archiver = build_logged(consumer, FakeSink(), audit := FakeSink(), stream, parser=broken)
    archiver.run_once()
    assert len(audit.inserted) == 1 and consumer.commits[-1] == [(0, 2)]
    (entry,) = lines(stream, "event_rejected")
    assert entry["code"] == "RECORD_UNPARSEABLE" and entry["offset"] == 0 and "unhashable" not in stream.getvalue()


def test_the_topic_routing_parser_handles_a_mixed_batch_and_turns_any_audit_crash_into_a_rejection() -> None:
    parse_record = build_record_parser("prod")
    event_row = parse_record(Topic("_tb.prod.events", 0, orjson.dumps(base())))
    audit_row = parse_record(audit_msg(1))
    assert getattr(event_row, "table", "events") != "audit_log" and audit_row.table == "audit_log"  # type: ignore[attr-defined]
    with pytest.raises(AuditRejected):
        parse_record(Topic("_tb.prod.audit", 2, b"{broken"))
    with pytest.raises(EventRejected) as event_error:
        parse_record(Topic("_tb.prod.events", 3, b"{broken"))
    assert not isinstance(event_error.value, AuditRejected), "an event rejection stays an event rejection"
    # An audit record on the events topic is just a (bad) event; an event on the audit topic is a bad audit record.
    with pytest.raises(AuditRejected):
        parse_record(Topic("_tb.prod.audit", 4, orjson.dumps(base())))


def test_the_audit_flag_restricts_the_subscription_to_events() -> None:
    from tributary.archiver.config import load_config

    class Resolver:
        def resolve_text(self, ref: str, key: str) -> str:
            return "x"

    environ = {"TB_ARCHIVE_CLICKHOUSE_HOST": "ch.internal", "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "ch-creds"}
    assert subscription_topics(load_config("prod", environ, Resolver())) == ["_tb.prod.events", "_tb.prod.audit"]  # type: ignore[arg-type]
    off = load_config("prod", {**environ, "TB_ARCHIVE_AUDIT_CONSUME": "false"}, Resolver())  # type: ignore[arg-type]
    assert subscription_topics(off) == ["_tb.prod.events"]


def test_an_unknown_or_unauthorised_audit_topic_logs_once_a_minute_not_at_every_refresh(monkeypatch: Any) -> None:
    import tributary.archiver.archiver as module

    clock = [1000.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    messages = [FakeMessage(i, None, error=_NamedError(-190, "UNKNOWN_TOPIC_OR_PART")) for i in range(5)]
    consumer = FakeConsumer([messages])
    stream = io.StringIO()
    archiver = build_logged(consumer, FakeSink(), FakeSink(), stream)
    archiver.run_once()
    assert len(lines(stream, "kafka_error")) == 1, "five repeats inside a minute: one line"
    clock[0] += 61.0
    consumer.batches.append([FakeMessage(9, None, error=_NamedError(-190, "UNKNOWN_TOPIC_OR_PART"))])
    archiver.run_once()
    again = lines(stream, "kafka_error")
    assert len(again) == 2 and again[1]["suppressed"] == 4
    consumer.batches.append([FakeMessage(10, None, error=_NamedError(29, "TOPIC_AUTHORIZATION_FAILED"))])
    archiver.run_once()
    assert len(lines(stream, "kafka_error")) == 3, "each code has its own interval"


class _NamedError(FakeKafkaError):
    def __init__(self, code: int, name: str) -> None:
        super().__init__(code)
        self._name = name

    def name(self) -> str:
        return self._name


def test_a_permanent_audit_failure_names_its_table_and_halts_events_too() -> None:
    consumer = FakeConsumer([[Topic("_tb.prod.events", 0, orjson.dumps(base())), audit_msg(1)]])
    stream = io.StringIO()
    archiver = build_logged(consumer, sink := FakeSink(), FakeSink(fail_times=3, transient=False), stream)
    archiver.run_once()
    (halted,) = lines(stream, "insert_halted")
    assert halted["table"] == "audit_log"
    assert len(sink.inserted) == 1 and len(consumer.commits) == 1, "committed once, after the audit insert recovered"
    assert sorted(consumer.commits[0]) == [(0, 1), (0, 2)], "both topics' offsets in the one commit"


def test_a_permanent_events_failure_names_the_events_table() -> None:
    consumer = FakeConsumer([[Topic("_tb.prod.events", 0, orjson.dumps(base())), audit_msg(1)]])
    stream = io.StringIO()
    archiver = build_logged(consumer, FakeSink(fail_times=2, transient=False), audit := FakeSink(), stream)
    archiver.run_once()
    assert lines(stream, "insert_halted")[0]["table"] == "events" and len(audit.inserted) == 1


def test_every_audit_rejection_through_the_real_loop_is_invalid_and_each_outcome_is_scraped(monkeypatch: Any) -> None:
    """Exposition end to end: written, invalid (not JSON, bad field, other environment, parser crash) and
    unsupported_version all reach `tb_archiver_audit_records_total`; none is counted as an event rejection."""
    import tributary.archiver.main as main_module
    from tributary.archiver.exposition import ArchiverExposition

    real = main_module.parse_audit_record

    def flaky(value: Any, **kw: Any) -> Any:
        if kw["offset"] == 3:
            raise TypeError("boom")  # a validator bug on an audit record
        return real(value, **kw)

    monkeypatch.setattr(main_module, "parse_audit_record", flaky)

    v1 = {**load("valid/content-view-requested.json"), "v": 1}
    batch = [
        audit_msg(0),
        Topic("_tb.prod.audit", 1, b"{broken"),
        Topic("_tb.prod.audit", 2, orjson.dumps({**load("valid/content-view-requested.json"), "environment": "dev"})),
        audit_msg(3),
        Topic("_tb.prod.audit", 4, orjson.dumps(v1)),
        Topic("_tb.prod.audit", 5, orjson.dumps({**load("valid/content-view-requested.json"), "action": ["x"]})),
    ]
    archiver = build(FakeConsumer([batch]), FakeSink(), FakeSink())
    archiver._parse = build_record_parser("prod")  # type: ignore[attr-defined]
    exposition = ArchiverExposition(archiver.metrics, strict=True)
    archiver.run_once()
    text = exposition.render().decode()
    assert 'tb_archiver_audit_records_total{outcome="written"} 1' in text
    assert 'tb_archiver_audit_records_total{outcome="invalid"} 4' in text
    assert 'tb_archiver_audit_records_total{outcome="unsupported_version"} 1' in text
    assert (
        'tb_archiver_events_total{outcome="invalid"} 0' in text
        and 'tb_archiver_events_total{outcome="written"} 0' in text
    )
