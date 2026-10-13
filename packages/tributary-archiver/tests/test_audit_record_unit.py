"""Audit records into `audit_log` rows: the contract's examples, the schema's conditionals, the loop's routing."""

from __future__ import annotations

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
from tributary.archiver.archiver import Archiver, ArchiverSettings, parse_event_record
from tributary.archiver.audit_record import COLUMNS, MAX_RECORD_BYTES, AuditRow, parse_audit_record
from tributary.archiver.errors import EventRejected, SinkError
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
INVALID = [e["file"] for e in MANIFEST["invalid"]]


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


def test_a_record_that_carries_content_is_rejected_not_stored_without_it() -> None:
    record = {**load("valid/content-view-requested.json"), "payload": "SYNTH-PAYLOAD-BYTES"}
    with pytest.raises(EventRejected) as raised:
        parse(record)
    assert raised.value.code == "AUDIT_UNKNOWN_FIELD"


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ({"v": 1}, "AUDIT_VERSION_UNSUPPORTED"),
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
        ({"message_id": None}, "AUDIT_FIELD_MISSING"),
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
    assert archiver.metrics.events_rejected == 1
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
