"""`parse_event` against the contract's own examples, and the PHI rules the archive adds."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import orjson
import pytest
from tributary.archiver.errors import EventRejected
from tributary.archiver.event import COLUMNS, IDENTIFIER_NAMES, clean_attrs, parse_event, parse_ts
from tributary.archiver.testing import sample_event

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "events" / "v0"
VALID = sorted((CONTRACT / "examples" / "valid").glob("*.json"))
MSG_ID = str(uuid.UUID(int=7))


base = sample_event


@pytest.mark.parametrize("path", VALID, ids=lambda p: p.stem)
def test_every_contract_example_parses_to_one_full_row(path: Path) -> None:
    row = parse_event(path.read_bytes())
    assert len(row.values) == len(COLUMNS)


def test_schema_denylist_matches_the_archiver_copy() -> None:
    schema = json.loads((CONTRACT / "event.schema.json").read_text())
    listed = schema["properties"]["attrs"]["propertyNames"]["allOf"][1]["not"]["enum"]
    assert set(listed) == IDENTIFIER_NAMES


def test_hmac_and_identifier_attrs_are_dropped_not_stored() -> None:
    attrs, dropped = clean_attrs({"mrn_hmac": "a" * 64, "mrn": "X1", "trigger": "A01", "step": "to_fhir"})
    assert attrs == {"trigger": "A01", "step": "to_fhir"}
    assert dropped == 2


def test_free_text_attr_value_is_dropped() -> None:
    attrs, dropped = clean_attrs({"reason": "has space", "ok": "A01"})
    assert attrs == {"ok": "A01"} and dropped == 1


def test_attrs_never_gain_an_entry() -> None:
    row = parse_event(orjson.dumps(base()))
    assert row.values[COLUMNS.index("attrs")] == {}


def test_message_event_has_empty_scope_so_a_re_emit_collapses() -> None:
    first = parse_event(orjson.dumps(base()))
    again = parse_event(orjson.dumps(base(ts="2026-01-01T00:00:09.000Z")))
    scope = COLUMNS.index("scope")
    assert first.values[scope] == again.values[scope] == ""


def test_partition_events_get_distinct_scopes() -> None:
    def ev(ts: str) -> bytes:
        return orjson.dumps(base(kind="partition", stage="partition_paused", topic="t", partition=0, ts=ts))
        # message_id is optional here

    a = parse_event(ev("2026-01-01T00:00:00.001Z")).values[COLUMNS.index("scope")]
    b = parse_event(ev("2026-01-01T00:00:00.002Z")).values[COLUMNS.index("scope")]
    assert a != b


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (b"not json", "EVENT_NOT_JSON"),
        (b"[1]", "EVENT_NOT_OBJECT"),
        (orjson.dumps(base(v=1)), "EVENT_VERSION_UNSUPPORTED"),
        (orjson.dumps(base(kind="other")), "EVENT_KIND_UNKNOWN"),
        (orjson.dumps(base(ts="yesterday")), "EVENT_TS_INVALID"),
        (orjson.dumps({k: v for k, v in base().items() if k != "message_id"}), "EVENT_FIELD_MISSING"),
        (orjson.dumps(base(message_id="nope")), "EVENT_FIELD_TYPE"),
        (orjson.dumps(base(attempt=-1)), "EVENT_FIELD_TYPE"),
    ],
)
def test_unstorable_records_are_rejected_with_a_code_only(raw: bytes, code: str) -> None:
    with pytest.raises(EventRejected) as caught:
        parse_event(raw)
    assert caught.value.code == code
    assert str(caught.value) == code


def test_rejection_never_echoes_the_value() -> None:
    marker = "SYNTH-MARKER-9981"
    with pytest.raises(EventRejected) as caught:
        parse_event(orjson.dumps(base(message_id=marker)))
    assert marker not in str(caught.value) and marker not in repr(caught.value)


def test_nanosecond_timestamps_truncate_to_the_column_precision() -> None:
    assert parse_ts("2026-01-01T00:00:00.123456789Z").microsecond == 123456


@pytest.mark.parametrize(
    ("over", "code"),
    [
        ({"channel": "Bad Channel"}, "EVENT_CHANNEL_INVALID"),
        ({"stage": "mystery"}, "EVENT_STAGE_UNKNOWN"),
        ({"status": "maybe"}, "EVENT_STATUS_UNKNOWN"),
        ({"destination": "Bad Dest"}, "EVENT_DESTINATION_INVALID"),
    ],
)
def test_identity_fields_failing_the_schema_are_rejected(over: dict[str, Any], code: str) -> None:
    with pytest.raises(EventRejected) as caught:
        parse_event(orjson.dumps(base(**over)))
    assert caught.value.code == code


def test_non_key_fields_failing_the_schema_are_blanked_and_counted() -> None:
    row = parse_event(
        orjson.dumps(
            base(
                error_code="lower",
                traceparent="not-a-traceparent",
                claim_check="https://user:pw@host/x?sig=abc",
            )
        )
    )
    for name in ("error_code", "traceparent", "claim_check"):
        assert row.values[COLUMNS.index(name)] == ""
    assert row.fields_blanked == 3


def test_valid_claim_check_is_kept() -> None:
    row = parse_event(orjson.dumps(base(claim_check="s3://bucket/a/b.bin", size_bytes=0)))
    assert row.values[COLUMNS.index("claim_check")] == "s3://bucket/a/b.bin" and row.fields_blanked == 0
