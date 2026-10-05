"""`JsonLogger` output against `contracts/observability/v0/log-line.schema.json` (README section 4).

Two directions are tested, because either alone passes vacuously:

* what the logger writes validates (every well-known field, every level, bound fields, the deprecated
  message-id aliases, and arbitrary guard-accepted fields by property);
* what the schema refuses, the guard refuses before writing (`payload`, every L3 and L4 name, nested
  values, CR/LF). Each refusal is paired with a control showing the schema itself rejects the line the
  guard stopped, so a schema that accepted everything could not make this file pass.

All values are synthetic. `message_id` is canonical (L7); `tb_msg_id` and `msg_id` are deprecated
aliases kept valid in v0.
"""

from __future__ import annotations

import io
import json
import string
from typing import Any

import orjson
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.runtime.logs import MAX_FIELDS, RESERVED_FIELDS, JsonLogger
from tributary.sdk import guards
from tributary.sdk.errors import LogFieldError
from tributary.sdk.ids import new_traceparent, uuid7

from .log_line_schema import SCHEMA_PATH, assert_lines_conform, validator, violations

SYNTH_ID = "0192f3a4-5b6c-7d8e-9f01-23456789abcd"
LEVELS = ("debug", "info", "warning", "error")


def logger(stream: io.StringIO, **base: object) -> JsonLogger:
    return JsonLogger("worker", stream=stream, level="debug", base=base or None, strict=True)


def one_line(stream: io.StringIO) -> dict[str, Any]:
    [line] = assert_lines_conform(stream.getvalue())
    return dict(line)


def schema_well_known() -> set[str]:
    return set(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))["properties"])


# -- what the logger writes validates ---------------------------------------------------------------


def test_every_well_known_field_validates_with_its_contract_shape() -> None:
    _version, trace_id, span_id, _flags = new_traceparent().split("-")
    stream = io.StringIO()
    logger(stream, channel="adt-passthrough", channel_version=3, instance="worker-0").error(
        "dead_lettered",
        message_id=str(uuid7()),
        parent_id=str(uuid7()),
        correlation_id=str(uuid7()),
        destination="fhir-server",
        error_code="TB_STEP_FAILED",
        error="ValueError",
        ready=False,
        trace_id=trace_id,
        span_id=span_id,
        attempt=2,
        duration_ms=12.5,
        partition=0,
        offset=41,
        paused=None,
    )
    line = one_line(stream)
    covered = set(line) - {"attempt", "duration_ms", "partition", "offset", "paused"}
    assert covered == schema_well_known() - {"tb_msg_id", "msg_id"}, "a well-known field went untested"


@pytest.mark.parametrize("level", LEVELS)
def test_every_level_validates(level: str) -> None:
    stream = io.StringIO()
    getattr(logger(stream), level)("spec_applied", channel="adt-passthrough", channel_version=1)
    assert one_line(stream)["level"] == level


def test_bound_fields_validate_and_a_call_field_wins_over_a_bound_one() -> None:
    stream = io.StringIO()
    logger(stream, channel="adt-passthrough").bind(destination="fhir-server", partition=2).info(
        "paused", destination="fhir-backup", attempt=3
    )
    line = one_line(stream)
    assert (line["channel"], line["destination"], line["partition"]) == ("adt-passthrough", "fhir-backup", 2)


@pytest.mark.parametrize("alias", ["tb_msg_id", "msg_id"])
def test_deprecated_message_id_aliases_still_validate_with_the_uuid_shape(alias: str) -> None:
    stream = io.StringIO()
    logger(stream).info("acked", **{alias: SYNTH_ID})
    assert one_line(stream)[alias] == SYNTH_ID
    # Same shape rule as `message_id`: an alias is not a way around it.
    record = {"ts": "2026-10-05T00:00:00.000Z", "level": "info", "event": "acked", "component": "worker"}
    assert violations({**record, alias: "0192"}) == [f"acked:{alias}:pattern"]
    assert violations({**record, "message_id": "0192"}) == ["acked:message_id:pattern"]


def test_the_timestamp_is_utc_milliseconds_with_z() -> None:
    stream = io.StringIO()
    logger(stream).info("started")
    ts = one_line(stream)["ts"]
    assert ts.endswith("Z") and len(ts) == len("2026-10-05T00:00:00.000Z")


def test_a_line_at_the_field_cap_validates() -> None:
    stream = io.StringIO()
    fields = {f"f{index}": index for index in range(MAX_FIELDS - len(RESERVED_FIELDS))}
    logger(stream).info("many", **fields)
    assert len(one_line(stream)) == MAX_FIELDS


FIELD_NAME = st.from_regex(r"\A[a-z][a-z0-9_]{0,31}\Z", fullmatch=True)
SCALAR = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(alphabet=st.characters(exclude_characters="\r\n", exclude_categories=["Cs"]), max_size=256),
)


@settings(max_examples=300, deadline=None)
@given(fields=st.dictionaries(FIELD_NAME, SCALAR, max_size=MAX_FIELDS - len(RESERVED_FIELDS)))
def test_any_line_the_guard_accepts_validates(fields: dict[str, object]) -> None:
    """Guard-accepted implies schema-valid, for every name that is not a well-known (shaped) field.

    Well-known fields carry their own shape (uuid, channel name, class name) that the guard does not
    check; the cases above cover them. Everything else is the schema's `additionalProperties`, which
    must agree with `guards.validate_log_field` exactly.
    """
    fields = {name: value for name, value in fields.items() if name not in schema_well_known() | RESERVED_FIELDS}
    stream = io.StringIO()
    try:
        logger(stream).info("property", **fields)
    except LogFieldError:
        refused = {name for name in fields if name in guards.IDENTIFIER_NAMES | guards.LOG_CONTENT_NAMES}
        assert refused, "the guard refused a line with no deny-listed name"
        return
    assert_lines_conform(stream.getvalue())


# -- what the schema refuses, the guard refuses first ------------------------------------------------

BASE_RECORD = {"ts": "2026-10-05T00:00:00.000Z", "level": "info", "event": "delivered", "component": "worker"}


def test_a_field_named_payload_is_refused_by_the_guard_and_by_the_schema() -> None:
    """The change request named in observability/v0 section 5."""
    stream = io.StringIO()
    with pytest.raises(LogFieldError):
        logger(stream).info("delivered", payload="SYNTH")
    assert stream.getvalue() == "", "nothing is written when the guard refuses"
    assert violations({**BASE_RECORD, "payload": "SYNTH"}), "control: the schema refuses the same line"


@pytest.mark.parametrize("name", sorted(guards.IDENTIFIER_NAMES | guards.LOG_CONTENT_NAMES))
def test_every_deny_listed_name_is_refused_by_both(name: str) -> None:
    with pytest.raises(LogFieldError):
        logger(io.StringIO()).info("delivered", **{name: "A01"})
    assert violations({**BASE_RECORD, name: "A01"}), f"control: the schema refuses {name}"


@pytest.mark.parametrize(
    "value",
    [{"nested": "SYNTH"}, ["SYNTH"], b"SYNTH", "forged\nline", "carriage\rreturn", "x" * 257],
)
def test_values_the_schema_refuses_are_refused_by_the_guard(value: object) -> None:
    with pytest.raises(LogFieldError):
        logger(io.StringIO()).info("delivered", note=value)
    if not isinstance(value, bytes):  # bytes have no JSON form; the guard is the only line of defence
        assert violations({**BASE_RECORD, "note": value}), "control: the schema refuses the same value"


@pytest.mark.parametrize("name", sorted(RESERVED_FIELDS))
def test_a_caller_cannot_override_an_envelope_field(name: str) -> None:
    """`info("x", event="anything at all")` would otherwise replace the validated event name."""
    stream = io.StringIO()
    with pytest.raises(LogFieldError):
        logger(stream).info("delivered", **{name: "free text the event guard never saw"})
    with pytest.raises(LogFieldError):
        logger(stream).bind(**{name: "x"})
    with pytest.raises(LogFieldError):
        JsonLogger("worker", stream=stream, base={name: "x"}, strict=True)
    assert stream.getvalue() == ""


def test_a_line_over_the_field_cap_is_refused() -> None:
    stream = io.StringIO()
    bound = logger(stream, channel="adt-passthrough")
    fields = {f"f{index}": index for index in range(MAX_FIELDS - len(RESERVED_FIELDS))}
    with pytest.raises(LogFieldError):
        bound.info("many", **fields)  # 4 reserved + channel + 28 = 33
    assert stream.getvalue() == ""
    over = {**BASE_RECORD, "channel": "adt-passthrough", **fields}
    assert "delivered:-:maxProperties" in violations(over), "control: the schema refuses 33 fields"


def test_the_schema_is_well_formed_and_the_helper_is_not_vacuous() -> None:
    assert validator().is_valid(BASE_RECORD)
    assert not validator().is_valid({k: v for k, v in BASE_RECORD.items() if k != "event"})
    with pytest.raises(AssertionError):
        assert_lines_conform(orjson.dumps({**BASE_RECORD, "error": "boom: " + string.ascii_letters}).decode())
