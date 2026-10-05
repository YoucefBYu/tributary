"""The invariant-6 guards, and a drift test against the contract they implement.

The deny-list lives in two places — `contracts/events/v0/event.schema.json` and `guards.py` — because
the schema cannot run on the hot path and the SDK cannot depend on a JSON Schema validator. Two copies
of one rule drift, so the drift is what this file tests.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.sdk import guards
from tributary.sdk.errors import AttrError, LogFieldError, UsrHeaderError

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"


def test_contracts_directory_is_found() -> None:
    """If this fails the drift test below would silently pass on an empty file set."""
    assert (CONTRACTS / "events" / "v0" / "event.schema.json").is_file()


def test_identifier_deny_list_matches_the_events_contract() -> None:
    schema = json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    contract_names = set(schema["properties"]["attrs"]["propertyNames"]["allOf"][1]["not"]["enum"])
    assert set(guards.IDENTIFIER_NAMES) == contract_names


def test_attr_name_and_value_charsets_match_the_events_contract() -> None:
    schema = json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    attrs = schema["properties"]["attrs"]
    assert guards.ATTR_NAME.pattern == attrs["propertyNames"]["allOf"][0]["pattern"]
    assert guards.ATTR_VALUE.pattern == attrs["additionalProperties"]["pattern"]
    assert attrs["maxProperties"] == guards.MAX_ATTRS


@pytest.mark.parametrize("name", ["mrn", "patient_id", "dob", "ssn", "key", "partition_key", "message_key"])
def test_identifier_names_are_refused_in_clear_form(name: str) -> None:
    with pytest.raises(AttrError):
        guards.validate_attr(name, "A01")


def test_hyphenated_spelling_of_an_identifier_name_is_also_refused() -> None:
    """`tb-usr-patient-id` and `patient_id` are the same mistake with different punctuation."""
    with pytest.raises(UsrHeaderError):
        guards.validate_usr("patient-id", "12345")


def test_hmac_attrs_are_refused_in_v0() -> None:
    """Events contract 3.1: the mechanism is unresolved (ADR-004), so the shape is not enough."""
    with pytest.raises(AttrError) as caught:
        guards.validate_attr("mrn_hmac", "9f" * 32)
    assert "ADR-004" in str(caught.value)


@pytest.mark.parametrize(
    "value",
    [
        "SYNTH^JANE",  # HL7 delimiter
        "admitted for observation",  # spaces: free text is where PHI arrives
        "line\nbreak",
        "x" * 65,
        "",
    ],
)
def test_attr_values_must_be_short_codes(value: str) -> None:
    with pytest.raises(AttrError):
        guards.validate_attr("trigger", value)


def test_attr_error_never_quotes_the_value() -> None:
    with pytest.raises(AttrError) as caught:
        guards.validate_attr("reason", "SMITH^JOHN^19700101")
    assert "SMITH" not in str(caught.value)


def test_valid_attrs_pass() -> None:
    guards.validate_attrs({"trigger": "A01", "step": "to_fhir_bundle", "dest": "fhir-server"})


def test_attrs_are_capped_by_count_and_serialized_size() -> None:
    with pytest.raises(AttrError):
        guards.validate_attrs({f"code{index}": "v" for index in range(guards.MAX_ATTRS + 1)})
    with pytest.raises(AttrError):
        guards.validate_attrs({f"code{index}": "v" * 60 for index in range(16)})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("mrn", "123"),  # deny-listed name
        ("note", b"bytes"),  # the payload and the key are the two bytes values in the system
        ("note", "x" * 257),
        ("note", "forged\nlog line"),
        ("Note", "ok"),  # name charset
        ("note", object()),
    ],
)
def test_log_fields_are_guarded(name: str, value: object) -> None:
    with pytest.raises(LogFieldError):
        guards.validate_log_field(name, value)


def test_ordinary_log_fields_pass() -> None:
    guards.validate_log_fields(
        {"message_id": "0192-uuid", "attempt": 2, "duration_ms": 12.5, "paused": True, "destination": None}
    )


@pytest.mark.parametrize("value", ["", "x" * 257, "with\nnewline", "\x00null"])
def test_usr_header_values_are_bounded(value: str) -> None:
    with pytest.raises(UsrHeaderError):
        guards.validate_usr("batch-id", value)


# -- observability/v0 section 4: the log-line deny-lists (L3, L4) --------------------------------------

LOG_LINE_SCHEMA = CONTRACTS / "observability" / "v0" / "log-line.schema.json"


def _log_line_deny_lists() -> tuple[set[str], set[str]]:
    assert LOG_LINE_SCHEMA.is_file(), "log-line schema not found: the drift tests would pass vacuously"
    schema = json.loads(LOG_LINE_SCHEMA.read_text(encoding="utf-8"))
    identifier, content = schema["propertyNames"]["allOf"][1:3]
    return set(identifier["not"]["enum"]), set(content["not"]["enum"])


def test_log_identifier_deny_list_matches_the_log_line_contract() -> None:
    """L3: byte-for-byte the events/v0 attrs deny-list and `IDENTIFIER_NAMES`."""
    identifier, _ = _log_line_deny_lists()
    assert identifier == set(guards.IDENTIFIER_NAMES)


def test_log_content_deny_list_matches_the_log_line_contract() -> None:
    _, content = _log_line_deny_lists()
    assert set(guards.LOG_CONTENT_NAMES) == content
    assert not content & set(guards.IDENTIFIER_NAMES), "the two lists are disjoint in the contract"


@pytest.mark.parametrize("name", sorted(guards.LOG_CONTENT_NAMES))
def test_every_content_name_is_refused_as_a_log_field_whatever_the_value(name: str) -> None:
    # A harmless-looking value: the name is refused on its own, before the value is looked at.
    with pytest.raises(LogFieldError) as caught:
        guards.validate_log_field(name, "A01")
    assert "L4" in str(caught.value)


@pytest.mark.parametrize("name", ["payload", "message", "exception", "traceback"])
def test_content_names_stay_legal_as_attrs_and_usr_is_unaffected(name: str) -> None:
    """L4 is a log-line rule. The events/v0 attrs deny-list is the identifier list only; widening it here
    would refuse events the contract accepts."""
    guards.validate_attr(name, "A01")


@pytest.mark.parametrize("name", ["payload_bytes", "message_id", "body_size", "error", "reason"])
def test_names_that_only_contain_a_content_word_are_allowed(name: str) -> None:
    """The contract's list is exact names, not substrings: `message_id` is the canonical field (L7)."""
    guards.validate_log_field(name, "A01")
