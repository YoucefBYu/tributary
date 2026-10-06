"""`LocalTerminology`: table-backed lookups, and the "unloaded system" vs "code not found" distinction
architect review criterion 3 asks for."""

from __future__ import annotations

import pytest
from tributary.fhir.errors import TerminologyConfigError
from tributary.fhir.terminology import CodeLookup, LocalTerminology, load_table

LOINC = "http://loinc.org"
SNOMED = "http://snomed.info/sct"

HEART_RATE = CodeLookup(system=LOINC, code="8867-4", display="Heart rate")


def test_lookup_returns_the_entry() -> None:
    terminology = LocalTerminology(load_table([HEART_RATE]))
    assert terminology.lookup(LOINC, "8867-4") == HEART_RATE


def test_lookup_returns_none_for_a_code_not_in_a_loaded_system() -> None:
    terminology = LocalTerminology(load_table([HEART_RATE]))
    assert terminology.lookup(LOINC, "99999-9") is None


def test_lookup_raises_for_a_system_never_loaded() -> None:
    terminology = LocalTerminology(load_table([HEART_RATE]))
    with pytest.raises(TerminologyConfigError):
        terminology.lookup(SNOMED, "271649006")


def test_the_two_failure_modes_are_distinguishable() -> None:
    """The whole point of criterion 3: a caller must be able to tell "code absent" from "lookup
    failed", and here that is a `None` return versus a raised, typed exception -- never the same shape.
    """
    terminology = LocalTerminology(load_table([HEART_RATE]))
    absent = terminology.lookup(LOINC, "not-a-real-code")
    assert absent is None
    with pytest.raises(TerminologyConfigError):
        terminology.lookup("http://unknown.example/cs", "x")


def test_validate_is_true_only_for_a_present_code() -> None:
    terminology = LocalTerminology(load_table([HEART_RATE]))
    assert terminology.validate(LOINC, "8867-4") is True
    assert terminology.validate(LOINC, "99999-9") is False


def test_known_systems_without_any_codes_can_be_declared() -> None:
    terminology = LocalTerminology(known_systems=[SNOMED])
    assert terminology.lookup(SNOMED, "271649006") is None  # loaded, just empty -- no raise


def test_systems_reports_every_loaded_system() -> None:
    terminology = LocalTerminology(load_table([HEART_RATE]), known_systems=[SNOMED])
    assert terminology.systems() == {LOINC, SNOMED}


def test_error_message_never_contains_a_code_value() -> None:
    terminology = LocalTerminology()
    with pytest.raises(TerminologyConfigError) as caught:
        terminology.lookup("http://unknown.example/cs", "SENSITIVE-CODE-VALUE")
    assert "SENSITIVE-CODE-VALUE" not in str(caught.value)
