"""Typed exceptions: no PHI, and the base classes a caller expects to catch."""

from __future__ import annotations

from tributary.fhir.errors import (
    BundleEntryError,
    FhirError,
    TerminologyConfigError,
    TerminologyError,
    TerminologyUnavailableError,
)


def test_bundle_entry_error_is_a_fhir_error() -> None:
    assert issubclass(BundleEntryError, FhirError)


def test_terminology_errors_are_terminology_errors() -> None:
    assert issubclass(TerminologyConfigError, TerminologyError)
    assert issubclass(TerminologyUnavailableError, TerminologyError)
    assert issubclass(TerminologyError, FhirError)


def test_bundle_entry_error_carries_resource_type_not_field_values() -> None:
    error = BundleEntryError("Patient", "no identifier given and the resource has none of its own")
    assert error.resource_type == "Patient"
    assert "Patient" in str(error)


def test_terminology_unavailable_error_names_the_system() -> None:
    error = TerminologyUnavailableError("http://loinc.org")
    assert error.system == "http://loinc.org"
    assert "http://loinc.org" in str(error)
