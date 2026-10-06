"""W2-2 condition 4: validate W2-3 golden outputs against HL7's official FHIR R4 (4.0.1) JSON Schema.

The schema is vendored test data (CC0, see `fixtures/fhir-r4/PROVENANCE.md`). Offline; the compiled
validator is cached per session. Failure messages carry JSON paths only, never field values (invariant 6).
"""

from __future__ import annotations

import hashlib
import json
import re
from functools import cache, lru_cache
from pathlib import Path
from typing import Any

import pytest
from jsonschema import FormatChecker
from jsonschema.protocols import Validator
from jsonschema.validators import validator_for

from .conftest import FHIR_FIXTURES

SCHEMA_DIR = Path(__file__).parent / "fixtures" / "fhir-r4"
SCHEMA_PATH = SCHEMA_DIR / "fhir.schema.json"
PROVENANCE_PATH = SCHEMA_DIR / "PROVENANCE.md"
GOLDEN_BUNDLE_FILES = sorted(FHIR_FIXTURES.glob("*.bundle.json"))


@lru_cache(maxsize=1)
def _schema() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return loaded


@cache
def _validator(resource_type: str) -> Validator:
    """Validator pinned to `#/definitions/<resourceType>`. The schema's root `oneOf` would collapse every
    failure into an opaque `/: oneOf`, so a wrong type or any other error could not be told apart."""
    schema = _schema()
    assert resource_type in schema["definitions"], "unknown resourceType (not an R4 resource)"
    root = {
        "$schema": schema["$schema"],
        "$ref": f"#/definitions/{resource_type}",
        "definitions": schema["definitions"],
    }
    cls = validator_for(root)  # honours the declared "$schema" (draft-06)
    cls.check_schema(root)
    return cls(root, format_checker=FormatChecker())


def _errors(instance: dict[str, Any]) -> list[tuple[str, str]]:
    """(JSON path, keyword) pairs only, never instance values (invariant 6)."""
    validator = _validator(str(instance["resourceType"]))
    found = [("/" + "/".join(map(str, e.absolute_path)), str(e.validator)) for e in validator.iter_errors(instance)]
    return sorted(found)


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def test_vendored_schema_is_r4_4_0_1_and_draft06() -> None:
    schema = _schema()
    assert schema["$schema"] == "http://json-schema.org/draft-06/schema#"
    # The schema's id carries only the minor version; the patch level is not in `id` or `description`.
    assert schema["id"] == "http://hl7.org/fhir/json-schema/4.0"
    # CapabilityStatement.fhirVersion enumerates released versions: R4 ends at 4.0.1 (R4B would add 4.3.0).
    versions = schema["definitions"]["CapabilityStatement"]["properties"]["fhirVersion"]["enum"]
    assert versions[-1] == "4.0.1"


def test_vendored_schema_sha256_matches_provenance() -> None:
    provenance = PROVENANCE_PATH.read_text(encoding="utf-8")
    match = re.search(r"SHA-256 of `fhir\.schema\.json`: `([0-9a-f]{64})`", provenance)
    assert match, "PROVENANCE.md has no SHA-256 line for fhir.schema.json"
    assert hashlib.sha256(SCHEMA_PATH.read_bytes()).hexdigest() == match.group(1)


def test_at_least_one_golden_exists() -> None:
    assert GOLDEN_BUNDLE_FILES


@pytest.mark.parametrize("golden_path", GOLDEN_BUNDLE_FILES, ids=lambda p: p.name)
def test_golden_bundle_and_each_resource_validate_against_r4_schema(golden_path: Path) -> None:
    bundle = _load(golden_path)
    assert _errors(bundle) == []
    for index, entry in enumerate(bundle.get("entry", [])):
        resource = entry["resource"]
        assert _errors(resource) == [], f"entry[{index}] ({resource['resourceType']})"


def test_control_valid_observation_passes() -> None:
    assert _errors({"resourceType": "Observation", "status": "final", "code": {"text": "x"}}) == []


def test_negative_control_r4b_only_element_is_rejected_as_additional_property() -> None:
    """`instantiatesCanonical` was added in R4B and is absent from R4."""
    bad = {
        "resourceType": "Observation",
        "status": "final",
        "code": {"text": "x"},
        "instantiatesCanonical": "http://example.org/x",
    }
    assert _errors(bad) == [("/", "additionalProperties")]


def test_negative_control_invalid_status_code_is_rejected_as_enum() -> None:
    bad = {"resourceType": "Observation", "status": "not-a-real-status", "code": {"text": "x"}}
    assert _errors(bad) == [("/status", "enum")]


def test_mutation_control_patient_gender_banana_fails_enum() -> None:
    assert _errors({"resourceType": "Patient", "gender": "male"}) == []
    assert _errors({"resourceType": "Patient", "gender": "banana"}) == [("/gender", "enum")]


def test_unknown_resource_type_cannot_pass_silently() -> None:
    with pytest.raises(AssertionError):
        _errors({"resourceType": "NotAResource"})
