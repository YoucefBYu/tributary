"""W2-2 condition 4: validate W2-3 golden outputs against HL7's official FHIR R4 (4.0.1) JSON Schema.

The schema is vendored test data (CC0, see `fixtures/fhir-r4/PROVENANCE.md`). Offline; the compiled
validator is cached per session. Failure messages carry JSON paths only, never field values (invariant 6).
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from functools import lru_cache
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


@lru_cache(maxsize=1)
def _validator() -> Validator:
    schema = _schema()
    cls = validator_for(schema)  # honours the declared "$schema" (draft-06)
    cls.check_schema(schema)
    return cls(schema, format_checker=FormatChecker())


def _errors(instance: object) -> list[str]:
    """Error descriptions with JSON path and validator keyword only, no instance values."""
    found = sorted(_validator().iter_errors(instance), key=lambda e: list(map(str, e.absolute_path)))
    return [f"/{'/'.join(map(str, e.absolute_path))}: {e.validator}" for e in found]


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def test_vendored_schema_is_r4_4_0_1_and_draft06() -> None:
    schema = _schema()
    assert schema["$schema"] == "http://json-schema.org/draft-06/schema#"
    assert schema["id"] == "http://hl7.org/fhir/json-schema/4.0"
    assert "4.0.1" in schema["description"] or "4.0.1" in SCHEMA_PATH.read_text(encoding="utf-8")


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


def test_negative_control_r4b_only_element_is_rejected() -> None:
    """A valid Observation passes; adding `instantiatesCanonical` (an R4B addition, absent from R4) or an
    invalid status code must fail. The schema sets additionalProperties=false on every resource."""
    good = {"resourceType": "Observation", "status": "final", "code": {"text": "x"}}
    assert _errors(good) == []
    bad = copy.deepcopy(good)
    bad["instantiatesCanonical"] = "http://example.org/x"  # not an R4 element
    errors = _errors(bad)
    assert errors, "R4-invalid resource unexpectedly passed the R4 schema"
    bad2 = {"resourceType": "Observation", "status": "not-a-real-status", "code": {"text": "x"}}
    assert _errors(bad2), "invalid status code passed"
