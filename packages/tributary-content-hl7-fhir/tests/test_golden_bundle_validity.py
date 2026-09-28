"""Structural and conditional-write checks on every golden Bundle via the R4B pydantic models.

Validation against HL7's official FHIR R4 JSON Schema lives in `test_golden_fhir_r4_schema.py`
(W2-2 condition 4, closed 2026-09-28). This file keeps the conditional-request invariants.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fhir.resources.R4B.bundle import Bundle

from .conftest import FHIR_FIXTURES

GOLDEN_BUNDLE_FILES = sorted(FHIR_FIXTURES.glob("*.bundle.json"))


@pytest.mark.parametrize("golden_path", GOLDEN_BUNDLE_FILES, ids=lambda p: p.name)
def test_golden_bundle_is_a_structurally_valid_r4b_transaction_bundle(golden_path: Path) -> None:
    bundle = Bundle.model_validate_json(golden_path.read_text())
    assert bundle.type == "transaction"
    assert bundle.entry
    for entry in bundle.entry:
        assert entry.request is not None
        assert entry.request.method in ("PUT", "POST", "DELETE")
        # Channel-spec README 3.4.2: every entry conditional on exactly one identifier -- never an
        # unconditional create. A `PUT`'s condition lives entirely in its URL's query (`ifNoneExist` is
        # meaningless there, and `LocalFhir.upsert` no longer sets it -- security review, LOW); a `POST`
        # or `DELETE`'s condition is `ifNoneExist`/the URL's query respectively.
        if entry.request.method == "PUT":
            assert "?identifier=" in (entry.request.url or ""), f"{golden_path.name}: unconditional PUT"
        elif entry.request.method == "POST":
            assert entry.request.ifNoneExist, f"{golden_path.name}: entry has no conditional ifNoneExist"
        else:
            assert "?identifier=" in (entry.request.url or ""), f"{golden_path.name}: unconditional DELETE"

        # Security review, MEDIUM 4: the resource body itself must carry the identifier its entry is
        # conditional on (Patient's own PID-3-derived identifier already qualifies for that entry).
        if entry.resource is not None:
            identifiers = getattr(entry.resource, "identifier", None) or []
            assert identifiers, f"{golden_path.name}: {type(entry.resource).__name__} has no .identifier"


def test_at_least_one_golden_file_exists() -> None:
    """A parametrize list built from an empty glob silently "passes" with zero test cases -- this catches
    that failure mode, since it would otherwise hide every golden file going missing."""
    assert GOLDEN_BUNDLE_FILES
