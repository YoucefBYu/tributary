"""Shared golden-file test plumbing: load an `.hl7` fixture, run `parse` then a mapping step, compare the
resulting Bundle JSON against a golden file under `tests/fixtures/fhir/`.

Fixtures live at the repository's shared `tests/fixtures/hl7/` and `tests/fixtures/fhir/`
(`healthcare-standards-engineer` owns both, OWNERSHIP.md), not under this package's own `tests/`, so a
QA-owned conformance suite can reuse the same `.hl7` inputs without duplicating them.
"""

from __future__ import annotations

import json
from pathlib import Path

from tributary.fhir.fakes import fake_fhir_context
from tributary.hl7v2.step import parse
from tributary.sdk.step import Step
from tributary.sdk.testing import make_message, run_step

REPO_ROOT = Path(__file__).resolve().parents[3]
HL7_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "hl7"
FHIR_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "fhir"

#: A fixed, synthetic `tb-msg-id` (a real UUIDv7) so every golden file is reproducible: `message_id_
#: factory` and `message_identifier` both derive their output from it, and `sequential_id_factory`
#: (the `fake_fhir_context` default) is independently deterministic, so the whole Bundle is byte-stable.
GOLDEN_MSG_ID = "0199036e-7a3e-7f00-8c21-abc123456789"


def map_fixture(hl7_filename: str, mapping_step: Step) -> dict[str, object]:
    """Parse `tests/fixtures/hl7/<hl7_filename>`, run `mapping_step`, return the resulting Bundle dict."""
    raw = (HL7_FIXTURES / hl7_filename).read_bytes()
    ctx = fake_fhir_context()
    message = make_message(raw, content_type="application/hl7-v2+er7", msg_id=GOLDEN_MSG_ID)
    parsed = run_step(parse, message, ctx=ctx)
    mapped = run_step(mapping_step, parsed, ctx=ctx)
    assert mapped.payload is not None
    assert mapped.content_type == "application/fhir+json"
    result: dict[str, object] = json.loads(mapped.payload)
    return result


def load_golden(golden_filename: str) -> dict[str, object]:
    result: dict[str, object] = json.loads((FHIR_FIXTURES / golden_filename).read_text())
    return result
