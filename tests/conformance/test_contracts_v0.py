"""Conformance: every contract's `valid` examples must validate and every `invalid` example must fail.

The two PHI regression guards in the events contract are asserted by name and never skipped.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.validators import validator_for

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
PHI_GUARDS = {"invalid/payload-in-event.json", "invalid/attrs-clear-text-key.json"}


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _validator(schema_path: Path) -> Draft202012Validator:
    schema = _load(schema_path)
    cls = validator_for(schema, default=Draft202012Validator)
    cls.check_schema(schema)
    return cls(schema, format_checker=FormatChecker())


def _file_cases() -> list[Any]:
    cases: list[Any] = []
    for name in ("envelope", "events"):
        manifest_path = CONTRACTS / name / "v0" / "examples" / "manifest.json"
        m = _load(manifest_path)
        base = manifest_path.parent
        schema = (base / m["schema"]).resolve()
        cases += [pytest.param(schema, base / e["file"], True, id=f"{name}:valid:{e['file']}") for e in m["valid"]]
        cases += [pytest.param(schema, base / e["file"], False, id=f"{name}:invalid:{e['file']}") for e in m["invalid"]]
    return cases


@pytest.mark.parametrize(("schema", "example", "should_pass"), _file_cases())
def test_example(schema: Path, example: Path, should_pass: bool) -> None:
    errors = list(_validator(schema).iter_errors(_load(example)))
    if should_pass:
        assert not errors, [e.message for e in errors][:3]
    else:
        assert errors, f"{example.name} validated but the manifest says it must fail"


def _topic_cases() -> list[Any]:
    m = _load(CONTRACTS / "topics/v0/examples/topic-names.json")
    schema = (CONTRACTS / "topics/v0/examples" / m["schema"]).resolve()
    return [pytest.param(schema, n, True, id=f"topic:valid:{n}") for n in m["valid"]] + [
        pytest.param(schema, e["name"], False, id=f"topic:invalid:{e['name']}") for e in m["invalid"]
    ]


@pytest.mark.parametrize(("schema", "name", "should_pass"), _topic_cases())
def test_topic_name(schema: Path, name: str, should_pass: bool) -> None:
    ok = _validator(schema).is_valid(name)
    assert ok is should_pass


def test_phi_guards_are_listed_in_manifest() -> None:
    m = _load(CONTRACTS / "events/v0/examples/manifest.json")
    listed = {e["file"] for e in m["invalid"]}
    assert PHI_GUARDS <= listed
