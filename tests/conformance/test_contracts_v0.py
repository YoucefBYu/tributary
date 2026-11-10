"""Conformance: every contract's `valid` examples must validate and every `invalid` example must fail.

The PHI regression guards are asserted by name and never skipped. An invalid example must fail for the documented reason: the
expected (json_path, validator keyword) pair is asserted, not just "some error". Reports carry paths and keywords only, never the instance.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.validators import validator_for

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
# Contracts with a manifest.json of {schema, valid[], invalid[]} under v0/examples/. Add a new
# contract here (one line) rather than in each function below.
FILE_CONTRACTS = ("envelope", "events", "channel-spec", "observability", "openapi", "audit")
EVENT_PHI_GUARDS = {
    "invalid/payload-in-event.json",
    "invalid/attrs-clear-text-mrn.json",
    "invalid/attrs-hmac-not-fixed-length-hex.json",
    "invalid/attrs-free-text-value.json",
    "invalid/attrs-too-many-entries.json",
    "invalid/attrs-hl7-delimiters.json",
    "invalid/claim-check-presigned-url.json",
    "invalid/record-with-attrs.json",
}
OBSERVABILITY_PHI_GUARDS = {
    "invalid/readiness-free-text-reason.json",
    "invalid/readiness-exception-text-field.json",
    "invalid/log-field-mrn.json",
    "invalid/log-field-payload.json",
    "invalid/log-value-multiline.json",
    "invalid/log-message-id-not-uuid.json",
    "invalid/log-nested-object.json",
    "invalid/log-error-exception-text.json",
    "invalid/metrics-label-message-id.json",
    "invalid/metrics-label-partition-key.json",
}
OPENAPI_PHI_GUARDS = {
    "invalid/response-problem-echoes-input.json",
    "invalid/response-validation-issue-with-message.json",
    "invalid/response-problem-unknown-code.json",
}
ENVELOPE_PHI_GUARDS = {
    "invalid/claim-check-presigned-url.json",
    "invalid/claim-check-with-userinfo.json",
    "invalid/correlation-id-sender-token.json",
}


def _safe(errors: list[Any]) -> list[str]:
    """Describe failures without the instance: a ValidationError message embeds the failing value (PHI risk, invariant 6)."""
    return [f"{e.json_path} [{e.validator}]" for e in errors][:5]


Failure = tuple[str, str]


def _failures(validator: Draft202012Validator, instance: Any) -> set[Failure]:
    """(json_path, keyword) pairs only: a ValidationError message embeds the failing value, so it is never used."""
    return {(e.json_path, str(e.validator)) for e in validator.iter_errors(instance)}


def _mismatch(actual: set[Failure], expected: Failure) -> str | None:
    """None when the documented failure is among the actual ones, else a PHI-safe description."""
    if expected in actual:
        return None
    return f"expected {expected[1]} at {expected[0]}; got {sorted(actual)[:5]}"


def _expected(name: str, entry: dict[str, Any]) -> Failure:
    """The manifest is the only source; an invalid entry without expectation fields fails the suite."""
    assert "expected_json_path" in entry and "expected_validator" in entry, (
        f"{name}: no expected_json_path/expected_validator"
    )
    return entry["expected_json_path"], entry["expected_validator"]


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


SchemaRef = tuple[Path, str]  # (file, JSON pointer into it or "")


def _ref(base: Path, text: str) -> SchemaRef:
    """`file.json#/pointer` as the manifests write it, resolved against the manifest's directory (set 1.8)."""
    file_part, _, pointer = text.partition("#")
    return (base / file_part).resolve(), pointer


def _validator(ref: SchemaRef | Path) -> Draft202012Validator:
    schema_path, pointer = (ref, "") if isinstance(ref, Path) else ref
    schema = _load(schema_path)
    if pointer:
        # Whole document as root so internal `#/components/schemas/...` refs resolve; OpenAPI 3.1 schema objects are 2020-12.
        schema = {**schema, "$schema": "https://json-schema.org/draft/2020-12/schema", "$ref": "#" + pointer}
    cls = validator_for(schema, default=Draft202012Validator)
    cls.check_schema(schema)
    return cls(schema, format_checker=FormatChecker())


def _file_cases() -> list[Any]:
    cases: list[Any] = []
    for name in FILE_CONTRACTS:
        manifest_path = CONTRACTS / name / "v0" / "examples" / "manifest.json"
        m = _load(manifest_path)
        base = manifest_path.parent

        def ref_of(e: dict[str, Any], m: dict[str, Any] = m, base: Path = base) -> SchemaRef:
            return _ref(base, e.get("schema", m.get("schema")))  # an entry's own schema overrides the manifest's

        cases += [
            pytest.param(ref_of(e), base / e["file"], True, None, id=f"{name}:valid:{e['file']}") for e in m["valid"]
        ]
        cases += [
            pytest.param(
                ref_of(e),
                base / e["file"],
                False,
                _expected(f"{name}:{e['file']}", e),
                id=f"{name}:invalid:{e['file']}",
            )
            for e in m["invalid"]
        ]
    return cases


@pytest.mark.parametrize(("schema", "example", "should_pass", "expected"), _file_cases())
def test_example(schema: SchemaRef, example: Path, should_pass: bool, expected: Failure | None) -> None:
    validator = _validator(schema)
    if should_pass:
        errors = list(validator.iter_errors(_load(example)))
        assert not errors, _safe(errors)
        return
    assert expected is not None
    actual = _failures(validator, _load(example))
    assert actual, f"{example.name} validated but the manifest says it must fail"
    problem = _mismatch(actual, expected)
    assert problem is None, f"{example.name}: {problem}"


def _topic_cases() -> list[Any]:
    m = _load(CONTRACTS / "topics/v0/examples/topic-names.json")
    schema = (CONTRACTS / "topics/v0/examples" / m["schema"]).resolve()
    return [pytest.param(schema, n, True, None, id=f"topic:valid:{n}") for n in m["valid"]] + [
        pytest.param(
            schema,
            e["name"],
            False,
            _expected(f"topics:{e['name']}", e),
            id=f"topic:invalid:{e['name']}",
        )
        for e in m["invalid"]
    ]


@pytest.mark.parametrize(("schema", "name", "should_pass", "expected"), _topic_cases())
def test_topic_name(schema: Path, name: str, should_pass: bool, expected: Failure | None) -> None:
    validator = _validator(schema)
    if should_pass:
        assert validator.is_valid(name)
        return
    assert expected is not None
    problem = _mismatch(_failures(validator, name), expected)
    assert problem is None, f"{name}: {problem}"


def test_every_invalid_entry_has_an_expectation() -> None:
    """A new invalid example without an expected failure must fail the suite, not slide in as 'any error'."""
    for name in FILE_CONTRACTS:
        m = _load(CONTRACTS / name / "v0/examples/manifest.json")
        for e in m["invalid"]:
            _expected(f"{name}:{e['file']}", e)
    t = _load(CONTRACTS / "topics/v0/examples/topic-names.json")
    for e in t["invalid"]:
        _expected(f"topics:{e['name']}", e)


def test_negative_control_wrong_reason_fails() -> None:
    """An example that fails for a different reason than documented must make test_example fail."""
    env = CONTRACTS / "envelope/v0/examples"
    m = _load(env / "manifest.json")
    schema = _ref(env, m["schema"])
    by_file = {e["file"]: e for e in m["invalid"]}
    example = env / "invalid/missing-msg-id.json"
    wrong = _expected("wrong", by_file["invalid/msg-id-not-uuid7.json"])
    with pytest.raises(AssertionError):
        test_example(schema, example, False, wrong)
    test_example(
        schema,
        example,
        False,
        _expected("right", by_file["invalid/missing-msg-id.json"]),
    )
    with pytest.raises(AssertionError):
        _expected("missing", {"file": "invalid/x.json"})


@pytest.mark.parametrize(
    ("contract", "guards"),
    [
        ("events", EVENT_PHI_GUARDS),
        ("envelope", ENVELOPE_PHI_GUARDS),
        ("observability", OBSERVABILITY_PHI_GUARDS),
        ("openapi", OPENAPI_PHI_GUARDS),
    ],
)
def test_guards_are_listed_and_enforced(contract: str, guards: set[str]) -> None:
    """Guards must stay in the manifest (not silently dropped) and must fail validation."""
    mp = CONTRACTS / contract / "v0/examples/manifest.json"
    m = _load(mp)
    assert guards <= {e["file"] for e in m["invalid"]}
    by_file = {e["file"]: e for e in m["invalid"]}
    for g in guards:
        v = _validator(_ref(mp.parent, by_file[g].get("schema", m.get("schema"))))
        assert not v.is_valid(_load(mp.parent / g)), g


def test_safe_reporting_omits_instance() -> None:
    v = _validator(CONTRACTS / "events/v0/event.schema.json")
    errs = list(v.iter_errors({"v": 0, "secret-value-123": "x"}))
    assert errs
    assert "secret-value-123" not in " ".join(_safe(errs))
