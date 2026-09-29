#!/usr/bin/env python3
"""Validate every published contract against its examples.

Standalone on purpose: it depends only on `jsonschema` and the standard library, so it runs on a
clean clone before the uv workspace exists. `tests/conformance/` (owned by `qa-engineer`) is the
same check wired into pytest and CI; this script is the one a contract author can run while editing
a schema, and the one that MEASURES the expected-failure pairs the manifests pin.

    uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py
    uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py --emit-expectations

Exit code 0 when every schema compiles, every `valid` example validates, and every `invalid`
example fails at the `expected_json_path` / `expected_validator` its manifest pins. Non-zero
otherwise, with a summary.

PHI: this script never prints an instance or any part of one. A `jsonschema.ValidationError`
message embeds the failing value, and a contract example is the shape of a real record, so only
`json_path` and the failing keyword are ever emitted (contracts/envelope/v0 section 1.1).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

try:
    from jsonschema import Draft202012Validator, FormatChecker
    from jsonschema.validators import validator_for
except ImportError:  # pragma: no cover - the message is the point
    sys.exit(
        "jsonschema is not installed. Run:\n"
        "  uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py"
    )

CONTRACTS = Path(__file__).resolve().parent

FILE_CONTRACTS = ("envelope", "events", "channel-spec", "engine-kafka")  # engine-kafka: set 1.3, ADR-008
TOPIC_MANIFEST = CONTRACTS / "topics/v0/examples/topic-names.json"
CONTEXT_REGISTRY = CONTRACTS / "context-extension/v0/capabilities.json"  # set 1.5, ADR-007 D21


class Failure(NamedTuple):
    json_path: str
    validator: str


class Case(NamedTuple):
    contract: str
    label: str
    instance: Any
    should_pass: bool
    expected: Failure | None


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_validator(schema_path: Path) -> Draft202012Validator:
    """Compile a schema, failing loudly if the schema itself is malformed."""
    schema = load(schema_path)
    cls = validator_for(schema, default=Draft202012Validator)
    cls.check_schema(schema)
    return cls(schema, format_checker=FormatChecker())


def failures(validator: Draft202012Validator, instance: Any) -> set[Failure]:
    """(json_path, keyword) pairs only. The error message embeds the instance and is never used."""
    return {Failure(e.json_path, str(e.validator)) for e in validator.iter_errors(instance)}


def expected_of(contract: str, label: str, entry: dict[str, Any]) -> Failure:
    missing = [k for k in ("expected_json_path", "expected_validator") if k not in entry]
    if missing:
        raise SystemExit(
            f"{contract}: {label} is an invalid example with no {', '.join(missing)}. "
            "Every invalid example pins the reason it fails; see contracts/README.md."
        )
    return Failure(entry["expected_json_path"], entry["expected_validator"])


def check_runnable_metadata(contract: str, manifest: dict[str, Any]) -> None:
    """Channel-spec valid examples must classify themselves as runnable or not.

    Only this contract needs it: its examples name code (`uses` entry points, `ref` dotted paths),
    so an example can be schema-valid and still reference something nothing provides - which is how
    the walking-skeleton example shipped unrunnable. Envelope and events examples are record
    projections and event bodies; they name nothing that has to exist.

    This checks the metadata is present, not that the names resolve: resolution needs the installed
    distribution, and this script deliberately runs on a clean clone with only jsonschema. The
    resolving test lives in tributary-runtime and reads these same fields.
    """
    if contract != "channel-spec":
        return
    for entry in manifest["valid"]:
        name = entry["file"]
        if "runnable" not in entry:
            raise SystemExit(
                f"{contract}: {name} does not declare `runnable`. Every valid channel-spec example "
                "says whether its steps resolve today; see the manifest's runnable_note."
            )
        if not entry["runnable"] and not entry.get("requires"):
            raise SystemExit(
                f"{contract}: {name} is runnable: false with no `requires`. Name what is missing "
                "and the task that delivers it, or the exemption is permanent by accident."
            )


def check_context_registry() -> None:
    """The `pipeline.context` item enum and the context-extension registry must name the same capabilities.

    Two files hold the list on purpose: the schema is what a spec author and the control plane see,
    the registry is what binds each name to the one distribution allowed to supply it
    (contracts/context-extension/v0 section 4). Drift between them would let a spec validate against
    a name the worker then refuses, or leave a registered name undeclarable. Capability names are
    package metadata, not instance data, so printing them is PHI-safe.
    """
    schema = load(CONTRACTS / "channel-spec" / "v0" / "channel.schema.json")
    declared = set(schema["$defs"]["pipeline"]["properties"]["context"]["items"]["enum"])
    registered = set(load(CONTEXT_REGISTRY)["capabilities"])
    if declared != registered:
        raise SystemExit(
            "channel-spec pipeline.context enum and context-extension/v0/capabilities.json differ: "
            f"only in the schema {sorted(declared - registered)}, only in the registry "
            f"{sorted(registered - declared)}. They must list the same names (context-extension README 4)."
        )


def file_cases(contract: str) -> tuple[Path, list[Case]]:
    manifest_path = CONTRACTS / contract / "v0" / "examples" / "manifest.json"
    manifest = load(manifest_path)
    check_runnable_metadata(contract, manifest)
    base = manifest_path.parent
    schema = (base / manifest["schema"]).resolve()
    cases = [Case(contract, e["file"], load(base / e["file"]), True, None) for e in manifest["valid"]]
    cases += [
        Case(
            contract,
            e["file"],
            load(base / e["file"]),
            False,
            expected_of(contract, e["file"], e),
        )
        for e in manifest["invalid"]
    ]
    return schema, cases


def topic_cases() -> tuple[Path, list[Case]]:
    manifest = load(TOPIC_MANIFEST)
    schema = (TOPIC_MANIFEST.parent / manifest["schema"]).resolve()
    cases = [Case("topics", n, n, True, None) for n in manifest["valid"]]
    cases += [
        Case("topics", e["name"], e["name"], False, expected_of("topics", e["name"], e)) for e in manifest["invalid"]
    ]
    return schema, cases


def all_suites() -> Iterator[tuple[Path, list[Case]]]:
    check_context_registry()
    for contract in FILE_CONTRACTS:
        yield file_cases(contract)
    yield topic_cases()


def check() -> int:
    problems: list[str] = []
    passed = 0
    for schema_path, cases in all_suites():
        validator = build_validator(schema_path)
        for case in cases:
            actual = failures(validator, case.instance)
            if case.should_pass:
                if actual:
                    problems.append(f"{case.contract}: {case.label} must validate, but failed at {sorted(actual)[:5]}")
                else:
                    passed += 1
                continue
            if not actual:
                problems.append(f"{case.contract}: {case.label} validated, but the manifest says it must fail")
            elif case.expected not in actual:
                problems.append(
                    f"{case.contract}: {case.label} failed for the wrong reason - expected "
                    f"{case.expected.validator} at {case.expected.json_path}, got {sorted(actual)[:5]}"
                )
            else:
                passed += 1

    print(f"{passed} checks passed, {len(problems)} failed")
    for problem in problems:
        print(f"  FAIL {problem}")
    return 1 if problems else 0


def emit_expectations() -> int:
    """Print the MEASURED (json_path, validator) pairs for every invalid example.

    Paste these into the manifests. This exists because the architect session has no shell: the
    pairs it writes are predictions until someone runs this. Paths and keywords only.
    """
    for schema_path, cases in all_suites():
        validator = build_validator(schema_path)
        for case in cases:
            if case.should_pass:
                continue
            actual = sorted(failures(validator, case.instance))
            pinned = case.expected in actual if case.expected else False
            print(
                json.dumps(
                    {
                        "contract": case.contract,
                        "example": case.label,
                        "pinned_pair_matches": pinned,
                        "measured": [
                            {"expected_json_path": f.json_path, "expected_validator": f.validator} for f in actual
                        ],
                    }
                )
            )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--emit-expectations",
        action="store_true",
        help="print the measured failure pairs for each invalid example instead of checking",
    )
    args = parser.parse_args()
    return emit_expectations() if args.emit_expectations else check()


if __name__ == "__main__":
    raise SystemExit(main())
