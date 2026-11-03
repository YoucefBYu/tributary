#!/usr/bin/env python3
"""Validate every published contract against its examples.

Standalone on purpose: it depends only on `jsonschema`, `openapi-spec-validator` (set 1.8, for the
OpenAPI document only) and the standard library, so it runs on a clean clone before the uv workspace
exists. `tests/conformance/` (owned by `qa-engineer`) is the same check wired into pytest and CI; this
script is the one a contract author can run while editing a schema, and the one that MEASURES the
expected-failure pairs the manifests pin.

    uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py
    uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py --emit-expectations
    uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py --check-expectations

Default mode: exit 0 when the OpenAPI document is valid, every schema compiles, every `valid` example
validates, and every `invalid` example fails at the `expected_json_path` / `expected_validator` its
manifest pins. Non-zero otherwise, with a summary. Without `openapi-spec-validator` the default mode
fails (one check not run is a failure, not a skip). `--check-expectations` (W3-11) checks only the
pinned pairs and needs only `jsonschema`. `--emit-expectations` prints them and always exits 0.

PHI: this script never prints an instance or any part of one. A `jsonschema.ValidationError`
message embeds the failing value, and a contract example is the shape of a real record, so only
`json_path` and the failing keyword are ever emitted (contracts/envelope/v0 section 1.1).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

try:
    from jsonschema import Draft202012Validator, FormatChecker
    from jsonschema.exceptions import SchemaError
    from jsonschema.validators import validator_for
except ImportError:  # pragma: no cover - the message is the point
    sys.exit(
        "jsonschema is not installed. Run:\n"
        "  uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py"
    )

CONTRACTS = Path(__file__).resolve().parent

# engine-kafka: set 1.3, ADR-008. observability and openapi: set 1.8 (W3-12, W3-1a). audit: set 1.8 part 2 (W3-1b).
FILE_CONTRACTS = ("envelope", "events", "channel-spec", "engine-kafka", "observability", "openapi", "audit")
OPENAPI_DOCUMENT = CONTRACTS / "openapi/v0/openapi.json"
TOPIC_MANIFEST = CONTRACTS / "topics/v0/examples/topic-names.json"
CONTEXT_REGISTRY = CONTRACTS / "context-extension/v0/capabilities.json"  # set 1.5, ADR-007 D21
STEP_NAMESPACES = CONTRACTS / "channel-spec/v0/step-namespaces.json"  # set 1.6, ADR-007 D23
METRIC_REGISTRY = CONTRACTS / "observability/v0/metrics.json"  # set 1.8, observability/v0 section 5
CHANNEL_SPEC_MANIFEST = CONTRACTS / "channel-spec/v0/examples/manifest.json"
PACKAGES = CONTRACTS.parent / "packages"
STEP_GROUP = "tributary.steps"
RESERVED_FIRST_SEGMENT = "tributary"
DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"


class Failure(NamedTuple):
    json_path: str
    validator: str


class SchemaRef(NamedTuple):
    """A schema file, and optionally a JSON pointer into it (`""` for the whole document).

    The pointer exists for `openapi/v0` (set 1.8): its examples validate against one component schema of
    `openapi.json`, written `"../openapi.json#/components/schemas/Name"` in the manifest.
    """

    path: Path
    pointer: str


class Case(NamedTuple):
    contract: str
    label: str
    instance: Any
    should_pass: bool
    expected: Failure | None
    schema: SchemaRef


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def schema_ref(base: Path, text: str) -> SchemaRef:
    file_part, _, pointer = text.partition("#")
    return SchemaRef((base / file_part).resolve(), pointer)


def build_validator(ref: SchemaRef) -> Draft202012Validator:
    """Compile a schema, failing loudly if the schema itself is malformed.

    With a pointer, the whole document is the root (so its internal `#/components/schemas/...` references
    resolve) and a root `$ref` selects one schema in it. OpenAPI 3.1 schema objects are JSON Schema 2020-12;
    the OpenAPI keys around them (`openapi`, `paths`, ...) are unknown keywords, which 2020-12 ignores.
    """
    document = load(ref.path)
    if ref.pointer:
        schema = {**document, "$schema": DRAFT_2020_12, "$ref": "#" + ref.pointer}
        cls: Any = Draft202012Validator
    else:
        schema = document
        cls = validator_for(schema, default=Draft202012Validator)
    cls.check_schema(schema)
    validator: Draft202012Validator = cls(schema, format_checker=FormatChecker())
    return validator


def validator_of(ref: SchemaRef, cache: dict[SchemaRef, Draft202012Validator]) -> Draft202012Validator:
    if ref not in cache:
        cache[ref] = build_validator(ref)
    return cache[ref]


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


def normalise_distribution(name: str) -> str:
    """PEP 503: lowercase, and every run of '-', '_' and '.' becomes one '-'."""
    return re.sub(r"[-_.]+", "-", name).lower()


def namespace_of(uses: str) -> str | None:
    """The first two segments of a `uses` name, or None when it has fewer than three (README 3.3.5 N1)."""
    parts = uses.split(".")
    return ".".join(parts[:2]) if len(parts) >= 3 else None


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def example_uses(document: Any) -> list[str]:
    """Every `uses` in a channel-spec document, pipeline and destination steps alike.

    Tolerant of shape on purpose: invalid examples are malformed somewhere else, and this check must
    not crash on them before the schema run reports why they fail.
    """
    spec = _as_dict(_as_dict(document).get("spec"))
    step_lists: list[Any] = [_as_dict(spec.get("pipeline")).get("steps")]
    destinations = spec.get("destinations")
    if isinstance(destinations, list):
        step_lists += [_as_dict(destination).get("steps") for destination in destinations]
    names: list[str] = []
    for steps in step_lists:
        if isinstance(steps, list):
            names += [s["uses"] for s in steps if isinstance(s, dict) and isinstance(s.get("uses"), str)]
    return names


def spec_declared_namespaces(document: Any) -> set[str]:
    """`pipeline.stepNamespaces` keys, minus reserved ones: a spec can never bind a `tributary.*` namespace."""
    pipeline = _as_dict(_as_dict(_as_dict(document).get("spec")).get("pipeline"))
    declared = _as_dict(pipeline.get("stepNamespaces"))
    return {name for name in declared if name.split(".", 1)[0] != RESERVED_FIRST_SEGMENT}


def check_step_namespaces() -> None:
    """The engine step-namespace registry, the channel-spec examples and the repo's packages must agree.

    Three checks (channel-spec README 3.3.5, ADR-007 D23):

    1. every registry namespace is a two-segment `tributary.*` namespace bound to a PEP 503 normalised
       distribution name;
    2. every `uses` in every channel-spec example (valid and invalid) has at least three segments, and
       its namespace is in the registry or in that example's own `pipeline.stepNamespaces`;
    3. the `tributary.steps` entry points declared in `packages/*/pyproject.toml` and the registry match
       both ways: each entry point is in a registered namespace bound to its own project name, and each
       registered namespace has at least one entry point. That is the review gate: a new engine
       namespace fails here until the architect registers it.

    Check 3 reads source `pyproject.toml` files with `tomllib`, not installed metadata, so it still runs
    on a clean clone. It answers "does the registry agree with what the packages declare", not "does
    the name resolve", which remains the runtime corpus test's question (contracts/README.md). Names
    printed here are package metadata, never instance data, so the output is PHI-safe. Adds no count.
    """
    registry = load(STEP_NAMESPACES)
    engine = {name: str(entry["distribution"]) for name, entry in registry["namespaces"].items()}
    problems: list[str] = []

    for name, distribution in sorted(engine.items()):
        parts = name.split(".")
        if len(parts) != 2 or parts[0] != RESERVED_FIRST_SEGMENT:
            problems.append(f"registry namespace {name} is not a two-segment {RESERVED_FIRST_SEGMENT}.* namespace")
        if normalise_distribution(distribution) != distribution:
            problems.append(f"registry namespace {name}: distribution {distribution} is not PEP 503 normalised")

    manifest = load(CHANNEL_SPEC_MANIFEST)
    for entry in [*manifest["valid"], *manifest["invalid"]]:
        document = load(CHANNEL_SPEC_MANIFEST.parent / entry["file"])
        allowed = set(engine) | spec_declared_namespaces(document)
        for uses in example_uses(document):
            namespace = namespace_of(uses)
            if namespace is None:
                problems.append(f"channel-spec {entry['file']}: uses {uses} has fewer than three segments (N1)")
            elif namespace not in allowed:
                problems.append(
                    f"channel-spec {entry['file']}: uses {uses} is in namespace {namespace}, which is neither in "
                    "step-namespaces.json nor declared in the example's pipeline.stepNamespaces (N2, N3)"
                )

    pyprojects = sorted(PACKAGES.glob("*/pyproject.toml"))
    if not pyprojects:
        problems.append(f"no packages/*/pyproject.toml under {PACKAGES}: cannot cross-check {STEP_GROUP}")
    seen: set[str] = set()
    for path in pyprojects:
        project = _as_dict(tomllib.loads(path.read_text(encoding="utf-8")).get("project"))
        distribution = normalise_distribution(str(project.get("name", "")))
        for point in _as_dict(_as_dict(project.get("entry-points")).get(STEP_GROUP)):
            namespace = namespace_of(point)
            if namespace is None or namespace not in engine:
                problems.append(
                    f"{path.parent.name}: {STEP_GROUP} entry point {point} is not in a namespace registered in "
                    "step-namespaces.json. Propose the namespace to the architect (README 3.3.5)"
                )
            elif engine[namespace] != distribution:
                problems.append(
                    f"{path.parent.name}: {STEP_GROUP} entry point {point} is declared by {distribution}, but "
                    f"step-namespaces.json binds {namespace} to {engine[namespace]}"
                )
            else:
                seen.add(namespace)
    for namespace in sorted(set(engine) - seen):
        problems.append(
            f"step-namespaces.json registers {namespace}, but no packages/*/pyproject.toml declares a "
            f"{STEP_GROUP} entry point in it"
        )

    if problems:
        raise SystemExit("step namespaces (channel-spec README 3.3.5):\n  " + "\n  ".join(problems))


def check_metric_registry() -> None:
    """`observability/v0/metrics.json` must be internally consistent (observability README section 5).

    The schema checks each piece; this checks how they refer to each other: every label a metric uses is
    defined, a `per_metric` label has `label_values` on every metric that uses it, `label_values` names
    only that metric's labels and only `per_metric` ones, each histogram's bucket set exists, and no name
    is registered twice. Metric and label names are contract text, never instance data. Adds no count.
    """
    registry = _as_dict(load(METRIC_REGISTRY))
    labels = _as_dict(registry.get("labels"))
    bucket_sets = _as_dict(registry.get("buckets"))
    metrics = registry.get("metrics")
    problems: list[str] = []
    seen: set[str] = set()
    for metric in metrics if isinstance(metrics, list) else []:
        entry = _as_dict(metric)
        name = str(entry.get("name"))
        if name in seen:
            problems.append(f"{name} is registered twice")
        seen.add(name)
        raw_labels = entry.get("labels")
        used = [label for label in raw_labels if isinstance(label, str)] if isinstance(raw_labels, list) else []
        values = _as_dict(entry.get("label_values"))
        for label in used:
            definition = labels.get(label)
            if not isinstance(definition, dict):
                problems.append(f"{name}: label {label} is not defined under labels")
            elif definition.get("per_metric") and label not in values:
                problems.append(f"{name}: label {label} is per_metric but has no label_values")
        for label in values:
            if label not in used:
                problems.append(f"{name}: label_values names {label}, which is not one of its labels")
            elif not _as_dict(labels.get(label)).get("per_metric"):
                problems.append(f"{name}: label_values given for {label}, whose values are fixed under labels")
        bucket_set = entry.get("buckets")  # a list here would be unhashable: test the type before `in`
        if entry.get("type") == "histogram" and not (isinstance(bucket_set, str) and bucket_set in bucket_sets):
            problems.append(f"{name}: bucket set {bucket_set!r} is not the name of a set defined under buckets")
    if problems:
        raise SystemExit("metric registry (observability/v0 section 5):\n  " + "\n  ".join(problems))


def check_openapi_document() -> list[str]:
    """`openapi/v0/openapi.json` must be a valid OpenAPI 3.1 document, and each component schema valid 2020-12.

    Needs `openapi-spec-validator` (Apache-2.0; its closure is permissive, checked from PyPI metadata on
    2026-10-05: CHANGELOG set 1.8). A missing library is a FAILURE, not a skip: a check that silently
    does not run is indistinguishable from one that passes. The document is contract text, never instance
    data, so quoting the library's message is PHI-safe. Returns problems; an empty list counts one check.
    """
    try:
        from openapi_spec_validator import validate as validate_openapi  # 0.7 and later
    except ImportError:
        try:
            from openapi_spec_validator import validate_spec as validate_openapi  # before 0.7
        except ImportError:
            validate_openapi = None
    if validate_openapi is None:
        return [
            "openapi: openapi-spec-validator is not installed, so openapi.json was NOT checked. Run:\n"
            "    uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator "
            "python contracts/validate.py"
        ]
    document = load(OPENAPI_DOCUMENT)
    problems: list[str] = []
    try:
        validate_openapi(document)
    except Exception as exc:  # the library raises several types; any of them means "not valid"
        problems.append(f"openapi: openapi.json is not valid OpenAPI 3.1 ({type(exc).__name__}): {str(exc)[:400]}")
    schemas = _as_dict(_as_dict(_as_dict(document).get("components")).get("schemas"))
    for name, schema in sorted(schemas.items()):
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            problems.append(f"openapi: components.schemas.{name} is not valid JSON Schema 2020-12 at {exc.json_path}")
    return problems


def file_cases(contract: str) -> list[Case]:
    """Cases from one contract's manifest. An entry's own `schema` key overrides the manifest's (set 1.8)."""
    manifest_path = CONTRACTS / contract / "v0" / "examples" / "manifest.json"
    manifest = load(manifest_path)
    check_runnable_metadata(contract, manifest)
    base = manifest_path.parent
    default: str | None = manifest.get("schema")

    def ref_of(entry: dict[str, Any]) -> SchemaRef:
        chosen = entry.get("schema", default)
        if not isinstance(chosen, str):
            raise SystemExit(f"{contract}: {entry['file']} names no schema and the manifest has no default")
        return schema_ref(base, chosen)

    cases = [Case(contract, e["file"], load(base / e["file"]), True, None, ref_of(e)) for e in manifest["valid"]]
    cases += [
        Case(contract, e["file"], load(base / e["file"]), False, expected_of(contract, e["file"], e), ref_of(e))
        for e in manifest["invalid"]
    ]
    return cases


def topic_cases() -> list[Case]:
    manifest = load(TOPIC_MANIFEST)
    ref = schema_ref(TOPIC_MANIFEST.parent, manifest["schema"])
    cases = [Case("topics", n, n, True, None, ref) for n in manifest["valid"]]
    cases += [
        Case("topics", e["name"], e["name"], False, expected_of("topics", e["name"], e), ref)
        for e in manifest["invalid"]
    ]
    return cases


def all_cases() -> Iterator[Case]:
    check_context_registry()
    check_step_namespaces()
    check_metric_registry()
    for contract in FILE_CONTRACTS:
        yield from file_cases(contract)
    yield from topic_cases()


def check() -> int:
    problems = check_openapi_document()
    passed = 0 if problems else 1  # the OpenAPI document counts as one check (set 1.8)
    cache: dict[SchemaRef, Draft202012Validator] = {}
    for case in all_cases():
        actual = failures(validator_of(case.schema, cache), case.instance)
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
    cache: dict[SchemaRef, Draft202012Validator] = {}
    for case in all_cases():
        if case.should_pass:
            continue
        actual = sorted(failures(validator_of(case.schema, cache), case.instance))
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


def check_expectations() -> int:
    """Exit 1 when any invalid example's pinned (json_path, validator) pair is not among its measured ones.

    W3-11. Replaces piping `--emit-expectations` into `.github/scripts/check_expectations.py`, with the
    same verdicts: a pair drifts when the example validates (nothing measured) or fails only elsewhere;
    zero invalid examples is itself a failure, so a broken loader cannot pass by checking nothing. Valid
    examples and the OpenAPI document are the default mode's job, not this one's. Prints contract and
    example names, never an instance.
    """
    cache: dict[SchemaRef, Draft202012Validator] = {}
    total = 0
    drifted: list[str] = []
    for case in all_cases():
        if case.should_pass:
            continue
        total += 1
        actual = failures(validator_of(case.schema, cache), case.instance)
        if not actual or case.expected not in actual:
            drifted.append(f"{case.contract}: {case.label}")
    for item in drifted:
        print(f"  DRIFT {item}")
    print(f"{total} invalid examples, {len(drifted)} drifted")
    if total == 0:
        print("no expectations checked")
        return 1
    return 1 if drifted else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--emit-expectations",
        action="store_true",
        help="print the measured failure pairs for each invalid example instead of checking (always exits 0)",
    )
    mode.add_argument(
        "--check-expectations",
        action="store_true",
        help="exit 1 if any invalid example's pinned (json_path, validator) pair does not match a measured one",
    )
    args = parser.parse_args()
    if args.emit_expectations:
        return emit_expectations()
    if args.check_expectations:
        return check_expectations()
    return check()


if __name__ == "__main__":
    raise SystemExit(main())
