# Contracts

Owned by the `architect`. Everything the teams code against in parallel lives here, published
before the work starts. Others propose changes by message with the exact diff and the reason; wait
for approval before implementing.

## Published

| Contract | Version | Status | Spec | Machine-readable |
|---|---|---|---|---|
| Message envelope (`tb-*` headers, key, payload) | v0 | Draft | [`envelope/v0/README.md`](envelope/v0/README.md) | `envelope/v0/envelope.schema.json` |
| Topic naming, defaults, required config | v0 | Draft | [`topics/v0/README.md`](topics/v0/README.md) | `topics/v0/topic-name.schema.json`, `topics/v0/topic-defaults.json` |
| Lifecycle events (`_tb.<env>.events`) | v0 | Draft | [`events/v0/README.md`](events/v0/README.md) | `events/v0/event.schema.json` |
| Channel spec (source, steps, destinations, ordering, retry) | v0 | Draft | [`channel-spec/v0/README.md`](channel-spec/v0/README.md) | `channel-spec/v0/channel.schema.json` |

### Contract set 1.0 — what "contracts v1" means

The Phase 1 wave 1 plan calls for "contracts v1". That is the **contract set** milestone, not a
directory rename: set 1.0 is the four contracts above, frozen together at the wave-1 gate.

| Set version | Members | Frozen |
|---|---|---|
| 1.0 | `envelope/v0`, `topics/v0`, `events/v0`, `channel-spec/v0` | at the Phase 1 wave 1 gate |
| 1.1 | Same four directories. **Additive only**: wave 2 connector types, destination steps and key semantics in `channel-spec/v0`; consumer-group role `receiver` in `topics/v0` | Published 2026-09-24 at the start of wave 2; **unverified** until the conformance run. Freezes at the wave 2 gate |

A minor set version is additive by definition: every document valid under 1.0 is valid under 1.1.
A change that would make a 1.0-valid document invalid is breaking, creates a `v1/` directory and needs
an ADR.

**Why not `envelope/v1/` and so on.** The rule below says a new major *directory* is created by a
breaking change **after** a freeze, not by the freeze itself. Copying the three contracts into `v1/`
directories would move about sixty files, break every path in `tests/conformance`, ADR-002, ADR-003
and `STATUS.md`, and change no schema — a rename presented as a version. The contracts are still
Draft, so wave-1 changes go into the existing directories and are recorded in
[`CHANGELOG.md`](CHANGELOG.md), which is what Draft status is for.

**Give up:** the deliverable's name and the directory names no longer match, so this table is the
mapping and has to stay accurate. Architect decision, 2026-09-22; the lead and the human can
overrule it, and it is much cheaper to overrule now than after wave 1 references land.

## Planned

| Contract | Format | Target | Consumers |
|---|---|---|---|
| Control-plane API | OpenAPI 3.1 | Phase 1 wave 3 (W3-1) | API, UI client generation, CLI, tests |
| Audit events (`_tb.<env>.audit`) | JSON Schema | Phase 1, with RBAC — **blocked on the retention decision** (`topics/v0/README.md` 2.1): whether Kafka, ClickHouse or object storage is the system of record changes the contract, so settle that first | control plane, archive |

## Rules

- **Versioned by directory.** `<contract>/v<major>/`. While a version is Draft, additive changes are
  recorded in [`CHANGELOG.md`](CHANGELOG.md). Once frozen at a phase gate, a breaking change creates
  the next major directory and needs an ADR.
- **Every contract ships valid and invalid examples** that tests load, with the rule each invalid
  example breaks. Examples use synthetic data only.
- **Strict producers, lenient consumers.** Schemas set `additionalProperties: false` so CI catches a
  typo at the produce side. At runtime, a consumer must ignore fields and headers it does not know
  rather than rejecting the record; otherwise an additive change becomes an outage.
- **No PHI in any example.** Keys and identifiers carry a `TRIBUTARY-SYNTH` marker.

## Conformance suite

Every contract directory has `examples/manifest.json` with the same shape:

```json
{ "contract": "...", "schema": "../x.schema.json",
  "valid":   [{ "file": "valid/a.json", "note": "..." }],
  "invalid": [{ "file": "invalid/b.json", "violates": "the rule it breaks" }] }
```

One parameterised test per contract walks the manifest: every `valid` file must validate, every
`invalid` file must fail. `qa-engineer` owns the suite in `tests/conformance/`. The topics contract
uses inline string lists instead of files, since a topic name is one string.

Two ways to run it. [`validate.py`](validate.py) is the architect-owned checker: standalone,
`jsonschema` plus the standard library only, so it runs on a clean clone before the uv workspace
exists, and it is what a contract author runs while editing a schema.

```
uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py
uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py --emit-expectations
```

`--emit-expectations` prints the **measured** `(json_path, validator)` pair for every invalid
example, paste-ready. It exists because the architect session has no shell: pairs written without
it are predictions, and the point of the mode is that nobody has to accept a prediction. Neither
tool ever prints an instance or part of one — a contract example is record-shaped, so that is a PHI
rule (invariant 6), not a style one.

### Two checkers, and the split is structural — do not merge them

A third checker sits outside `contracts/`: the schema-corpus test in `tributary-runtime`, which loads
the channel-spec examples and resolves the steps they name.

| Checker | Answers | Cannot answer |
|---|---|---|
| `contracts/validate.py` and `tests/conformance` | Is the document well-formed, and does each invalid example fail for its documented reason? | Whether anything the document *names* exists — it has only `jsonschema` and no installed distribution |
| Corpus test in `tributary-runtime` | Do the steps an example names actually resolve? | Nothing, on a clean clone: it needs the workspace installed |

**Neither could have caught the wave-1 walking-skeleton defect alone, and neither can catch the next
one alone.** A channel spec referencing `adt_flow.steps.passthrough` — a package nothing in the repo
publishes — was schema-valid and unrunnable, and `validate.py` was green throughout. That is a property
of the problem, not a gap to close: name resolution needs a runtime, and running on a clean clone before
`uv sync` is why the standalone checker exists at all. Recorded here because the obvious later
"simplification" is to merge them, which would quietly drop one of the two questions.

Three of the four contract defects found during wave 1 were found by an implementer rather than by any
checker, all with the same shape: the document was internally valid and referred to something that did
not exist. So **a green conformance run means the contracts are self-consistent, not that they agree
with the code.**

**Status: `validate.py` 102 / 0 and `pytest tests/conformance` 106 passed, 2026-09-23.** The two
counts differ because `validate.py` counts examples across all four contracts while pytest counts
test cases including its own meta-tests.

All 24 wave-1 expected-failure pairs were written as architect predictions and **all 24 matched the
measured run**, so no manifest has needed correcting. That record covers *invalid* examples, where the
prediction is about schema semantics. It does **not** extend to hand-authored *valid* examples: one
was committed with a 34-character `traceparent` trace id where the pattern requires 32, and only the
run caught it. History, newest first:

| Run | Result | Covers |
|---|---|---|
| W1-4 follow-up (2026-09-23) | **`validate.py` 102 / 0; pytest 106**, on the second run | Envelope 2.3.1, the ordinary reject shape, plus the blob framing, stage and `RECORD_UNDECODABLE` decisions. The first run failed on an architect typo in a new valid example (34-hex trace id), which is the case for keeping the run blocking |
| W1-4 questions (2026-09-22) | **`validate.py` 100 / 0; pytest 105** (`f5bc92d`) | The quarantine shape for unidentifiable records: `kind: record` plus four examples. `record-with-attrs.json` joins `EVENT_PHI_GUARDS`, now 8 |
| W1-1 (2026-09-22) | **`validate.py` 96 / 0; pytest 101** | Contract set 1.0: the new `channel-spec` contract (23 examples) and two envelope examples for requirement 13. `validate.py` is new and covers all four contracts standalone, run by the lead; pytest run by `qa-engineer` (99 at `5eb3949`, 101 once the envelope pair landed) |
| `83a1fd2` (2026-09-22) | **76 / 0 / 0** | Expected-failure assertions: every `invalid` example must now fail at the `expected_json_path` and `expected_validator` recorded in its manifest, so an example that fails for the *wrong* reason goes red. Closes `code-reviewer` Medium 2 from the Phase 0 gate |
| after security round 2 | 74 / 0 / 0 | Four invalid examples added for findings 7, 9 and 13 |
| commit `ae4d3bd` | 70 / 0 / 0 | The five HIGH security fixes of round 1 |
| 2026-09-20, first run | 62 / 0 / 0, no schema bugs | The contracts as first published; **superseded** |

**Why the expectations matter:** until `83a1fd2` the suite asserted only that an invalid example fails,
not *why*. A guard could have stopped guarding — the `attrs-clear-text-mrn` example, for instance, would
still fail if the identifier deny-list were removed, because the key would trip a different rule. The
manifests now pin the reason, and the expected values were measured against a real validator run rather
than read off the schema.

No schema changed at P0-3 (the partition-default change is in `topic-defaults.json`, which the suite
does not validate).

Two fixture changes the suite must absorb: the envelope projection replaces `key` with `key_present`
plus `key_size`, and a `ValidationError` from either schema must not be logged or printed verbatim.

```
uv run --no-project --python 3.12 --with pytest --with jsonschema python -m pytest tests/conformance
```

If a schema rejects something you believe is valid, that is a contract bug — message `architect`
rather than adjusting the example.
