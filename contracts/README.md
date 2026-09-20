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

## Planned

| Contract | Format | Target | Consumers |
|---|---|---|---|
| Channel spec | JSON Schema plus examples | Phase 1 wave 1 | control plane, runtime, CLI, UI, migration |
| Control-plane API | OpenAPI 3.1 | Phase 1 wave 1 | API, UI client generation, CLI, tests |
| Audit events (`_tb.<env>.audit`) | JSON Schema | Phase 1, with RBAC | control plane, archive |

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

**Status: re-run pending.** `tests/conformance/test_contracts_v0.py` (`qa-engineer`, 2026-09-20) passed
62 / 0 / 0 and found no schema bugs — but that run covered the contracts *before* the security fixes of
the same day (see `CHANGELOG.md`). Those fixes changed both schemas and added 6 examples, so the result
is **superseded, not current**. New expected counts: envelope 7 valid / 16 invalid, events 8 / 14,
topic names 10 / 12.

Two fixture changes the suite must absorb: the envelope projection replaces `key` with `key_present`
plus `key_size`, and a `ValidationError` from either schema must not be logged or printed verbatim.

```
uv run --no-project --python 3.12 --with pytest --with jsonschema python -m pytest tests/conformance
```

If a schema rejects something you believe is valid, that is a contract bug — message `architect`
rather than adjusting the example.
