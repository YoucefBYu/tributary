# ADR-007: Channel Spec Shape and What It Refuses

**Status:** Proposed — decided by the architect at W1-1. No invariant changes; every rule here realises an existing invariant or a Phase 0 gate requirement.
**Date:** 2026-09-22
**Deciders:** architect, with the lead and human able to overrule before the wave-1 gate
**Relates to:** ADR-001 sections 6.1, 6.2, 6.4 · ADR-003 D1-D6 · invariants 1, 3, 4, 5, 6, 7 · gate `phase-0-spikes.md` 7.1 requirements 5, 7, 8, 13
**Contracts:** `contracts/channel-spec/v0` (new), `contracts/envelope/v0` 2.3-2.4, `contracts/topics/v0` 3.7-3.8
**Evidence:** none of these decisions is a measurement — they are design choices, and the options and costs are written out below. The numeric defaults referenced (3 partitions, `session.timeout.ms` 15000, `commit_every`) come from S4 and S5 via ADR-002; the channel spec only points at them. The schema itself is **verified**: `contracts/validate.py` 96 / 0 and `pytest tests/conformance` 99 passed, 2026-09-22 (section 6, follow-up 1).

---

## 1. Context

Wave 1 needs one document that the runtime, the control plane, the CLI, the UI and the Mirth
importer all agree on, published before those five things are written. ADR-001 6.1 sketches it as an
illustration; an illustration cannot be validated, so three teams were about to invent details
independently.

Two constraints shape it more than convenience does:

- **Invariant 3** makes `_tb.<env>.channels` the worker's only source of truth. A spec is therefore a
  document that a tenant can supply and the data plane then trusts completely.
- **Phase 0's two High findings were both controls that were specified and then quietly not applied.**
  The gate report says so explicitly. That is an argument for putting rules in a validator rather
  than in prose, wherever a validator can hold them.

## 2. Options considered

### Option A — Mirth-parity spec: inline source in the document

Filter and transformer bodies live in the spec as strings, as Mirth does with JavaScript.

| | |
|---|---|
| Pros | Shortest path for a migrating Mirth user; a one-line filter costs one line; the importer maps almost directly; no bundle build needed for a trivial channel |
| Cons | Breaks invariant 7: inline source cannot be imported by pytest, reviewed in a diff, type-checked by `mypy`, or pinned by a digest. It puts arbitrary code on the `_tb.channels` topic, which is the one artefact write access to means remote code execution across the data plane (`topics/v0` 3.4). Every "run this string" path needs a sandbox we have not designed |
| Complexity | Low to write, high to secure |
| Cost | The trust model becomes load-bearing immediately, before ADR-005 exists |
| Scalability | Fine |
| Team familiarity | High — it is the incumbent's model |

### Option B — Declarative spec referencing code in a digest-pinned bundle (**selected**)

Steps name a registered step (`uses`) or a dotted path inside an OCI bundle (`ref`). The spec
carries configuration; the bundle carries code.

| | |
|---|---|
| Pros | Every step is an ordinary Python function, so invariant 7's "unit-testable without Kafka" is true by construction; code is reviewed, typed and tested in its own repo; the digest makes "what code ran" answerable from a record's `tb-channel-version`; the spec stays small enough to validate strictly |
| Cons | A one-line filter needs a bundle, a module and a test. That is a real adoption cost and the first thing a Mirth user will complain about |
| Complexity | Moderate: needs a bundle build, which wave 1 does not have yet |
| Cost | Migration friction, mitigated by `packages/tributary-mirth-import` |
| Scalability | Good — the spec is config-sized regardless of pipeline size |
| Team familiarity | Normal Python |

### Option C — Code-first: the channel *is* Python, no document

A decorated pipeline object defines source, steps and destinations.

| | |
|---|---|
| Pros | No schema to maintain; full expressiveness; testable |
| Cons | The UI cannot render or edit a channel without executing it; the control plane cannot validate a deploy without running tenant code; diffing two versions means diffing arbitrary code; invariant 3 would put a Python interpreter on the spec-load path in every worker |
| Complexity | Low at first, high once the UI and control plane need to reason about a channel |
| Cost | Rules out the visual channel view and safe server-side validation |
| Scalability | Fine |
| Team familiarity | High |

## 3. Decision

**Option B.** Configuration is declarative and validated; code lives in a digest-pinned bundle. Nine
sub-decisions follow, each expressed in the schema rather than in prose wherever possible.

| # | Decision | What it gives up |
|---|---|---|
| D1 | **No field anywhere carries inline code.** No `script`, no `code`, no `transformer` body, in this major version | A one-line filter costs a bundle, a module and a test. The conversion path is the importer's job, which is where that cost belongs |
| D2 | **`ordering` is required with no default**; `partitionKey` required iff `per-key`, forbidden otherwise | A spec author must decide ordering before writing anything else. Intended: invariant 5 says ordering is explicit, and a default would be the thing nobody reads |
| D3 | **`ack.mode` is a one-member enum.** `after-commit` is the only representable value | The field looks redundant. It is not: a reader sees the guarantee instead of assuming it, and there is no syntax for the invariant-1 violation |
| D4 | **`idempotency` is required per destination**, with a justification note when `none` | Extra ceremony on every destination. It makes invariant 4 reviewable per channel and gives each loader's duplicate-delivery test something to assert against |
| D5 | **`retry-topic` is in the enum and rejected by the validator** | A channel with an order-insensitive sink waits for the spike. Keeping the name shared across specs, docs and code while refusing selection is the honest position: ADR-003 D1 already forbids it and D2 leaves its mechanism undecided. Removing the `not` later is a one-line relaxation |
| D6 | **Connector `type` enums list only what has been built** (`mllp`, `http`) | Every new connector costs one contract change and one approval round. Intended: the alternative is publishing invented config for unwritten code, and an enum relaxation is additive anyway |
| D7 | **Security rules are validator rules, not review notes**: TLS unless loopback, `clientAuth` stated explicitly, plaintext HTTP only to loopback, no query string in a destination URL, credential-shaped property names rejected, `queue.maxBytes` required | Some legitimate configurations are refused — notably a FHIR destination that needs `?identifier=`, which becomes an explicit wave-2 contract change with a no-log rule rather than a default |
| D8 | **`metadata.version` is set by the control plane and optional in the schema**; the runtime rejects a published record without it | The schema does not enforce it. Alternatives were a second schema file (cross-file `$ref` needs a resolver the conformance harness does not have) or a placeholder version in authored files (invites drift). Same arrangement as the envelope's per-class key identity rules |
| D9 | **Retention is the only per-channel topic override** | A channel cannot tune its own durability. Correct: invariant 1 is not per-channel |

## 4. Consequences

- Three teams can start against a fixed surface: `platform-engineer` (runtime spec load, W1-4),
  `connector-engineer` (MLLP and HTTP config, W1-5/W1-6), `devops-engineer` (topic creation).
- `_tb.<env>.channels` has a contract for the first time: key, value, tombstone, and the
  `metadata.version` rule (`channel-spec/v0/README.md` 6). Invariant 3 made that topic load-bearing
  and nothing had specified it.
- Phase 1 requirements 5 and 8 become validation failures instead of review findings. Requirement 7's
  digest pinning extends from base images to bundles.
- The Mirth importer now has a target it cannot fully hit: a channel using inline JavaScript cannot be
  translated mechanically. S6 is already INCONCLUSIVE on import feasibility; this makes the lower
  bound of that estimate worse, not better, and it should be said out loud rather than discovered.
- The UI can render a channel diagram from the document without executing anything.

## 5. Impact on invariants and contracts

**No invariant changes.** Invariants 1, 3, 4, 5, 6 and 7 gain declarative enforcement; none is
reworded. Contracts: `channel-spec/v0` is new; `envelope/v0` gains sections 2.3 and 2.4;
`topics/v0` gains 3.7 and 3.8. All additive, all recorded in `contracts/CHANGELOG.md`.

## 5a. Amendment 1 — contract set 1.1, wave 2 connectors (2026-09-24)

**Decided by the architect at the start of Phase 1 wave 2. No invariant changes.** Every decision
below is additive to the schema; D6 is amended in wording, and the amendment is D10. Contract text:
`contracts/channel-spec/v0/README.md` sections 3.0, 3.2.2-3.2.5, 3.4.1-3.4.7, 3.5, 4.1.1, 4.3, 5.1, 10.
**The schema and its 20 new examples are unverified until the conformance run** (follow-up 7).

| # | Decision | Options considered | What it gives up |
|---|---|---|---|
| D10 | **D6 amended: connector types enter the enum at the start of the wave that builds them**, with their config shape, not when the code lands | (a) keep D6: each shape arrives with its connector; (b) publish all at wave start (chosen); (c) publish every connector ADR-001 ever names | (a) has seven loaders and three receivers inventing the contract in parallel with a runtime and an evaluator — the opposite of contract-first. (c) is invented detail. **Cost of (b):** a spec can select a type whose connector has not shipped and fail at startup rather than at validation. Loud, tracked in README 3.0, and the same trade already accepted for `ref` |
| D11 | **Kafka connectors always use an explicit connection with their own credentials**, even to the engine's own cluster, and cannot name `_tb.*` or channel-shaped topics | (a) allow "local cluster" with the engine's principal; (b) explicit only (chosen) | (a) needs READ/WRITE grants on arbitrary foreign topics for data-plane principals, which is an ACL-model change owned by ADR-004 and a human. **Cost of (b):** a same-cluster bridge needs a second credential. Channel chaining is out of set 1.1 |
| D12 | **The `fhir` destination is transaction-only** | (a) create/update/conditional operations with a no-log exception for `?identifier=`; (b) transaction only, conditions in the Bundle body (chosen); (c) leave FHIR to the `http` type | (a) puts identifiers in request lines that proxies log, rescued by a rule the Phase 0 pattern says will be missed somewhere. (c) gives FHIR no idempotency or outcome semantics. **Cost of (b):** servers without transaction support must use `http`, and plain create/update/batch are unavailable |
| D13 | **SQL sinks take a row-shaped JSON payload built by a step**; no column-mapping language in the spec | (a) declarative column mapping (`column: json:/path`); (b) row built by a step (chosen) | (a) is where configuration becomes a program the loader interprets, the same line D1 draws for inline code. **Cost of (b):** a trivial mapping still needs a step, a module and a test |
| D14 | **Each destination type may declare only the idempotency techniques it can honour**, and the SQL loaders verify theirs at startup | (a) any technique on any type, trust the declaration; (b) restrict per type in the schema, verify in the loader (chosen) | (a) lets a spec claim invariant 4 at a sink that cannot deliver it, which is a false guarantee, worse than none. **Cost:** a novel sink-side mechanism on a restricted type needs a contract change |
| D15 | **`retry-topic` stays refused; nine lift criteria are written down now**, including that a per-key channel selecting it must also say `acceptReordering: true` | (a) lift on "spike done"; (b) criteria fixed before the spike (chosen) | (a) lets the spike define its own pass mark, and invites the claim that retry-topic preserves order, which ADR-003 D1 already says it does not. **Cost:** the spike has more to show |
| D16 | **A refused Kafka-source record halts the source partition** | (a) write it to the channel's `dlq`; (b) skip and log; (c) halt (chosen) | (a) is unimplementable: the receiver's ACL is WRITE on `raw` only (`topics/v0` `security.acls`), and widening it is ADR-004's. (b) is "ACK and drop", which invariant 1 forbids. **Cost of (c):** one bad record stalls a source partition until an operator acts |
| D17 | **Per-destination steps** (`destinations[].steps`), run in the pipeline worker before `out.<dest>` | (a) one channel per sink; (b) a routing return type in the SDK (`Routed({...})`); (c) per-destination steps (chosen) | (a) cannot share one MLLP listener across two channels. (b) moves routing into code where the UI cannot see it. **Cost of (c):** the executor becomes a small tree; a failure in one destination's steps dead-letters that destination only, so a message can be partially delivered, which is already true of per-destination loaders |

Also fixed as semantics rather than as schema: the `partitionKey` schemes (README 4.3) — key bytes
exactly as they appear, no normalisation, and an unresolved key is a receiver `parse-error`, never a
null key. The no-normalisation rule is flagged in the contract as the one most worth challenging with
evidence from real senders.

**Escalated, not decided:** the SFTP client and PostgreSQL driver licences (README 10.2). To the
architect's knowledge, unverified, the common Python SFTP libraries are LGPL or EPL/GPL, which
invariant 10 does not permit, and `psycopg` 3 is LGPL-3.0. That is a human decision.

## 6. Follow-ups

1. ~~**Measure the expected-failure pairs.**~~ **DONE 2026-09-22.** `contracts/validate.py` 96 / 0
   (lead) and `--emit-expectations` (`qa-engineer`): all 21 predicted `(json_path, validator)` pairs
   matched the measured run, so no manifest needed correcting. Latest run `f5bc92d`:
   `validate.py` **100 / 0**, `pytest tests/conformance` **105 passed**. CI job wired at `f649665`.
2. **`queue.maxBytes` default** is `TODO(connector-engineer)` — the contract fixes the unit, not the
   number, and the number should come from a measurement.
3. **`retry-topic` spike** (ADR-003 D2, gate 7.2 item 18) removes the `not` in D5 — **only against
   the D15 criteria**, README 4.1.1.
4. **Claim-check spike** decides whether a channel-level knob is needed at all. Not scheduled in
   wave 2.
5. **Sync-reply fields** arrive with W2-8. ~~`partitionKey` expression semantics with W2-4.~~
   Semantics fixed in set 1.1 (README 4.3); the evaluator is W2-4.
6. ~~**Destination-level filters** were deliberately omitted.~~ A `Drop` in a destination step (D17).
7. **Measure set 1.1**: `validate.py`, `--emit-expectations`, `pytest tests/conformance`. Correct any
   predicted pair from the measurement, never the other way round.
8. **SFTP and PostgreSQL driver licences** — human decision before either dependency is added.
