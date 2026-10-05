# Phase 1, wave 3 — archive, control plane, operations

**Owner of this document:** `architect`.
**Authoritative scope:** [`phase-1.md`](phase-1.md), Wave 3 section (W3-1..W3-9 and the four exit
criteria). Where this document and `phase-1.md` disagree, `phase-1.md` wins and this document is wrong.
**Added scope:** the wave 2 backlog deferred on 2026-10-05 (STATUS ranks 4-10) and six follow-ups named
by the lead at the wave 2 close. Each added task names where it came from.
**Branch:** `wave/1-2` until the lead or a human creates `wave/1-3`. Agents never switch branches.
**Started:** 2026-10-05, on the human's instruction relayed by the lead: *"Execute them by priority and
let the lead and architect decide."* That delegates sequencing and scope, **not** the items CLAUDE.md
lists under "Escalate to a human" (section 7).
**Live status:** [`STATUS.md`](../STATUS.md). This file is the plan, not the tracker.

---

## 1. Starting position, 2026-10-05

| Item | State |
|---|---|
| Wave 2 | **Closed with one condition** (STATUS, Wave 2 exit): criteria 1-4 met; `security-reviewer` closure of the `fb60f3a` High is not yet recorded. CI green on `06fd4b6` (lead-relayed). **Not merged to `main`** (still `2c25448`); merging is a human action |
| Contracts | Sets 1.0-1.7.1. Sets 1.2-1.7 **verified 2026-10-05**: `validate.py` 156 / 0, 110 pinned pairs all matching, conformance 150 passed (`docs/spikes/wave-2-ci-confirmation.md`) |
| Packages | No `tributary-archiver`, `tributary-api` or `tributary-cli` yet. The workspace globs `packages/*`, so a new package needs no root-config change |
| Licences pre-staged | `licence_check.py` already names `tributary-archiver` as the only runtime importer allowed for `certifi` (`MPL_RUNTIME_IMPORTERS`, `BUNDLED_LICENCE_FILES`). It has not run against a real archiver |
| PostgreSQL driver | `psycopg` 3 (CLAUDE.md stack) is LGPL-3.0 and **denied** by `licence_check.py`. `asyncpg` (Apache-2.0) is in the lockfile via the `postgresql` loader |
| Not published, though promised | `contracts/observability/v0` (W2-5 ruling, 2026-09-24). The readiness probe and the Prometheus rules both need it |

**Naming.** `phase-1.md` calls W3-1 "Contracts v1.1". Contract sets 1.1-1.7 were used in wave 2, so
W3-1 publishes **contract set 1.8** (additive). Same convention as set 1.0 = "Contracts v1"
(`contracts/README.md`).

---

## 2. Tasks

Phase 1 tasks keep their IDs; three are split into `a`/`b` where the first half has no dependency on
the second. Added tasks are W3-10 onward. **Verify** is the exact command or observable result.

### 2.1 From `phase-1.md`

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W3-1a | `architect` | Set 1.8 part 1: `contracts/openapi/v0` for channels, versions, validate, deploy, rollback, health; security scheme (OIDC bearer, resource-server validation, role claims) recorded in ADR-009 | none | `uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py` reports N / 0 including the OpenAPI document; valid and invalid request/response examples load in `tests/conformance` |
| W3-1b | `architect` | Set 1.8 part 2: messages search and detail, reprocess, replay, stats, events (SSE); `_tb.<env>.audit` event shape with retention as `TODO(human)` | W3-1a, W3-2a (table shape) | Same command, new examples; `qa-engineer`'s conformance count rises by the predicted number |
| W3-2a | `archive-engineer` | `tributary-archiver`: consume `_tb.<env>.events` into ClickHouse (schema, idempotent on `tb-msg-id` + stage, TTL), `clickhouse-connect` with `query_retries=0` and its internal HTTP retries off. **No `_hmac` / identifier-derived attributes** (human-reserved) | none | `uv run pytest -m integration packages/tributary-archiver`: real ClickHouse, redelivered events counted once under `FINAL`; archiver stopped while a worker runs shows unchanged worker throughput and no blocked produce (invariant 9) |
| W3-2b | `archive-engineer` | Content storage, retention TTL, backfill from `raw` | W3-2a, W3-10 (archive and PHI sections) | Integration tests: stored content matches the stage it claims; TTL applied (`SHOW CREATE TABLE`); backfill is idempotent on a second run |
| W3-3 | `archive-engineer` | Query library, reprocess, replay. Reprocess sets `tb-parent-id`; a `dlq` record carrying `tb-destination` is reprocessed destination-scoped (ADR-003 follow-up 10) | W3-2b, W3-1b | Reprocess sets `tb-parent-id`; replay by time range republishes exactly the range; destination-scoped reprocess reaches only that destination (integration test with a two-destination control) |
| W3-4a | `control-plane-engineer` | PostgreSQL schema, Alembic migrations, repository layer for channels, versions and deployments. SQLAlchemy 2 on `asyncpg` until the human answers 7.1; the driver is isolated in the engine URL | none | `uv run pytest -m integration packages/tributary-api -k migrations`: upgrade, downgrade, upgrade on real PostgreSQL; `licence_check.py` passes |
| W3-4b | `control-plane-engineer` | FastAPI skeleton, authn per ADR-009, channel CRUD, validate, deploy, rollback | W3-1a, W3-4a | Contract tests against `contracts/openapi/v0`; with the API process stopped, a running worker keeps processing (integration test) |
| W3-5 | `control-plane-engineer` | Reconciler to `_tb.<env>.channels`; `git-managed` vs `ui-managed` modes | W3-4b | A channel change is picked up by a running worker with no restart (integration test asserts the same PID and the new `metadata.version`) |
| W3-6 | `control-plane-engineer` | Search, message detail, reprocess, replay endpoints; audit events for content views | W3-3, W3-4b, W3-1b | Contract tests against OpenAPI; an audit event is emitted for every content view, and none carries payload bytes (invariant 6) |
| W3-7 | `control-plane-engineer` | CLI: `dev`, `test`, `deploy`, `channels`, `messages` | W3-4b | CLI smoke tests against the Compose stack |
| W3-8 | `devops-engineer` | Helm chart (Strimzi values, KEDA on lag), OTel collector, Prometheus rules, Grafana dashboards. **Includes:** `--instance` from the pod name (STATUS rank 10, set 1.7); receiver readiness wiring (W3-17); an alert on `partition_halted` (`wave-2.md` 6.2, unassigned until now) | W3-12, W3-17 for the readiness and alert parts | `helm lint` and `helm template` pass; dashboards load against Compose; a rendered receiver pod has `--instance` from `metadata.name` and a readiness probe. **No image is published until W3-13 is green** |
| W3-9a | `qa-engineer` | Chaos: stop ClickHouse, PostgreSQL and the API during load (invariants 3 and 9; wave 1 gate item 19) | W3-2a, W3-4a; W3-4b for the API case | `pytest -m chaos tests/chaos -k outage_control_plane`: each service shown down (`assert_fault_fired`), throughput read against a same-host control (section 4, criterion 2), 0 order violations, mutation control red |
| W3-9b | `qa-engineer` | E2E for search, reprocess and replay; benchmark suite and committed report | W3-6, W3-3 | `pytest -m integration tests/e2e -k browser`; report in `docs/spikes/` with method, environment and raw rows |

### 2.2 Added: architect

| ID | Owner | Deliverable | From | Depends on | Verify |
|---|---|---|---|---|---|
| W3-10 | `architect` | ADR-004 draft: archive design, PHI at rest, content retention mechanics, Kafka role model. Key custody, `_hmac` construction and audit retention are written as **human questions**, not designed | ADR README "before the archiver ships"; gate item 6 | none | ADR exists as Proposed; every key-management item is a `TODO(human)` with options; `security-reviewer` has read it |
| W3-11 | `architect` | `contracts/validate.py --check-expectations`: exits 1 when any pinned `(json_path, validator)` pair does not match | lead's list (CI gap 1) | none | Exit 0 on the tree; exit 1 on a scratchpad copy with one manifest pair altered |
| W3-12 | `architect` | `contracts/observability/v0`: metric names and allowed labels (no PHI, seeded from `platform-engineer`'s names), readiness endpoint (states from `topics/v0` 3.3.1 rule 2, loopback default, warm-producer-before-ready), log line schema | W2-5 ruling 2026-09-24; STATUS rank 6 | none | `validate.py` N / 0 with examples; readiness section published **first**, because W3-17 waits on it |

### 2.3 Added: devops, connector, qa

| ID | Owner | Deliverable | From | Depends on | Verify |
|---|---|---|---|---|---|
| W3-13 | `devops-engineer` | Image-build check: refuse duplicate distribution names (ADR-007 f-up 18, D24); also check `tributary.context` registrants against the context-extension registry and `tributary.*` step entry points against `step-namespaces.json` | STATUS rank 4; sets 1.5/1.6 optional items | none | CI step passes on the real image; a fixture with two installations of one distribution name fails it (negative control) |
| W3-14 | `devops-engineer` | CI hardening: `contracts` job calls `validate.py --check-expectations` and retires `.github/scripts/check_expectations.py`; integration job fails unless `passed == expected` and `skipped == 0`, reading W3-21's file | lead's list | W3-11, W3-21 | A branch that deletes one integration test fails CI; a branch that drifts one pinned pair fails CI |
| W3-15 | `devops-engineer` | Confirm the pre-staged `certifi` archiver scope against the real `tributary-archiver`; add a negative test where another member pulls `certifi` into its runtime closure | lead's list; decisions log 2026-09-28 | W3-2a | `licence_check.py` passes with the archiver; the negative fixture fails; SBOM lists `certifi` under the archiver only |
| W3-16 | `devops-engineer` | Revoke `tb-lite-users-init` ALTER on CLUSTER after provisioning, re-run-safe; fix the `add_scram_users.py` docstring | STATUS rank 7; ADR-008 f-up 9 | W3-10 (role model section) | `docker compose down -v && up` twice; the principal's ACL list has no CLUSTER ALTER after `up`; second `up` still provisions |
| W3-17 | `connector-engineer` | Readiness probe on every receiver; producer warmed before ready (S2 finding). Listener goes to `security-reviewer` | STATUS rank 6 | W3-12 (readiness section) | Per receiver: not ready before the first `raw` metadata round trip and while the reply consumer is disconnected or `_FATAL`; ready otherwise (integration tests) |
| W3-18 | `connector-engineer` | Real-thread recovery test for the deferred channels probe (Medium, `2baa061`), asserted through readiness transitions | STATUS rank 5 | W3-17 | Integration test: a stuck spec reader makes the receiver refuse (never ACK and drop) and report not ready, then recover |
| W3-19 | `connector-engineer` | HTTP sync reply: status-mapping **contract proposal first** (no auto-retry status; `AE` = committed, outcome unknown), then build. `platform-engineer` consulted only if the worker side changes | STATUS rank 8 | architect ruling on the proposal | Latency and timeout tests as W2-8; `http` stops refusing `source.reply` only after the ruling is published |
| W3-20 | `qa-engineer` | Move the sync-reply oracle into `tests/support/`; `connector-engineer` switches `receivers/tests/reply/test_properties.py` to import it | STATUS rank 2 note | none | Both property suites green and the four broken-implementation controls red against the moved oracle (method of `wave-2-ci-confirmation.md`) |
| W3-21 | `qa-engineer` | Exact expected integration count: `tests/support/integration_expected.json` (count and per-module list). Authors of new integration tests message `qa-engineer` in the same change | lead's list | none | File matches the current `-m integration` collection exactly |
| W3-22 | `qa-engineer` | Broker-kill scenario keyed: the channel spec sets a receiver-side `partitionKey`, and the test asserts per-key order and fault fired. Closes "receiver-originated keys are not under chaos" | lead's list; duplicate-rate caveat 1 | none | `pytest -m chaos tests/chaos/test_kafka_broker_kill.py` 3 runs: 0 order violations, fault fired 3/3; row added to the duplicate-rate report |
| W3-23 | `qa-engineer` | All four receivers under their **exact** set 1.4 grants on a deny-by-default broker, with negative cases (no spec READ → refuses to start; no WRITE on a non-`raw` topic) | invariant-3 receiver item "done when", carried from wave 2; set 1.3/1.4 qa follow-ups | none | Integration tests green; each negative case shows the broker's authorization error, not a timeout |
| W3-24 | `qa-engineer` | Sync-reply tail: reproduce inside a full integration session, at least 3,000 requests per run, with receiver and broker logs captured. If it reproduces, `platform-engineer` takes the cause | STATUS rank 9 | none | Report in `docs/spikes/`: distribution per run, logs for every request over 200 ms, or "not reproduced in N requests" |

**Dependencies not in the phase table but real:** W3-2b's content storage needs ADR-004's PHI-at-rest
section; W3-8's readiness wiring needs W3-17; W3-14 needs W3-11 and W3-21; W3-16 needs ADR-004's role
model.

---

## 3. Execution sequence

Seats per batch are capped at six (`phase-1.md` notes): `architect`, `archive-engineer`,
`control-plane-engineer`, `devops-engineer`, `qa-engineer`, `connector-engineer`. `platform-engineer`
and `healthcare-standards-engineer` are on call as subagents. Tasks in one batch have different owners
or are listed in the order the owner runs them. A batch starts when its dependencies land, not when the
whole previous batch is done.

Priority tiers, as in wave 2: blocks exit, then invariant or security, then contract, then deferrable.

### Batch 1 — start now, every dependency exists

| Order | ID | Owner | Why now |
|---|---|---|---|
| 1 | W3-1a | `architect` | Blocks W3-4b, W3-6, W4-1. Contract-first |
| 2 | W3-23 | `qa-engineer` | Security tier: the ACL table has been wrong on every first run (STATUS risk) |
| 3 | W3-13 | `devops-engineer` | Required before any image is published (D24) |
| 4 | W3-2a | `archive-engineer` | Exit criteria 1 and 2 need the archive |
| 5 | W3-4a | `control-plane-engineer` | Exit criteria 2 and 3 need PostgreSQL; no OpenAPI dependency |
| 6 | W3-12, then W3-17 | `architect`, `connector-engineer` | Readiness: contract section first, then the probe. Connector builds the state machine and tests while the section lands |
| 7 | W3-11 | `architect` | Small; unblocks W3-14 |
| 8 | W3-22, W3-20, W3-21 | `qa-engineer` (after W3-23) | Test-evidence gaps from wave 2 |
| 9 | W3-10 | `architect` | Blocks W3-2b; human questions go out from here |

Architect order: W3-11 (small), W3-1a, W3-12 readiness section, W3-10, rest of W3-12.

### Batch 2 — after W3-1a, W3-2a, W3-4a, W3-11, W3-12

W3-1b (`architect`); W3-4b (`control-plane-engineer`); W3-2b (`archive-engineer`, once ADR-004's PHI
section is drafted); W3-14, then W3-15, then start W3-8 (`devops-engineer`); W3-9a ClickHouse and
PostgreSQL cases, then W3-24 (`qa-engineer`); W3-18, then the W3-19 proposal (`connector-engineer`).

### Batch 3 — after W3-4b, W3-2b, W3-1b

W3-3 (`archive-engineer`); W3-5, then W3-7 (`control-plane-engineer`); W3-8 continued, then W3-16
(`devops-engineer`); W3-9a API case (`qa-engineer`); W3-19 build after the ruling
(`connector-engineer`); the architect rules on proposals and chases section 7.

### Batch 4 — after W3-3

W3-6 (`control-plane-engineer`); W3-9b (`qa-engineer`); `security-reviewer` on API authn, audit and
PHI (exit criterion 4); `code-reviewer` on the wave diff; architect gate report.

---

## 4. Exit criteria

Verbatim from `phase-1.md`, with the reading this wave is gated on.

| # | Criterion | What counts |
|---|---|---|
| 1 | Message browser API returns per-stage content and destination statuses for a test run | W3-9b: a synthetic message sent through MLLP, then read through the W3-6 endpoints. Every stage's content and each destination's status is present, against real ClickHouse, and the responses validate against `contracts/openapi/v0` |
| 2 | Workers process at full rate with ClickHouse, PostgreSQL and the API stopped | W3-9a: five runs with all three stopped and five control runs on the same host. Pass when the stopped-run median is inside the control runs' min-max range. Any shortfall is reported, not rounded away. Each outage shown to have fired |
| 3 | Deploy and rollback of a channel version works through the API and CLI | W3-4b, W3-5, W3-7: deploy v2, observe the worker on v2 with no restart, roll back, observe v1. Once through the API and once through the CLI |
| 4 | `security-reviewer` clean on authn, audit and PHI handling of the API | No unresolved Critical or High. Needs the audit retention answer (7.2) before the reviewer can close audit |

**Also gate-tracked (not phase-1 criteria):** no image published before W3-13 is green; ADR-004
drafted with its human questions listed; the wave 2 condition (`fb60f3a` closure) recorded.

---

## 5. Contracts this wave

Set 1.8, additive: `contracts/openapi/v0` (W3-1a/b), `contracts/observability/v0` (W3-12), the audit
event shape (W3-1b), `validate.py --check-expectations` (W3-11). Expected proposals: the HTTP sync
reply status mapping (W3-19). Every count is a prediction until `qa-engineer` measures it.
Contract changes during the wave: message the architect with the exact diff and the reason; breaking
changes need an ADR.

---

## 6. Carried, not scheduled in a batch

| Item | Owner | Why not scheduled |
|---|---|---|
| `retry-topic` spike report (W2-4); the refusal stays | `platform-engineer` | Not filed. "Stays refused" is an accepted outcome; file it when on call |
| Claim-check spike | lead decides | Unscheduled since wave 2 (`wave-2.md` 5.2) |
| Operator clear or skip for a halted partition | none yet | A skip is a drop; it needs a contract and an audit event first. Candidate for W3-7 only if the lead adds it |
| W2-2 item 4: FHIR R4 schema validation of golden files | `healthcare-standards-engineer` | Waits on the human's network allow-list (7.5) |
| RF=3 re-run on dedicated capacity | `qa-engineer` | Cloud spend (7.6); W4-6 |

---

## 7. Open decisions — human only

| # | Decision | Blocks | Default until answered |
|---|---|---|---|
| 7.1 | `psycopg` 3 (LGPL-3.0): approve as an unmodified dependency, or amend the CLAUDE.md stack table to `asyncpg` | Nothing, given the default | W3-4a builds on `asyncpg` (Apache-2.0, already in the lockfile), driver isolated in the engine URL |
| 7.2 | Audit retention for `_tb.<env>.audit` and the archive's audit tables (30 d vs a possible six-year duty; legal reading not done by agents) | Exit criterion 4 (audit) | Shape published; retention `TODO(human)` |
| 7.3 | `_hmac` keyed-hash mechanism. The 2026-09-21 decision deferred it "until before the archiver is built"; the archiver starts now. Decide now, or extend the deferral. Also: is a keyed hash of an MRN de-identified for your purposes? | Per-patient search (W3-6, W4-3) | Archiver ships with no identifier-derived attributes; per-patient search stays refused |
| 7.4 | ADR-004 key custody: encryption at rest for archive volumes and the claim-check bucket; the `_hmac` key | Production use of the archive, not the build | Operator-provided volume encryption (`topics/v0` 3.5); no agent designs keys |
| 7.5 | Add `hl7.org` or `build.fhir.org` to the network allow-list | W2-2 item 4 | Golden files stay unchecked against R4 |
| 7.6 | Paid dedicated capacity for an RF=3 re-run | Meaningful latency claims (W4-6) | Single-broker numbers, labelled as such |
| 7.7 | Merge `wave/1-1` and `wave/1-2` into `main` | Nothing in wave 3 | Wave 3 builds on `wave/1-2` |
| — | Carried: Mirth corpus; project name and trademark (cost rising: the `tributary.dev` identifier system is written into FHIR resources); ADR-006; OIE engagement | Release, migration estimate | — |

Not escalated, decided here: API authn is OIDC bearer validation with a vetted library (ADR-009, W3-1a),
not a designed scheme. Any new dependency still passes `licence_check.py` and, for auth, `security-reviewer`.

---

## 8. Risks for this wave

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `clickhouse-connect` re-sends silently (`query_retries`, internal HTTP retries) | Medium | High | Both off in W3-2a; review targets library defaults (`wave-2.md` 6.3) |
| The archiver ends up in the message path (back-pressure, shared producer, a blocking emit) | Low | High: invariant 9 | W3-2a verify; W3-9a mutation control |
| API responses or audit events carry PHI (search terms, identifiers in query strings) | Medium | High: invariant 6 | Identifier search refused until 7.3; audit records ids only; `security-reviewer` at exit |
| Content storage puts PHI at rest before key custody is decided | High | Medium | Synthetic data only in wave 3; 7.4 gates production, not the build |
| A new client library default breaks a guarantee (SQLAlchemy pool retries, FastAPI error bodies echoing input) | Medium | High | Same review focus as wave 2 |
| The exact-count CI gate creates friction for every new integration test | High | Low | One-line edit to a qa-owned file in the same change. **Give up:** convenience; **gain:** a silently dropped module fails CI |
| Seven owners want seats | Medium | Medium | Six-seat cap; `platform-engineer` and `healthcare-standards-engineer` on call |
