# Phase 1, wave 3 — archive, control plane, operations

**Owner of this document:** `architect`.
**Authoritative scope:** [`phase-1.md`](phase-1.md), Wave 3 section (W3-1..W3-9 and the four exit
criteria). Where this document and `phase-1.md` disagree, `phase-1.md` wins and this document is wrong.
**Added scope:** the wave 2 backlog deferred on 2026-10-05 (STATUS ranks 4-10) and six follow-ups named
by the lead at the wave 2 close. Each added task names where it came from.
**Branch:** **`wave/1-3`** from 2026-10-06 for all further wave 3 work. The human instructed: merge
`wave/1-1` and `wave/1-2` into `main`, and open a PR for `wave/1-3`. The lead does the git side, and the
architect has not seen the merge or the PR. Agents never switch branches.
**Started:** 2026-10-05, on the human's instruction relayed by the lead: *"Execute them by priority and
let the lead and architect decide."* That delegates sequencing and scope, **not** the items CLAUDE.md
lists under "Escalate to a human" (section 7).
**Live status:** [`STATUS.md`](../STATUS.md). This file is the plan, not the tracker.

---

## 1. Starting position, 2026-10-05

| Item | State |
|---|---|
| Wave 2 | **Closed; condition lifted 2026-10-05** (STATUS, Wave 2 exit): criteria 1-4 met; `security-reviewer` closed `fb60f3a` and three other findings at `b6b0c97`. CI green on `06fd4b6` (lead-relayed). **Not merged to `main`** (still `2c25448`); merging is a human action |
| Contracts | Sets 1.0-1.7.1. Sets 1.2-1.7 **verified 2026-10-05**: `validate.py` 156 / 0, 110 pinned pairs all matching, conformance 150 passed (`docs/spikes/wave-2-ci-confirmation.md`) |
| Packages | No `tributary-archiver`, `tributary-api` or `tributary-cli` yet. The workspace globs `packages/*`, so a new package needs no root-config change |
| Licences pre-staged | `licence_check.py` already names `tributary-archiver` as the only runtime importer allowed for `certifi` (`MPL_RUNTIME_IMPORTERS`, `BUNDLED_LICENCE_FILES`). It has not run against a real archiver |
| PostgreSQL driver | `psycopg` 3 (CLAUDE.md stack) is LGPL-3.0 and **denied** by `licence_check.py`. `asyncpg` (Apache-2.0) is in the lockfile via the `postgresql` loader. **Decided 2026-10-06: `asyncpg`** (2.7 D1) |
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
| W3-1b | `architect` | Set 1.8 part 2: messages search and detail, reprocess, replay, stats, events (SSE); `_tb.<env>.audit` event shape. **Amended 2026-10-06 (2.7 D3):** retention is no longer `TODO(human)`. Add the read-only effective-retention `GET`, the events-archiver READ LITERAL `_tb.<env>.audit` grant in `topics/v0` 3.4.1 and `topic-defaults.json`, and the `audit_log` row shape | W3-1a, W3-2a (table shape) | Same command, new examples; `qa-engineer`'s conformance count rises by the predicted number |
| W3-2a | `archive-engineer` | `tributary-archiver`: consume `_tb.<env>.events` into ClickHouse (schema, idempotent on `tb-msg-id` + stage, TTL), `clickhouse-connect` with `query_retries=0` and its internal HTTP retries off. **No `_hmac` / identifier-derived attributes** (human-reserved) | none | `uv run pytest -m integration packages/tributary-archiver`: real ClickHouse, redelivered events counted once under `FINAL`; archiver stopped while a worker runs shows unchanged worker throughput and no blocked produce (invariant 9) |
| W3-2b | `archive-engineer` | Content storage, retention TTL, backfill from `raw` | W3-2a, W3-10 (archive and PHI sections) | Integration tests: stored content matches the stage it claims; TTL applied (`SHOW CREATE TABLE`); backfill is idempotent on a second run |
| W3-3 | `archive-engineer` | Query library, reprocess, replay. Reprocess sets `tb-parent-id` and copies `tb-received-at` (`envelope/v0` 6 rule 5). **Amended 2026-10-06 (2.5 H5):** a `dlq` record carrying `tb-destination` (or `tb-parent-id`, until the discriminator) is **refused**, not reprocessed; destination-scoped re-entry is carried to wave 4 (section 6) | W3-2b, W3-1b; W3-29 (`reprocessor` role) | Reprocess sets `tb-parent-id`; replay by time range republishes exactly the range; a `dlq` record carrying `tb-destination` is refused with W3-1b's problem code, and a two-destination integration test shows **neither** destination receives anything (control: a pipeline-stage `dlq` record reprocesses and reaches both). **Added 2026-10-06 (2.6 H8):** replay into a per-key channel is refused without `allow_reorder`, and accepted with it, the flag appearing in the audit record; replay never targets another channel |
| W3-4a | `control-plane-engineer` | PostgreSQL schema, Alembic migrations, repository layer for channels, versions and deployments. SQLAlchemy 2 on `asyncpg` (**decided 2026-10-06**, 2.7 D1); the driver stays isolated in the engine URL | none | `uv run pytest -m integration packages/tributary-api -k migrations`: upgrade, downgrade, upgrade on real PostgreSQL; `licence_check.py` passes |
| W3-4b | `control-plane-engineer` | FastAPI skeleton, authn per ADR-009, channel CRUD, validate, deploy, rollback | W3-1a, W3-4a | Contract tests against `contracts/openapi/v0`; with the API process stopped, a running worker keeps processing (integration test) |
| W3-5 | `control-plane-engineer` | Reconciler to `_tb.<env>.channels`; `git-managed` vs `ui-managed` modes | W3-4b | A channel change is picked up by a running worker with no restart (integration test asserts the same PID and the new `metadata.version`) |
| W3-6 | `control-plane-engineer` | Search, message detail, reprocess, replay endpoints; audit events for content views | W3-3, W3-4b, W3-1b | Contract tests against OpenAPI; an audit event is emitted for every content view, and none carries payload bytes (invariant 6) |
| W3-7 | `control-plane-engineer` | CLI: `dev`, `test`, `deploy`, `channels`, `messages` | W3-4b | CLI smoke tests against the Compose stack |
| W3-8 | `devops-engineer` | Helm chart (Strimzi values, KEDA on lag), OTel collector, Prometheus rules, Grafana dashboards. **Includes:** `--instance` from the pod name (STATUS rank 10, set 1.7); receiver readiness wiring (W3-17); an alert on `partition_halted` (`wave-2.md` 6.2, unassigned until now). **Amended 2026-10-06 (section 2.5, H1-H4):** every data-plane workload a StatefulSet; `KafkaUser` CRs generated from `topic-defaults.json`; no `tributary-contracts` ConfigMap; ops port, liveness probe and scrape for worker, loader and archiver once W3-25..W3-27 land | W3-12, W3-17 for the readiness and alert parts; W3-25..W3-27 for scrape and probes; W3-29 for the `lag_reader` user | `helm lint` and `helm template` pass; a rendered receiver pod has `--instance` from `metadata.name` and a readiness probe; `helm_check.py` asserts `kind: StatefulSet`, `podManagementPolicy: Parallel`, no `volumeClaimTemplates`, ScaledObject `scaleTargetRef.kind: StatefulSet`, rendered `KafkaUser` ACLs equal to `topic-defaults.json` `security.acls`, and no `contracts` volume. **Dashboards and the `partition_halted` alert are verified against a real scrape**, not only loaded: a forced halt on Compose shows `tb_partitions_halted 1` and the alert fires. **No image is published until W3-13 is green** |
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

### 2.4 Added 2026-10-06: from `devops-engineer`'s W3-8 open risks (rulings in 2.5)

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W3-25 | `platform-engineer` (on call; lead schedules) | `tributary.runtime.ops`: the ops listener moved from `receivers/ops.py` with O1-O6 unchanged, plus a `GET /metrics` exporter bound to `observability/v0` `metrics.json` (unregistered name or label refused at registration). Worker wired: `/livez` and `/metrics`, including `tb_partitions_halted`, `tb_spec_version`, `tb_messages_total`. No worker `/readyz` (`observability/v0` 1) | none | Two-way test, registry vs `metrics.json` (as `test_codes_contract.py`); a broken-registry control goes red; worker integration test: a forced halt scrapes `tb_partitions_halted{...} 1`, and a redeploy moves `tb_spec_version`. Listener change goes to `security-reviewer` |
| W3-26 | `connector-engineer` | Receivers: `/metrics` on the existing ops listener via W3-25. All eight loaders: ops listener with `/livez` and `/metrics`, same flags (`--ops-bind`, `--ops-port`, O2) | W3-25 | Per receiver and loader type: scrape returns only registered names; the receivers' existing readiness tests stay green; `tb_deliveries_total` counts a redelivery |
| W3-27 | `archive-engineer` | Archiver: ops listener with `/livez` and `/metrics` (`tb_archiver_*`, `tb_commits_total`, `tb_build_info`) | W3-25 | Integration test scrapes the counters after a batch; invariant 9 test unchanged (archiver stopped, worker throughput unchanged) |
| W3-28a-d | `platform-engineer` (a, worker), `connector-engineer` (b, loaders), `archive-engineer` (c, archiver), `devops-engineer` (d, Compose) | **No pid-derived instance default.** `pipeline-<pid>`, `loader-<dest>-<pid>` and `archiver-<pid>` are identical in every container (pid 1), so replicas fence each other: the defect set 1.7 fixed for receivers. Refuse to start without `--instance-id` / `TB_INSTANCE_ID` (`startup_refused`, `code: INSTANCE_REQUIRED`, exit 2). Compose sets one per service | W3-29 text | Unit test per entry point: no instance → exit 2; Compose `up` starts every service |
| W3-29 | `architect` | Set 1.8 additions: (1) `lag_reader` role in `topics/v0` 3.4.1 and `topic-defaults.json`: DESCRIBE on topics PREFIXED `<env>.` and LITERAL `_tb.<env>.events`, DESCRIBE on groups PREFIXED `tb.<env>.`; no READ, no WRITE. Used by KEDA and the lag exporter. (2) `topics/v0` 3.3: the instance rule restated for worker, loader and archiver (both spellings, required, unique per replica, the instance is `group.instance.id` verbatim). (3) `INSTANCE_REQUIRED` named as a startup-refusal code (L6a, not registered). (4) **`reprocessor` role** (H5; **amended 2026-10-06, 2.6 H8**): READ, WRITE and DESCRIBE on LITERAL `<env>.<ch>.raw` per channel, IDEMPOTENT_WRITE on the cluster, nothing else. READ was added because reprocess and replay fetch the original payload from `raw` (W3-3 security review, lead-relayed). It assigns partitions and never commits. ~~A LITERAL group only if needed~~ **Measured by `archive-engineer` (2026-10-06): a `group.id` is required and FindCoordinator is sent, so the role gets DESCRIBE (not READ) on LITERAL group `tb.<env>.reprocessor`. Items 1 and 4 PUBLISHED 2026-10-06** in `topic-defaults.json`, README 3.4.1/3.6 and the schema `$defs` (CHANGELOG); **items 2 and 3 PUBLISHED 2026-10-12** (`topics/v0` 3.3 rules 1-6, `observability/v0` L6a; README only). Not PREFIXED `<env>.`, which would also grant WRITE on `out`, `dlq` and `reply`. Its producer enforces the receivers' 1,000,000-byte input limit, because an oversize `raw` record halts the partition. Human sign-off folds into ADR-004 H7 | none | `validate.py` N / 0; **UNVERIFIED until W3-23-style test:** KEDA reads lag under `lag_reader` alone, and reprocess publishes under `reprocessor` alone, on a deny-by-default broker (`qa-engineer`, added to W3-23 scope) |

**W3-4b amendment (`control-plane-engineer`):** the API serves `/metrics` (`tb_api_*`, `route` = OpenAPI
template) on its own listener; it enters the criterion 4 review.

### 2.5 Rulings on the W3-8 open risks, 2026-10-06

- **H1. Metrics and ops listeners.** No contract change: `observability/v0` 1 already makes `/livez` and
  `/metrics` normative for every data-plane process, the archiver and the API, and `/readyz` normative
  for receivers only. The gap is code. One shared implementation in `tributary-runtime` (W3-25), not four
  copies; owners per package (W3-26, W3-27, W3-4b). `prometheus-client` is permitted **for exposition
  only** (`generate_latest` on an explicit `CollectorRegistry`): its global `REGISTRY` exports unregistered
  `process_*`/`python_*` names, and its HTTP server bypasses O5. It passes `licence_check.py` and
  `security-reviewer` (dependency) first. **Exit criteria: blocks neither.** Criterion 2 is measured by
  W3-9a's own harness (no `tests/chaos` module reads Prometheus, grep 2026-10-06); criterion 4 covers API
  authn, audit and PHI, and API `/metrics` joins that review if it ships. **New gate-tracked item:**
  `tb_partitions_halted` exported and the `partition_halted` alert shown firing (W3-25, W3-8). A halted
  partition loses nothing but stalls silently, and the wave 2 residual assumed an operator learns of it.
  **Give up:** W3-8 cannot close on `helm template` alone.
- **H2. `--instance` vs `--instance-id`.** Consistent in **value and rule** (pod name via
  `metadata.name`, unique per replica; `helm_check.py` asserts both), inconsistent in **spelling**. Kept as
  two spellings in v0, written down in W3-29. Rejected: renaming ten entry points, `tests/support` and
  Compose for cosmetics. Real defect found while ruling: pid defaults (W3-28). **Give up:** operators
  learn two flag names.
- **H3. StatefulSets.** **Yes, for worker, loader, archiver and receivers**, `podManagementPolicy:
  Parallel`, no `volumeClaimTemplates`, one headless Service. Evidence: same-id restart recovers in
  **3.21 s** for 2.0 s downtime (S5); a static member that never returns holds its partitions until
  `session.timeout.ms` (S4: 43.8 s at the default; ours is 15 s, S5 takeover 10.01/11.32 s at 10 s), and a
  new-name pod then forces a rebalance. So a Deployment rollout stalls each ordered partition for about 15 s
  instead of about 3 s, worse than dynamic membership. KEDA scales any `/scale` target, StatefulSets
  included (`scaleTargetRef.kind: StatefulSet`, KEDA docs). **Give up:** no `maxSurge` (capacity -1 during
  a rollout); a pod on a lost node is not recreated until the node is removed (the group still recovers
  after 15 s); scale-down removes the highest ordinal, whose partitions wait 15 s (as a Deployment
  scale-down does); one release name per `env` per Kafka cluster, or ids collide (chart README).
  **Unverified:** the 15 s rollout stall is derived, not measured on Kubernetes.
- **H4. Chart prerequisites.** `kafka.roleSecretRefs`: with `strimzi.enabled` the chart renders one
  `KafkaUser` per role, ACLs **generated** from `topic-defaults.json` by the parser Compose uses, with a CI
  drift check, never hand-written (the set 1.4 lesson). With external Kafka the operator creates them; the
  chart README lists names, keys and grants. Follows today's per-role `topics/v0` 3.4.1; re-render if
  ADR-004's per-channel principals are approved (H7). `keda.authSecretRef`: the new `lag_reader`
  principal (W3-29), rendered as a `KafkaUser` like the others, otherwise operator-created. It can never
  read a payload. `tributary-contracts` ConfigMap: **removed**. Each image ships `contracts/` with
  `TB_CONTRACTS_DIR` set (devops, image build next to W3-13). `topic-defaults.json` carries invariant-1
  producer values that the effective-config test pins to the code version, and an operator-built
  ConfigMap from another checkout diverges silently. **Give up:** retuning a client default needs a new
  image, which a contract change needs anyway.

### 2.6 Rulings on `archive-engineer`'s W3-3 questions, 2026-10-06

- **H5. Destination-scoped reprocess: not needed in wave 3; the refusal stays.** `phase-1.md` W3-3
  asks only "reprocess sets `tb-parent-id`; replay by time range works", and no exit criterion uses
  reprocess. Wave 3 had added the destination-scoped verify line, and it was premature. Re-injecting a
  destination-tagged record into `raw` re-delivers to every destination under a new id (ADR-003 f-up
  10), so refusal is the only safe wave 3 behaviour. **Direction for wave 4, recorded so it is not
  relitigated:** two re-entry points, chosen by the failure stage. Loader exhaustion re-enters on
  `out.<dest>`: the payload is the `out` record, and `out` is an existing durability boundary, so
  invariant 2 is untouched. A destination-step failure has the step input as payload (channel-spec 3.5),
  so it needs the worker to run only that destination's steps, through an additive `raw` header, not a
  new topic class (a new class would be an invariant 2 change). Both need WRITE beyond `raw` for the
  `reprocessor`, which is an ADR-003 amendment plus ADR-004 H7. Rejected: an unscoped `raw` re-injection
  with an operator warning (silent duplicate side effects). **Give up:** in wave 3, a destination
  failure is fixed by replaying or by the operator re-sending, not by a reprocess button.
- **H6. `FINAL` wording:** corrected in `events/v0` 4.1 rule 2. The engineer is right, and `LIMIT 1 BY`
  stays.
- **H7. `tb-received-at` on reprocess: copied unchanged** (the engineer's reading; `envelope/v0` 6 rule
  5 now says so). `republish.py` already does this.
- **H8. Reprocess and replay read `raw`; the lead's replay ruling (2026-10-06, lead-relayed from the
  W3-3 security review).** (a) The `reprocessor` gets LITERAL READ on each channel's `raw`, beside its
  WRITE and DESCRIBE (W3-29 item 4). It never commits offsets. It holds DESCRIBE only on LITERAL group
  `tb.<env>.reprocessor`, because `archive-engineer` measured that assign still needs a `group.id`
  and FindCoordinator. The topic grant stays LITERAL, never PREFIXED, and never `<env>.`-wide. **Give up:** the role now reads PHI on every
  channel it is granted, which it must do to republish. It folds into ADR-004 H7. (b) **Lead's
  ruling, recorded:** replay targets only its source channel. There is no cross-channel republish, so
  the grant is per channel and a record never moves between channels' `raw`. (c) **"Shadow" keeps the
  contract's consumer-group meaning** (`topics/v0` 3.6: a `.shadow-<label>` group on the same `raw`).
  Replay is not a shadow mechanism, and no replay feature reuses the word. (d) **A replay into a
  per-key channel needs an explicit `allow_reorder` flag, recorded in the audit event**, because
  republishing a time range interleaves with live traffic per key (invariant 5: never reorder
  silently). Without the flag, the request is refused with a W3-1b problem code. The flag goes into
  the W3-1b replay request shape and the W3-3 verify: refused without it, and accepted with it with
  the flag in the audit record.

### 2.7 Human answers to section 7, 2026-10-06, and the decisions they delegated

**Answers, attributed to the human, 2026-10-06, relayed by the lead.** Verbatim as relayed:

| # | Question | Human's answer |
|---|---|---|
| 7.1 | `psycopg` vs `asyncpg` | *"let the lead and architect decide"* |
| 7.3 | `_hmac` search attributes | *"let the lead and architect decide"* |
| 7.2 | Retention for audit records and archived events | *"Give the user a possibility to change it"* |
| 7.4 | Archive key custody | *"Let the lead, architect and security engineer decide"* |
| 7.6 | 3-broker test cluster | *"find alternative free solution"* |
| 7.7 | Merge | Merge `wave/1-1` and `wave/1-2` into `main`, and open a PR for `wave/1-3` (the lead's wording; no verbatim text relayed) |

Not answered: 7.5 (network allow-list), ADR-004 H6 (erasure) and H7 (role-model sign-off), and the
carried items.

- **D1. PostgreSQL driver: `asyncpg`** (lead and architect). Options: `psycopg` 3 as an approved
  unmodified LGPL dependency, or `asyncpg`. Criteria: invariant 10, what already ships, asyncio fit,
  SQLAlchemy 2 and Alembic support. `asyncpg` is Apache-2.0 and already in the lockfile, in
  `tributary-api` (W3-4a) and in the `postgresql` loader. `licence_check.py` passes it and denies
  `psycopg`. SQLAlchemy 2 ships a `postgresql+asyncpg` dialect, and Alembic has an async `env.py`
  template. `psycopg` would need a fresh LGPL approval, which is an invariant-10 escalation, for no
  functional gain. **Give up:** `psycopg`'s single sync-and-async driver (sync tools such as Alembic
  run through the async engine). With PgBouncer in transaction mode, asyncpg's prepared-statement
  cache must be off (`prepared_statement_cache_size=0` in SQLAlchemy). That is an operator-docs item
  for `control-plane-engineer`. **CLAUDE.md is human-owned. Proposed one-line stack-table change, for
  the lead to apply under this delegation:**
  `| Control plane | FastAPI, SQLAlchemy 2, asyncpg (not psycopg: LGPL-3.0), Alembic, PostgreSQL |`
- **D2. `_hmac`: DEFERRED** (lead and architect). Revisit trigger: H1 concurrence, and before any
  per-patient search task (W4-3). `*_hmac` values are treated as PHI whatever their construction, so
  the design never depends on de-identification. Reasoning and constraints: ADR-004 H3. Nothing changes
  in code: the archiver keeps dropping `*_hmac`.
- **D3. Retention: operator-configurable** (lead and architect, per the human's answer). Defaults:
  events `TB_ARCHIVE_TTL_DAYS` 90; content `TB_ARCHIVE_CONTENT_TTL_DAYS` 30 (W3-2b); audit
  `TB_ARCHIVE_AUDIT_TTL_DAYS` 2190. Integer days, 1-36500. Audit system of record: ClickHouse
  `audit_log`, with `_tb.<env>.audit` as a 30 d buffer. Changes are applied only by the migrate step
  (`MODIFY TTL`), and a decrease is refused without `TB_ARCHIVE_ALLOW_TTL_DECREASE=true`. Helm
  `archiver.retention.*`; the API view is read-only. Full spec: ADR-004 section 4 (R1-R8); contract
  text: `topics/v0` 2.1, `events/v0` 4.1 rule 4. **Drift found:** the chart sets `archiver.ttlDays: 365`
  against the documented 90 (W3-31 removes it). **Give up:** a retention change needs a redeploy.
- **D4. Key custody: DECIDED in ADR-004 H1 (with H2 and H5); `security-reviewer` CONCUR WITH
  CONDITIONS C1-C8 (ADR-004 7.1, lead-relayed 2026-10-06), tasks W3-34..W3-36.** Recommendation: Tributary manages no keys. `production` requires customer-managed keys
  (KMS CMK or CMEK, or HSM or Vault on premises) on every PHI-at-rest surface, and lite may use
  provider-managed keys. H2 is none beyond volume encryption in v0. H5: file-based secrets filled by
  the operator's store, with 90-day recommended cadence (judgement). The lead obtains the review next.
- **D5. RF=3 testing without paid capacity: W3-33a/b** (below). A 3-broker KRaft cluster on the free
  CI runner and on developer hosts. It proves correctness under RF=3 and `min.insync.replicas=2`, and
  the RF=3-to-RF=1 cost *ratio* on identical hardware. It proves no absolute latency or sizing figure.
  W4-6 (dedicated capacity) stays an optional, human cloud-spend item.

**What the free cluster can and cannot prove (D5):**

| Can prove | Cannot prove |
|---|---|
| No ACK unless two replicas hold the record; NACK or refuse when ISR < 2 (invariant 1 at RF=3) | Absolute ACK p99 or msg/s for sizing: shared vCPUs, noisy neighbours |
| No ACKed loss across a leader kill and a rolling restart; unclean election stays off | Real replication cost: all brokers share one kernel and one disk, so there is no inter-broker network RTT and fsync contention is unrealistic (the bias could go either way) |
| Per-key order through leader changes; idempotent producer leaves no duplicate `tb-msg-id` on `raw` | Cross-zone latency, partition capacity at scale (S4's numbers stay as they are), long soaks |
| RF=3 vs RF=1 ACK latency *ratio*, same runner, same run, interleaved | Behaviour under real network partitions (out of scope here; `docker network disconnect` is a later add) |

### 2.8 Tasks added 2026-10-06 (from 2.7)

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W3-30 | `archive-engineer` | Retention in the archiver (ADR-004 R2, R6, R7): (1) the migrate step applies every archive table's TTL from its env var: an increase is applied with `MODIFY TTL` (`materialize_ttl_after_modify` left at 1); a decrease exits non-zero with `RETENTION_DECREASE_REFUSED` unless `TB_ARCHIVE_ALLOW_TTL_DECREASE=true`; it writes an `archive_settings` row and logs `retention_applied`. Archivers stop reading TTL vars. (2) `audit_log` table plus the events archiver consuming `_tb.<env>.audit`, TTL from `TB_ARCHIVE_AUDIT_TTL_DAYS` (2190). (3) `TB_ARCHIVE_CONTENT_TTL_DAYS` (30) when W3-2b creates `message_content` | (1) none; (2) W3-1b audit shape and grant; (3) W3-2b | `uv run pytest -m integration packages/tributary-archiver -k retention`, real ClickHouse: migrate 30 → 60 makes `SHOW CREATE TABLE` show 60, and a part written before the change shows `system.parts.delete_ttl_info_max` moved by 30 days. 60 → 20 without the flag exits non-zero with the TTL still 60; with the flag it becomes 20. `archive_settings` has one row per applied change; 0 and 36501 refused at start |
| W3-31 | `devops-engineer` | Helm `archiver.retention.{eventsDays: 90, contentDays: 30, auditDays: 2190, allowDecrease: false}` into the migrate Job and archiver env; `archiver.ttlDays` removed, render `fail`s if set. Compose: the same four vars and defaults on the migrate service, which runs before the archiver. Topic bootstrap creates `_tb.<env>.audit` from `topic-defaults.json` (flip `test_bootstrap_topics.py`, fix its docstring). Chart README: "Changing retention" (ADR-004 R6, R1 cost) and the H1 custody checklist (customer-managed keys in production, claim-check KMS permissions, content-archiver Secret) | W3-30 (1) for the end-to-end check | `helm template` renders the four vars on both workloads with the R2 defaults; `--set archiver.ttlDays=365` fails render; `helm_check.py` asserts the defaults; Compose `up` creates `_tb.dev.audit`; a Compose re-run with a lower `TB_ARCHIVE_TTL_DAYS` stops at the migrate service with the refusal |
| W3-32 | `control-plane-engineer` | Production `AuditHook`: each audit record produced to `_tb.<env>.audit` (`acks=all`, idempotent) in the W3-1b shape. `requested` fails closed, so a content view or re-publish that cannot be recorded does not happen. Read-only effective-retention `GET` from `archive_settings`. W3-6 uses the hook | W3-1b, W3-30 (1) | Integration: with Kafka stopped, a content view returns the W3-1b problem code and reads no content. With Kafka up, exactly one record per view lands on the topic with no payload bytes (invariant 6). The `GET` returns what the migrate step wrote; contract tests pass against `openapi/v0` |
| W3-33a | `devops-engineer` | Free RF=3 cluster, infrastructure: `deploy/compose/docker-compose.rf3.yml` override for developer hosts (3 KRaft nodes, SASL_SSL and deny-by-default ACLs as lite, loopback-bound, heap 512 MiB, `mem_limit` 1 GiB each). A CI job `chaos-rf3` on `ubuntu-latest`, triggered by `workflow_dispatch` plus a schedule (nightly on a public repo; weekly if private, see budget), 45 min timeout, running `pytest -m "chaos and rf3"`, uploading the report rows and a `docker stats` sample | none | `workflow_dispatch` run green on `wave/1-3`; job log shows peak container memory under 5 GiB; on a developer host, metadata lists 3 brokers and `raw` has `min.insync.replicas=2` read from `topic-defaults.json` |
| W3-33b | `qa-engineer` | Free RF=3 cluster, tests: `tests/support/kafka_cluster.py`, promoted from `test_kafka_broker_kill.py`, with broker and topic values read from `topic-defaults.json`, not literals. Topologies: `combined` (3 nodes) and `split` (3 brokers + 1 controller), with marker `rf3`. **C1** two of three brokers stopped (`split`, so the quorum survives): every message sent during the outage is NACKed or refused, and 0 ACKed are lost after recovery. Fault fired: ISR = 1 in metadata and a not-enough-replicas or timeout error seen by the receiver's producer. Mutation control: a `min.insync.replicas=1` topic ACKs during the same outage and the test goes red. **C2** rolling restart of all three under keyed MLLP load: 0 ACKed lost, 0 per-key order violations, duplicates counted. **C3** leader SIGKILL mid-batch: distinct `tb-msg-id` count on `raw` equals the record count. **B1** (`bench`) S2 ACK p50/p99 at about 1k msg/s, RF=1 vs RF=3, 3+3 interleaved runs on one runner, reported as a ratio | W3-33a for the CI job (tests run locally first) | `uv run pytest -m "chaos and rf3" tests/chaos`, 3 runs: fault fired 3/3 per scenario, 0 ACKed lost, 0 order violations; C1 control red. Report `docs/spikes/wave-3-rf3-free-cluster.md` with runner type, vCPU, RAM, peak memory and raw rows, and the "cannot prove" list from 2.7 |

| W3-34 | `control-plane-engineer` | ADR-004 C1/C2 for the API: PostgreSQL over TLS with `sslmode=verify-full` semantics on asyncpg (CA file, hostname checked); plaintext refused unless the host is loopback; DB credential from a `secretRef` file under `TB_SECRETS_DIR`, retiring `TB_DATABASE_URL` (refuse to start if it is set, so it is never read silently). The C2 exception closes when this lands | W3-4a | Integration against PostgreSQL with a test CA: verify-full connects; a certificate for the wrong hostname is refused; plaintext to a non-loopback host refused at start; `TB_DATABASE_URL` set → exit non-zero; the credential is read from the file |
| W3-35 | `devops-engineer` | ADR-004 C1, C3, C4, C8 in Helm, Compose and Strimzi: broker `connections.max.reauth.ms` ≤ 3600000; ClickHouse and PostgreSQL plaintext ports unexposed except on loopback (NetworkPolicy and Service ports); no `subPath` Secret mounts (helm_check); chart README checklist: customer-managed keys incl. tiered-storage, S3 disks and `BACKUP` targets (C5); claim-check bucket deny-non-TLS policy (C1); revocation order (remove ACLs or `KafkaUser` first, then rotate; restart pods, C4); ClickHouse `default` user disabled and admin queries logged outside ClickHouse before content is turned on (C8) | none | `helm template` shows the reauth setting on brokers and Strimzi `Kafka`; `helm_check.py` fails a fixture with a `subPath` Secret mount and one exposing ClickHouse 8123 or PostgreSQL plaintext off loopback; checklist present in the chart README |
| W3-37a | `control-plane-engineer` | ADR-004 C1a, API side: TLS listener from `secretRef` files (cert and key under `TB_SECRETS_DIR`, read once, minimum TLS 1.2). Plaintext on a non-loopback bind refused (`startup_refused`, `API_TLS_REQUIRED`, exit 2) unless `TB_API_TLS_MODE=mesh`, which logs `api_tls_delegated_to_mesh` at WARN on every start. Goes to `security-reviewer` (network listener) | W3-34 pattern | Tests: a client verifying a test CA connects, and a wrong hostname is refused; plaintext with a non-loopback bind exits 2 with the code; loopback plaintext starts; mesh mode starts and logs the WARN; the key file is never logged |
| W3-37b | `devops-engineer` | ADR-004 C1a, deployment side: Helm `api.tls.mode: listener` (default) or `mesh`. Listener mode: the TLS Secret mounted without `subPath`, probes `scheme: HTTPS`, Service port `https`, ingress-nginx backend annotations re-encrypting with upstream verification against the CA Secret (other controllers documented in the chart README). Mesh mode: a NetworkPolicy admitting only the ingress controller to the API port, plus a NOTES warning. Compose: the API stays loopback-published | W3-37a | `helm_check.py`: the default render has `TB_API_TLS_MODE` unset or `listener`, mounts the cert Secret and uses no plaintext API port; a mesh-mode render without its NetworkPolicy fails; negative fixtures for both |
| W3-36 | `devops-engineer` | ADR-004 C7 docs: Compose and chart READMEs state that lite without a customer-managed key, or on a host without full-disk encryption, holds synthetic data only (`topics/v0` 3.8 rule 4, done) | none | Both READMEs carry the rule, citing `topics/v0` 3.8 rule 4 |

**W3-33 resource budget (estimate, unverified until W3-33a measures it):** brokers 3 x 1 GiB (512 MiB
heap); `split` controller 512 MiB (256 MiB heap); receiver, worker and pytest about 1.5 GiB. Peak
**about 5 GiB**, about 2 vCPU sustained at 1k msg/s, under 2 GiB of disk, cluster start 30-60 s, suite
at most 45 min. GitHub's standard Linux runner is **4 vCPU / 16 GB on public repositories (free, no
minute limit)** and **2 vCPU / 8 GB on private ones (included minutes, then billed)**. *Docs:* GitHub
"Supported runners and hardware resources". So it fits either. **Repository visibility is unverified**:
the architect has no shell. If private, nightly at 45 min is about 1,350 min a month against the plan's
included minutes, so the schedule is weekly plus `workflow_dispatch`. Exceeding included minutes is
cloud spend and goes back to the human. Developer-host runs are always free. B1's numbers are a ratio
on a 2-4 vCPU shared VM and are labelled that way.

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

**Added 2026-10-06 (2.4):** W3-29 (`architect`) and W3-25 (`platform-engineer`, on call) start now;
W3-28a-d at any time (small); W3-26 and W3-27 follow W3-25 in their owners' queues.

**Added 2026-10-06 (2.8):** W3-30 part (1) now (`archive-engineer`, no dependency); W3-31 after it
(`devops-engineer`); W3-32 after W3-1b (`control-plane-engineer`); W3-33b starts locally now
(`qa-engineer`), W3-33a in parallel (`devops-engineer`). ~~`security-reviewer` on ADR-004 H1/H2/H5~~
done: concurred with conditions. W3-34 (`control-plane-engineer`, after W3-4a), W3-35 and W3-36
(`devops-engineer`) at any time.

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
| 4 | `security-reviewer` clean on authn, audit and PHI handling of the API | No unresolved Critical or High. The audit retention answer arrived on 2026-10-06 (2.7 D3). The reviewer closes audit against W3-30 (2) and W3-32 |

### 4.1 Exit review at `wave/1-3` `f52644f` (architect, 2026-10-13; CI green per the coordinator)

The architect read the tree; nothing was run. Commit ids and CI state are coordinator-relayed.

| # | State | Evidence | Missing |
|---|---|---|---|
| 1 | **PARTIAL** | `tests/e2e/test_archive_search_reprocess_replay_e2e.py` (4 passed twice, `docs/spikes/wave-3-archive-search-replay.md`). Real ClickHouse; search, detail (`transformed`, `routed`), `received` content by SHA-256, audit record on the topic, reprocess, replay | The criterion's reading is "every stage's content and each destination's status, responses validated against `openapi/v0`". The test asserts neither `destinations[]` states nor `routed` content per destination, and validates no response against `openapi.json` components (`qa-engineer`, W3-9b). Re-run with the worker's new `received` event in the timeline |
| 2 | **MET** (lead's one-sided rule) | `docs/spikes/wave-3-outage-control-plane.md`, `ec7a3ec` | none (STATUS caveat on set 3's outlier control minimum) |
| 3 | **PARTIAL** | API: `test_hot_reload_integration.py` (v1 to v2 on a running worker, same PID, through the reconciler). CLI: `test_smoke_integration.py` (deploy and rollback reach `published`) | Rollback is never observed on a worker, and the CLI path is never observed on a worker. Needed: one integration test, API and CLI variants, of deploy v2, worker applies 2 with the same PID, then rollback, worker applies 1 with the same PID (`control-plane-engineer`) |
| 4 | **NOT MET** | W3-32 audit producer, W3-37a TLS listener and pin, and W3-6 are in the tree | The `security-reviewer` pass on API authn, audit and PHI. Audit closes against W3-30 (2), which has no `audit_log` writer yet |

**Open tasks.**

| Task | Owner | Blocks |
|---|---|---|
| W3-30 (2): the events archiver consumes `_tb.<env>.audit` into `audit_log` (plain `MergeTree`, R7 rev 3) | `archive-engineer` | Criterion 4 |
| `queued` emission by the worker (`events/v0` 2.1; `received` is in) | `platform-engineer` | none (completeness) |
| Criterion 1 test gaps (above) | `qa-engineer` | Criterion 1 |
| Rollback-on-worker test, API and CLI (above) | `control-plane-engineer` | Criterion 3 |
| `security-reviewer` pass on the API, including W3-37a/b | `security-reviewer` | Criterion 4 |
| `received` `ts` rule (`events/v0` 2.1, ruled 2026-10-13) | `platform-engineer` | none |
| Reprocessor grants under deny-by-default (only `lag_reader` and `api` are verified) | `qa-engineer` | none (W3-29 verify) |
| K2 / H7 role-model sign-off (ADR-004 section 5) | **human** | Not a wave 3 criterion; recorded as open |

The following were found done in the tree: `AuditEvent.error_code` and the widened `ACTOR`; the
archive SQL moved into the query library (`channel_stats`, `read_content`); the
`archive_settings` content flag; `PRODUCE_TIMEOUT` distinct from `PRODUCE_FAILED`; no `kafka-0`
default. Gate-tracked items (W3-13 image check, `partition_halted` alert firing) were **not
re-checked** in this review.

**Also gate-tracked (not phase-1 criteria):** no image published before W3-13 is green;
`tb_partitions_halted` exported and the `partition_halted` alert shown firing on a forced halt (2.5 H1,
added 2026-10-06); ADR-004
drafted with its human questions listed; the wave 2 condition (`fb60f3a` closure) recorded (**done 2026-10-05**, STATUS).

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
| Destination-scoped reprocess (`out.<dest>` re-entry for loader exhaustion; destination-only step re-run via an additive `raw` header) | `architect` (ADR-003 amendment), then `archive-engineer`, `platform-engineer` | Wave 4. Needs `reprocessor` grants beyond `raw` (ADR-004 H7, human). 2.6 H5 |
| Replay cancel (`POST .../replays/{replay_id}/cancel`, status `cancelled`, PostgreSQL flag polled by the running replica) | `architect` (contract), then `control-plane-engineer` | Wave 4, ruled 2026-10-13 on W3-9b's question. Wave 3 relies on `max_records`, `deadline_s` and `replay_id` idempotency (`openapi/v0` README 6) |
| Claim-check spike | lead decides | Unscheduled since wave 2 (`wave-2.md` 5.2) |
| Operator clear or skip for a halted partition | none yet | A skip is a drop; it needs a contract and an audit event first. Candidate for W3-7 only if the lead adds it |
| W2-2 item 4: FHIR R4 schema validation of golden files | `healthcare-standards-engineer` | Waits on the human's network allow-list (7.5) |
| RF=3 re-run on dedicated capacity | `qa-engineer` | Cloud spend (7.6); W4-6. **2026-10-06:** the human asked for a free alternative, which is W3-33a/b. Dedicated capacity stays optional and human-only, needed only for absolute latency or sizing claims |

---

## 7. Open decisions — human only

| # | Decision | Blocks | Default until answered |
|---|---|---|---|
| 7.1 | ~~`psycopg` 3 or `asyncpg`~~ **ANSWERED 2026-10-06:** *"let the lead and architect decide"*, so **`asyncpg`** (2.7 D1). CLAUDE.md line proposed to the lead | — | — |
| 7.2 | ~~Audit retention~~ **ANSWERED 2026-10-06:** *"Give the user a possibility to change it"*, so operator settings with defaults events 90 / content 30 / audit 2190 d (2.7 D3, ADR-004 section 4) | — | — |
| 7.3 | ~~`_hmac` mechanism~~ **ANSWERED 2026-10-06:** *"let the lead and architect decide"*, so **deferred** until H1 concurrence and before per-patient search; `_hmac` treated as PHI (2.7 D2, ADR-004 H3) | Per-patient search (W4-3) | Archiver drops `*_hmac`; per-patient search refused |
| 7.4 | ~~Key custody~~ **ANSWERED 2026-10-06:** *"Let the lead, architect and security engineer decide"*, so **decided** in ADR-004 H1/H2/H5, with `security-reviewer` concurring under conditions C1-C8 (2.7 D4) | Production content storage, until W3-34/W3-35 land | Content off by default in production (A4) |
| 7.5 | Add `hl7.org` or `build.fhir.org` to the network allow-list. **Still open** | W2-2 item 4 | Golden files stay unchecked against R4 |
| 7.6 | ~~Paid RF=3 capacity~~ **ANSWERED 2026-10-06:** *"find alternative free solution"*, so W3-33a/b (2.7 D5). Paid capacity is needed only for absolute latency or sizing claims and stays human | — | Absolute numbers stay single-host, labelled |
| 7.7 | ~~Merge~~ **ANSWERED 2026-10-06:** merge `wave/1-1` and `wave/1-2` into `main`; PR for `wave/1-3`. The lead executes | — | Further wave 3 work is on `wave/1-3` |
| — | **Still human (ADR-004):** H6 erasure; H7 role-model sign-off (now including the `reprocessor` READ on `raw`, 2.6 H8) | Production use | — |
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
