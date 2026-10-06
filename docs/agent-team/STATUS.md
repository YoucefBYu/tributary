# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

### Wave 3, started 2026-10-05

- **Phase / wave:** **Phase 1, wave 3: archive, control plane, operations. STARTED 2026-10-05** under
  the human's instruction relayed by the lead: *"Execute them by priority and let the lead and
  architect decide."* That delegates sequencing and scope only. Items under "Escalate to a human" in
  CLAUDE.md stay with the human (`phases/wave-3.md` section 7).
- **Plan:** [`phases/wave-3.md`](phases/wave-3.md). It merges `phase-1.md` W3-1..W3-9 with the
  deferred wave 2 backlog and the lead's six follow-ups (W3-10..W3-24). Team: six seats (`architect`,
  `archive-engineer`, `control-plane-engineer`, `devops-engineer`, `qa-engineer`, `connector-engineer`).
  `platform-engineer` and `healthcare-standards-engineer` are on call.
- **Batch 1 (unblocked):** W3-1a OpenAPI part 1 + ADR-009 (`architect`), W3-23 receivers under exact
  ACL grants (`qa-engineer`), W3-13 image-build check (`devops-engineer`), W3-2a archiver events path
  (`archive-engineer`), W3-4a PostgreSQL schema and migrations on `asyncpg` (`control-plane-engineer`),
  W3-12 observability contract then W3-17 readiness probe (`architect`, `connector-engineer`), W3-11
  `validate.py --check-expectations` (`architect`), W3-22/W3-20/W3-21 (`qa-engineer`), W3-10 ADR-004
  draft (`architect`).
- **Branch: `wave/1-3` for all further wave 3 work (2026-10-06).** The human instructed that
  `wave/1-1` and `wave/1-2` be merged into `main` and a PR opened for `wave/1-3`. The lead is doing the
  git side, and the architect has not seen the merge commit or the PR. Until the lead confirms, `main`
  is recorded at `2c25448`.
- **Human answers, 2026-10-06 (relayed by the lead): `phases/wave-3.md` 2.7.** 7.1 → `asyncpg`;
  7.2 → retention is an operator setting (W3-30, W3-31, W3-32); 7.3 → `_hmac` deferred; 7.4 → ADR-004
  H1/H2/H5 drafted, **pending `security-reviewer` concurrence**; 7.6 → free 3-broker cluster (W3-33a/b);
  7.7 → merge plus `wave/1-3`. Still open with the human: 7.5, ADR-004 H6 and H7, and the carried items.
- **2026-10-06, lead input on W3-29 (`wave-3.md` 2.6 H8):** the `reprocessor` gains LITERAL READ on
  each channel's `raw`. A group is granted only if assign without `group.id` proves impossible. The
  lead ruled that replay targets its source channel only, and "shadow" keeps its consumer-group
  meaning. A replay into a per-key channel needs an audited `allow_reorder` flag.
- **Human-only questions for wave 3:** `phases/wave-3.md` section 7, mirrored under Open questions below.
- **Architect batch 1, delivered 2026-10-05, ALL UNVERIFIED (the architect has no shell; the lead
  runs the verify lines):**

  | Task | Delivered | Verify owed |
  |---|---|---|
  | W3-12 | `contracts/observability/v0`: readiness section 2 (**unblocks W3-17**), metric registry (35 metrics, 19 labels), log line schema | `validate.py` N / 0; observability adds 30 checks, 19 pinned pairs |
  | W3-1a | `contracts/openapi/v0` part 1; ADR-009 Proposed | `validate.py --with openapi-spec-validator`: predicted **210 / 0** in total; openapi adds 24 checks, 10 pairs |
  | W3-11 | `validate.py --check-expectations` | Exit 0 on the tree (predicted `139 invalid examples, 0 drifted`); exit 1 on a scratchpad copy with one pair altered |
  | W3-10 | ADR-004 draft, H1-H7 `TODO(human)` | `security-reviewer` reads it |
  | W3-17 amend | `reply_consumer` reason `unauthorized`; per-check reason binding; `spec` gets no `unauthorized` (ruling, README 2.3); five `log_only` codes in `error-codes.json` (set `1.8`); `OPS_LISTENER_BIND_FAILED` named in O4, not registered (L6a) | `validate.py` predicted **213 / 0**; `--check-expectations` `141 ..., 0 drifted`; `platform-engineer` adds the five codes to `sdk.codes` in the same push or `test_codes_contract.py` goes red |

  **CI note:** the `contracts` job must add `--with openapi-spec-validator`, or the default run fails by
  design (W3-14, `devops-engineer`).
- **W3-8 open risks ruled 2026-10-06** (`phases/wave-3.md` 2.4-2.5; chart uncommitted under
  `deploy/helm/tributary/`). New tasks: **W3-25** runtime ops listener + `/metrics` + worker
  (`platform-engineer`, on call); **W3-26** receivers `/metrics`, loaders ops listener
  (`connector-engineer`); **W3-27** archiver ops listener (`archive-engineer`); **W3-28a-d** no pid
  instance default (worker, loaders, archiver, Compose); **W3-29** set 1.8 `lag_reader` role + instance
  rule (`architect`). W3-8 amended (StatefulSets, generated `KafkaUser`s, contracts in the image, real-scrape
  verify); W3-4b gains API `/metrics`. Neither exit criterion 2 nor 4 is blocked; the `partition_halted`
  alert firing is now gate-tracked.
- **W3-3 questions ruled 2026-10-06** (`phases/wave-3.md` 2.6; set 1.8 README-only, counts
  unchanged): `FINAL` wording corrected (`events/v0` 4.1 rule 2); reprocess copies `tb-received-at`
  (`envelope/v0` 6 rule 5); destination-scoped reprocess deferred to wave 4, the refusal stays, and the
  W3-3 verify is amended to a refusal test. `reprocessor` role added to W3-29.

### Wave 2 exit, 2026-10-05: CLOSED, condition lifted

**Verdict: criteria 1-4 met.** Closed by the lead and the architect under the human's delegation
(decisions log, 2026-10-05). It is not a human signature on a gate report, and no merge is implied.

**Condition (DoD item 6): LIFTED 2026-10-05.** `security-reviewer` re-checked the four outstanding
closures at `b6b0c97` (lead-relayed; the architect has not read the review itself):

| # | Finding | Closure | Evidence the reviewer cited |
|---|---|---|---|
| 1 | `fb60f3a` High, context extensions L1-L7 | **CLOSED** | `runtime/extensions.py:146-276`, `worker.py:450-497` |
| 2 | W2-3 `Encounter` Critical (PV1-19 keying) | **CLOSED** | Fix `e48d937`, follow-ups `1aba292`, `ab4a4bf`. **Citation corrected:** `4998f33` introduced the bug; earlier text here named it as the fix |
| 3 | W2-5 findings | **CLOSED** | `EnvelopeError` crash loop (`worker.py:589-603`), splitters as generators, time budget, filter allow-list, DLQ-unwritable halt |
| 4 | `EnvelopeError` PHI in exception text | **CLOSED** | Same review |

**Caveat:** bandit, pip-audit and gitleaks were **not installed** for this review, so it is a manual
review without scanner evidence. **New Medium, not a reopen:** empty assigning authority in conditional
FHIR keys (risk row "Conditional FHIR keys collide across senders", `healthcare-standards-engineer`).

**CI evidence (lead-relayed; the architect has not seen the logs):** head `06fd4b6`, GitHub Actions,
`youcefbyu/tributary`, branch `wave/1-2`, all jobs green. These were lint/types/unit/conformance,
integration (testcontainers), contract validation with the new `--emit-expectations` drift check,
licence+SBOM+secret scan, and ui. The integration log at `e563d42` showed **112 passed, 0 skipped**. The
integration job now fails if zero tests pass. All five `tests/chaos/` modules carry
`pytest.mark.integration`, so the chaos suite runs in that job.

| # | State | Evidence |
|---|---|---|
| 1 ADT to FHIR and ClickHouse | **Met** (condition above) | `tests/e2e/test_adt_mllp_to_fhir_and_clickhouse_e2e.py`: the published example, real HAPI `v7.4.0` and real ClickHouse, `context: [fhir]` asserted, control without the FHIR destination. Green in CI, 0 skipped |
| 2 Per-key order under chaos | **Met** | `docs/spikes/wave-2-duplicate-rate.md`: 15/15 runs green, fault fired 15/15, **0 order violations** in all 12 per-key runs. The worker kill gives **16.67% duplicates** (20 of 120, deterministic: the uncommitted tail of `commit_every = 50`). Rebalance x2 and outage: 0%. Broker kill: 0 acknowledged lost, 3-broker RF=3. **Gap carried (W3-22):** the broker kill has no partition key, so receiver-originated keys are not under chaos |
| 3 Duplicate delivery, every loader | **Met** | Eight 5.1 redelivery modules: seven in `tests/integration/` (http, fhir, postgresql, clickhouse with a `FINAL` count, sftp, s3, kafka) plus `packages/tributary-loaders/tests/mllp/test_integration_mllp_loader.py`. Green in CI, 0 skipped |
| 4 Throughput vs Phase 0 | **Met as a report, low evidence rating** | `docs/spikes/wave-2-linux-rerun.md` (2026-09-28): S1 parse+map 565-617 msg/s/core against the 200 threshold (not like for like with Phase 0's 36,322: real pydantic Bundles); S2 ACK p99 12.9-24.8 ms at about 1k msg/s against 50 ms. Single broker, RF=1, shared host. RF=3 stays a human cloud-spend question |

**Also closed by the 2026-10-05 evidence** (`docs/spikes/wave-2-ci-confirmation.md`, tree `ec4b9d1`):
- **Contract sets 1.2-1.7 VERIFIED:** `validate.py` **156 / 0** (set 1.6's prediction); **110 pinned
  pairs, all matching** (86 + 9 + 9 + 1 + 3 + 2 predicted across sets 1.1-1.6, exact); conformance
  **150 passed**. Every "unverified" label on sets 1.2-1.7 below is superseded.
- **Sync-reply invariant-1 risk:** `receivers/tests/reply/test_properties.py` 16 passed. Its four
  broken-implementation controls went red (6 failed), and CI runs it in a named step. Risk row closed.
- **Sync-reply latency:** five isolated runs (n = 300 each), p50 17.4-18.4 ms, **p99 25.0-29.4 ms**,
  none over 100 ms. The earlier 270 ms tail did not reproduce (`docs/spikes/wave-2-sync-reply-latency.md`).

**Wave 2 backlog, disposition 2026-10-05:** ranks 1-3 done (rank 1 except the condition above). Ranks
4-10 moved into `phases/wave-3.md`: 4 → W3-13, 5 → W3-18, 6 → W3-12 + W3-17 (+ W3-8 wiring),
7 → W3-16, 8 → W3-19, 9 → W3-24, 10 → W3-8.

**Invariant-3 receiver item: closed for invariant 3.** All four receivers start from
`_tb.<env>.channels` (`receivers/cli.py` uses `open_spec_source`, `startup.py` refuses
`--channel-spec`; `1cd9fe4`, `359f5fd`, `737fcd8`). `*_spec_lifecycle.py` integration tests are green
in CI. Receivers also apply republished specs live (`*/reload.py`), which was out of scope;
`nackOn: spec-invalid` stays refused. **Not found in the tree:** an integration test of receivers
under their exact set 1.4 grants on a deny-by-default broker. A grep of the receiver integration tests,
`tests/` and `deploy/` found none. The worker has one (`test_integration_engine_kafka.py`, with broad
PREFIXED grants). This is carried as W3-23, batch 1. It verifies the role table, not the spec source,
and no wave 2 criterion depends on it. That reverses the 2026-09-29 line "wave 2 does not exit with
this open", for that reason. The replier reads `clients.reply_consumer` (`c5fc09a`, `e563d42`).

### Wave 2 record (kept for history; dated lines, superseded where marked)

- **Phase / wave (as of 2026-09-24):** Phase 1, wave 2 — pipelines and delivery, started 2026-09-24
  on the human's approval (relayed by the lead). **Closed 2026-10-05** (above).
- **Branch:** `wave/1-2`. It builds on the wave 1 branch; **wave 1 is not merged to `main`**, which is
  still at `2c25448`.
- **Plan:** [`phases/wave-2.md`](phases/wave-2.md), which replaces six lead drafts the architect had not
  reviewed. They contained errors, including three open human decisions marked "approved" and a claim
  that the architect had approved the plan. `wave-2.md` section 9 lists every removal; the six files are
  now one-line pointers, for the lead to `git rm`. Authoritative scope remains
  [`phase-1.md`](phases/phase-1.md), Wave 2.
- **Exit criteria** (verbatim, `phase-1.md`): (1) an HL7 ADT flow to a FHIR server and to ClickHouse
  works end to end from a YAML channel; (2) an ordered channel preserves per-key order under chaos
  tests; (3) every loader has a passing duplicate-delivery test; (4) throughput — msg/s per core and ACK
  p99 against the Phase 0 numbers. Operational readings are in `wave-2.md` section 3.
- **Contracts: set 1.1 published and VERIFIED 2026-09-24** (`qa-engineer`, committed `ffc1580`):
  `validate.py` **122 / 0**; `--emit-expectations` **86 pairs, 0 mismatches**, all 16 set 1.1
  predictions included; `pytest tests/conformance` **127 passed**. Set 1.1 adds the wave 2 connector
  shapes (3 source types, 7 destination types), per-destination steps, `partitionKey` semantics, and
  written lift criteria for `retry-topic`, which stays refused. `contracts/CHANGELOG.md`, ADR-007
  D10-D17. **Later the same day**: README follow-ups from implementers, with no schema change, and one
  valid example's key changed to `hl7:PID.3[5:MR].1`. Re-verified by the lead: `validate.py`
  **122 / 0**, conformance **127 passed** (`e628891`).
- **Linux re-run of S1/S2: DONE 2026-09-28** (`docs/spikes/wave-2-linux-rerun.md`, single broker,
  shared host). The 2026-09-24 "NOT DONE" text is removed. The RF=3 re-run on dedicated capacity
  remains a human cloud-spend question.
- **Exit criterion 3 gap, found by `qa-engineer`:** the existing HTTP and PostgreSQL
  `test_duplicate_delivery_is_harmless` tests call `deliver()` twice in one process. Under channel-spec
  README 5.1 that does not count as a genuine redelivery. `qa-engineer` builds the redelivery harness
  (a real consumer loop with offset replay) in W2-9, coordinating the interface with
  `connector-engineer`.
- **Superseded 2026-10-05: verified** by the set 1.6 measurement (156 / 0, conformance 150; Wave 2
  exit above). Original note: **Verification owed for the 2026-09-25 contract edits** (`qa-engineer`):
  - `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expect
    **122 / 0**, unchanged;
  - `uv run --no-project --python 3.12 --with pytest --with jsonschema python -m pytest tests/conformance`:
    expect **127 passed**, unchanged;
  - `uv run pytest packages/tributary-runtime/tests/test_core_steps.py`: the manifest `requires`
    edits, where no `runnable: false` example may resolve completely;
  - one independent `hashlib` recomputation of the three child-id vectors in channel-spec README
    3.3.3.

  `--emit-expectations` is not needed, because no schema or invalid example changed.
- **Only one wave 2 item is blocked on a human: W2-2 item 4.** Decided 2026-09-28 (vendor the FHIR R4
  schema); it now waits only on the human adding `hl7.org` or `build.fhir.org` to the network allow-list. SFTP was unblocked on 2026-09-24: the human approved
  `paramiko` (see decisions log). The
  PostgreSQL *loader* is **not** blocked: it uses `asyncpg` (Apache-2.0 per the lead; `licence_check.py`
  confirms it when the dependency lands). `psycopg` matters only to the wave 3 control plane.
- **2026-10-05: in CI green; reviewer clearance not recorded here (Wave 2 exit, above).** Was:
  **W2-5 (built-in steps, splitter, destination steps; `platform-engineer`): DELIVERED, IN REVIEW.**
  Commits `74c9853`, `290a553`, `2cabf4a`, `627f547`, `7849d1f`, `288903a`. `code-reviewer` and
  `security-reviewer` are both running, so it is not done until any Critical or High they find is
  closed. The architect ruled on its six contract proposals on 2026-09-25 (decisions log). That
  produced four change requests back to `platform-engineer`:
  - the executor emits `TRANSFORM_ERROR`, not `STEP_ERROR`;
  - eight reserved prefixes;
  - the registry cross-check test;
  - the `ids.py` docstring.

  **Risk: a produce-time `EnvelopeError` crashes the worker. Fix delivered, pending review.** In
  `2a16e24`, `8af0d63` and `6daf4f4` the worker catches it, dead-letters the consumed record with
  `TB_OUTPUT_ENVELOPE_INVALID` and produces nothing (`worker.py`, tests in
  `test_envelope_limits_and_budget.py`). The architect has read this but not run it. It closes when
  the reviewers clear it. A second round of rulings the same day covered the filter allow-list, the
  split terminator, two `TB_` codes, `Reject` declarations and the `ref` scope (decisions log).
- **Residual risk (W2-5, accepted for wave 2): a `raw` payload over 1 MB halts its partition.**
  This happens when a producer bypasses the receivers. Such a record cannot be dead-lettered, because
  the `dlq` record would break the same size limit. Since `361935e` the worker no longer crash-loops:
  it pauses the partition and emits `partition_halted` with `TB_DLQ_UNWRITABLE`, and the other
  partitions keep flowing. Nothing is lost, and the record stays on `raw`. **An operator must clear
  the halt**; no clear or skip command exists yet, so today that means a manual offset reset past the
  record (unverified procedure). The real fix is claim-check, which is not scheduled in wave 2.
  - It needs WRITE on `raw`, which `topics/v0` 3.4 grants to the receiver principal only. So it
    means a misconfigured ACL or a compromised receiver credential, not ordinary traffic.
  - The broker's `max.message.bytes` on `raw` does not prevent it (decisions log, 2026-09-26).
- **2026-10-05: fix in the tree** (`adt.py` keys `Encounter` on PV1-19, `4998f33`); reviewer
  clearance not recorded here (Wave 2 exit, above). Was: **W2-3 (HL7 v2 to FHIR content;
  `healthcare-standards-engineer`): Critical found, fix in progress.** `code-reviewer` found that `Encounter` was keyed on `tb-msg-id` instead of PV1-19 (visit
  number). One visit would then be a new Encounter per message, and conditional-update idempotency
  (channel-spec 3.4.2) would hold per message but not per encounter, which is the wrong resource
  identity. `security-reviewer` is running. Not done until both have cleared.
- **W2-2 item 4: decided 2026-09-28, waiting on network access** (open questions). Validating the
  golden files against the FHIR R4 JSON Schema could not be done: `hl7.org` is blocked by the proxy,
  and no installed package ships the schema. The human chose to vendor the file; fetching it needs the
  allow-list change. That validation was a condition of the W2-2 approval: it
  guards against the R4B models accepting R4-invalid output. So W2-2 stays approved with that
  condition open, not closed.
- **Contract edits 2026-09-25: verified 2026-10-05** (set 1.6 measurement, Wave 2 exit above).
- **W2-8 (synchronous reply): MLLP BUILT, HTTP NOT BUILT** (2026-10-05, relayed by the lead; README
  3.0). Contract set 1.2, 2026-09-28; receiver rules set 1.7, 2026-10-05.
  - **Done and reviewed (`platform-engineer`):** the worker reply producer and status rules,
    `ctx.reply` on `ReplyContext` with `FakeContext` (shape ruled 2026-09-29, decisions log), and the
    `REPLY_` codes.
  - **Built (`connector-engineer`):** MLLP receiver pending map, `replier` consumer and answers,
    `body: ack` and `body: step`: `8bfe205`, `33b2207`, `f420634`, `4134ab3`, `c051002`. Refuses to start
    without `--instance`; a `_FATAL` consumer error is not ready and exits 1 (real-Kafka test in
    `c051002`); `AE` / `REPLY_ROUTE_LOST` only for committed entries. `pending.py` drives the
    runtime's `SyncRequest` and says the property test drives the map itself (read, not run).
  - **2026-10-05: W2-8 done for MLLP.** The property test and its controls are confirmed, and the
    latency distribution is filed. CI green means the SDK code cross-check holds. Deferred to wave 3:
    Helm `--instance` (W3-8), readiness (W3-12/W3-17), HTTP sync reply (W3-19). *Original list:* the
    set 1.7 follow-ups (two codes, reply-consumer profile, Helm `--instance`); the latency risk and the
    missing readiness probe; HTTP sync reply (W2-7).
  - **What the property test proves, and what it does not.**
    `packages/tributary-runtime/tests/test_reply_properties.py` (one answer per request, never ahead
    of the `raw` callback, three broken-implementation controls) runs against the **model** in
    `tributary.runtime.sync_answer`, not against a real receiver. It fixes the answer rules. It does
    not show that the receiver keeps them. **The invariant-1 risk row stays open** until the receiver
    either uses `sync_answer` or has the same property test run against it, with a broken-receiver
    control that goes red.
  - The HTTP mapping waits for W2-7. `body: step` waits on its memory-bound condition and on
    `healthcare-standards-engineer` making `attach()` forward `reply` (channel-spec 3.2.6).
  - ~~Verification owed: `validate.py` 134 / 0 for set 1.2; nine predicted pairs; conformance 139.~~
    **Superseded 2026-10-05** by the cumulative measurement (156 / 0, 110 pairs, 150 passed).
- **Contract set 1.3 published 2026-09-29, additive, unverified** (decisions log; `contracts/CHANGELOG.md`;
  ADR-008; ADR-007 D18-D20). It answers the W2-7 security-review HIGH (receivers produced to `raw` over
  PLAINTEXT, unauthenticated; fixed for receivers in `e3ddee1`) and the lead's W2-6 items. **Workers and
  loaders still connect to Kafka unauthenticated**, and **the Compose lite stack does not start** (its
  receiver is refused since `e3ddee1`) until these follow-ups land:
  - `devops-engineer` (first, critical path for any Compose demo): Compose on `SASL_SSL` per
    `engine-kafka/v0` section 6: dev CA and per-component SCRAM users generated at `up`, deny-by-default
    ACLs from `topic-defaults.json`, a `TB_ENGINE_KAFKA_FILE` document per service, no opt-out. Helm
    mounts the same per role.
  - `platform-engineer`: the `engine-kafka/v0` loader in `tributary-runtime` (section 4 rules 1-6),
    `KafkaSettings` key allow-list with contract values winning (the `producer_config` merge-order
    defect, invariant 1), worker and `_tb.<env>.channels` reader migrated, the `localhost:9092`
    default removed. **Also, with the contract commit:** the seventeen new codes in `ENGINE_CODES`.
  - `connector-engineer`: every receiver and loader entry point on the runtime loader, retiring
    `--bootstrap`, `--engine-security` and `engine_kafka.py`; `tls.allowedClientSans` in the `http`
    receiver, then `mllp` (refused at startup until then); `S3_` minted codes that collide with a fixed
    name become `S3_UNKNOWN`.
  - `qa-engineer`: `engine-kafka` conformance suite; `validate.py` expected **148 / 0** and
    `--emit-expectations` for nine predicted pairs; confirm the `fhir`/`sftp`/`s3` redelivery tests green
    in CI (promotes them to Implemented); an ACL negative test in the Compose e2e.
- **Set 1.3.1 (patch, 2026-09-29, README-only, counts unchanged)**, with three follow-ups:
  - `connector-engineer`: move `RawPublisher` onto the runtime loader and delete the `ssl.`/`sasl.`
    prefix check in `tributary.receivers.engine_kafka`. It is shadowed by `KafkaSettings`' exact-list
    `check_link` (read from the source, not run), so this is drift risk, not an open hole. ADR-008
    follow-up 6.
  - `devops-engineer`: record `botocore`'s bundled `cacert.pem` runtime scope (`tributary-loaders`,
    `s3` only) in `licence_check.py`. A name entry in `MPL_APPROVED` is never consulted for a package
    whose metadata says Apache-2.0, so this needs a bundled-file check (ADR-001 2.3).
  - `healthcare-standards-engineer`: `attach()` of a `ReplyContext` must be a `ReplyContext` under
    `mypy --strict` and forward `reply`, with a test. It blocks lifting `body: step`, and nothing
    else.
- **Contract set 1.4 published 2026-09-29, unverified. It unblocks the deny-by-default data plane**
  (`contracts/CHANGELOG.md`; ADR-008 D6-D9; ADR-003 D10 amendment). The Compose lite stack runs
  `SASL_SSL` now. On it, the loader could not read `_tb.<env>.channels`, and the worker's spec reader
  had no group grant. The `security.acls` table was rebuilt from what each role's code touches: spec
  READ plus a delimited spec-reader group grant for receiver, worker and loader; unused `.retry` grants
  removed; group prefixes delimited; a missing transactional-id grant added. `super.users` is restated
  to the implementation, `s3` gains `expectedBucketOwner`, and `REPLY_EXPIRED` is registered. Additive
  for every document. Follow-ups:
  - `devops-engineer` (critical path): update three `test_bootstrap_topics.py` tests, drop the
    temporary grant, `down`, re-run MLLP → worker → loader.
  - `platform-engineer`: `REPLY_EXPIRED` in `ENGINE_CODES` and `reply.py`, with the contract commit;
    `spec_reader_group` helper.
  - `connector-engineer`: loaders onto the helper; receivers onto `open_spec_source`.
  - `qa-engineer`: `validate.py` **150 / 0**, one predicted pair, conformance +2; ACL negative cases
    in the Compose e2e.
- **Set 1.4.1 (patch, 2026-09-29, documentation only, counts unchanged: `validate.py` still 150 / 0,
  unverified).** `kafka-users-init` is no longer a super user. It holds only ALTER on CLUSTER
  (`5495498`; checked by reading `tls-init.sh` and `start-broker.sh`). Super users are now the broker,
  `topic-bootstrap` and `admin`. **Caveat:** ALTER on CLUSTER also authorises `CreateAcls`, so the key
  stays super-user-grade (ADR-008 D8). One non-README edit: the `topic-defaults.json`
  `security.broker.super.users` description string, which no code reads. Optional follow-up for
  `devops-engineer` (ADR-008 follow-up 9): revoke the grant once the SCRAM users exist, and fix the
  `add_scram_users.py` docstring, which still says "a super user".
- **2026-10-05: implemented; reviewer closure NOT RECORDED (the wave 2 close condition, above).**
  Was: OPEN, wave 2, security High on `fb60f3a` (context extensions).
  Ruled 2026-09-29 as **contract set 1.5** (additive, unverified; ADR-007 D21-D22; new
  `contracts/context-extension/v0`). A channel declares `spec.pipeline.context: [fhir]`. The worker
  imports only declared `tributary.context` entry points, each from the distribution the registry pins
  (`fhir` → `tributary-fhir`), and refuses the spec otherwise. **Until `platform-engineer` lands L1-L7,
  every installed registrant still runs in every channel.** Follow-ups:
  - `platform-engineer`: enforcement, the stronger probe (a real `reply` call; `log` and `secrets`
    identity), and log lines with distribution and version;
  - `healthcare-standards-engineer`: P1-P7 tests for `tributary-fhir`, docstrings;
  - `qa-engineer`: exit-criterion-1 spec asserts `context: [fhir]` (inherited from the example), plus a
    control without it; `validate.py` **153 / 0** with three predicted pairs;
  - optional `devops-engineer`: an image-build check against `capabilities.json`.
- **OPEN, wave 2: security Medium (re-review, pre-existing), `uses` steps load from the first
  registrant** (`steps.py:78-82`). Ruled 2026-09-30 as **contract set 1.6** (additive, unverified;
  ADR-007 D23; `platform-engineer`'s S1, amended). Engine namespaces are pinned by the new
  `contracts/channel-spec/v0/step-namespaces.json` (`tributary.core` → `tributary-runtime`,
  `tributary.hl7v2` → `tributary-hl7v2`, `tributary.content_hl7_fhir` → `tributary-content-hl7-fhir`).
  Third-party namespaces are bound per channel by the optional `pipeline.stepNamespaces`, so no
  customer package needs a core contract change. A duplicate, mismatched or unbound name refuses the
  spec, and nothing else is imported. Follow-ups:
  - `platform-engineer`: N1-N6 (README 3.3.5), strict `stepNamespaces` parse, runtime registry copy
    with a two-way test, the two log lines, and the `t.broken`/`t.exiting` tests moved onto the seam;
  - `qa-engineer`: `validate.py` **156 / 0**, two predicted pairs, conformance +3;
  - optional `devops-engineer`: an image-build check for `tributary.*` step entry points.
- **Contract set 1.7 published 2026-10-05, additive, unverified** (`contracts/CHANGELOG.md`; ADR-007
  D24; ADR-003 D10 amendment). No schema or example change, so `validate.py` stays at set 1.6's
  predicted **156 / 0**. Rulings: N4 keeps resolving against the first installation and logs hidden
  ones `duplicate` (no runtime refusal; the image build refuses duplicate names); `body: step` bodies
  the receiver cannot write answer `AE` with the new `REPLY_BODY_INVALID` / `REPLY_BODY_OVER_BUDGET`;
  per-connection fairness `max(1, maxPending // 4)` ratified, not a spec field; `--instance` required
  and unique per replica; the replier reads `clients.reply_consumer`. Follow-ups:
  - `platform-engineer`: both codes in `ENGINE_CODES` with the contract commit (SDK cross-check is red
    until then); `TopicDefaults` reply-consumer profile; shadow-aware `extensions.py` (ADR-007 f-up 19);
  - `connector-engineer`: emit the two codes; `feed.py` reads the profile, no literals; separate
    per-connection refusal metric; confirm `http` refuses `source.reply` at startup;
  - `devops-engineer`: Helm `--instance` from the pod name; the image-build check is now required and
    refuses duplicate distribution names (ADR-007 f-up 18);
  - `qa-engineer`: `validate.py` and conformance unchanged from the set 1.6 measurement.
- **Closed for invariant 3 on 2026-10-05; the ACL-grants test is carried as W3-23** (Wave 2 exit,
  above). Text as of 2026-09-29: **OPEN, wave 2: invariant 3 is broken on every
  receiver. Owner: `connector-engineer`, started 2026-09-29 with `mllp`.** `mllp`, `http`, `sftp` and `kafka_source` all take a required
  `--channel-spec <file>` (confirmed by grep; none calls `open_spec_source`). So each needs a file that
  someone other than `_tb.<env>.channels` supplies. Workers and loaders already read the topic.
  - **No contract change is needed.** Set 1.4 already grants receivers READ on `_tb.<env>.channels` and
    the spec-reader group, and `tributary.runtime.channels.open_spec_source` and
    `tributary.runtime.topics.spec_reader_group` exist.
  - **Scope:** read the spec from the topic at start and retire `--channel-spec`. Live reload of a
    running receiver stays out of scope, and `nackOn: spec-invalid` stays reserved (ADR-007 D19).
  - **Done when:** all four receivers start from `_tb.<env>.channels` alone, with a test that a
    receiver given no spec file starts, and an integration test against the deny-by-default Compose
    ACLs.
  - Wave 2 does not exit with this open.
- **Carried into wave 3, unscheduled (`wave-3.md` section 6):** final W2-2 signatures after the
  conditions (`healthcare-standards-engineer`; item 4 waits on the network allow-list); the
  `retry-topic` spike report (`platform-engineer`; not in `docs/spikes/`, and the refusal stays).
  Original note follows. The first model result has pause-until-due at
  p50 lateness of about 515 s at production tiers, which is option (a)'s head-of-line cost made
  concrete. A delay-buffer variant is being measured.

### Wave 1 exit

- **State: pushed, CI green (human-confirmed 2026-09-24, relayed by the lead), every gate condition
  closed, NOT merged to `main`.** The gate report's one remaining condition was "a human pushes
  `wave/1-1` and the CI integration job goes green". Per the human, that is now met.
- **Gate report:** [`gates/phase-1-wave-1.md`](gates/phase-1-wave-1.md). **GO WITH ONE CONDITION**,
  and that condition is now met. All eight tasks were delivered.
- **Reviews:** `code-reviewer-8` APPROVE, zero findings at any severity. **`security-reviewer`, final:
  0 Critical, 0 High, 0 Medium, 3 Low**, so the gate passes from their side. Their first pass found 2
  High, and **both were fixed inside the wave and confirmed closed with the mechanism re-derived, not
  taken on trust:**
  (1) `certifi` (MPL-2.0) human-approved 2026-09-23 (the lead attests first-hand; the reviewer cannot
  verify a human decision) — dev group only, never shipped, scoped to its `testcontainers[kafka]` →
  `docker` → `requests` path, recorded as **its own approval, not an extension** of the three-package one
  (ADR-001 2.3); `MPL_APPROVED` landed at `a35528c`, `licence_check.py` passes 44 components / 0 failed.
  (2) The HTTP loader silently followed 3xx with the default `urllib` opener — a redirected POST dropped
  its body, the target's 200 read as success and **the offset committed for a message that was never
  delivered** — fixed at `bec8029`, with three extra gap-checks the reviewer added themselves.
- **Phase 0 is complete and signed** (2026-09-22).
- **Exit criterion 4 is PASS.** It is specifically "`code-reviewer` clean on the wave diff", and that
  reviewer found nothing. While the two Highs were open they blocked through the definition of done
  item 6 (`security-reviewer` on auth, PHI, listeners and dependencies), **not** through criterion 4 —
  the distinction determines who has work to do, and is worth keeping straight at future gates.
- **Worth carrying into wave 2:** that message-loss bug was in code that had already passed a clean code
  review, `mypy --strict` and 701 tests. **Reviews find what they look for.** Both Highs, both Phase 0
  Highs and three of the four contract defects share one shape — *a control never applied to a path
  nobody thought about* — so wave 2 should spend review effort on library defaults and transitive edges
  rather than on paths that already have tests.
- **Branch:** `wave/1-1`
- **Exit criteria:** 1 **PASS**. It was PASS locally and pending CI at the gate; the human confirmed on
  2026-09-24 that CI passed on the pushed branch. The architect has not seen the CI run itself. 2 **PASS for
  invariants 1, 2, 5; PARTIAL for 3**; 3 **PASS**;
  4 **PASS** (`code-reviewer-8`, zero findings). Engineering checks green as run by the lead: `uv sync`,
  `ruff`, `ruff format`, `mypy packages` (56 files), fast suite **701 passed**, `validate.py` **102 / 0**,
  `pytest tests/conformance` **101 passed**, `pytest -m integration` **6 passed in 227.91 s**.
- **Invariant 3 is now PARTIAL, not "not exercised".** The mechanism is implemented and tested —
  `runtime/channels.py` reads the compacted `_tb.<env>.channels` topic and imports no database driver,
  no HTTP client and calls no API, with 12 tests — but PostgreSQL, the API and ClickHouse do not exist
  yet, so "keeps running while they are down" is *vacuously* true rather than tested. The chaos coverage
  in gate `phase-0-spikes.md` 7.2 item 19 stays open and moves to wave 3.
- **The wave's most useful finding is a pattern, not a defect: four contract defects, all found by
  implementers, none by any checker** — each a document that was internally valid and named something
  that did not exist. Recorded in gate report 5.1 and in `contracts/README.md`. Consequence to carry:
  **a green conformance run means the contracts are self-consistent, not that they agree with the
  code**, and the two-checker split (`validate.py` for well-formedness, the corpus test in
  `tributary-runtime` for name resolution) must not be merged.
- **W1-1 contracts (architect): DONE and verified.**
  `tributary.channel-spec/v0` is published — schema, README, 23 examples (4 valid, 19 invalid) — plus
  `contracts/validate.py`, a standalone checker for all four contracts. Envelope gained section 2.3
  (every header parsed defensively: reject, never crash — Phase 1 requirement 13 generalised beyond
  the single header SR-10 hit) and 2.4 (Phase 0 spike drift not to reproduce). Topics gained 3.7
  (`topic-defaults.json` is read, never restated as a literal) and 3.8 (the lite RF=1 startup warning
  is mandatory and testable). `events/v0` needed no change, recorded with the reason. Decisions in
  [ADR-007](../adr/ADR-007-channel-spec-shape-and-refusals.md).
  **Verified by three first-run-green checks, none of them by the architect, who has no shell:**
  `contracts/validate.py` **96 / 0** (lead); `--emit-expectations` showed **all 21 new
  `(json_path, validator)` pairs matched** the measured run, so no manifest needed correcting
  (`qa-engineer`); `pytest tests/conformance` **105 passed** and `validate.py` **100 / 0** at
  `f5bc92d`, the latest run, covering the requirement-13 and quarantine additions (`qa-engineer`).
  `devops-engineer`
  wired a standalone `contracts` CI job at `f649665`, running before `uv sync` and separate from the
  pytest conformance job. The 21 pairs were written as predictions and labelled UNMEASURED
  everywhere until they were measured.
- **"Contracts v1" is contract set 1.0 — APPROVED by the lead 2026-09-22**, not escalated to the
  human: a documentation and versioning convention, not a licensing or invariant question. Set 1.0 is
  `envelope/v0` + `topics/v0` + `events/v0` + `channel-spec/v0`, frozen together at the wave-1 gate;
  there are deliberately **no `contracts/*/v1/` directories**. Mapping table and the rejected
  alternative are in `contracts/README.md`. The lead will name the choice in the wave summary so the
  human is not surprised by the absent directories.
- **Phase 0 branch:** `wave/0`
- **All four blocking gate conditions are met:** (a) the root `.gitignore` restored and the stray root
  `Dockerfile` removed — human commit `325cef1`, 31 lines back, lead-verified that `samples/mirth/*`,
  `.env`, `secrets/` and `benchmarks/results/` are ignored again; (b) the spike data plane bound to
  localhost — `e553fd7` (MLLP default `127.0.0.1`) and `71364c7` (all four published ports on
  `127.0.0.1`, `DYNAMIC_CONFIG_ENABLED` removed), after the lead stopped the cluster on 2026-09-21;
  (c) the three MPL-2.0 dependencies approved by the human on 2026-09-22; (d) the sign-off itself.
  **What remains are the Phase 1 requirements** in the gate report section 7.1 and three non-blocking
  open items: audit retention, the Mirth corpus, and the keyed-hash mechanism.
- **Last gate report:** [`docs/agent-team/gates/phase-0-spikes.md`](gates/phase-0-spikes.md) — the
  exit-criteria check (**5 of 5 PASS**), the invariant 1-10 audit and both reviewers' findings
  (no Critical; both Highs now fixed). **GO WITH CONDITIONS, signed 2026-09-22.**
  The technical decision memo behind it is `docs/spikes/PHASE-0-DECISION.md` — **GO WITH CHANGES**, no
  invariant changes. Two planning values were disproved and corrected: the 12-partition default (now 3)
  and the 200-500 msg/s/core estimate (measured 2,446 with `python-hl7`). Two thresholds were not met
  as written: S2's 50 ms ACK p99 (cause unresolved, environment-dominated) and S4's 500 channels at 12
  partitions per topic. **9 decisions wait on the human** — see below.
- **Contracts:** v0 Draft published for envelope, topics and events. Conformance passed 62 / 0 / 0 on
  2026-09-20, then `security-reviewer` raised 5 HIGH findings; all 5 accepted and fixed in place (still
  Draft, no version bump). The suite then passed 70 / 0 / 0 on those fixes at commit `ae4d3bd` — envelope 7 valid /
  16 invalid, events 8 / 14, topic names 10 / 12. (Reported by the lead; the architect has no shell
  and did not run it. The working tree has moved past `ae4d3bd`, so that result covers the committed
  state, not the current one.)
  **Round 2 (findings 6-14) is triaged: 7 fixed, 1 escalated, 1 referred to the human, none
  disputed.** Finding 6 (the `_hmac` mechanism) is answered as UNRESOLVED-by-design and escalated to
  ADR-004 plus a human; finding 12 (audit retention) is in the open questions below. The other seven
  are fixed in place — per-finding record in `contracts/CHANGELOG.md`, status table in
  `docs/security/contracts-v0-review.md`. **Conformance: 76 passed at `83a1fd2`** (was 74, then 70 at
  `ae4d3bd`, 62 on the first run). The jump to 76 is `qa-engineer`'s expected-failure assertions: every
  invalid example must now fail at the `expected_json_path` and `expected_validator` recorded in its
  manifest, so an example failing for the wrong reason goes red — `code-reviewer` Medium 2 from the gate.
  No schema bugs at any point. Run by the lead and `qa-engineer`; the architect has no shell.

### Spike reports filed

All six filed and synthesised. Full threshold table with evidence ratings quoted verbatim:
`docs/spikes/PHASE-0-DECISION.md` section 3.

| Spike | Owner | Report | Verdict |
|---|---|---|---|
| S1 HL7 parse plus transform throughput | `qa-engineer` | `S1-hl7-throughput.md` (**accepted** 2026-09-20) | **PASS.** Solo host: lazy 36,322 msg/s/core, `python-hl7` 2,446 (12.2x); `hl7apy` 100 = **FAIL**, off the hot path. S1b (Rust/PyO3) not needed |
| S2 MLLP ACK-after-commit | `connector-engineer` | `S2-mllp-kafka.md` | **Mixed.** Zero-loss (20/20 trials, "no loss observed") and no-ACK-while-unavailable (6/6) **MET**; ACK p99 **NOT MET as measured** (33-125 ms vs 50 ms, cause unresolved). New finding: one broker kill stalls *all* connections 7.5-12.7 s |
| S3 synchronous reply | `connector-engineer`, `platform-engineer` | `S3-sync-reply.md` | **Established against a stand-in worker.** +40 ms p50 at 100 msg/s, 4.6 KB per in-flight request, 0 lost across a restart. 1,026 timed-out requests were already committed → sync mode needs loader idempotency |
| S4 topic-count stress | `qa-engineer` | `S4-topic-scale.md` | **PASS at 3 partitions/topic** (1,000 channels healthy); **FAIL at the plan's 12** (cluster lost at ~425-450 channels). Rebalance 4.3 s; static removal 43.8 s is session-timeout-bound. 2G heap inconclusive |
| S5 ordering, blocking retry, DLQ | `platform-engineer` | `S5-ordering-retry.md` | **PASS.** 0 order violations everywhere; 12-message replay after a kill still ordered; cooperative rebalance moved exactly 2 of 6 partitions; txn exactly-once across a kill. **`retry-topic` not implemented or measured** |
| S6 Mirth import corpus | `migration-engineer` | `S6-mirth-import-corpus.md` | **INCONCLUSIVE** — no corpus. 43-60+ person-days and "5-20% importable" are guesses |
| Evidence review | `code-reviewer` | `reviews/phase-0-evidence-review.md` | *"Nothing UNSUPPORTED. No Critical findings."* Two residual `WEAKLY SUPPORTED` ratings (S4 heap as the cause; the 8k/20k trigger judgement) and finding L2 open |

## Sign-offs (human)

| Checkpoint | Decision | Date | Notes |
|---|---|---|---|
| Phase 0 go/no-go | **GO WITH CONDITIONS (signed by the human)** | 2026-09-22 | Instruction: *"Approve orjson, hypothesis and pathspec; record Phase 0 GO WITH CONDITIONS signed"*. All four blocking conditions were met first. **The conditions still standing are the non-blocking open items only:** audit retention (open, blocks the `_tb.<env>.audit` contract), the Mirth corpus (not decided), the keyed-hash mechanism (unresolved until before the archiver is built), and the Phase 1 requirements table in `gates/phase-0-spikes.md` 7.1. **Signing does NOT start Phase 1 — Phase 1 needs the human's separate go-ahead.** |
| Phase 1 wave 1 exit | **CI passed on the pushed branch; starting wave 2 approved** | 2026-09-24 | Recorded exactly as relayed by the lead: the human confirmed that CI passed and approved starting wave 2. That met the gate's one remaining condition. **Not recorded here, because it was not stated:** a merge to `main` (none has happened; `main` is at `2c25448`) and any separate signature on the gate report |
| Phase 1 wave 2 exit | **Closed by the lead and architect under the human's delegation; condition lifted 2026-10-05** | 2026-10-05 | The human's words, relayed by the lead: *"Execute them by priority and let the lead and architect decide."* Criteria 1-4 met (Current, Wave 2 exit). Condition lifted: `security-reviewer` closed `fb60f3a` and three other wave 2 findings at `b6b0c97` (lead-relayed, no scanners run). **Not a human signature on a gate report. No merge to `main`** |
| Merge of `wave/1-1` and `wave/1-2` to `main`; PR for `wave/1-3` | **Instructed by the human** (relayed by the lead) | 2026-10-06 | The lead executes the git side. Merge commit and PR not yet seen by the architect |
| Phase 1 wave 3 exit | pending | | |
| MVP release decision | pending | | |
| Phase 2 v1.0 release decision | pending | | |

## Decisions log

| Date | Decision | Reference |
|---|---|---|
| 2026-09-20 | Contracts v0 (Draft) published: envelope, topic naming and defaults, lifecycle events, each with a JSON Schema and valid/invalid examples | `contracts/README.md`, `contracts/CHANGELOG.md` |
| 2026-09-20 | Four headers added beyond ADR-001 5.4: `tb-destination`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`. Additive; needed for retry and DLQ triage | `contracts/CHANGELOG.md` |
| 2026-09-20 | Platform topics are environment-scoped (`_tb.<env>.events`). Deviates from ADR-001 5.3 prose; topic class unchanged. **Needs human confirmation at the gate** | ADR-002 D2 |
| 2026-09-20 | Events carry a `kind` discriminator (`message` / `partition`) and an RFC 3339 `ts`, differing from the S5 spike emitter | `contracts/events/v0/README.md` |
| 2026-09-20 | ADR-002 and ADR-003 drafted as skeletons with `TODO(Sn)` placeholders; no unmeasured number presented as a result | `docs/adr/README.md` |
| 2026-09-20 | 5 HIGH security findings against contracts v0 all accepted, none disputed. Fixed in place while Draft: PHI key removed from the validation projection, `attrs` PHI rules made enforceable, claim-check URIs constrained, required security profile added, and the durability floor pinned per topic (an invariant-1 bug) | `contracts/CHANGELOG.md`, ADR-002 D8/D9 |
| 2026-09-20 | Transport security and authorization belong in the topics contract, not deferred wholesale to ADR-004. Boundary: contract fixes mechanisms and ACL shape; ADR-004 plus a human own the role model, rotation and key management | ADR-002 D9 |
| 2026-09-20 | Security review round 2 (findings 6-14): 7 fixed, 1 escalated (6), 1 referred to the human (12), none disputed | `contracts/CHANGELOG.md`, `docs/security/contracts-v0-review.md` |
| 2026-09-20 | `tb-correlation-id` is a UUID minted by the engine, not free text. A sender-supplied token is mapped by the receiver in memory and never becomes the reply record key | ADR-002 D10, `contracts/envelope/v0/README.md` 3.2 |
| 2026-09-20 | `tb-usr-*` headers are **not** projected into `_tb.events` in v0. Chose this over defining a header-to-`attrs` mapping: an automatic projection makes every channel-defined header a silent archive column | ADR-002 D11, `contracts/envelope/v0/README.md` 2.2 |
| 2026-09-20 | Topic sharing must not span a tenant or trust boundary — a restriction enforced by the control plane, not a documented cost | `contracts/topics/v0/README.md` 1.3 |
| 2026-09-20 | Encryption at rest is required by the topics contract (broker disks, claim-check bucket, archive volumes, backups), stated with the threats it does not cover. Key custody stays with ADR-004 | `contracts/topics/v0/README.md` 3.5 |
| 2026-09-20 | **Phase 0 verdict: GO WITH CHANGES.** No invariant changes; ADR-001 5.3, 6.3, 7.1, 7.3, 13 and 14 updated with measurements | `docs/spikes/PHASE-0-DECISION.md` |
| 2026-09-20 | **Partition default cut from 12 to 3 per channel topic**, and the sharing trigger restated in projected partitions (warn 8k, share 12k, believed fatal ~20k). The 12 was an ADR-001 planning value that S4 disproved | ADR-002 D4/D5, `contracts/topics/v0` 2.2 |
| 2026-09-20 | `session.timeout.ms` set explicitly to 15 s (band 10-15 s): it is the recovery-time floor for an ordered channel, confirmed by two independent harnesses (S5, S4) | ADR-002 D5.1, `contracts/topics/v0` 3.3 |
| 2026-09-20 | Retry defaults: `blocking` is the default policy, `blocking` → `halt-partition`, `retry-topic` → `dlq-and-continue`; poison retried 3 times (judgement, unmeasured). **`retry-topic` may not be selected until it is spiked** | ADR-003 D1/D3/D4 |
| 2026-09-20 | Kafka transactions stay **opt-in**, not on by default: their cost against a plain at-least-once stage was never isolated, and a dead producer stalls `read_committed` consumers for up to `transaction.timeout.ms` | ADR-003 D6 |
| 2026-09-20 | The retry-delay mechanism (one `retry` topic plus `tb-retry-not-before` versus one topic per tier) is **deferred to a Phase 1 spike** rather than decided on no evidence | ADR-003 D2 |
| 2026-09-20 | Synchronous reply requires loader idempotency on `tb-msg-id`; the timeout NACK means "committed, outcome unknown"; the reply worker uses fail-fast plus DLQ and the batched-commit path | ADR-003 D9 |
| 2026-09-21 | **Human, "proceed with the recommendations of the decisions":** invariants 1-10 confirmed unchanged — **ADOPTED** | gate report section 8 |
| 2026-09-21 | **Human:** `_tb.<env>.*` platform topic naming (ADR-002 D2) confirmed as a refinement of ADR-001 5.3 prose, not an invariant-2 change — **ADOPTED** | ADR-002 D2, gate report section 8 |
| 2026-09-21 | **Human:** lite mode with RF=1 — **ADOPTED** as a documented profile with a startup warning and no RPO=0 promise. Recorded as an explicit, human-approved **profile exception to invariant 1's durability promise**; the invariant is not edited and the exception lives in ADR-002 D14, `contracts/topics/v0` and the docs | ADR-002 D14 |
| 2026-09-21 | **Human:** `_hmac` mechanism — recommendation **ADOPTED**: leave UNRESOLVED until before the archiver is built, no agent designs it, and per-patient message-browser search stays blocked on it | `contracts/events/v0/README.md` 3.1 |
| 2026-09-21 | **Human:** ADR-004 scope **APPROVED** — Kafka role/ACL model, credential rotation, key custody for encryption at rest and the `_hmac` key, claim-check object lifecycle. Needed before the archiver or production | gate report section 8 |
| 2026-09-21 | **Human:** Linux re-run of S1 and S2 **APPROVED** as the first Phase 1 CI task, before the receiver is built | `PHASE-0-DECISION.md` section 7 item 7 |
| 2026-09-21 | **Human:** revised Phase 1 scope (`PHASE-0-DECISION.md` section 8) **APPROVED** | `PHASE-0-DECISION.md` section 8 |
| 2026-09-21 | Lead stopped the running spike cluster (`docker compose down`) because of the all-interfaces exposure (`security-reviewer` High 2). Nothing lost; storage was ephemeral | gate report 1.1 |
| 2026-09-22 | **Human, "Approve orjson, hypothesis and pathspec":** the three MPL-2.0 dependencies are **APPROVED** as unmodified — `orjson` (shipping runtime, `MPL-2.0 AND (Apache-2.0 OR MIT)`), `hypothesis` (test-only), `pathspec` (build-time). The approval covers these three packages unmodified; **any new or modified MPL dependency needs fresh approval**, and the inventory is known to be incomplete because `[build-system] requires` resolves outside the lockfiles | gate report 5.1, ADR-001 2.3 |
| 2026-09-22 | **Human: Phase 0 gate signed as GO WITH CONDITIONS.** Does not start Phase 1 | gate report, sign-offs table |
| 2026-09-23 | **Human: `certifi` (MPL-2.0) APPROVED** as an unmodified, **dev-group-only, never-shipped** dependency, scoped to its current transitive path `testcontainers[kafka]` → `docker` → `requests`. Recorded as **its own approval, not an extension** of the `orjson`/`hypothesis`/`pathspec` decision, whose scope note covered *those three packages* — a transitive arrival is exactly the case that note anticipated. A different path, or a runtime group, is a different dependency needing a fresh decision | ADR-001 2.3, gate `phase-1-wave-1.md` 5.3 |
| 2026-09-23 | **A hand-written licence comment is not a licence clearance.** `certifi` was found by `licence_check.py` exiting 1 in CI, not by review: the root manifest annotates `testcontainers[kafka]  # Apache-2.0`, which is correct about testcontainers and silent about its transitive closure. Second near-miss of the same shape after Phase 0's `pathspec`, which a lockfile-only inventory missed because `[build-system] requires` resolves separately | ADR-001 2.3 |
| 2026-09-22 | **Channel spec v0 published** (W1-1): declarative spec referencing code in a digest-pinned bundle. Rejected: a Mirth-parity spec with inline source (breaks invariant 7 and puts arbitrary code on `_tb.channels`, which is RCE across the data plane) and a code-first channel with no document (the UI and control plane could not reason about a channel without executing tenant code) | ADR-007, `contracts/channel-spec/v0` |
| 2026-09-22 | **`retry-topic` is in the policy enum and rejected by the validator.** Vocabulary stays shared across specs, docs and code; selection is a validation failure until the spike. Chosen over a prose-only ban because both Phase 0 High findings were controls that were specified and then not applied | ADR-007 D5, ADR-003 D1/D2 |
| 2026-09-22 | **Security rules that a schema can hold are held by the schema**: TLS unless loopback, `tls.clientAuth` stated explicitly, plaintext HTTP only to loopback, no query string in a destination URL, credential-shaped property names rejected, `queue.maxBytes` required. Phase 1 requirements 5 and 8 become validation failures rather than review findings | ADR-007 D7 |
| 2026-09-22 | **`idempotency` is required on every destination**, with a written justification when `none`. Invariant 4 becomes declarative and per-channel reviewable instead of a sentence in a loader docstring. The key is always `tb-msg-id` and is not configurable | ADR-007 D4 |
| 2026-09-22 | **The `_tb.<env>.channels` record shape is specified for the first time**: key, value, tombstone, and the rule that the runtime rejects a published record without `metadata.version`. Invariant 3 made that topic load-bearing and nothing had defined it | `contracts/channel-spec/v0/README.md` 6 |
| 2026-09-22 | **"Contracts v1" = contract set 1.0**, not `v1/` directories — **lead-approved**, deliberately not escalated to the human as a versioning convention rather than a licensing or invariant question. Rejected: copying three contracts into new major directories — ~60 files moved, every path in `tests/conformance`, ADR-002, ADR-003 and this file broken, no schema changed. A new major directory is created by a breaking change *after* a freeze, not by the freeze | `contracts/README.md`, `contracts/CHANGELOG.md` |
| 2026-09-22 | **`.claude/agent-memory/` covered by a directory pattern in `.gitignore`** (lead, `49b8a91`, verified with `git check-ignore`), replacing the per-file list the human added in `5aa6c54`, which two new memory files had already outgrown in a single wave. Additive and in its own commit — the opposite of Phase 0 High 1, where a `.gitignore` change rode along in another commit and silently removed protection. Recorded because gate item 22 assigns `.gitignore` to `devops-engineer`, and this is Phase 1's first change to it | gate `phase-0-spikes.md` 7.2 item 22 |
| 2026-09-22 | **Phase 1 requirement 13 generalised**: every integer, timestamp and UUID header is parse-or-reject, with `reject` defined (dlq + `HEADER_PARSE_ERROR` + event + commit) and the failing value kept out of the error string. A missing required header is the same class of failure | `contracts/envelope/v0` 2.3 |
| 2026-09-22 | **`topic-defaults.json` is read, never restated as a literal**, in runtime, control plane, topic creation, Compose and Helm. Reason: `min.insync.replicas` is topic-level, so a copied-and-forgotten literal makes invariant 1 silently false — the HIGH finding's original mechanism | `contracts/topics/v0` 3.7 |
| 2026-09-22 | Both gate High findings closed: root `.gitignore` restored (human `325cef1`) and the spike data plane bound to localhost (`e553fd7`, `71364c7`). Conformance suite now asserts *why* each invalid example fails (`83a1fd2`, 76 passed), and the AA-withheld-until-publish-resolves unit test exists (`a5df2c7`) | gate report 1.1 |
| 2026-09-24 | **Human (relayed by the lead): wave 1 CI passed; start wave 2.** Wave 1 is not merged to `main` | sign-offs table |
| 2026-09-24 | **Contract set 1.1 published, additive, unverified**: wave 2 connector shapes, per-destination steps, `partitionKey` semantics, receiver consumer-group role | `contracts/CHANGELOG.md` |
| 2026-09-24 | **ADR-007 D6 amended (D10):** connector types enter the enum at the start of the wave that builds them. Rejected: shapes arriving with each connector, which would have ten connectors inventing the contract in parallel | ADR-007 5a |
| 2026-09-24 | **Kafka connectors always use their own explicit connection and credentials** (D11). Rejected: a "local cluster" mode on the engine's principal, which is an ACL-model change owned by ADR-004 | ADR-007 5a |
| 2026-09-24 | **The `fhir` destination is transaction-only** (D12), which keeps identifiers out of request lines without a no-log exception. Rejected: conditional create/update with `?identifier=` | ADR-007 5a |
| 2026-09-24 | **SQL sinks take a step-built row; no mapping language in the spec** (D13). Each destination type may declare only the idempotency techniques it can honour, and the SQL loaders verify theirs at startup (D14) | ADR-007 5a |
| 2026-09-24 | **`retry-topic` lift criteria fixed before the spike** (D15), including that retried messages' reordering is characterised, not denied, and that a per-key channel must say `acceptReordering: true` | channel-spec README 4.1.1 |
| 2026-09-24 | **A refused Kafka-source record halts the source partition** (D16). The first draft dead-lettered it; the receiver ACL has no `dlq` WRITE, so that rule was unimplementable, and it was caught by tracing it into `topics/v0` before publishing | ADR-007 5a |
| 2026-09-24 | **Per-destination steps** (D17), run in the pipeline worker with no new hop. Exit criterion 1 needs a FHIR Bundle and a table row from one channel | channel-spec README 3.5 |
| 2026-09-24 | **Wave 2 planning drafts consolidated into `phases/wave-2.md`**; six lead drafts reduced to pointers, and errors listed in its section 9 | `phases/wave-2.md` |
| 2026-09-24 | **W2-2 `ctx.fhir` / `ctx.terminology` APPROVED WITH CONDITIONS.** Local Bundle-entry construction with no network; terminology behind a Protocol with fakes; no `ctx.ids` (it would reach the human-reserved `_hmac` question). Conditions: `fhir.resources`/`fhir-core` pass `licence_check.py`; per-message cost measured; R4B models guarded by validating the golden files against the FHIR R4 JSON schema; conditional entries on exactly one unique identifier system; no unconditional create; deterministic fullUrls; runtime attachment optional, not a hard import (agreed with `platform-engineer`) | channel-spec README 3.4.2 (corrected the same day: a business-identifier conditional PUT qualifies) |
| 2026-09-24 | **Human: `paramiko` (LGPL-2.1) APPROVED** as an unmodified dependency of `tributary-loaders` and `tributary-receivers`, for SFTP only. The lead asked the human directly and the human chose option 1 of three (shell-out to OpenSSH and descoping SFTP were the others). Recorded as **its own approval, not an extension** of the MPL or `certifi` decisions. Any other package, or a modified copy, needs a fresh decision. paramiko's own dependencies must each still pass `licence_check.py`; confirming that reading with the human is with the lead. SFTP contract shapes unchanged, `hostKeys` stay SHA-256 fingerprints | ADR-001 2.3 |
| 2026-09-24 | **Contract set 1.1 verified**: 122 / 0, 86 pairs matched, conformance 127 passed (`qa-engineer`, `ffc1580`) | `contracts/CHANGELOG.md` |
| 2026-09-24 | **Key evaluator APPROVED** as `tributary.sdk.keys` (`platform-engineer`): pure, stdlib only, and returns `ResolvedKey \| UnresolvedKey(reason code)` rather than raising, so the unresolved path cannot carry the value. The repr is redacted | channel-spec README 4.3 |
| 2026-09-24 | **PID-3 repetition selector** `hl7:PID.3[5:MR].1` added now, not in v1. Evidence (`platform-engineer`): repetition order differs between senders, so a bare `PID.3.1` gives one patient two keys. The "normalise in a step" advice was withdrawn as impossible, because keys are evaluated before steps | channel-spec README 4.3 |
| 2026-09-24 | **`with` binds to keyword-only step parameters by name**, refused loudly at load if wrong. The SDK's `@step` changes so that a keyword-only parameter without a default means "required from `with`". Rejected: an untyped `ctx.params` | channel-spec README 3.3 |
| 2026-09-24 | **Reprocess of any `dlq` record carrying `tb-destination` must be destination-scoped**, and is refused until W3-3 builds it. This covers wave 1 loader-exhaustion records too. A destination-step `dlq` record carries the step's input | ADR-003 follow-up 10 |
| 2026-09-24 | **W2-5 public surface APPROVED WITH CHANGES.** filter needs an explicit `onUnresolved` and may not filter on identifier fields; the enrich allow-list is trimmed; `Reject(code)` is added to the SDK and may not use engine-reserved codes; split becomes a `@splitter` node with **deterministic child ids** and is refused on per-key channels. Metrics and tracing wait for `security-reviewer` on the `/metrics` listener. `contracts/observability/v0` is to be published by the architect, seeded with `platform-engineer`'s names | channel-spec README 3.3.2 |
| 2026-09-24 | **W2-2 implemented (`39a4006`); the interface is approved from the architect's side, with five items open.** W2-3 itself stays gated behind the W2-1 security fixes (the lead's gate). (1) ~~Licence of `fhir.resources`/`fhir-core` UNVERIFIED~~ **Closed at `f083c49`**: `licence_check.py` now resolves them, both BSD. (2) Confirm conditional-only upsert, create and delete on one required identifier. (3) Production fullUrls derived from `tb-msg-id`, not uuid4, so shadow and reprocess diffs stay comparable. (4) Golden files validated against the FHIR R4 JSON schema. (5) Optional runtime attachment, agreed with `platform-engineer`. Informal cost is about 313 µs per resource against the 5 ms budget | `packages/tributary-fhir` |
| 2026-09-24 | **`postgresql` loader BUILT** (`eca445a`, `connector-engineer`), not yet "Implemented": its duplicate tests repeat `deliver()` in one process, so the 5.1 redelivery test waits for the W2-9 harness. Adopted from the build: all authentication material under `auth.secretRef`; the uniqueness check reads the catalog, because a bare unique index is invisible in `information_schema` | channel-spec README 3.0, 3.4.3 |
| 2026-09-24 | **Manifest `runnable` checked against W2-1** (`9078982` registers `tributary.hl7v2.parse` only). No example flips: each one naming `parse` also names a `ref` step, and the fanout example needs the unregistered `tributary.hl7v2.to_json`. `requires` lists trimmed to what is still missing | `examples/manifest.json` |
| 2026-09-25 | **Human (asked directly by the lead): the `clickhouse` loader uses ClickHouse's HTTP interface through the Python standard library**, built like the HTTP loader: redirects refused, no pooling, no hidden retries, auth in headers from `secretRef`, stdlib TLS. No new dependency. Found by `connector-engineer` on W2-6: `clickhouse-connect` (Apache-2.0) unconditionally requires `certifi` (MPL-2.0), approved dev-only, so `licence_check.py` failed when it entered `tributary-loaders`' runtime closure; the engineer stopped and reverted. Its defaults (`query_retries=2`, internal HTTP retries) also re-send silently on connection failure (`wave-2.md` 6.3). **`certifi`'s scope is unchanged: non-runtime only.** Rejected: approving `certifi` for the runtime; deferring the loader. **Deviates from the CLAUDE.md stack table** (Archive names `clickhouse-connect`) for the loader only; the table is human-owned and the proposed one-line change went to the lead. The wave 3 archiver's client is **not decided** (open questions) | ADR-001 2.3 |
| 2026-09-25 | **W2-5 contract proposals ruled (six).** (1) A keyword-named `with` key binds to `key_`, and `**kwargs` is refused: **approved**. (2) Derived child-id encoding: **approved with the layout pinned exactly.** Bits 12-17 of the 80-bit digest prefix are discarded, and the "set version and variant bits" wording, which also reads as overwrite-in-place, is replaced. (3) Engine error-code registry: **approved with amendments.** The contract is the source and code is checked against it, not generated from it. Eight loader-family prefixes are added. **Drift found:** the executor emits `STEP_ERROR` where the contract says `TRANSFORM_ERROR`, and the contract wins. (4) Built-in `with` shapes: **approved.** The filter's JSON deny-list applies to every pointer token, which corrects my "ends in" wording. (5) Reprocess refuses `tb-parent-id` records until W3-3: **approved as refuse-only.** It over-refuses reprocessed records, on purpose. (6) Manifest: **approved.** It was three examples, not two. The `parse` `version` gap is added to two examples, and nothing flips to runnable. Rejected: generating the registry from `codes.py`, and "refuse or scope" for split children before a re-entry point exists | `contracts/CHANGELOG.md` 2026-09-25, channel-spec README 3.3-3.3.3, envelope README 2.3.2, ADR-003 follow-up 10 |
| 2026-09-25 | **W2-5 second round (five items).** (1) **Filter allow-list ratified** (21 fields; security MEDIUM 3), **with an amendment**: a repeated `ORC`/`OBR`/`OBX` (any repeated segment) is unresolved for the filter, so a first-occurrence read cannot silently drop a multi-result message. No example is refused. Rejected: a longer deny-list, because it fails open. (2) hl7-batch final CR only if the parent had it: **ratified**. (3) `TB_OUTPUT_ENVELOPE_INVALID` and `TB_PIPELINE_TIME_BUDGET` **registered by exact name**; fixed names under a reserved prefix are listed. (4) Bundle-declared `Reject` codes **deferred to bundle-fetch**; interim, codes are literals, and a data-built code is Critical in review. (5) `ref` restricted to the pinned bundle's own packages: **approved now**, enforced with bundle-fetch | `contracts/CHANGELOG.md` 2026-09-25 (later), channel-spec README 3.3.1, 3.3.2, 9 |
| 2026-09-26 | **`raw` `max.message.bytes` stays 1,048,576 and is ruled not an envelope control.** Kafka checks it against the batch after compression, and `raw` is `compression.type: producer` with `lz4` from receivers, so a compressible payload over 1,000,000 bytes passes at any value. Rejected: lowering it to the envelope-derived 1,019,105, which would refuse only incompressible oversize records and look like a guarantee. The controls that hold: receiver-only WRITE on `raw`, receivers refusing input over 1,000,000 bytes, and the worker halting the partition with `TB_DLQ_UNWRITABLE` (registered, additive). Real fix: claim-check | `contracts/CHANGELOG.md` 2026-09-26, `topic-defaults.json` `classes.raw.max.message.bytes_source`, `envelope/v0/error-codes.json` |
| 2026-09-28 | **Human (asked directly by the lead): vendor the FHIR R4 JSON Schema, fetched over the network (option a).** The file is HL7's official `fhir.schema.json` for FHIR 4.0.1, published under CC0, which the human ruled compatible with Apache-2.0. It is **test data, not a code dependency**. Plan: commit it under a test-fixtures path, recording its source URL, FHIR version and SHA-256; a test then validates every W2-3 golden Bundle against it with `jsonschema` (already a dev dependency). Scope: the JSON Schema checks structure only; business rules and terminology are covered by the exit-criterion-1 end-to-end test against a real HAPI FHIR server. **BLOCKED until the human adds `hl7.org` (or `build.fhir.org`) to this environment's network allow-list**; the sandbox proxy refuses it today. Closes W2-2 open item 4 once done | open questions below |
| 2026-09-28 | **Human (asked directly by the lead): the wave 3 archiver uses `clickhouse-connect` (option B), so `certifi` (MPL-2.0) is APPROVED in the runtime**, unmodified, scoped to its use through `clickhouse-connect` in the archiver package. Recorded as **its own approval, not an extension** of the 2026-09-23 dev-only `certifi` approval or of the 2026-09-25 loader decision. **The `clickhouse` loader keeps its stdlib HTTP client.** Conditions: `clickhouse-connect`'s silent retries (`query_retries` and its internal HTTP retries) turned off, so the runtime retry policy is the only retry; the archiver gets the same code and security review as the loaders. Follow-up for wave 3, when the archiver package is created: `devops-engineer` updates `MPL_APPROVED` in `.github/scripts/licence_check.py` for the new runtime scope. CLAUDE.md stack table already updated by the lead on `wave/1-2`. Rejected: building the archiver on the loader's stdlib HTTP client | ADR-001 2.3 |
| 2026-09-28 | **W2-8 sync-reply proposal ruled (`platform-engineer`, `b0c91cb`); contract set 1.2 published, additive, unverified.** (1) `spec.source.reply` on `mllp`/`http`: **approved with amendments** (`after` = `routed` only; `reply.partitions` alone sizes `reply`; transactional refusal moved into the schema; split refused at load). (2) Targeted routing through the `replier` group: **approved with amendments** (`group.instance.id` per process; advertise only after an established fetch position; unparseable reply records dropped, never crash). Broadcast rejected as default. (3) `AR` = commit not confirmed, `AE` = committed, `AA` = committed and routed: **approved with an amendment**: neither `AA` nor `AE` precedes the `raw` callback, so a revoke before it waits. `Reject` → `AE` **approved**; `Drop` → `AA` **approved**; partial failure takes the first failure in spec order; absent/unknown status → `AE`. HTTP mapping **deferred to W2-7** under a no-auto-retry-status constraint (the proposed `500` breaks it). (4) Transactional and split refusals **approved**. (5) Headers **approved with amendments** (`tb-reply-status` schema-optional to stay additive; `tb-reply-deadline` present iff `tb-reply-partition`; a bad routing header disables the reply, not the message). `REPLY_` + six codes **approved**, `REPLY_PARTITION_INVALID` renamed `REPLY_ROUTE_INVALID`. ACLs and `reply.partitions` **approved**. `ctx.reply` **approved with amendments** (pipeline steps only; failed-attempt bodies discarded; `body: step` must charge bodies to `queue.maxBytes`). (6) **D9 finding correct**: a resend after `AE` has a new `tb-msg-id`; ADR-003 D9 amended, ADR-001 7/13 and the risk row corrected. No invariant changes, nothing breaking | `contracts/CHANGELOG.md` 2026-09-28, channel-spec README 3.2.6, envelope README 2.3.3, topics README 3.3.1, ADR-003 D9/D10 |
| 2026-09-29 | **Engine Kafka credentials, all data-plane roles: one `engine-kafka/v0` document per process, named by `TB_ENGINE_KAFKA_FILE`** (bootstrap servers + `kafkaSecurity` shape, secrets by `secretRef` from local files; invariant 3 holds). Workers, loaders and the archiver enforce the same rules as `e3ddee1`: PLAINTEXT only to loopback, **no opt-out**; verified TLS with the AEAD pin; credentials present; `SSL` needs a client certificate. `--bootstrap`, `--engine-security`, `TB_BOOTSTRAP_SERVERS` retired. Rejected: per-role CLI flags, env vars carrying values, fetching from the control plane | ADR-008 D1-D3, `contracts/engine-kafka/v0` |
| 2026-09-29 | **Defect found while ruling (invariant 1):** `KafkaSettings.security` is merged after the contract's producer defaults, so it can override `acks` and idempotence. No passthrough; only security keys accepted; contract values win | ADR-008 D4 |
| 2026-09-29 | **Compose lite stack runs `SASL_SSL`**, dev CA and per-component SCRAM users generated at `up`, deny-by-default ACLs from `topic-defaults.json`. Rejected: a dev-only PLAINTEXT opt-out (no definition keeps it out of production: a container network is not loopback, and lite is allowed for small sites carrying PHI), and a shared network namespace (leaves TLS, SCRAM and ACLs untested until Helm). Lite lowers durability, never transport security | ADR-008 D5, `engine-kafka/v0` 6 |
| 2026-09-29 | **`tls.allowedClientSans` APPROVED WITH AMENDMENTS** (proposed as `allowedSubjects`): typed `dns:`/`uri:` SANs only, exact, no wildcard, only with `clientAuth: required`, `mllp` and `http`, checked before any application byte; `TLS_PEER_NOT_ALLOWED`; fingerprint in logs, never names. Rejected: subject DN strings (no canonical form), required-by-default (breaking) | ADR-007 D18, channel-spec README 3.2.7 |
| 2026-09-29 | **`nackOn: spec-invalid` stays refused on every source type, documented as reserved** until receiver live reload exists, which must also define it | ADR-007 D19, channel-spec README 3.2.8 |
| 2026-09-29 | **`oauth2-client-credentials` not defined; exit-criterion-1 example switched to `bearer`**; `oauth2-client-credentials` and `mtls` reserved for `http`/`fhir`. OAuth needs a token client and, for SMART Backend Services, `private_key_jwt` key management (ADR-004 + human). Unblocks exit criterion 1 | ADR-007 D20, channel-spec README 3.4.1 |
| 2026-09-29 | **Seventeen fixed error codes registered** (the lead's four plus every loader fixed name never registered since set 1.1, and `TLS_PEER_NOT_ALLOWED`); minted `S3_` codes that collide with a fixed name become `S3_UNKNOWN`. **`fhir`, `sftp`, `s3` loaders → Built**; `s3` flagged with the two licence questions the lead has taken to the human (botocore `cacert.pem`, MinIO), not ruled here. Set 1.3 published, unverified | envelope README 2.3.2, channel-spec README 3.0, `contracts/CHANGELOG.md` 2026-09-29 |
| 2026-09-29 | **Human (asked directly by the lead): `botocore` APPROVED in the runtime**, unmodified, scoped to the `s3` loader in `tributary-loaders`, **including its bundled, unmodified `cacert.pem`** (Mozilla CA data, MPL-2.0). Recorded as **its own approval, not an extension** of the archiver's 2026-09-28 `certifi` approval. The loader still prefers the OS CA file. Found by `security-reviewer`, not by `licence_check.py`: the checker reads package metadata (botocore declares Apache-2.0), not bundled files. Follow-up for `devops-engineer`: extend `MPL_APPROVED` for this runtime scope, as a bundled-file check scoped to `tributary-loaders`, because a metadata-keyed entry is never consulted | ADR-001 2.3 |
| 2026-09-29 | **Human (asked directly by the lead): MinIO (AGPL-3.0) ALLOWED as a test-only container image** for the `s3` integration and redelivery tests. **Never in Compose, Helm or any shipped artefact.** "S3/MinIO" elsewhere means an operator-supplied S3-compatible store. `s3` loader licence questions closed; it is Implemented once its redelivery test is confirmed green in CI | ADR-001 2.3, channel-spec README 3.0 |
| 2026-09-29 | **`engine-kafka/v0` rule 4 narrowed to an exact ten-key allow-list** (set 1.3.1, patch), matching `platform-engineer`'s `SECURITY_KEYS`. Rejected: the `ssl.*`/`sasl.*` prefixes, which admit `ssl.engine.location` and `ssl.providers` (shared-object loading) and inline PEM keys. **Give up:** PKCS#11 engines and HSM providers need a contract change. Follow-up for `connector-engineer`: `RawPublisher` onto the runtime loader, and delete the receivers' prefix check (shadowed by `KafkaSettings` today; read from source, not run) | ADR-008 D4 and follow-up 6, `engine-kafka/v0` 4 |
| 2026-09-29 | **`ctx.reply` stays on the additive `ReplyContext(Context)` protocol**; `Context` is frozen for set 1.x, and new `ctx` capabilities arrive as sub-protocols. Rejected: `reply` on `Context` plus a forward in `_Attached`, which is breaking for every structural implementer and fixes only the in-tree one. **Give up:** wrappers do not gain `reply` by themselves. `healthcare-standards-engineer` makes `attach()` of a `ReplyContext` a `ReplyContext` before `body: step` is lifted | ADR-003 D10, channel-spec README 3.2.6 |
| 2026-09-29 | **ACL role table reconciled with the code (set 1.4).** Receiver, worker and loader each get READ on `_tb.<env>.channels` and on group PREFIXED `tb.<env>.<ch>.channels.`. The receiver gets it ahead of its invariant-3 migration. Worker `.retry` WRITE and loader `.retry` READ/WRITE removed (no code path). `pipeline*`/`loader.<dest>*`/`replier*` delimited. Transactional-id grant added for transactional channels. Rejected: dropping the reader's `group.id` (unverified that `confluent-kafka` allows it; kept as a later tightening) and per-channel spec topics (partition budget). **Give up:** every data-plane principal reads every spec in its environment (topology, no secrets, no PHI) | ADR-008 D6-D7, `topics/v0` 3.4.1 |
| 2026-09-29 | **Super users as implemented:** broker, `kafka-users-init`, `topic-bootstrap`, `admin`. `channel-publish` gets `control_plane` grants only; the healthcheck principal has no ACL; never a data-plane role or `ANONYMOUS`. The security review judged this tighter than the set 1.3 text | ADR-008 D8, `engine-kafka/v0` 6 rule 3 |
| 2026-09-29 | **Retry groups ruled ahead of `retry-topic`:** a shared per-channel `retry` topic, each destination in its own `loader.<dest>` group, filtering on `tb-destination`; a channel-wide group is forbidden and the ACL enforces it. Rejected: per-destination retry topics (budget, grammar) | ADR-008 D9, ADR-003 D10 amendment, channel-spec README 4.1.1 |
| 2026-09-29 | **`REPLY_EXPIRED` registered;** the event stays `status: dropped`. A `dropped` event may carry only a registered engine skip code. Rejected: no code (not queryable), `status: error` (pages on a correctly handled backlog) | envelope README 2.3.2, ADR-003 D10 amendment |
| 2026-09-29 | **`s3` `expectedBucketOwner` added** (optional, 12 digits); the loader already sends it | channel-spec README 3.4.5 |
| 2026-09-29 | **Set 1.4.1: `kafka-users-init` is not a super user** (amends the super-users row above). It holds only ALTER on CLUSTER, granted by `start-broker.sh` with the broker certificate (`5495498`). Super users: broker (`CN=kafka`), `topic-bootstrap`, `admin`. Recorded, not hidden: ALTER on CLUSTER also authorises `CreateAcls`/`DeleteAcls`, so escalation needs one explicit, listable ACL write. Rejected: keeping the set 1.4 super-user text, which is looser than the code. **Give up:** nothing functional. The key stays super-user-grade, and revoking the grant after provisioning is optional | ADR-008 D8 and follow-up 9, `engine-kafka/v0` 6 rule 3, `topics/v0` 3.4 |
| 2026-09-29 | **Context extensions load only when the channel spec declares them (set 1.5, additive).** `spec.pipeline.context` lists registered capabilities (`fhir`). A registry in the new `contracts/context-extension/v0` pins each name to one distribution. The worker never imports an undeclared registrant, and refuses a spec whose name is unregistered, duplicated or from another distribution. The group is now a contract: factory `() -> ContextExtension`, `ReplyContext` preserved, rules L1-L7, plugin obligations P1-P7 (no own logger, no global mutable state, no I/O or side effects outside `ctx`, identity delegation of `log`/`secrets`/`vars`/`kv`). `context_extension_loaded` carries distribution and version. Rejected: deployment config as the allow-list (the reviewer cannot see it, and it varies per site), both lists (drift, no extra threat covered), per-step declaration (breaks one `ctx.fhir` per branch), and load-all plus probe (the finding). **Give up:** a declared capability reaches every step on the channel; third-party capabilities need a contract change until ADR-005; an undeclared `ctx.fhir` use dead-letters late | ADR-007 D21-D22, `contracts/context-extension/v0`, channel-spec README 3.3.4 |
| 2026-09-29 | **Receivers' `--channel-spec` files are an invariant-3 defect, not a stand-in to keep.** They are to be migrated to `_tb.<env>.channels` in wave 2, starting with `mllp` (`connector-engineer`). No contract change: set 1.4 grants already cover it. Live reload stays out of scope (ADR-007 D19) | ADR-008 D6, invariant 3 |
| 2026-09-30 | **`uses` steps load only from one pinned distribution (set 1.6, additive; `platform-engineer`'s S1 approved with an amendment).** The namespace is the first two segments of a `uses` name, and a name needs three. `tributary.*` namespaces are reserved and bound by `step-namespaces.json`. Every other namespace is bound by the spec's `pipeline.stepNamespaces`. Exactly one installed entry point must carry the name, from the bound distribution (PEP 503), or the spec is refused with nothing imported. `step_entry_point_loaded` and `step_entry_point_skipped` log lines. `validate.py` cross-checks the registry against the examples and `packages/*/pyproject.toml`. Rejected: engine registry only (blocks in-house step packages, against ADR-001 2.2), an operator namespace file (invisible to the reviewer, varies per site, as in D21), exactly-one without a pin, deriving the distribution from the prefix (`tributary.core` is `tributary-runtime`). **Give up:** three-segment names, bindings restated per spec, no second distribution in an engine namespace, a same-named replacement package undetected at runtime | ADR-007 D23, channel-spec README 3.3.5, `step-namespaces.json` |
| 2026-10-05 | **Wave 2 backlog ranked; seven items deferred to wave 3** (Current, ranked table). In wave 2: exit evidence, `qa-engineer`'s CI confirmation, and set 1.7.1. Deferred: the image-build check (required before W3-8 publishes an image), the channels-probe recovery test, readiness probes, the ALTER-on-CLUSTER revoke, HTTP sync reply, the sync-reply tail's cause, and Helm. Rejected: building HTTP sync reply in wave 2. No exit criterion needs it, and its status mapping is still a contract proposal. **Give up:** HTTP senders get the after-commit answer only until wave 3, and sync senders behind an orchestrator have no readiness signal until Helm | `phases/wave-2.md` section 3 |
| 2026-10-05 | **Set 1.7.1 (patch, documentation only).** README 3.0: receivers `http`, `sftp`, `kafka` and loaders `clickhouse`, `mllp`, `kafka` Built; the `mllp` replier reads its own profile. `error-codes.json`: the five receiver-side `REPLY_` codes are emitted by `mllp` only. Sets 1.2 and 1.7 had listed `http`, which refuses `source.reply` | `contracts/CHANGELOG.md` |
| 2026-10-05 | **Wave 2 closed; wave 3 started** (lead and architect; authority: the human's instruction relayed by the lead, *"Execute them by priority and let the lead and architect decide"*). Criteria 1-4 met on CI `06fd4b6` (lead-relayed) and the three 2026-10-05 spike reports. One condition: record `security-reviewer`'s closure of the `fb60f3a` High. Wave 3 plan `phases/wave-3.md`: `phase-1.md` W3-1..W3-9 plus W3-10..W3-24 from the backlog, six seats, four batches. Rejected: holding wave 3 until the condition is recorded. Batch 1 touches nothing in `extensions.py`, and the wave 1 gate set the precedent of a condition running alongside the next wave. **Give up:** if the reviewer reopens the High, wave 3 runs alongside an open High | `phases/wave-3.md`, sign-offs table |
| 2026-10-05 | **Wave 3 defaults chosen without a human, pending human answers** (`wave-3.md` 7). W3-4a builds on `asyncpg` because `psycopg` is denied by `licence_check.py` today; the driver is isolated in the engine URL. The archiver ships with no identifier-derived attributes. The audit shape is published with retention `TODO(human)`. API authn is OIDC bearer validation with a vetted library (ADR-009, W3-1a), not a designed scheme. Rejected: blocking W3-4a on the `psycopg` answer. **Give up:** if the human approves `psycopg`, a dialect swap and a re-run of the migration tests | `phases/wave-3.md` 7 |
| 2026-10-05 | **Invariant-3 receiver item closed for invariant 3; the exact-grants ACL test is carried as W3-23.** Reverses the 2026-09-29 "wave 2 does not exit with this open" for the ACL half only. That half verifies the role table, not the spec source, and no wave 2 criterion needs it | Current, Wave 2 exit |
| 2026-10-05 | **Set 1.7 (additive, no schema change).** (1) **N4 shadowing: no runtime refusal** of same-named installations; resolve against the first on `sys.path` (what `import` loads), log hidden ones `duplicate`, refuse duplicate names at image build. Rejected: refusal at run time, which an actor with import-path write access bypasses with no metadata or a `.pth`, and which breaks stale-metadata and dev editable installs. (2) **`body: step` body unwritable → `AE` with new codes** `REPLY_BODY_INVALID` (framing byte) and `REPLY_BODY_OVER_BUDGET`; rejected reusing `REPLY_BUSY` (an `AR` code) and `REPLY_BODY_MISSING` (worker code). (3) **Per-connection fairness `max(1, maxPending // 4)` ratified**, not a spec field; give up: one connection uses a quarter of the pool. (4) **`--instance` required with sync reply, unique per replica, Helm from pod name.** (5) **Replier reads `clients.reply_consumer`** (typed since 1.2); `isolation.level` now in `must_not_hardcode`. (6) README 3.0: MLLP sync reply Built, HTTP not built | ADR-007 D24, ADR-003 D10 amendment, channel-spec README 3.0/3.2.6/3.3.5, `topics/v0` 3.3.1, `error-codes.json` |

| 2026-10-05 | **Wave 2 condition lifted.** `security-reviewer` closed four findings at `b6b0c97` (lead-relayed): `fb60f3a` High, W2-3 `Encounter` Critical (fix `e48d937` + `1aba292`, `ab4a4bf`; `4998f33` was the cause, earlier citation corrected), W2-5 findings, `EnvelopeError` PHI in exception text. New Medium (empty assigning authority in conditional FHIR keys) tracked as a wave 3 risk, owner `healthcare-standards-engineer`. **Give up:** nothing; scanner evidence is missing from that review and is carried as a risk | Current, Wave 2 exit; sign-offs table |

| 2026-10-05 | **Archive rulings on W3-2a (set 1.8).** (1) `message_id` non-nullable, nil UUID means no identity; archive dedup key `(channel, scope, message_id, stage, destination, attempt)`. (2) Archiver group is the platform group `tb.<env>.archiver`, and its ACL is LITERAL (the PREFIXED set 1.4 grant matched a channel named `archiver`). (3) Archive event TTL defaults to 90 d; the final value is human, decided with audit retention (7.2). (4) Month partitions accepted; dedup at read in the query library. Lead decision recorded: `*_hmac` and deny-listed `attrs` keys are dropped and counted, and the event is stored. Rejected: partitioning on UUIDv7 time. **Give up:** rare cross-month duplicates in storage; a duplicate partition event without `offset` | events README 4.1, `topics/v0` 3.6, CHANGELOG set 1.8 |

| 2026-10-05 | **Receiver readiness (`observability/v0` 2, W3-12).** Separate ops listener, loopback `127.0.0.1:9180` by default, explicit non-loopback for Kubernetes, fail closed on bind, no I/O on the request path. `/livez` never depends on Kafka. Gating checks: `spec`, `component`, `raw_producer`, `reply_consumer` (sync). R1: all-brokers-down makes a receiver not ready. R3: a channels-topic outage after the first spec is `degraded` only (invariant 3); W3-18's "not ready" applies before the first spec. W1: one `raw` metadata round trip before the listener binds. Whether that alone removes the S2 first-ACK outlier is unverified; W3-17 measures. Rejected: liveness tied to Kafka (restart storms); readiness dropping on spec staleness (turns a control-plane fault into an ingest outage); a warm-up record on `raw` (only sender messages go there). **Give up:** in a total Kafka outage senders see connection refused rather than NACK; a receiver keeps serving a spec the control plane has since changed | `observability/v0` README 2 |
| 2026-10-05 | **Metric labels are a closed list; log lines get a schema** (`observability/v0` 3-4). No id, key, partition, peer, URL or subject label; route templates only. `message_id` is the canonical log spelling, with `tb_msg_id` and `msg_id` accepted as deprecated aliases. New content deny-list (`payload`, `body`, `message`, `traceback`, ...). **Give up:** per-partition halt detail lives in events, not metrics | `observability/v0` README 3-4 |
| 2026-10-05 | **OpenAPI part 1 (W3-1a).** The channel document is typed loosely, with `channel-spec/v0` the authority (no cross-file `$ref` to an unserved `$id`). The server numbers versions. **Rollback republishes the old document unchanged**, so `tb-channel-version` is not monotonic. `published` means committed, not applied. Optimistic concurrency. Per-environment sub-resource paths instead of ADR-001's `:verb` sketch. Restricted problem bodies (closed codes, no `instance`, no echo). `openapi-spec-validator` added to `validate.py`; its licence closure was checked from PyPI metadata and is all permissive, no `certifi`; a missing library fails the run. Rejected: minting `n+1` on rollback (misattributes code in the archive) | `openapi/v0` README 3 |
| 2026-10-05 | **ADR-009 Proposed: OIDC bearer, API as resource server, PyJWT.** Header-only tokens; `RS256`/`PS256`/`ES256` allow-list; `iss`/`aud`/`exp` required; JWKS refresh on unknown `kid` rate-limited, with bounded staleness; roles from a configured claim through a role map, global in v0; a local issuer for lite and tests, never an auth-off flag. Rejected: API-issued sessions and tokens (key management, escalated), introspection per request (IdP becomes a hard dependency), mTLS only. **Give up:** revocation bounded by token lifetime | ADR-009 |
| 2026-10-05 | **ADR-004 drafted (Proposed).** Content archiver as a separate process and principal consuming `raw`/`out`/`dlq`; content off by default in `production` until H1/H4 are answered; per-channel principals; temporary provisioning grant (W3-16 spec); structured grants. Seven human questions H1-H7. Rejected: workers writing content (invariant 9), object-store-only content before custody is decided, per-channel content archivers | ADR-004 |
| 2026-10-05 | **ADR-009 rev 2 after `security-reviewer` (no Critical/High; Mediums settled).** D5: own key-set wrapper (background off-loop refresh, 5 s timeout, 5-300 s jittered backoff after failures, unknown-`kid` refresh single-flight and at most 1 per 60 s counting failures, max stale default 1 h, max 4 h); `PyJWKClient` not used, PyJWT parses and verifies only. D2 `require=[exp, iss, aud, sub]`. D4 with `ACCEPT_JWT_TYP`: refuse start if audience = UI client id; reject `nonce`, `at_hash`, claim `typ: ID`. D6: refuse start on empty role map; roles claim must be a string array; deny ops without `x-tributary-roles`. D1: access log off by rule; one token method only. D13: discovery `issuer` match, https `jwks_uri`, `jku`/`x5u`/`x5c`/`jwk` ignored (required test). **D14: only `operator` and `admin` see PHI content** (roles are global). Rejected: content for `developer` or `auditor`. **Give up:** dev-env content debugging needs global `operator` until env-scoped roles | ADR-009 D1-D14 |
| 2026-10-06 | **W3-8 open risks ruled (four).** (1) `/livez` + `/metrics` for every component are already normative (`observability/v0` 1); one runtime implementation (W3-25) wired by each owner (W3-26, W3-27, W3-4b); `prometheus-client` for exposition only, explicit registry. Blocks neither criterion 2 nor 4; `partition_halted` alert firing gate-tracked. (2) `--instance` vs `--instance-id`: same value and rule, two spellings kept in v0; pid defaults collide (pid 1) → required (W3-28). (3) StatefulSets for all data-plane workloads, `Parallel`, no PVCs; S5 3.21 s same-id restart vs a ~15 s session-timeout stall per new-name pod (derived, not measured on Kubernetes); KEDA scales StatefulSets. (4) `KafkaUser`s generated from `topic-defaults.json` when Strimzi is on; new DESCRIBE-only `lag_reader` role for KEDA (W3-29); contracts ship in the image, ConfigMap removed. Rejected: renaming flags now; Deployments with random names; hand-written ACLs; KEDA on a data-plane principal; operator-supplied contracts. **Give up:** rollout surge, auto-replacement on a lost node, two flag spellings, retuning without a new image | `phases/wave-3.md` 2.4-2.5 |
| 2026-10-06 | **W3-3 rulings (`archive-engineer`).** (a) `FINAL` collapses across month partitions at read time (setting `do_not_merge_across_partitions_select_final` = 0); only background merges do not. Text corrected on the engineer's 6-vs-5 measurement; `LIMIT 1 BY` stays. (b) Destination-scoped reprocess **deferred to wave 4**, refusal kept; `phase-1.md` never required it. Direction: `out.<dest>` re-entry for loader exhaustion, additive `raw` header for destination-step re-run, never a new topic class (invariant 2). New `reprocessor` role (LITERAL `raw` WRITE per channel) in W3-29. (c) `tb-received-at` copied unchanged on reprocess and replay. Rejected: unscoped re-injection with a warning; reprocess time in the header. **Give up:** no destination-level reprocess in wave 3; `now - tb-received-at` excludes records with `tb-parent-id` | `events/v0` 4.1, `envelope/v0` 6, ADR-003 f-up 10, `phases/wave-3.md` 2.6 |
| 2026-10-05 | **ADR-004 amended.** K2/H7: the PREFIXED `<env>.` READ also covers `reply`, `retry` and any future `<env>.` topic; the human approves that or per-topic LITERAL grants. A5 kept: archivers INSERT only; schema migration is a separate step with its own CREATE/ALTER credential (`archive-engineer` implementing) | ADR-004 A5, K2, H7 |
| 2026-10-06 | **Human answers to wave-3 section 7 (relayed by the lead), verbatim.** 7.1 `psycopg` vs `asyncpg`: *"let the lead and architect decide"*. 7.3 `_hmac` search attributes: *"let the lead and architect decide"*. 7.2 retention for audit records and archived events: *"Give the user a possibility to change it"*. 7.4 archive key custody: *"Let the lead, architect and security engineer decide"*. 7.6 3-broker test cluster: *"find alternative free solution"*. 7.7: merge `wave/1-1` and `wave/1-2` into `main`, and open a PR for `wave/1-3` (the lead's wording; no verbatim text relayed). Not answered: 7.5, ADR-004 H6, H7 | `phases/wave-3.md` 2.7, section 7 |
| 2026-10-06 | **PostgreSQL driver: `asyncpg`** (lead and architect, under the human's delegation). Apache-2.0, already in the lockfile, in `tributary-api` and the `postgresql` loader; `psycopg` (LGPL-3.0) stays denied by `licence_check.py`. Rejected: approving `psycopg` (an invariant-10 escalation for no functional gain). **Give up:** a single sync+async driver; with PgBouncer in transaction mode, the prepared-statement cache must be off (operator docs). **CLAUDE.md stack-table line proposed to the lead:** `\| Control plane \| FastAPI, SQLAlchemy 2, asyncpg (not psycopg: LGPL-3.0), Alembic, PostgreSQL \|` | `wave-3.md` 2.7 D1 |
| 2026-10-06 | **`_hmac` DEFERRED** (lead and architect, under the delegation) until ADR-004 H1 has concurrence and before any per-patient search task (W4-3). Any `*_hmac` value is treated as PHI whatever its construction, so the design never relies on de-identification and no agent makes a compliance reading. The archiver keeps dropping `*_hmac`. When revisited: a documented construction from an existing library only (RFC 2104 HMAC-SHA-256 via stdlib `hmac` is the candidate). Rejected: building it now (a reversible PHI index before key custody is settled). **Give up:** no per-patient search in wave 3 | ADR-004 H3, `wave-3.md` 2.7 D2 |
| 2026-10-06 | **Retention is operator-configurable** (per the human's answer). Defaults: events `TB_ARCHIVE_TTL_DAYS` 90, content `TB_ARCHIVE_CONTENT_TTL_DAYS` 30, audit `TB_ARCHIVE_AUDIT_TTL_DAYS` 2190. The audit default is conservative because deletion is irreversible; it is not a compliance determination. Integer days, 1-36500. Audit system of record: ClickHouse `audit_log`, fed by the events archiver from `_tb.<env>.audit` (a 30 d buffer). Changes are applied only by the migrate step (`MODIFY TTL`); a decrease is refused without `TB_ARCHIVE_ALLOW_TTL_DECREASE=true`. Helm `archiver.retention.*`; the API view is read-only. Kafka topic retention is unchanged. Rejected: an API write endpoint (it needs the DDL credential, and a compromised admin could erase its own trail); per-channel retention (later, additive). **Give up:** a change needs a redeploy. **Drift found:** the chart's `archiver.ttlDays: 365` against the documented 90. Contract text: set 1.8 README-only patch. Tasks W3-30, W3-31, W3-32 | ADR-004 section 4, `topics/v0` 2.1, `events/v0` 4.1, CHANGELOG 2026-10-06 |
| 2026-10-06 | **Key custody DRAFTED, pending `security-reviewer` concurrence** (lead, architect, security; the human's delegation). Tributary manages no keys. `production` requires customer-managed keys (KMS CMK or CMEK, or HSM or Vault on premises) on broker, ClickHouse, PostgreSQL, claim-check and backup storage; lite may use provider-managed keys. The claim-check bucket uses SSE-KMS by bucket default. Rotation is the KMS's automatic rotation. H2: no application-level encryption in v0 (column envelope encryption is key-management design, so human). H5: file-based secrets filled by the operator's store; 90-day recommended cadence (judgement). Documented features only; provider claims not re-checked in this session. **Give up:** production needs a KMS before content is turned on | ADR-004 H1, H2, H5 |
| 2026-10-06 | **Free RF=3 testing (W3-33a/b)** instead of paid capacity: a 3-broker KRaft cluster on the standard GitHub runner and developer hosts (`combined`, plus `split` with 1 controller so a two-broker outage keeps quorum). C1 ISR below `min.insync.replicas` gives NACK, never ACK, with a mutation control; C2 rolling restart; C3 leader kill without duplicates; B1 RF=3/RF=1 ratio. Budget about 5 GiB, 2 vCPU, ≤ 45 min (estimate). It proves correctness at RF=3 and a cost ratio, not absolute latency or sizing. Repository visibility is unverified (runner size and minutes depend on it). Paid capacity stays an optional human item (W4-6) | `wave-3.md` 2.7 D5, 2.8 |
| 2026-10-06 | **W3-29 amended; the lead's replay ruling recorded** (W3-3 security review, lead-relayed). `reprocessor` = READ, WRITE and DESCRIBE on LITERAL `<env>.<ch>.raw` plus IDEMPOTENT_WRITE; it never commits; a LITERAL `tb.<env>.reprocessor` group only if `archive-engineer` shows assign needs a `group.id`. Lead: replay targets only its source channel (no cross-channel republish); "shadow" keeps the `topics/v0` 3.6 consumer-group meaning; a replay into a per-key channel needs an explicit `allow_reorder` flag, recorded in the audit event, or it is refused (invariant 5). **Give up:** the reprocessor reads PHI on each granted channel (folds into H7) | `wave-3.md` 2.6 H8, W3-29, W3-3; ADR-004 section 5 |

## Open questions for the human

**ADR-004, state after the human's 2026-10-06 answers:** H1, H2 and H5 drafted, **pending
`security-reviewer` concurrence** (delegated to the lead, architect and security). H3 deferred, and
H4 decided (lead and architect). **Still `TODO(human)`:** H6 erasure (delete, crypto-shred, or
neither) and H7 sign-off on the Kafka role model. H7 includes the content archiver reading every
channel's PHI and, since 2026-10-06, the `reprocessor`'s READ on `raw`.

**Seven of the nine Phase 0 gate items were decided on 2026-09-21, and the MPL approval plus the gate
sign-off followed on 2026-09-22** — see the decisions log above. What remains open is listed here. Items
struck through were decided and are kept for one revision so the change is visible.

**Nothing blocks the Phase 0 gate any more — it is signed.** Wave 1 is complete. Wave 2 closed on
2026-10-05 and its one condition was lifted the same day, and wave 3 has started under the human's delegation to the lead and
architect.

**Open, new in wave 2:**

- **FHIR R4 JSON Schema for W2-2 golden-file validation: DECIDED 2026-09-28, WAITING ON NETWORK
  ACCESS.** The human chose to vendor HL7's official `fhir.schema.json` (FHIR 4.0.1, CC0, ruled
  compatible with Apache-2.0; test data, not a code dependency), fetched over the network (decisions
  log). The file is fetched once and committed under a test-fixtures path with its source URL, version
  and SHA-256, so the check is reproducible offline and in CI. It checks structure only; business rules
  and terminology are the exit-criterion-1 end-to-end test against a real HAPI FHIR server.
  **Still needed from the human:** add `hl7.org` (or `build.fhir.org`) to this environment's network
  allow-list; the proxy refuses it today. Until then W2-2 condition 4 stays open and the golden files
  are unchecked against R4.

- ~~**SFTP client licence.**~~ **DECIDED 2026-09-24: `paramiko` approved**, unmodified and scoped
  (decisions log, ADR-001 2.3). Kept for one revision so the change is visible. The scope question is
  settled: the "dependency closure" wording was the lead's relay, not the human's. The human's choice
  approved paramiko alone, so each of its dependencies must pass `licence_check.py` on its own.
- **Resolved 2026-09-24: `licence_check.py` did short-circuit on combined SPDX expressions**, in the
  MPL branch from `b773158` and in the LGPL branch modelled on it. Found by `security-reviewer`; fixed
  by `devops-engineer` at `f083c49`. Any expression containing "mpl" was approved as a whole, without
  checking the other terms: `MPL-2.0 AND Proprietary` passed as approved.
  **Note on the wave 1 licence evidence** (gate `phase-1-wave-1.md` 5.3, "44 components / 0 failed"):
  - For `orjson`, whose resolved expression is `MPL-2.0 AND (Apache-2.0 OR MIT)`, that result did
    **not** check the non-MPL terms. The conclusion was still right, since Apache-2.0 and MIT are
    permissive, but it came from a checker that could not have failed on a bad extra term.
  - `certifi` is affected only if its resolved expression is compound; a plain `MPL-2.0` hits no
    short-circuit.
  - The human approvals stand either way: they rest on the licences, not on the checker.
  - **The evidence is now real.** The fixed checker passes on the current tree. Its regression tests
    (`test_mpl_compound_expression_with_an_unapproved_extra_term_is_rejected`, and four LGPL cases)
    were shown red against the pre-fix code before going green, and the suite is 31 / 31.
  - Same fix: `tributary-fhir`'s `fhir.resources` and `fhir-core` are now actually checked (BSD), which
    closes W2-2 open item 1.
- **Wave 3 human questions (`phases/wave-3.md` section 7), 2026-10-05. ANSWERED 2026-10-06**
  (decisions log; `wave-3.md` 2.7), except 7.5. The text below is kept for one revision:
  - **`_hmac` keyed-hash mechanism is now due.** The 2026-09-21 decision deferred it "until before the
    archiver is built", and W3-2a starts the archiver now. Decide now or extend the deferral. Until
    then the archiver writes no identifier-derived attributes and per-patient search stays refused.
    The de-identification question below still stands.
  - **ADR-004 key custody** (archive volumes, claim-check bucket, `_hmac` key). W3-10 drafts options
    only. The answer gates production use of the archive, not the build.
  - **Audit retention**, below, now blocks wave 3 exit criterion 4 (audit), not only the contract.
    **Same answer, one more place (2026-10-05):** archived lifecycle events in ClickHouse default to
    90 days (`TB_ARCHIVE_TTL_DAYS`, placeholder). Say whether the retention duty reaches them.
  - **Merge `wave/1-1` and `wave/1-2` to `main`**: a human action, not blocking.
- **DECIDED 2026-10-06: `asyncpg`** (human: *"let the lead and architect decide"*; the CLAUDE.md line
  is proposed to the lead, decisions log). Kept for one revision: **`psycopg` 3 in the CLAUDE.md stack
  table. Default chosen 2026-10-05: W3-4a builds on `asyncpg`;
  the human answer is still needed** (approve `psycopg` or amend the table). CLAUDE.md names it for
  the control plane. It is LGPL-3.0, and `licence_check.py` denies it (reported by the lead and
  `connector-engineer`). Invariant 10 has no LGPL path. The options: approve it as an unmodified
  dependency, or amend the stack table to a permissive driver. The wave 2 loaders already use `asyncpg`
  (Apache-2.0), which shows one exists. Choosing the stack is the human's decision. Not a blocker,
  given the default.
- ~~**ClickHouse client for the wave 3 archiver — answer needed before the archiver is built.** On
  2026-09-25 the human decided that the `clickhouse` *loader* uses the HTTP interface via the standard
  library, because `clickhouse-connect` unconditionally pulls in `certifi` (MPL-2.0, approved dev-only)
  and retries silently by default (decisions log). That decision does **not** cover the archiver. If the
  archiver uses `clickhouse-connect`, as the CLAUDE.md stack table still says, the same question
  returns: approve `certifi` for the runtime, or build the archiver on the same stdlib HTTP client as
  the loader.~~ **DECIDED 2026-09-28: `clickhouse-connect`, with `certifi` approved in the runtime**
  for the archiver only, silent retries off, loader-level review (decisions log, ADR-001 2.3). The
  loader keeps its stdlib client. Kept for one revision so the change is visible.
- **ANSWERED 2026-10-06:** *"find alternative free solution"*, so W3-33a/b, a free 3-broker cluster.
  Paid capacity is needed only for absolute latency or sizing claims (W4-6). Kept for one revision:
  **Linux re-run capacity.** The single-broker Linux re-run was done on 2026-09-28. The memo priced the
  full version as "CI runner time and a dedicated 3-broker cluster", and a hosted runner is not a
  dedicated cluster. Whether to pay for dedicated RF=3 capacity is cloud spend (W4-6).

**Open, carried:**

- **DECIDED 2026-10-06: audit retention is an operator setting** (`TB_ARCHIVE_AUDIT_TTL_DAYS`,
  default 2190 d; the system of record is ClickHouse `audit_log`; decisions log). The audit contract is
  unblocked (W3-1b). Kept for one revision:
  **Audit retention — still open, and it blocks the `_tb.<env>.audit` contract.** What retention duty
  applies to the control-plane and access audit trail? HIPAA §164.316(b)(2)(i) requires six-year
  retention of records of required actions and activities; whether that reaches this audit trail is a
  legal-interpretation question and **no legal review has been done, nor should an agent do one**. No
  number has been written: the current 30 days is marked `TODO(human)`. The likely answer is
  architectural (ClickHouse or object storage as the system of record, Kafka as a buffer), and it also
  puts `_tb.<env>.events` (3 d) and `raw` (7 d, the replay source) in scope.
- **Mirth corpus — not decided.** Supply anonymized exports in `samples/mirth/`, or accept that the
  migration estimate stays a guess. S6 is INCONCLUSIVE without them, and migration cost is the
  highest-likelihood, highest-impact risk in ADR-001 section 13. The ordering risk that used to attach to
  this — `samples/mirth/` not being git-ignored — is **closed**: the `.gitignore` was restored in
  `325cef1`, so exports can safely land on disk now.
- Project name and trademark check (codename Tributary). **Its cost now rises with time:**
  `tributary-fhir`'s identifier system `https://tributary.dev/fhir/identifier/tb-msg-id` is written into
  resources in customers' FHIR servers. Once real data carries it, renaming means a data migration.
- License and governance model (ADR-006).
- Whether to engage the Open Integration Engine community, and how.

**Decided 2026-09-21** (detail in the decisions log and gate report section 8):

- ~~Confirm ADR-002 D2 (environment-scoped platform topics) as a refinement rather than an invariant-2
  change.~~ **ADOPTED.**
- ~~Lite mode (RF=1) cannot satisfy RPO=0. Confirm that shipping it with a startup warning and explicit
  documentation is acceptable.~~ **ADOPTED**, and recorded as an explicit human-approved profile
  exception to invariant 1's durability promise (ADR-002 D14). The invariant itself is unchanged.
- ~~Confirm that no invariant changes.~~ **ADOPTED**: invariants 1-10 unchanged.
- ~~ADR-004 scope~~ **APPROVED**: the Kafka role/ACL model, credential rotation, key custody for
  encryption at rest and the `_hmac` key, and claim-check object lifecycle. `contracts/topics/v0` 3.4
  and 3.5 state the required mechanisms and ACL shape; the model and anything touching key management
  stay a human decision. Needed before the archiver or any production deployment.
- ~~Linux re-run of S1 and S2~~ **APPROVED** as the first Phase 1 CI task, before the receiver is built.
- ~~Revised Phase 1 scope~~ **APPROVED** (`PHASE-0-DECISION.md` section 8).
- ~~The `attrs` `_hmac` mechanism (security finding 6, HIGH).~~ **Recommendation ADOPTED: it stays
  deliberately UNRESOLVED** until before the archiver is built, no agent designs it, and per-patient
  search in the message browser stays blocked on it. The schema pins the 64-lowercase-hex *shape*, which
  is all a schema can do — an unkeyed `sha256(mrn)` has the same shape and is reversible by exhaustive
  search over a bounded identifier space. Construction, key scope, rotation and key custody are ADR-004
  (approved scope) plus a human. Interim contract position stands: producers SHOULD NOT emit
  identifier-derived `attrs`, and `_hmac` values are PHI in ClickHouse. One question still needs a human
  answer when ADR-004 is written: is a keyed hash of an MRN de-identified for your compliance purposes?
  (Under a Safe Harbor reading it is not.) Detail: `contracts/events/v0/README.md` 3.1.

## Risks (live)

From ADR-001 section 13, with the evidence column Phase 0 was meant to fill. Now filled — the full
version is `docs/spikes/PHASE-0-DECISION.md` section 9 and ADR-001 section 13.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka too heavy for small sites | Medium | High | none — no spike addressed footprint | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | ~~Medium~~ **Low** | Medium | S1 solo host: ~36.3k msg/s/core (lazy) and 2,446 (`python-hl7`) against a 200 threshold. `hl7apy` fails at 100 and is off the hot path | S1b (Rust/PyO3) **not needed**. Re-measure on Linux CI and on a production mapping with terminology lookups |
| Migration cost from JavaScript deters adopters | High | High | none — S6 INCONCLUSIVE, no corpus | Importer, compat shim, assisted porting; **timebox the Phase 1 effort rather than committing to 43-60 person-days** |
| Buyers want references, certification, support SLAs | High | High | none | Design partners, security docs, paid support |
| Topic/partition explosion with many channels | Medium, **now bounded** | Medium | **S4: 12,009 partitions healthy, ~21,000 fatal at 1G heap; 500 channels FAIL at 12 partitions/topic, 1,000 PASS at 3** | **Partition default 3; budget in projected partitions; re-measure on real broker memory** |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles |
| **Receiver-wide stall on one broker loss** | **Medium** | **Medium** | **S2: 7.5-12.7 s across all connections, 20/20 trials; cause inferred, not confirmed** | **Per-connection head-of-line isolation; broker session and election tuning** |
| **Sync-reply duplicates after a timeout NACK** | **High** | **Medium** | **S3: 1,026 of 9,000 requests timed out, all already committed** | **Corrected 2026-09-28: a resend after `AE` has a new `tb-msg-id`, so `tb-msg-id` idempotency does not absorb it (ADR-003 D9 amended). Mitigations: integrator doc and control-plane warning (channel-spec 3.2.6); business-identifier idempotency at the sink; edge dedupe (D8, deferred)** |
| **ACK latency target unverified on production-like hardware** | **High** | **Medium** | **S2: p99 33-125 ms at 1k msg/s against a 50 ms threshold; a bare producer with no MLLP had p99 131 ms on the same cluster** | **Linux re-run with a dedicated cluster before any sizing claim** |
| **`retry-topic` delay tiers carried forward untested** | **Medium** | **Medium** | **none — not implemented or measured in S5** | **Spike before any destination may select it (ADR-003 D2, follow-up 5)** |

### Risks added in Phase 0

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ~~Contract schemas pass conformance, but the green run predates the round-2 security fixes~~ **Closed**: re-run at 76 (`83a1fd2`), then 106 at the wave 1 gate; the suite is a required CI job (`f649665`). Set 1.1 is the new unverified case, under wave 2 risks | — | — | — |
| **A green test whose fault never fired is indistinguishable from a passing system.** Five S5 claims were first rated `WEAKLY SUPPORTED` for exactly this: a kill at `commit_every=1` replays nothing, a joiner assigned nothing measures no rebalance, a `txn_crash` between transactions proves nothing | Medium | High | Fixed in S5 by making the run **fail** when the fault does not fire, plus mutation controls. Carry the pattern into Phase 1 test design: assert the fault happened, and keep a broken-implementation control for any check backing a headline claim |
| **Every Phase 0 number comes from one Windows laptop with Docker Desktop.** Latency figures are the least transferable; capacity figures are lower bounds | High | Medium | Linux re-run of S1 and S2 as the first Phase 1 CI task; no external publication or capacity planning from these numbers until then |
| A contract can look complete and still omit a security control (the v0 config contract specified `acks` and `linger.ms` but no TLS, auth or ACLs; the durability floor was set per profile but not per topic) | Medium | High | Both fixed. Lesson applied going forward: every contract that configures a component states its required security posture and its failure-safe defaults explicitly, and `security-reviewer` sees contracts, not only code |
| Claim-check (payloads over 1 MB) is untested by any Phase 0 spike | Medium | Medium | Call it out as an untested path in the Phase 1 plan; ADR-004 owns object lifecycle |
| The record key is often a patient identifier, so any log, metric label or span attribute built from it is a PHI leak | Medium | High | Stated in `contracts/envelope/v0` section 4; reviewers treat it as Critical |

### Risks added in wave 2

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ~~No permissively licensed SFTP client~~ **Closed 2026-09-24**: `paramiko` approved. Residual risk: its auto-add host-key policy is the library default to watch | Medium | High | Fingerprint check before authentication, no auto-add fallback (channel-spec README 10.2) |
| **Contract set 1.1 has a defect only an implementer finds** (wave 1 had four, all found that way) | High | Medium | Conformance run first; implementers send diffs; the manifest's `connector:` entries keep unbuilt connectors out of "runnable" |
| **A client-library default silently breaks a guarantee** (redirects, TLS verification, driver retries, error text quoting payload). Wave 2 adds seven client libraries | Medium | High | The loader rules in channel-spec README 3.4; reviews target defaults and transitive edges (`wave-2.md` 6.3) |
| **Remote file names and object keys carry PHI** (new surfaces with SFTP and S3) | Medium | High | Names built from `tb-msg-id` only on output; inbound file names never logged (README 3.2.4, 3.4.4) |
| **ClickHouse duplicate test passes falsely**, because deduplication is eventual | Medium | Medium | Assert a `FINAL` count (README 5.1) |
| **Exit criterion 4 reported against laptop numbers only** | High | Medium | Run the Linux re-run, or say so in the report rather than compare as if the numbers were comparable |
| **Claim-check is unscheduled** while file and HTTP sources make large inputs more common | Medium | Medium | Every source refuses input over 1 MB; the lead decides whether to schedule the spike (`wave-2.md` 5.2) |
| ~~**A produce-time `EnvelopeError` crashes the pipeline worker**~~ **CLOSED 2026-10-05** (`security-reviewer` at `b6b0c97`, `worker.py:589-603`; PHI-in-exception-text also closed) | — | — | Dead-letter the consumed record as `TB_OUTPUT_ENVELOPE_INVALID`, producing nothing (`2a16e24`, `8af0d63`, `6daf4f4`) |
| **A `raw` payload over 1 MB halts its partition** (W2-5 residual, accepted for wave 2) | Low: needs WRITE on `raw`, which is receiver-only (`topics/v0` 3.4). Broker `max.message.bytes` does not stop it, because it checks the compressed batch | Medium: the partition halts until an operator clears it; nothing is lost | Claim-check (unscheduled). Interim done (`361935e`): `partition_halted` with `TB_DLQ_UNWRITABLE` instead of a crash loop. No clear or skip tooling yet |
| **Contract and code drift on names nobody checks** (latest: `STEP_ERROR` vs `TRANSFORM_ERROR`, found only while writing the registry) | Medium | Medium | Registry plus a two-way cross-check test in `tributary-sdk`. The same shape as the wave 1 finding: a green conformance run says the contracts are self-consistent, not that they agree with the code |
| **Sync reply answers `AE` or `AA` before the commit is confirmed** (W2-8; the proposal allowed it on a revoke before the callback) | Low once tested | High: invariant 1 in its sync form, a false "committed" | Amended in channel-spec 3.2.6: neither answer precedes the successful `raw` callback. The proposal's Hypothesis property is extended to `AE`, with a broken-implementation control. **2026-09-29: proved on the `sync_answer.py` model only; the receiver is not built.** Open until the receiver uses the model or the property runs against it. **2026-10-05:** the MLLP receiver's `pending.py` delegates every answer to `SyncRequest`, and its docstring says the property test drives the map (read, not run). Closes when `qa-engineer` confirms that test, with its broken-receiver control red, in CI. **CLOSED 2026-10-05:** 16 passed; four controls neutralised → 6 failed, exactly the control tests (`wave-2-ci-confirmation.md`); a named CI step runs both property suites. Oracle move to `tests/support` is W3-20 |
| **Workers and loaders connect to Kafka unauthenticated and unverified** (W2-7 review HIGH, fixed for receivers only in `e3ddee1`). The worker's `_tb.<env>.channels` link is the worst case: an injected spec is RCE across the data plane | High until fixed | High | ADR-008 follow-ups (`platform-engineer`, `connector-engineer`). Contract published 2026-09-29 |
| **Compose lite data plane fails on its deny-by-default ACLs**. The stack runs `SASL_SSL` (ADR-008 D5 landed); the loader cannot read `_tb.<env>.channels` without a temporary grant | Certain until the set 1.4 grants are applied from `down` | Medium: no Compose demo; exit-criterion tests use testcontainers and are unaffected | Set 1.4 ACL table (ADR-008 D6-D7); `devops-engineer` re-run |
| **The ACL table is written from intent, not from the code** (set 1.4 found a missing spec READ, a missing group, a missing transactional-id grant, unused `.retry` grants and an undelimited prefix, all on the first deny-by-default run) | Medium: every new topic, group or client repeats it | High: a role fails at start, or holds a grant nobody reviews | Reconciled 2026-09-29 (`topics/v0` 3.4.1). The Compose e2e runs every role under its exact grants, with negative cases. A structured grant list replaces the regex-parsed sentences with ADR-004 |
| **A future retry implementation shares a consumer group across destinations**, so one loader's commits skip another's retries (silent loss, invariant 4) | Low: ruled, and the LITERAL group ACL refuses a shared name at start | High | ADR-008 D9; the lift checklist gains a two-destination test with a shared-group control that must go red |
| **Sync reply latency tail, not reproduced in isolation** (W2-8). **Measured 2026-10-05** (`docs/spikes/wave-2-sync-reply-latency.md`, `qa-engineer`, tree `ec4b9d1`). Real MLLP receiver and worker, single broker RF=1, about 50 msg/s, 5 runs x 300: **p50 17.4-18.4 ms, p90 20.7-22.5 ms, p99 25.0-29.4 ms, max 42.5 ms, 0 of 1,500 over 100 ms**. With the CPU saturated (2 probe runs): p99 42.6-55.2 ms, still none over 100 ms. Receiver `raw` commit: p50 7.1-7.4 ms, p99 10.2-12.0 ms. *History:* an informal first reading had p99 about 440-500 ms, then the lead reported p99 about 270 ms in one run of four, both inside full integration sessions | **Low** in isolation (was High); **unknown** inside a busy integration session, which is where the outliers were seen | Medium: sync senders wait on it; not a correctness issue | **W3-24 (`qa-engineer`):** reproduce inside a full integration session, at least 3,000 requests per run, with receiver and broker logs. `platform-engineer` takes the cause only if it reproduces. Open hypotheses, none measured: a broker or group-coordinator event, Docker disk or network contention, a `replier` rebalance, producer cold start. **Not measured:** p99 at n = 300 rests on 3 samples, so a 1-in-1,000 event would usually not show. Single host, RF=1. No latency claim for RF=3 |
| **Receivers have no readiness probe** (2026-10-05). The contract's "not ready" states (no reply partition, reply consumer disconnected or `_FATAL`; `topics/v0` 3.3.1 rule 2) are computed but nothing exposes them, so an orchestrator routes senders to a process that answers every sync request `AR` / `REPLY_UNAVAILABLE` | Certain until built | Medium: safe (`AR`, nothing lost) but an avoidable outage for sync senders; Helm needs it | **Wave 3 batch 1:** W3-12 (`architect`, readiness contract section first), W3-17 (`connector-engineer`, probe on every receiver, producer warmed before ready), W3-8 wiring (`devops-engineer`). The probe's listener goes to `security-reviewer` (network listener) |
| **Plugin code runs beside steps without being named in the spec** (`tributary.context`, security High on `fb60f3a`): a transitive or typosquatted registrant sees every payload and `ctx.secrets` | Was High until L1 landed. **High CLOSED 2026-10-05** by `security-reviewer` at `b6b0c97` (`extensions.py:146-276`, `worker.py:450-497`); the residual below stays | High: invariant 6, secrets exposure | Set 1.5: spec-declared allow-list plus distribution pin (ADR-007 D21). **Residual:** a package replacing `tributary-fhir` under its own name is not detected at runtime (lockfile hashes, image digest and SBOM are the controls), and P2-P7 rest on review |
| **Licence data bundled inside a package is invisible to metadata checks** (botocore's MPL-2.0 `cacert.pem`, 2026-09-29; found by review, not CI) | Medium: CA bundles and vendored data are common | Medium: an unapproved licence ships unnoticed | **Mitigated:** `licence_check.py` has a scoped `BUNDLED_LICENCE_FILES` check that reads each wheel's `RECORD` and allows `botocore/cacert.pem` only in `tributary-loaders` and `certifi/cacert.pem` only in the archiver. **Residual:** it matches file **names** (`BUNDLED_FILE_NAMES`, today only `cacert.pem`), so a licensed file under any other name still passes. `security-reviewer` keeps inspecting new dependencies' contents; the W4-7 SBOM should scan file contents, not only metadata |

### Risks added in wave 3

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Conditional FHIR keys collide across senders** (`security-reviewer` Medium at `b6b0c97`, new, not a reopen). `adt.py:182` and `patient.py:42` build conditional keys with an empty assigning authority (`urn:id:` or `identifier=\|value`), so two senders' identical local identifiers resolve to one resource: one patient's data can be merged into another's (PHI integrity, not a log leak) | Medium: any channel with more than one sending facility | High: wrong-patient write in the customer's FHIR server | **Assigned to `healthcare-standards-engineer`.** Expected shape (architect, not yet a ruling): refuse to build a conditional key without an assigning authority, dead-letter with a registered code rather than write unconditionally; regression test with two senders sharing an identifier value. Any new code goes through `envelope/v0` `error-codes.json` |
| **Set 1.8 counts and pairs are architect predictions** (139 pinned pairs, 29 new; 54 new checks), and the `contracts` CI job fails until it installs `openapi-spec-validator` | High until run | Low: a red run, not a silent pass | Lead runs `validate.py` (both modes) before pushing. W3-14 adds the flag and retires `check_expectations.py` |
| **Readiness R1 flaps under a broker blip** (all-brokers-down clears only on delivery or a metadata probe) | Medium | Medium: brief loss of endpoints | W3-17 measures time to recovery; the 5 s probe interval is judgement, not measured |
| **The content archiver reads every channel's PHI** (ADR-004 K2) | Certain once built | High if its credential leaks | Own process, principal and secret; no WRITE; human sign-off H7 before production |
| **Worker, loader and archiver replicas fence each other outside Helm** (2026-10-06): default `group.instance.id` is `pipeline-<pid>` / `loader-<dest>-<pid>` / `archiver-<pid>`, and pid is 1 in every container | Certain with 2+ replicas and no explicit id | High: repeated `_FATAL` fencing, ordered partitions stall | W3-28a-d: refuse to start without an explicit instance id. Helm already sets it from the pod name |
| **Alerts and dashboards have no data** (2026-10-06): nothing exports `observability/v0` metrics; a halted partition stalls silently | Certain until W3-25..W3-27 | Medium | W3-25..W3-27; `partition_halted` alert firing is gate-tracked |
| **The 2026-10-05 wave 2 closures carry no scanner evidence** (bandit, pip-audit, gitleaks not installed for that review) | Low | Medium | CI's licence, SBOM and secret-scan jobs still run. Install the scanners for the wave 3 exit review (criterion 4) |
| **Audit records are not tamper-evident** (2026-10-06): a ClickHouse admin can alter or delete `audit_log` rows (ADR-004 R7) | Low | High for an investigation | Operational control in v0; object storage with object lock is the later option. Retention shortening is guarded by `TB_ARCHIVE_ALLOW_TTL_DECREASE` and logged (`retention_applied`) |
| **An operator shortens retention and loses records irreversibly**, or expects prompt deletion and gets up to one month's lag (`ttl_only_drop_parts`) (2026-10-06) | Medium | Medium | Decrease refused without an explicit flag (R6); lag documented (R1); erasure uses R4, not TTL |
| **Chart retention default drifts from the contract** (`archiver.ttlDays: 365` vs documented 90, found 2026-10-06) | Certain until W3-31 | Low | W3-31 removes the key; `helm_check.py` asserts the R2 defaults |
| **The free RF=3 cluster is read as a sizing result** (2026-10-06) | Medium | Medium | W3-33b reports a ratio only, with the "cannot prove" list (`wave-3.md` 2.7) |
