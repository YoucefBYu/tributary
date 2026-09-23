# Gate: Phase 1, wave 1 — walking skeleton

**Branch:** `wave/1-1` · **Date:** 2026-09-23 · **Author:** `architect`
**Recommendation:** **GO WITH CONDITIONS** — provisional, see section 8.

**This report is not final.** Two inputs are outstanding at the time of writing: `code-reviewer`'s
cumulative-diff pass (exit criterion 4 depends entirely on it) and `security-reviewer`'s pass, both
running. The full `pytest -m integration` run is also in flight. Sections 5 and 8 must be refilled
before this is signed. Nothing below is marked PASS on the strength of an expected result.

**What the architect did and did not verify.** I have no shell. Every command result quoted here was
run by the lead or by `qa-engineer`/`qa-engineer-2` and is attributed. What I verified myself is source
code, by reading it, and that is labelled *code-verified*. Where a claim rests on a teammate's report I
say so, because "reported and plausible" and "measured" are different kinds of evidence and Phase 0 cost
us a round for blurring them.

---

## 1. Summary

The wave built an end-to-end thread: MLLP in → `raw` → in-process pipeline → `out.<dest>` → HTTP out,
with lifecycle events on `_tb.<env>.events`, a channel-spec cache reading the compacted
`_tb.<env>.channels` topic, and a Compose stack. Four packages exist (`tributary-sdk`,
`tributary-runtime`, `tributary-receivers`, `tributary-loaders`) plus `tests/{conformance,e2e,chaos,support}`.

Contract set 1.0 is published and conformant. The engineering checks are green as run by the lead:
`uv sync`, `ruff check .`, `ruff format --check .`, `mypy packages` (56 files), fast suite **701 passed**,
`contracts/validate.py` **102 / 0**, `pytest tests/conformance` **101 passed**.

**The most important finding of this wave is not a defect, it is a pattern.** Four contract defects were
found by implementers rather than by any checker, all with the same shape: a document that was
internally valid and referred to something that did not exist. Section 5.1 sets them out. The
conformance suite could not have caught any of them, and that is structural rather than a gap to close.

---

## 2. Exit criteria

| # | Criterion | Verdict | Evidence |
|---|---|---|---|
| 1 | The E2E test passes in CI | **PASS locally, PENDING CI** | `tests/e2e/test_mllp_to_http_e2e.py::test_synthetic_adt_flows_from_mllp_to_the_http_stub` passes in isolated runs (lead). **It has never run in CI**, because nothing has been pushed. See 2.1. |
| 2 | Invariants 1, 2, 3 and 5 covered by automated tests | **PASS for 1, 2, 5; PARTIAL for 3** | Section 3. Invariant 3's *mechanism* is implemented and tested; its *failure mode* is not exercisable this wave. |
| 3 | Contracts v1 published and referenced by code and tests | **PASS** | Section 4. Published as **contract set 1.0**, lead-approved naming; there are deliberately no `contracts/*/v1/` directories. |
| 4 | `code-reviewer` clean on the wave diff | **PENDING** | Review in progress. Cannot be marked PASS. |

### 2.1 Why criterion 1 is not a bare PASS

The criterion says "in CI". The test passes on a developer machine against testcontainers Kafka, and
`devops-engineer` wired the workflow (`.github/workflows/ci.yml`, contracts job at `f649665`) — but no
commit has been pushed, so no CI run of any kind exists for this branch. Under the gate rule that a
skipped check is a FAIL until it runs, "passes locally" is the strongest honest verdict.

**What closing it requires: a human pushes `wave/1-1`, and the workflow's integration job goes green on
a runner.** That is not an agent action (CLAUDE.md: never push). Two things could plausibly differ on a
runner and neither has been tested: Docker-in-CI availability for testcontainers, and timing —
`session.timeout.ms` is 15 s and several tests wait on real rebalances, so a slower runner changes the
margins. Phase 0's numbers all came from one Windows laptop and this is the first chance to find out
what a Linux runner does with them.

---

## 3. Invariants 1-10

One row per invariant, with the evidence or an explicit "not exercised". Test names are given so a
reviewer can run the specific thing rather than the suite.

### Invariant 1 — ACK after commit

**COVERED, with real-Kafka evidence.**

*Code-verified:* `receivers/mllp/kafka_publisher.py` `RawPublisher.publish` resolves `True` only from
the delivery callback for that record. Every other path returns `False`: envelope validation failure,
`BufferError` on a full local queue, and `asyncio.wait_for` timeout. There is no parameter that lowers
`acks` — the producer config comes from `contracts/topics/v0/topic-defaults.json` via
`runtime.clients.producer_config`, so `acks=all`, `enable.idempotence` and `delivery.timeout.ms` cannot
be set to anything else locally. *Code-verified:* `runtime/producer.py` `flush_pending` raises unless
every delivery callback succeeded, and the consumer loop commits only after the handler returns.

Tests: `receivers/tests/mllp/test_server.py::test_ack_is_withheld_until_publish_resolves`,
`::test_publish_failure_gets_ar_unavailable`, `::test_partial_frame_never_acked_on_abrupt_close`,
`::test_budget_is_not_released_until_the_ack_write_completes`;
`runtime/tests/test_consumer_loop.py::test_offsets_are_committed_only_after_the_handler_returns`,
`::test_a_handler_that_never_returns_a_success_commits_nothing`,
`::test_without_a_dlq_a_permanent_failure_stops_the_worker_rather_than_committing`;
`runtime/tests/test_worker.py::test_a_produce_that_never_lands_fails_the_handler_rather_than_committing`.

Real Kafka: `tests/chaos/test_kafka_broker_kill.py::test_broker_kill_mid_stream_loses_no_acknowledged_message`.

**Named exception:** the `lite` profile (RF=1, `min.insync.replicas=1`) cannot guarantee RPO=0. It is a
human-approved profile exception (ADR-002 D14, 2026-09-21), the invariant is unedited, and the startup
warning is mandatory — `runtime/tests/test_topics_and_config.py::test_the_lite_profile_requires_a_startup_warning_and_production_does_not`,
`::test_lite_is_barred_from_production_use`, `test_worker.py::test_the_lite_profile_warning_is_emitted_at_startup`,
plus the same in both the receiver and the loader. See 7.2 for what is still unenforced.

### Invariant 2 — Kafka hops only at durability boundaries

**COVERED.** *Code-verified, and the form of the evidence matters:* a test cannot easily assert the
absence of a hop, so this rests on review. `runtime/executor.py` has no producer, no client and no
Kafka import of any kind — it is a `for` loop over steps, in process. `runtime/worker.py` produces to
exactly three places: `out_topic` (per destination), `dlq_topic`, and `events_topic` on a *separate*
producer instance. The consumer subscribes to `raw_topic` only. Every one of those is a boundary the
invariant names.

Tests: `runtime/tests/test_integration_kafka.py::test_a_message_flows_from_raw_to_out_with_events`
(real Kafka, one hop in, one out, events emitted). Topic names are constrained by
`contracts/topics/v0/topic-name.schema.json` with 22 conformance cases, so a fourth topic class cannot
be introduced by typo.

### Invariant 3 — the data plane is independent

**PARTIALLY COVERED. Upgraded from Phase 0's "not exercised", but not to PASS**, and the distinction is
worth stating precisely because it would be easy to over-claim.

*What is demonstrated.* The mechanism the invariant depends on exists and is tested.
`runtime/channels.py` is the whole of it: `ChannelCache` assigns every partition of
`_tb.<env>.channels` from the beginning, keeps the latest record per key, and polls for changes.
*Code-verified:* the module imports `orjson` and `confluent_kafka` and nothing else — no database
driver, no HTTP client, no API call anywhere on the path. `LiveSpecSource` is the only reload path and
is polled once per message, so a republished spec takes effect on the next record with no restart.
A worker today runs with no control plane in existence at all.

Tests (12 in `runtime/tests/test_channels.py`): `::test_a_published_spec_is_applied`,
`::test_a_tombstone_undeploys_the_channel`,
`::test_a_bad_record_is_rejected_and_the_previous_spec_keeps_running`,
`::test_catch_up_ends_on_partition_eof`,
`::test_catch_up_refuses_to_start_a_worker_without_its_specs`,
`::test_a_republished_version_takes_effect_without_a_restart`,
`::test_the_source_is_the_only_hot_reload_path`, `::test_a_rejected_record_never_logs_the_spec_body`.

*What is not demonstrated.* The invariant says the data plane keeps running when PostgreSQL, the API or
ClickHouse **is down**. None of those exists in wave 1, so that property is *vacuously* true rather than
tested: there is nothing to stop, and no chaos test stops it. A worker that never had a dependency has
not shown that it survives losing one. The two are easy to conflate and only one is evidence.

**Verdict: the mechanism is right and tested; the failure mode is untested and untestable until W3-2 and
W3-4 exist.** Gate `phase-0-spikes.md` 7.2 item 19 already carries the required chaos coverage (kill
PostgreSQL and the API, assert flow continues). It stays open and moves to wave 3.

### Invariant 4 — at-least-once, with a documented and tested idempotency technique per loader

**COVERED for the one loader that exists.** One of one is not a strong sample, and the contract is what
makes it scale: `contracts/channel-spec/v0` requires an `idempotency` block on every destination, with a
written justification when the technique is `none`, so a future loader cannot ship without declaring
one. The key is always `tb-msg-id` and is not configurable.

Tests: `loaders/tests/http/test_loader.py::test_duplicate_delivery_is_harmless`,
`::test_two_different_messages_get_two_different_idempotency_keys`,
`::test_deliver_success_sends_no_idempotency_header_by_default`;
`loaders/tests/http/test_client.py::test_idempotency_header_reaches_the_server` (real HTTP server);
`loaders/tests/http/test_config.py` parses all four channel-spec examples including the `sink-native`
and `conditional-update` techniques.

Replay window: `commit_every` is 50 from the contract, so an unclean death redelivers up to 50 records.
That is permitted, and the destination declaring `none` is the one with no absorber — see 7.1.

### Invariant 5 — ordering is explicit

**COVERED, with real-Kafka evidence.**

*Code-verified:* `worker.py::check_partition_counts` refuses to start when an `out.<dest>` topic's
partition count differs from `raw` on an ordered channel — a startup refusal, not a warning, because a
mismatch breaks per-key order silently. `out_message` leaves the key untouched.

Tests: `runtime/tests/test_integration_kafka.py::test_kill_and_restart_preserves_per_key_order_and_loses_nothing`
(real Kafka, kill and restart, order preserved, nothing lost);
`test_consumer_loop.py::test_a_cooperative_revoke_commits_before_handing_the_partition_over`,
`::test_lost_partitions_are_never_committed`,
`::test_a_retryable_failure_pauses_seeks_back_and_redelivers_the_same_offset`,
`::test_a_paused_partition_yields_nothing_until_the_backoff_elapses`;
`test_spec.py::test_a_per_key_channel_carries_its_key_expression`,
`::test_retry_topic_is_refused_by_the_runtime_as_well_as_the_schema`.

Retry is `blocking` only in wave 1; `retry-topic` is refused by both the schema and the runtime, so the
reordering policy cannot be selected before its spike (ADR-003 D1/D2).

### Invariant 6 — no PHI in logs, metric labels, trace attributes, exception messages or test output

**COVERED IN PART, with one named gap.** The strongest evidence is a guard that *raises* rather than a
convention: `runtime/tests/test_internals.py::test_a_phi_shaped_log_field_raises_rather_than_being_written`.

Also: `test_channels.py::test_a_rejected_record_never_logs_the_spec_body`;
`receivers/tests/mllp/test_ack.py::test_build_ack_never_embeds_raw_control_characters_from_text`;
the envelope validation projection carries `key_present`/`key_size` and never the key value;
`tests/conformance`'s `_safe()` and `contracts/validate.py` emit paths and keywords only; the 8
`EVENT_PHI_GUARDS` are asserted by name and never skipped. `platform-engineer` reports a quarantine
log-leak test driving a record whose payload, key and header *name* are all markers — **reported, not
seen by me.**

**Gap:** no automated PHI scan of log output in CI. Gate `phase-0-spikes.md` 7.1 requirement 15 names
it and it is `devops-engineer`'s. Every PHI control in this wave is a unit test plus source review;
nothing scans what a running component actually printed.

### Invariant 7 — channels are declarative, steps unit-testable without Kafka

**COVERED. Upgraded from Phase 0's "not exercised".** `tributary-sdk` ships `run_step`, `make_message`,
fakes and a pytest plugin; `test_step.py`, `test_testing_helpers.py`, `test_core_steps.py`. Side effects
go through `ctx`. The schema has no field that can carry inline code, and
`invalid/step-inline-script.json` pins that.

### Invariant 8 — envelope: metadata in headers, payload opaque, claim-check over 1 MB

**COVERED for the inline case; the claim-check half is NOT exercised.**

*Code-verified:* `validate_for_produce` runs before every produce in both the receiver and
`RecordProducer` — the checks JSON Schema cannot make (byte budgets, per-class key identity, the
`tb-x-*` ban) enforced where a record could otherwise reach a topic. Gate requirement 10 is met:
`sdk/tests/test_projection_conformance.py` and `runtime/tests/test_events_conformance.py` run the real
schemas over the real emitters, not over hand-built dicts.
`::test_no_experimental_header_survives_into_a_released_package` closes one of the Phase 0 drifts.

**Claim-check is not implemented and not tested.** The receiver caps `maxMessageBytes` at 1,000,000 so
nothing inline exceeds the limit, which means the >1 MB path cannot be reached rather than that it
works. Carried from Phase 0 (gate 7.2 item 18) and unchanged.

### Invariant 9 — the archive is off the critical path

**NOT EXERCISED.** No archiver exists. *Code-verified* and structurally correct:
`KafkaEventEmitter.emit` catches `BufferError` and every other exception, counts a drop, and logs the
*shape* of the loss at 1/10/100/1000 rather than every instance; `close()` bounds its flush at 2 s so a
slow events topic cannot hold up a drain; events use a separate `Producer` instance from the data path.
So nothing in message flow waits on it, by construction.

**What is missing is the mutation control.** No test asserts that a failing or full event emitter leaves
message flow unaffected — and that is exactly the check that distinguishes "cannot block" from "did not
happen to block in this run". **Recommended as a wave-2 task**, cheap: drive the handler with an emitter
whose `produce` always raises and assert the message still reaches `out.<dest>` and the offset still
commits.

### Invariant 10 — clean-room and licensing

**UPHELD, unchanged from Phase 0, and not re-verified this wave.** The three MPL-2.0 dependencies
(`orjson`, `hypothesis`, `pathspec`) are human-approved as unmodified, 2026-09-22, and that approval does
not extend to a new or modified MPL dependency. Wave 1 added runtime dependencies (`confluent-kafka`,
`httpx` and others) and **no licence or SBOM scan has been run on them** — gate 7.1 requirement 15,
`devops-engineer`. The Phase 0 lesson stands: never state a licence from a source annotation, and a
lockfile inventory is not complete because `[build-system] requires` resolves separately.

**This is a condition for GO** (section 8): the wave added dependencies and nothing has checked them.

---

## 4. Contract conformance

**PASS.** Contract set 1.0: `envelope/v0`, `topics/v0`, `events/v0`, `channel-spec/v0`.

| Check | By | Result |
|---|---|---|
| `contracts/validate.py` | lead | **102 / 0** |
| `pytest tests/conformance` | lead, `qa-engineer` | **101 passed** |
| Corpus resolution (steps an example names actually resolve) | `platform-engineer` | green, `729cdc1` |
| CI job | `devops-engineer` | `f649665`, runs before `uv sync`, blocking |

All 24 expected-failure pairs written during the wave were architect predictions and all 24 matched the
measured run, so no manifest needed correcting. That record covers *invalid* examples, where the
prediction is reasoning about schema semantics. **It does not extend to hand-authored valid examples:**
one shipped with a 34-character `traceparent` trace id where the pattern requires 32, and only the run
caught it. Recorded so "24 of 24" is not read as more than it is.

**Naming note for the human:** "contracts v1" in the wave plan is the contract *set* milestone, not a
directory rename. There are deliberately no `contracts/*/v1/` directories — a new major directory is
created by a breaking change *after* a freeze, not by the freeze. Lead-approved 2026-09-23 as a
versioning convention; the mapping table is in `contracts/README.md`.

Code and tests reference the contracts rather than restating them: `topic-defaults.json` is read for
`session.timeout.ms`, `commit_every`, `acks`, `enable.idempotence` and the profile warning text
(`test_topics_and_config.py::test_commit_every_is_read_from_the_contract_and_has_no_fallback`), the
loader parses the channel-spec examples as its config fixtures, and the emitters are validated against
the published schemas.

---

## 5. Findings

### 5.1 Four contract defects, all found by implementers, all the same shape

Not incidental. Each was a document that was internally valid and named something that did not exist —
which is precisely what the conformance suite cannot see.

| # | Defect | Found by | Resolution |
|---|---|---|---|
| 1 | `envelope/v0` 2.3 mandated a reject to `dlq` **with an event emitted**, but a `dlq` record requires the core header set — the exact bytes that could not be read — and `kind: message` requires a `message_id` that must never be trusted from a broken record. The mandated behaviour was unimplementable, and the honest workaround put unvalidated bytes in a log line. | `platform-engineer` | New `kind: record` / `stage: quarantined`, envelope 5.2, `RECORD_UNDECODABLE`. Deferral was offered and rejected: it would have required editing 2.3 anyway. |
| 2 | `quarantined` existed in `events/v0`'s stage enum and **not** in `envelope/v0`'s, so a conformant wrapper could not be stamped. | `platform-engineer` | Enum aligned; a conditional now requires `tb-stage: quarantined` and octet-stream content type whenever `tb-error-code` is `RECORD_UNDECODABLE`. |
| 3 | `commit_every` was on the topics contract's `must_not_hardcode` list while its value was the prose string "tunable per destination, default well above 1" — **the contract required reading a value that was not machine-readable.** | `platform-engineer` | Now the measured number `50`, with the replay-window consequence stated. |
| 4 | `valid/walking-skeleton-loopback.json` pointed at `adt_flow.steps.passthrough` inside an OCI bundle; nothing in the repo publishes `adt_flow` and **no bundle-fetch exists**. Schema-valid, unrunnable, and named as if it were the runnable reference. | `qa-engineer-2` | Now `uses: tributary.core.passthrough`, registered at `754845d`. `runnable` is a manifest field; README 3.3.1 states that `ref` needs bundle-fetch and wave 1 has none. |

**Two structural consequences, both recorded in `contracts/README.md`.**

First: **a green conformance run means the contracts are self-consistent, not that they agree with the
code.** Three of the four were invisible to every checker.

Second: there are now two checkers and **the split must not be merged.** `validate.py` and
`tests/conformance` answer whether a document is well-formed and whether each invalid example fails for
its documented reason; the corpus test in `tributary-runtime` answers whether the things a document
*names* resolve. Neither could have caught defect 4 alone — name resolution needs a runtime, and running
on a clean clone before `uv sync` is the entire reason the standalone checker exists.

Defect 4 also had a worse property than being broken: **it failed late.** Step resolution is lazy, so
`channel_spec_applied` succeeded at deploy and the error would have arrived with the first real message.
See 7.3.

### 5.2 Process: three shared-checkout commit-mixing incidents

Three incidents where a commit included files outside the committing agent's ownership. **Disclosed by
the agents involved and verified harmless by the lead.**

**I did not verify these** — I have no shell and have not read the commits. `TODO(lead)`: add the commit
hashes, which files crossed which boundary, and who disclosed each, before this is signed. The reason to
record them rather than treat them as noise is Phase 0 High 1: a `.gitignore` change riding along in
someone else's commit silently removed protection for `samples/mirth/`, and nothing caught it for a
whole wave. Disclosure is what made these three cheap; the pattern is worth naming at every gate.

One related change went the other way and is worth the contrast. The `.gitignore` gap for
`.claude/agent-memory/` — where a per-file list the human wrote in `5aa6c54` had been outgrown by two
new files in a single wave — was fixed by the lead with a directory pattern (`49b8a91`), in its own
commit, verified with `git check-ignore`. Additive, isolated, verified: the opposite of High 1's shape,
and recorded because gate 7.2 item 22 assigns `.gitignore` to `devops-engineer` and this is Phase 1's
first change to it.

### 5.3 `code-reviewer` and `security-reviewer`

**PENDING.** `TODO(lead)`: fold in both results here, one row per finding with severity and disposition.
Exit criterion 4 cannot be assessed until the first of these lands, and per the definition of done,
Critical and High findings must be addressed before the wave is done.

Two mechanical fixes to `contracts/validate.py` (`bcb651d`, `b6d136f`) were made by the lead during gate
checks — format and lint only, no design change, my file, and I have no objection.

---

## 6. What was built

| Task | Owner | State |
|---|---|---|
| W1-1 contracts v1 | `architect` | Done. Set 1.0 plus `contracts/validate.py`; five follow-up rounds from implementer questions |
| W1-2 monorepo bootstrap | `devops-engineer` | Done. `uv sync`, ruff, mypy, pytest markers, CI workflow |
| W1-3 SDK v0 | `platform-engineer` | Done. `Message`, `@step`, `Drop`, `ctx` protocol, fakes, `run_step`, projection helper |
| W1-4 runtime v0 | `platform-engineer` | Done. Consumer loop, executor, producer, events, blocking retry, graceful shutdown, `_tb.channels` cache, quarantine and reject paths |
| W1-5 MLLP receiver v0 | `connector-engineer` | Done. Framing, ACK-after-commit, TLS and mTLS, byte-budgeted queue, metrics |
| W1-6 HTTP loader v0 | `connector-engineer` | Done. Idempotency key, timeout, backoff, circuit breaker |
| W1-7 Compose lite stack | `devops-engineer` | Done. Kafka KRaft, ClickHouse, PostgreSQL, receiver, worker, loader, Dockerfiles |
| W1-8 E2E and first chaos test | `qa-engineer`, `qa-engineer-2` | Done locally; see 2.1 for the CI caveat |

---

## 7. Named limitations

Each of these is a real gap stated on purpose, not a defect to fix before the gate.

### 7.1 Numbers that are judgement, not measurement

| Value | Where | Status |
|---|---|---|
| `queue.maxBytes` 64 MiB | `channel-spec/v0` 3.2.1, `receivers/backpressure.py` | **Judgement.** No Phase 0 spike pinned steady-state buffered bytes at a receiver — S2 measured ACK latency and loss, not memory. Measure in the Linux S2 re-run. |
| `stepRetries` 3 | `channel-spec/v0` 3.3, ADR-003 D4 | **Judgement.** S5 exercised the DLQ path, not the retry count. |
| `commit_every` 50 | `topics/v0` 3.3 | **A measured point, not a measured optimum.** The surrounding curve was never run. It is also the replay window: up to 50 redeliveries after an unclean death, which a destination declaring `idempotency.technique: none` has no absorber for. |
| `PRODUCE_RETRY` (10 attempts, halt-partition) | `runtime/worker.py` | **Judgement**, labelled in the source. Halting preserves order and never drops, which is the safe direction when the broker is the unwell thing. |

Also unverified and labelled: no non-UTF-8 header name has been tested end to end, so the quarantine
blob's header-name fidelity limit (`envelope/v0` 5.2.1 — `surrogateescape` round-trips what the client
decoded, not necessarily what arrived) is an assumption.

### 7.2 Controls that are documented but not enforced

- **`lite` is barred from production, and nothing enforces it.** The startup warning is mandatory and
  tested; the bar is a sentence. A deployment can run RF=1 in production today and only a log line
  objects. Enforcement needs something that knows it is in production, which does not exist yet.
- **`halt-partition` needs an alert before the MVP ships, not after.** ADR-003 D3 says so. The runtime
  emits `partition_halted`; nothing alerts on it. A halted partition is silent lag until someone looks.
- **Envelope 2.3.1's reject placeholders are not schema-checkable**, because a normalised reject record
  is structurally identical to an ordinary `dlq` record — which is what normalising is for. Needs a unit
  test per row of that table; `platform-engineer` reports the reject path itself is not yet implemented,
  so every decode failure currently over-quarantines. Safe (nothing is lost, the bytes and identity are
  in the blob) but it inflates the `RECORD_UNDECODABLE` counter with cases that are really one broken
  producer field, which matters because that counter is meant to mean "framing damage, corruption or
  someone writing to the topic".

### 7.3 Step resolution is lazy, so a bad spec fails late

`channel_spec_applied` succeeds and the failure arrives with the first real message — in production, on
a patient record. `platform-engineer` proposes a deploy-time resolution check and is holding it for
wave 2 for two good reasons: it introduces a new apply-time refusal, which is the wrong thing to add
during a gate, and resolving `ref` eagerly means importing bundle code at apply time. Agreed shape:
check `uses` eagerly (cheap, in-process, no bundle), `ref` eagerly once bundle-fetch exists.

**One constraint on whoever builds it:** the spec-apply path is the one invariant 3 requires to keep
working when everything else is down, so a slow or hanging import there does not merely delay a deploy —
it blocks the mechanism that is supposed to be the last thing standing. It needs a timeout, and a
failure that leaves the previously applied spec running.

### 7.4 Carried forward unchanged from Phase 0

`retry-topic` delay tiers and claim-check remain unimplemented and unspiked (gate 7.2 item 18). The
contract refuses the first outright and gives the second no channel-level knob, so neither can be
reached for by accident. Every Phase 0 performance number still comes from one Windows laptop; the Linux
re-run of S1 and S2 is approved and has not happened.

---

## 8. Recommendation and conditions

**GO WITH CONDITIONS — provisional.** The architecture's load-bearing properties hold with real-Kafka
evidence: an acknowledged message is not lost across a broker kill, per-key order survives a kill and
restart, the pipeline takes exactly one hop in and one per destination, and a worker's only source of
channel truth is a compacted Kafka topic. No invariant needs to change. Nothing found this wave
contradicts ADR-001.

**Blocking sign-off:**

1. **`code-reviewer` clean on the cumulative wave diff.** Exit criterion 4 is unassessable without it,
   and Critical or High findings must be addressed before done.
2. **`security-reviewer` pass**, with attention to the quarantine path: it deliberately writes unvalidated
   bytes to a topic and is the newest PHI-adjacent code in the wave.
3. **Full `pytest -m integration` run green**, reported with the count. In flight at the time of writing;
   piece-by-piece runs are not the same evidence as one suite run.
4. **A licence and dependency check on what the wave added.** Invariant 10 was upheld in Phase 0 and has
   not been re-verified against `confluent-kafka`, `httpx` and the rest. Gate 7.1 requirement 15. Must
   cover `[build-system] requires` as well as the locked groups.
5. **A human pushes `wave/1-1` and the CI integration job goes green**, which is the only thing that
   turns criterion 1 from PASS-locally into PASS. Not an agent action.

**Not blocking, carried to wave 2:**

6. Invariant 9's mutation control: assert a failing event emitter does not affect message flow.
7. Invariant 3's real chaos coverage once PostgreSQL and the API exist (gate 7.2 item 19).
8. The reject path (envelope 2.3.1) implemented, with a unit test per placeholder.
9. Deploy-time step resolution, with the timeout constraint in 7.3.
10. An alert on `partition_halted` before the MVP.
11. The Linux re-run of S1 and S2.

**Why GO WITH CONDITIONS rather than GO:** two of four exit criteria are not closeable by any agent —
one needs a human push, one needs a reviewer that is still running. **Why not NO-GO:** every invariant
that could be exercised was, the two that could not are blocked on components that do not exist yet
rather than on anything discovered, and the four contract defects were all found, understood and fixed
within the wave.

**Why this wave's most useful output is a process finding.** Four contract defects were found by
implementers and none by a checker. The checkers were not weak — 102 conformance cases, 701 fast tests,
`mypy --strict` — they were answering a different question. A document can be perfectly well-formed and
refer to nothing at all. Wave 2 should expect the same class of defect and keep both checkers.

---

## 9. Sign-off

| Item | Status |
|---|---|
| Exit criterion 1 (E2E in CI) | **PASS locally, PENDING CI push** |
| Exit criterion 2 (invariants 1, 2, 3, 5 tested) | **PASS for 1, 2, 5; PARTIAL for 3** |
| Exit criterion 3 (contracts published and referenced) | **PASS** |
| Exit criterion 4 (`code-reviewer` clean) | **PENDING** |
| Invariants 1-10 | No change needed. 1, 2, 5, 7 upheld with evidence; 4, 6, 8 upheld in part with named gaps; 3 partial; 9 not exercised; 10 not re-verified this wave |
| Unresolved Critical | **Unknown — reviews outstanding** |
| Unresolved High | **Unknown — reviews outstanding** |
| Gate recommendation | **GO WITH CONDITIONS (provisional):** 5 blocking, 6 carried forward |
| Human sign-off | pending |
