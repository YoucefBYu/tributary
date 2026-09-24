# Gate: Phase 1, wave 1 — walking skeleton

**Branch:** `wave/1-1` · **Date:** 2026-09-23 · **Author:** `architect`
**Recommendation:** **GO WITH ONE CONDITION** — a human push, which is the only thing no agent can do.
See section 8.

**All inputs are in and both High findings are closed.** `code-reviewer-8`: APPROVE, zero findings at
any severity. `security-reviewer`, final: **0 Critical, 0 High, 0 Medium, 3 Low**, gate passes from their
side. `pytest -m integration`: 6 passed. `licence_check.py`: 44 components, 0 failed. Nothing below is
marked PASS on the strength of an expected result.

**One correction to how this has been described.** The two High findings do **not** change exit criterion
4, which is specifically "`code-reviewer` clean on the wave diff" — that reviewer found nothing, and
recording their clean pass as a failure would misattribute it. The Highs block the gate through the
definition of done, item 6: *"Changes to auth, crypto, network listeners, PHI handling or dependencies
also pass `security-reviewer`."* Criterion 4 stays PASS; the wave is not done. The distinction matters
because it determines who has work to do — `connector-engineer` and the human, not the code reviewer.

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
`contracts/validate.py` **102 / 0**, `pytest tests/conformance` **101 passed**, and
`pytest -m integration` **6 passed in 227.91 s** (4 runtime Kafka tests, 1 e2e, 1 chaos; 701 deselected).
`code-reviewer-8` on the cumulative wave diff: **APPROVE, zero findings at any severity.**

**The most important finding of this wave is not a defect, it is a pattern.** Four contract defects were
found by implementers rather than by any checker, all with the same shape: a document that was
internally valid and referred to something that did not exist. Section 5.1 sets them out. The
conformance suite could not have caught any of them, and that is structural rather than a gap to close.

---

## 2. Exit criteria

| # | Criterion | Verdict | Evidence |
|---|---|---|---|
| 1 | The E2E test passes in CI | **PASS locally, PENDING CI** | `pytest -m integration` **6 passed in 227.91 s** (lead), including `tests/e2e/test_mllp_to_http_e2e.py::test_synthetic_adt_flows_from_mllp_to_the_http_stub`. **It has never run in CI**, because nothing has been pushed. See 2.1. |
| 2 | Invariants 1, 2, 3 and 5 covered by automated tests | **PASS for 1, 2, 5; PARTIAL for 3** | Section 3. Invariant 3's *mechanism* is implemented and tested; its *failure mode* is not exercisable this wave. |
| 3 | Contracts v1 published and referenced by code and tests | **PASS** | Section 4. Published as **contract set 1.0**, lead-approved naming; there are deliberately no `contracts/*/v1/` directories. |
| 4 | `code-reviewer` clean on the wave diff | **PASS** | `code-reviewer-8`, cumulative diff: **APPROVE, zero Critical, High, Medium and Low findings.** Detail in 5.3. |

### 2.1 Why criterion 1 is not a bare PASS

The criterion says "in CI". The whole integration suite now passes on a developer machine against
testcontainers Kafka — one run, all six tests, 227.91 s — and `devops-engineer` wired the workflow
(`.github/workflows/ci.yml`, contracts job at `f649665`, with `contracts/validate.py` and
`tests/conformance` as required non-optional jobs, confirmed by `code-reviewer-8`). But no commit has
been pushed, so **no CI run of any kind exists for this branch**. Under the gate rule that a skipped
check is a FAIL until it runs, "passes locally" is the strongest honest verdict.

The 227.91 s figure is worth carrying: this suite is slow enough that a CI runner's timeout and
parallelism settings matter, and nobody has seen it run under those.

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

**COVERED — after a High finding that breached it was found and fixed inside the wave.**

`security-reviewer` High 2: the HTTP loader (`loaders/http/client.py:53`) used the default `urllib`
opener, so `HTTPRedirectHandler` silently followed 3xx. A redirected POST became a GET with the body
dropped, the redirect target's 200 was classified as success, and **the offset committed for a message
that was never delivered.** Not an idempotency weakness — silent message loss, the failure invariant 4
and invariant 1's "never ACK and drop" both exist to prevent.

**Closed at `bec8029`**, confirmed by `security-reviewer` with the mechanism re-derived rather than taken
on trust: all three consequences verified independently, plus three gap-checks they added themselves —
the opener correctly drops the redirect handler, TLS verification is preserved, and the module-level
opener singleton is safe to share. That last one is a defect the fix could plausibly have introduced and
nobody had asked about, which is what a confirmation pass is for.

Two things worth naming rather than filing. First, the tests that exist are good and could not have
caught it: `::test_duplicate_delivery_is_harmless` proves the idempotency technique works and
`::test_idempotency_header_reaches_the_server` proves the header arrives, but nothing asserted that a
non-2xx outcome reached by redirect is *not* treated as success. The suite tested the happy path and the
duplicate path; the misclassification path was not a test that failed, it was a test that did not exist.
Second, the same finding carries a credential leak (the `Authorization` header is not stripped on
redirect) and a TLS-downgrade path (the `http://`-loopback-only rule is checked on the configured URL,
not after a redirect) — so one default-constructor choice breaks three separate contract rules at once.

**The rest of the invariant holds.** One of one is not a strong sample, and the contract is what
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

`security-reviewer` confirmed the PHI guards are reached at **all real production log and event call
sites**, including the quarantine and reject paths after the later changes, and that the loader never
logs a response body or headers. That is the check I could not make by reading two files.

**Three gaps, two of them newly found and neither blocking.**

1. **No automated PHI scan of log output in CI.** Gate `phase-0-spikes.md` 7.1 requirement 15,
   `devops-engineer`'s. Every PHI control in this wave is a unit test plus source review; nothing scans
   what a running component actually printed.
2. **Two log call sites bypass the guard** (`security-reviewer` Low a/b): `receivers/mllp/server.py:205`
   and `:235` use raw `print(json.dumps(...))` instead of the guarded `JsonLogger`, and
   `loaders/http/loader.py:235` logs `error=repr(exc)` — a free-text field where the contract wants a
   classified code. Both are safe today by the content that happens to reach them, not by construction.
   **This is the shape of failure this project has hit repeatedly: a control that exists and is simply
   not applied at one site.** The guard raises when it is used; the point of a guard is that there is no
   way not to use it.

Not blocking, because neither leaks anything today and both are one-line changes — but they belong on
the wave-2 list rather than in a backlog, because "safe by luck" degrades the moment the surrounding
code changes.

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

**UPHELD, with a fourth MPL approval recorded.** The scan that condition 4 asked for was run, it found
something, and the human decided it.

`security-reviewer` High 1: **`licence_check.py` exits 1 on `certifi` (MPL-2.0)**, added this wave
(`d3b0e09`), pulled in transitively by `testcontainers[kafka]` → `docker` → `requests`.
**APPROVED by the human, 2026-09-23** — unmodified, dev group only, never shipped, scoped to that
transitive path. Recorded in ADR-001 2.3 as **its own approval, not an extension** of the
`orjson`/`hypothesis`/`pathspec` decision: that one was written as covering *those three packages*, and
a transitive arrival is precisely the case its scope note anticipated. Extending it quietly would have
made the scope note decorative. Arriving by a different path, or into a runtime group, is a different
dependency and needs a fresh decision.

*Architect-verified* by reading the manifests: `testcontainers[kafka]` is in the root
`[dependency-groups] dev` only, and no package declares `httpx` — the HTTP loader uses stdlib `urllib`
— so `certifi` is genuinely dev-group only and not in the default distribution. The risk is therefore
CI-and-policy, not shipped-artefact.

**Why it went to the human rather than being approved here.** CLAUDE.md permits "MPL only as an
unmodified dependency approved by the architect", so the authority arguably existed — but the same file
escalates "a dependency with a non-permissive license" to a human, and the Phase 0 precedent sent all
three MPL items that way. Recording the reasoning because the two clauses can be read as conflicting,
and the next transitive MPL arrival should not have to re-derive which one wins: **escalate.**

**One follow-up, and it is a code change I do not own.** `licence_check.py`'s `MPL_APPROVED` list must
gain `certifi`, and that file is `devops-engineer`'s. Routed to them. **CI stays red until it lands**,
so condition 3 (the push) cannot succeed before it.

**How this one hid is the reusable part.** The root manifest annotates the dependency
`"testcontainers[kafka]>=4.8",  # Apache-2.0`. That annotation is *correct about testcontainers itself*
and says nothing about its transitive closure — so it reads as a licence clearance while clearing
nothing. Same shape as Phase 0's `pathspec` case, where a lockfile-only inventory reported clean because
`[build-system] requires` resolves separately. **Two independent near-misses now say the same thing: a
licence claim is only as good as the resolution it came from, and a hand-written comment is not a
resolution.** When the human records the `certifi` decision, it is worth recording which source the
licence was read from.

Otherwise unchanged from Phase 0: `orjson`, `hypothesis` and `pathspec` are human-approved as unmodified
(2026-09-22); clean-room is intact, and `code-reviewer-8` confirmed no `spikes/` imports in `packages/`.
No SAST, CVE or image scanner exists on this machine, so the wave's position on those remains
source-review-only.

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

Three commits, all `platform-engineer`'s, swept in files owned by someone else because every agent shares
one git index in this checkout. Each was disclosed at the time.

| Commit | Swept in | Owner | Disclosed by |
|---|---|---|---|
| `92ecd6a` "feat(runtime): implement envelope 2.3.1, the ordinary reject path" | `receivers/mllp/server.py`, `receivers/tests/mllp/test_lite_profile_warning.py` (the lite-profile startup warning) | `connector-engineer` | `platform-engineer`, immediately after committing |
| `a96edb1` "feat(runtime): quarantine undecodable records per envelope 5.2…" | 4 files: `tributary-receivers/pyproject.toml`, `config.py`, `kafka_publisher.py`, `test_producer_config.py` — the `must_not_hardcode` fix reading `acks`/`enable.idempotence`/`delivery.timeout.ms` from the contract | `connector-engineer` | `connector-engineer` |
| `777c699` "fix(sdk): close the tb-usr-* deny-list bypass…" | then-uncommitted `deploy/compose/topic-bootstrap/*` and a 1-line `pyproject.toml` `testpaths` change | `devops-engineer` | `devops-engineer` |

**Verified, and not by me** (no shell, and I have not read the commits): `code-reviewer-8` found every
swept file has sane content at `HEAD` with nothing lost, and an ownership spot-check of six commits found
no violations beyond these three. The lead additionally confirmed each swept file was byte-for-byte
identical to the intended author's working tree at the time (`git diff HEAD -- <path>` empty) and that no
history was rewritten.

**A fix was adopted mid-wave and it is the part worth keeping:** commits moved to
`git commit -F - -- <paths>`, pathspec-limited, instead of `git add` followed by a plain `git commit`,
which commits the whole shared index regardless of what you staged. That is a mechanical fix to a
mechanical problem, which is the right kind.

"Sane content at HEAD" is the right check, and it is worth saying why rather than treating it as a
formality. Phase 0 High 1 was a `.gitignore` change riding along in someone else's commit, which
silently removed protection for `samples/mirth/` and went unnoticed for a whole wave — a *deletion*
surviving into `HEAD` is exactly what that check catches. What it does not tell us is whether an
intermediate commit was momentarily wrong, which for a branch nobody has pushed does not matter.

The mechanism, not the discipline, is the cause: a shared index means a `git add` of a path you own can
pick up a neighbour's in-flight edit to the same path prefix. Disclosure is what made these three cheap.
Worth naming at every gate while the shared checkout is the working mode; git worktrees are the
documented alternative (`OWNERSHIP.md`) if the frequency rises.

One related change went the other way and is worth the contrast. The `.gitignore` gap for
`.claude/agent-memory/` — where a per-file list the human wrote in `5aa6c54` had been outgrown by two
new files in a single wave — was fixed by the lead with a directory pattern (`49b8a91`), in its own
commit, verified with `git check-ignore`. Additive, isolated, verified: the opposite of High 1's shape,
and recorded because gate 7.2 item 22 assigns `.gitignore` to `devops-engineer` and this is Phase 1's
first change to it.

### 5.3 Reviews

**`code-reviewer-8`, cumulative wave diff: APPROVE, zero findings at any severity.** That closes exit
criterion 4. Four checks in that review are load-bearing for claims made elsewhere in this report, so
they are recorded rather than summarised away:

| Confirmed | Why it matters here |
|---|---|
| `validate_for_produce` is enforced at **every real produce call site** — receivers call it directly, loaders go through the runtime's shared `RecordProducer` / `KafkaDlqWriter` choke point | This is the evidence behind invariant 8 and half of invariant 6. My own reading covered the two files I opened; a reviewer checking every call site is what makes it a claim about the system rather than about two modules |
| Repo hygiene clean: no `.pem`/`.key` tracked, no `spikes/` imports in `packages/` | The Phase 0 ground rule that spike code stays out of `packages/` held under actual review, not just intent |
| `uv.lock`'s 1192 lines are proportionate to 47 real dependencies | A sanity check on dependency creep, though **not** a licence check — see condition 4 in section 8 |
| `contracts/validate.py` and `tests/conformance` run as **required, non-optional** CI jobs | The two-checker split in 5.1 is only protection if neither job can be skipped |

**`security-reviewer`, final: 0 Critical, 0 High, 0 Medium, 3 Low. Gate passes from their side.** The
first pass found 2 High; both were fixed inside the wave and confirmed closed in a second pass that
**re-derived the mechanism rather than accepting the commits**. The table below is the first pass with
dispositions.

**One provenance note, because the reviewer was right to raise it.** `security-reviewer` cannot verify
that the human actually gave the `certifi` approval on 2026-09-23 — it is outside anything they can
inspect. **The lead attests to it directly:** they put the question to the human themselves and received
an explicit approval, which is what was relayed to me and to `devops-engineer`. Recorded as the lead's
first-hand attestation, not as an inference from the relay chain, because an approval that only exists
as a forwarded claim is exactly the kind of thing that should not be load-bearing at a gate.

| # | Sev | Finding | Disposition |
|---|---|---|---|
| 1 | **HIGH** | `licence_check.py` exits 1 on `certifi` (MPL-2.0), added this wave (`d3b0e09`) via `testcontainers[kafka]` → `docker` → `requests`. Dev-group only, unmodified, not in the default distribution — architect-verified from the manifests | **CLOSED — human APPROVED 2026-09-23**, recorded as its own approval scoped to that transitive path, not as an extension of the existing three (ADR-001 2.3). The `licence_check.py` `MPL_APPROVED` edit is `devops-engineer`'s file and is routed to them; CI stays red until it lands |
| 2 | **HIGH** | HTTP loader (`client.py:53`) uses the default `urllib` opener, so 3xx is silently followed: a redirected POST becomes a GET with the body dropped, the target's 200 is read as success, **the offset commits and the message is lost.** Plus `Authorization` not stripped on redirect (credential leak to the redirect target) and the `http://`-loopback-only rule checked on the configured URL but not after a redirect (TLS downgrade) | **Fix landed `bec8029`; blocking condition 2 stays OPEN pending `security-reviewer` confirmation that all three consequences are closed and no new gap was introduced.** Invariant 4 assessment (section 3) stays downgraded until then |
| 3 | LOW | `receivers/mllp/server.py:205,235` use raw `print(json.dumps(...))` instead of the guarded `JsonLogger` | Track, wave 2. Recorded under invariant 6: safe by content today, not by construction |
| 4 | LOW | `loaders/http/loader.py:235` logs `error=repr(exc)` — free text where the contract wants a classified code | Track, wave 2. Same entry |
| 5 | LOW | `tls.py` has no CRL/OCSP revocation checking for mTLS client certificates | Track. Accepted for a walking skeleton; section 7.2 |

**Re-verified in this pass rather than carried from earlier rounds** — worth listing, because a review
that trusts its predecessors is how the Phase 0 High findings survived a whole wave: all 5 images
digest-pinned, non-root, read-only plus tmpfs; every published port bound to `127.0.0.1` with no
`0.0.0.0` host binding; `lite` RF=1 barred-from-production with unsuppressable warnings on the receiver,
loader and topic-bootstrap; no secrets or keys tracked; `secret_scan.py` and `gitignore_guard.py` pass;
the PHI guards reached at every real production log and event call site, quarantine and reject paths
included, *after* the later changes; the loader never logs a response body or headers.

**Explicitly relied on prior reviews and not re-derived:** the `tb-usr-*` fix, the MLLP bind rule, the
byte-budget queue, the stuck-executor fix, MLLP framing limits, and Kafka client config. **Unverified:**
Kafka ACLs and SASL are contract-only with no deployment artefact yet. **No SAST, CVE or image scanner
exists on this machine**, so that part of the wave's security position is source-review-only, exactly as
in Phase 0.

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
- **The CI control scripts are outside the type-check scope.** `mypy packages` is the command CLAUDE.md
  defines, and `.github/scripts/` is not under `packages/` — so `licence_check.py` and its test are not
  type-checked. `devops-engineer` found a pre-existing `mypy` error there (a `FakeDist.locate_file`
  signature mismatch in `test_licence_check.py`, predating this wave, lead-confirmed). The error itself
  is cosmetic and not worth fixing now. **The coverage gap is the part worth recording:** these scripts
  are controls — `licence_check.py` is what caught the `certifi` High that review missed — and a control
  that no static check covers is one nobody notices decaying. Wave 2: extend the type-check scope to the
  scripts, then fix what it reports.
- **No CRL or OCSP revocation checking for mTLS client certificates** (`tls.py`, `security-reviewer`
  Low 5). Accepted for a walking skeleton: a revoked sender certificate would still be trusted until it
  expires. Track for the point where mTLS is used against a real sender population.
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

1. ~~**The `certifi` (MPL-2.0) decision — human.**~~ **CLOSED.** **APPROVED 2026-09-23**, unmodified, dev
   group only, never shipped, scoped to the `testcontainers[kafka]` → `docker` → `requests` path and
   recorded as its own approval in ADR-001 2.3. `devops-engineer` landed the `MPL_APPROVED` edit
   (`a35528c`); `licence_check.py` now passes, **44 components, 0 failed**.
2. ~~**The HTTP loader redirect fix — `connector-engineer`.**~~ **CLOSED.** Fixed at `bec8029` and
   confirmed by `security-reviewer` with the mechanism re-derived: all three consequences verified
   independently, plus three self-initiated gap-checks (redirect handler dropped, TLS verification
   preserved, shared opener singleton safe). It was held open on the commit alone on purpose — "the
   redirect is fixed" and "all three consequences are closed with no new gap" are different claims, and
   the second one needed someone other than the author or the lead to make it.

**Still blocking:**

3. **A human pushes `wave/1-1` and the CI integration job goes green.** The only thing that turns
   criterion 1 from PASS-locally into PASS, and the only remaining condition. Not an agent action.

**Met since the first draft:**

4. ~~`code-reviewer` clean on the cumulative wave diff.~~ **MET** — APPROVE, zero at any severity.
5. ~~Full `pytest -m integration` run green.~~ **MET** — 6 passed in 227.91 s.
6. ~~A licence and dependency check on what the wave added.~~ **RUN, and it found finding 1.** The check
   existing is what turned an assumption into a decision; that is condition 4 working as intended even
   though the result is red.

**Not blocking, carried to wave 2:**

6. Invariant 9's mutation control: assert a failing event emitter does not affect message flow.
7. Invariant 3's real chaos coverage once PostgreSQL and the API exist (gate 7.2 item 19).
8. The reject path (envelope 2.3.1) implemented, with a unit test per placeholder.
9. Deploy-time step resolution, with the timeout constraint in 7.3.
10. An alert on `partition_halted` before the MVP.
11. The Linux re-run of S1 and S2.

**Why one condition rather than GO outright:** criterion 1 says "in CI", and no CI run exists for this
branch because nothing has been pushed. That is a fact about the process, not about the code, and a
skipped check is a FAIL until it runs.

**Why not NO-GO, when two Highs were open earlier in this gate.** Neither required redesigning anything:
one was a licensing decision on a dev-only transitive dependency, the other a default-constructor choice
with a one-line fix. Both were fixed and independently confirmed inside the wave. That is the Phase 0
distinction between a condition and a defect, applied the same way — Phase 0 also carried 2 Highs, a live
all-interfaces exposure among them, as blocking conditions rather than a NO-GO, and closed them before
signing.

**What the reviews are and are not evidence of.** `code-reviewer-8` found nothing at any severity across
the whole diff, and `security-reviewer` now reports zero Critical, High and Medium. That is a real
result. It is not evidence that the wave is defect-free: the first security pass found a live
message-loss bug in code that had already passed a clean code review, `mypy --strict` and 701 tests.
Reviews find what they look for.

**The pattern in both Highs is worth more than either fix.** High 2 exists because the loader accepted a
library default — `urllib`'s redirect handler — and the tests covered the happy path and the duplicate
path but not the misclassification path. High 1 exists because a hand-written `# Apache-2.0` comment sat
next to a dependency whose transitive closure nobody had resolved. Neither is carelessness; both are
**controls that were never applied to a path nobody thought about.** That is the same shape as the two
Phase 0 Highs and as three of the four contract defects in 5.1. Wave 2 should assume it recurs and spend
its review effort on defaults and transitive edges rather than on the paths that have tests.

**Why this wave's most useful output is a process finding.** Four contract defects were found by
implementers and none by a checker. The checkers were not weak — 102 conformance cases, 701 fast tests,
`mypy --strict` — they were answering a different question. A document can be perfectly well-formed and
refer to nothing at all. Wave 2 should expect the same class of defect and keep both checkers.

---

## 9. Sign-off

| Item | Status |
|---|---|
| Exit criterion 1 (E2E in CI) | **PASS locally, PENDING CI push.** `pytest -m integration` 6 passed in 227.91 s; no CI run exists |
| Exit criterion 2 (invariants 1, 2, 3, 5 tested) | **PASS for 1, 2, 5; PARTIAL for 3** |
| Exit criterion 3 (contracts published and referenced) | **PASS** |
| Exit criterion 4 (`code-reviewer` clean) | **PASS.** `code-reviewer-8`: APPROVE, zero findings at any severity |
| Invariants 1-10 | **No change needed to any invariant.** 1, 2, 4, 5, 7 upheld with evidence; 6, 8 upheld in part with named gaps; 3 partial; 9 not exercised; 10 upheld, with a fourth MPL approval recorded |
| `code-reviewer-8`, wave diff | **Zero Critical, High, Medium, Low** |
| `security-reviewer`, final | **0 Critical, 0 High, 0 Medium, 3 Low.** Gate passes from their side. First pass found 2 High; both fixed inside the wave and confirmed closed with the mechanism re-derived |
| Unresolved Critical | **None** |
| Unresolved High | **None.** High 1 (`certifi`) closed — human-approved 2026-09-23 (lead's first-hand attestation), `MPL_APPROVED` landed at `a35528c`, `licence_check.py` 44 components / 0 failed. High 2 (redirect) closed at `bec8029`, independently confirmed |
| Unresolved Low | **3**, all tracked in 7.2: two log sites bypassing the guarded logger, no CRL/OCSP for mTLS |
| Gate recommendation | **GO WITH ONE CONDITION:** a human pushes `wave/1-1` and the CI integration job goes green. Everything an agent can close is closed. 7 items carried to wave 2 |
| Human sign-off | **2026-09-24, relayed by the lead:** the human confirmed CI passed on the pushed branch and approved starting wave 2. That meets condition 3, the last one. Wave 1 is **not merged to `main`**. The architect has not seen the CI run itself |
