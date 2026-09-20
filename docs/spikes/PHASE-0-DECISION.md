# Phase 0 decision memo

**Verdict: GO WITH CHANGES.**

Author: `architect`, 2026-09-20. Inputs: `docs/spikes/S1`-`S6`, raw data in `docs/spikes/data/`,
`docs/spikes/reviews/phase-0-evidence-review.md`, `docs/security/contracts-v0-review.md`,
`docs/agent-team/STATUS.md`.

Human sign-off required before Phase 1 starts. Section 7 lists what the human must decide; nothing in
that list has been decided by an agent.

---

## 1. Verdict and why

**GO WITH CHANGES.** The architecture in ADR-001 survived Phase 0. The two properties the whole design
rests on — ACK only after a durable commit (invariant 1) and per-key ordering under failure
(invariant 5) — were measured and held, on a real 3-broker cluster, under kills, rebalances and a
two-broker outage. **No invariant needs to change**, and no spike produced a result that contradicts
one.

The changes are these, and they are why this is not a plain GO:

1. **The default partition count drops from 12 to 3 per channel topic.** At 12, the plan's own
   500-channel target killed the cluster at about 425-450 channels. At 3, 1,000 channels were healthy.
   This changes `contracts/topics/v0` and ADR-002 D5, not the architecture.
2. **The S2 ACK-latency threshold was not met and the reason is unresolved.** It is environment-
   dominated on the evidence available, but "environment-dominated" is an argument, not a measurement,
   and it must be settled on Linux before anyone treats 50 ms p99 as achievable.
3. **Two mechanisms in ADR-001 were never exercised**: the `retry-topic` policy with delay tiers, and
   claim-check. Both should be spiked or descoped rather than carried into the MVP as assumptions.
4. **The migration estimate is a guess** (S6, no corpus). Phase 1 scope for `mirth-import` cannot be
   committed on this evidence.
5. **Two design items came out of the failure measurements** and belong in Phase 1 receiver design:
   whole-receiver head-of-line stalling on a single broker loss (S2), and synchronous reply requiring
   downstream idempotency because timed-out requests are already committed (S3).

A NO-GO would require evidence that the Kafka-centric design cannot meet its own invariants. Nothing
found that. A plain GO would require the thresholds to have been met as written, on evidence strong
enough to build on. Two were not.

---

## 2. Environment caveat — read before quoting any number

Every Phase 0 measurement was taken on **one developer laptop**: AMD Ryzen 7 5800H (8 cores / 16
threads), 15.4 GB RAM, Windows 11 (10.0.26220), Docker Desktop on WSL2, with the Kafka brokers, the
load generators and the code under test competing for the same cores. The Docker VM ceiling is
7.4 GiB. Several cells were single runs, and S1 and S4 ran while other agents were active.

Consequences, stated once and assumed everywhere below:

- **Latency numbers are the least transferable.** S2's own attribution baseline shows it: a bare
  `acks=all` producer with no MLLP at all had p99 131 ms on this cluster, while the MLLP path with
  Kafka removed had p99 2.7 ms. The tail is the broker commit path on this host.
- **Capacity numbers are lower bounds**, not cluster limits. S4 says so explicitly.
- **Throughput numbers are order-of-magnitude.** S1's pass/fail margins (12x, 182x) are far wider than
  its run-to-run spread; a 1.5x difference between two candidates is not.
- **A Linux re-run of S1 and S2 would settle the headline numbers.** It is the single highest-value
  follow-up and is recommended as a Phase 1 CI task (section 7, item 7).

No number in this memo should be published externally or used for capacity planning without that
re-run.

---

## 3. Thresholds: result and evidence rating

Ratings in the "Rating" column are quoted verbatim from
`docs/spikes/reviews/phase-0-evidence-review.md`. Where the reviewer's first-round rating was
`WEAKLY SUPPORTED` and a re-run changed it, both are shown — the upgrade was earned by new runs, and
section 4 says what changed.

| Spike | Plan threshold | Measured result | Rating (verbatim) | Verdict |
|---|---|---|---|---|
| **S1** | at least 200 msg/s/core for parse plus transform | Solo host: lazy **36,322** msg/s/core (182x), `python-hl7` **2,446** (12.2x), `hl7apy` **100** (0.50x). Loaded host, conservative floor: 27,669 / 1,967 / 59 | not separately rated; S1 accepted 2026-09-20 | **PASS** for the lazy parser and `python-hl7`; **FAIL** for `hl7apy`. S1b (Rust/PyO3) not needed |
| **S2** | ACK p99 under 50 ms at 1k msg/s | 1k-cell steady p99 **33 to 125 ms across 11 cell runs**; 2 of 11 under 50 ms, none of the five 1k/10 runs. p50 5 to 25 ms | — | **NOT MET as measured; cause unresolved** |
| **S2** | zero acknowledged-message loss, 20 leader-kill trials | **no loss observed**, 20/20 trials, 244,710 messages acknowledged; 0 lost in 6/6 outage trials and every matrix cell | "SUPPORTED (small-sample: \"no loss observed\")" | **MET**, small-sample. 20 clean trials bound per-trial loss only to about 14% at 95% confidence |
| **S2** | no ACK while Kafka is unavailable | **0 ACKs inside the outage window in 6/6 trials**; first ACK 25 to 35 s after the kill; 10,613 to 13,660 NACKs per trial | "SUPPORTED (small-sample: \"no loss observed\")" | **MET** |
| **S3** | no numeric threshold; four items to establish | Added latency about **+40 ms p50** at 100 msg/s and **+50 to +100 ms** at 1k; timeout accurate to about 5 ms; **4.6 KB per in-flight request** (+46 MB at 10k); restart: 9,000 sent, 7,974 `AA`, **1,026 `AE`**, **0 lost**, 5 to 6 s stall | "SUPPORTED (stand-in worker)"; "0 lost": "SUPPORTED with an artefact" | **ESTABLISHED against a stand-in worker.** Real-worker cost unmeasured |
| **S4** | 500 channels operate normally, rebalance under 30 s | **3 partitions/topic: PASS** — 1,000 channels (12,009 partitions) healthy at 1G heap. **12 partitions/topic: FAIL** — cluster lost at about 425-450 channels (about 21,000 partitions, computed). 2G heap **inconclusive** | "SUPPORTED that it failed; WEAKLY SUPPORTED that JVM heap was the cause (logs transcribed, originals lost)"; "S4 rebalance under 30 s: SUPPORTED at 1G / 3 partitions only"; "S4 2G heap: inconclusive, worded as such" | **PASS at 3 partitions per topic; FAIL at the plan's 12.** Rebalance 4.3 s for 100 groups; static-member removal 43.8 s is session-timeout-bound, not topic-count-bound |
| **S5** | per-key order preserved across rebalances and consumer kills | **0 order violations** in 2,000 model fault schedules, 12 cluster scenarios and 13 integration tests, including a run where a kill replayed 12 messages | "S5 order preserved under kill (12 duplicates, 0 lost): SUPPORTED" (first round: "WEAKLY SUPPORTED"); "S5 model, 2,000 schedules (order, loss, commit-after-delivery): SUPPORTED (figures report-quoted)" | **PASS** |
| **S5** | duplicate rate reported | **0.60%** (`kill`, `static_restart` at `commit_every=20`), 0.10% (`broker_kill`), 0% without a fault | — | **PASS** (reported, not thresholded) |
| **S5** | commit-after-delivery verified | 0 violations; online in the model, sampled every 250 ms through the kill window on the cluster (144 samples) | "S5 commit-after-delivery on the cluster (144 samples): SUPPORTED" (first round: "WEAKLY SUPPORTED") | **PASS** |
| **S5** | cooperative-sticky and static membership behave as ADR-001 6.4 assumes | A joiner against a **stable** pair moved **exactly 2 of 6 partitions**, one from each founder; joiner delivered 540 records, 0 duplicates | "S5 cooperative rebalance, exactly 2 of 6 partitions moved (`rebalance-202000`): SUPPORTED (single run; journal checked by the reviewer)" | **PASS**, single run |
| **S5** | transactional Kafka-to-Kafka stage gives exactly-once | **50 records proven inside an open transaction at the kill**; after restart 2,000 records, 0 duplicates, 0 losses | "S5 `txn_crash` exactly-once (`txn_crash-200240`): SUPPORTED" (first round: "WEAKLY SUPPORTED") | **PASS** |
| **S5** | poison message reaches the DLQ without stalling the partition | Exactly 8 of 8 dead-lettered, partition kept moving; `halt-partition` verified as the ordered alternative | "S5 poison and DLQ: SUPPORTED" | **PASS** |
| **S5** | `retry-topic` policy with delay tiers | **not implemented, not measured** | — | **NOT TESTED** |
| **S6** | produce a migration estimate | No corpus. 43 to 60+ person-days and "5 to 20 percent importable with no manual work" are **reasoned guesses, not measurements** | — | **INCONCLUSIVE** |

Overall evidence position, quoted from the review: *"Nothing UNSUPPORTED. No Critical findings."* and,
after the re-runs, *"No Critical or High findings remain open; nothing is UNSUPPORTED."*

### 3.1 Ratings that remain weak, and one open finding

These are the parts a reader should not over-read:

- *"S4 1G / 12 partitions fails at about 425-450 channels — SUPPORTED that it failed; WEAKLY SUPPORTED
  that JVM heap was the cause (logs transcribed, originals lost)."* The failure is real; the
  **mechanism** rests on a hand transcription. It does not change the recommendation, because the
  recommendation follows from the failure, not from its cause.
- *"S4 defaults: warn at 8k, fatal at about 20k — WEAKLY SUPPORTED (judgement; one failure at a
  computed count)."* Nothing was tested between 12,000 and about 21,000 partitions, so the real cliff
  could be anywhere in that range. The 12,000 figure is measured; 8,000 is judgement.
- **L2 is open**: *"`verify_loss.py` passes when `acked_ids` is empty; its fixed 30 s read deadline can
  only give a false 'lost', never a false pass."* Low risk in the direction that matters (it cannot
  manufacture a clean loss result from a real loss) but it is not closed.
- **S5's ordering check does not count a redelivered sequence number as a reordering.** `1,2,3,1,3,2`
  passes. This is correct under at-least-once — a replay is a duplicate, not a reordering — and it is
  exactly why invariant 4 requires loader idempotency on `tb-msg-id`. A sink that is not idempotent
  would experience that sequence as reordered.
- **S3's numbers include a stand-in echo worker**, not a real pipeline worker, and S5's note says the
  real worker would need the batched-commit path, which would change S3's latency figures.
- Two S5 coverage figures (1,339/1,396 faults fired, 209 replays in 2,000 examples) were
  *"read from the S5 report and not reproduced by the reviewer."*

---

## 4. What the re-runs changed, and why that matters

The first round of S5 evidence was rated `WEAKLY SUPPORTED` on five claims. The reason was not sloppy
measurement — it was that several scenarios **could pass without exercising the fault they were named
after**. A kill scenario with `commit_every=1` replays nothing, so it measured takeover time while
appearing to prove replay ordering. A rebalance scenario where the joiner is assigned nothing measures
nothing. A `txn_crash` that kills between transactions proves nothing about open transactions.

Those were fixed by changing the harness so the run **fails** when the fault does not fire: kills now
trigger mid-commit-window and the verdict requires duplicates; the rebalance waits for a stable
assignment before the joiner starts; `txn_crash` reads `read_uncommitted` against `read_committed`
between kill and restart and fails if the difference is zero; the property suite counts fault firing
and asserts a floor; and three deliberately broken loaders prove the checks can go red.

This is recorded because it is the most reusable engineering lesson of Phase 0: **a green test whose
fault never fired is indistinguishable from a passing system.** Phase 1 test design should carry the
pattern — assert that the fault happened, and keep a mutation control for any check that carries a
headline claim.

---

## 5. Changes this memo makes

Each change is already reflected in the contract or ADR named.

| # | Change | From | To | Basis |
|---|---|---|---|---|
| 1 | Partition default per channel topic | 12 (ADR-001 5.3 planning value) | **3** | S4 measured: 1,000 channels healthy at 3; cluster lost at about 425-450 channels at 12 |
| 2 | Topic-sharing trigger | undefined | stated in **projected total partitions**, not channels: warn at 8,000, share at 12,000 per 3-broker 1G cluster | S4; 12,000 measured, 8,000 judgement |
| 3 | `session.timeout.ms` | Kafka default 45 s | **explicit, 10 to 15 s for ordered channels**, `heartbeat.interval.ms` at a third | S5 (10.01 s / 11.32 s takeover at 10 s) and S4 (43.8 s removal at the 45 s default), two independent harnesses |
| 4 | `commit_every` (loader offset commit batching) | implicit 1 | **tunable per destination, default well above 1** | S5: `commit_every=50` gave 4.9x throughput (151 → 733 msg/s) for a replay window of 50 instead of 1 |
| 5 | Receiver design item | not stated | **per-connection head-of-line isolation required** | S2: one broker kill stalled every connection for 7.5 to 12.7 s (cause inferred, not confirmed) |
| 6 | Sync reply | ADR-001 6.5 silent on it | **requires downstream idempotency on `tb-msg-id`; the timeout NACK means "committed, outcome unknown"** | S3: 1,026 timed-out requests were already committed and later processed |
| 7 | Retry latency | not stated | resume must not be driven by the poll timeout | S5: about 155 ms of each 169 ms retry cycle was `poll_timeout_s`, not backoff |
| 8 | `retry-topic` with delay tiers | assumed available | **not measured; must be spiked before a destination may select it** | S5 follow-up 4 |

None of these touches an invariant. Items 1 and 2 land in `contracts/topics/v0` and ADR-002 D4/D5;
3, 4, 7 in ADR-003 and the runtime defaults; 5 and 6 in the Phase 1 receiver design.

---

## 6. What Phase 0 did **not** establish

Listed plainly so Phase 1 does not inherit them as settled:

1. **Any Linux number.** Every measurement is Windows + Docker Desktop + WSL2.
2. **The 50 ms ACK p99 target**, in either direction.
3. **Real-worker synchronous reply.** S3's worker was a stand-in echo process.
4. **`retry-topic` delay tiers** — the topic exists in the layout and is unused.
5. **Claim-check** (payloads over 1 MB). No spike touched it; it is the largest single PHI store in the
   design and is entirely untested.
6. **Multi-tenant topic sharing throughput and skew.** S4's `shared` scenario was not run.
7. **Broker heap scaling.** The 2G runs disagreed with each other by more than 3x; *"2G behaviour was
   unstable and not reproducible across two runs."*
8. **Anything about a real Mirth estate.** No corpus exists, so the importer's coverage and effort are
   unvalidated.
9. **Key-level parallelism inside a partition** (ADR-001 7.2) — a different proof, needs its own spike.
10. **A production HL7 mapping.** S1's transform is a small ADT-shaped mapping with no terminology
    lookups. The threshold leaves 5 ms per message; `python-hl7` already spends 0.5 ms of it.
11. **The archive path under load.** Whether the archiver keeps up at the S1/S2 event rate is still a
    TODO in the events contract.

---

## 7. Decisions for the human

Each item has a recommendation. **None has been decided by an agent.**

| # | Decision | Recommendation | Why, and what it costs |
|---|---|---|---|
| 1 | **Do any invariants change?** | **No. Recommend confirming all ten unchanged.** | Nothing measured in Phase 0 contradicted an invariant. Invariant 5 was the one at risk, and S5 measured 0 order violations across every harness; invariant 1 held under 26 broker-failure trials. This is the answer the plan asked for explicitly, so it is recorded rather than left implied. |
| 2 | **`_tb.<env>.*` platform topic naming** (ADR-002 D2) | **Confirm as a refinement of ADR-001 5.3 prose, not an invariant-2 change.** | The topic *class* `_tb.events` named in invariant 2 is unchanged; only the physical name gains an `<env>` segment. Without it, a dev control plane can publish channel specs that production workers consume, and invariant 3 makes workers trust that topic completely. Cost: longer names and a deviation from ADR-001's wording. The alternative is mandating one cluster per environment. |
| 3 | **Lite mode with RF=1** | **Ship it, with a startup warning and explicit documentation that RPO=0 is not guaranteed.** | Small sites and development need a single-broker option, and the alternative is those users running an unsupported configuration anyway. Cost: an acknowledged message can be lost if the single broker's disk dies — a real weakening of invariant 1's promise for that profile. This is a product decision, not an engineering one, which is why it is here. |
| 4 | **Keyed-hash (`_hmac`) mechanism** (security finding 6) | **Decide before the archiver is built; leave it unresolved until then.** No agent may design it (CLAUDE.md escalates cryptography and key management). | The events schema pins the 64-lowercase-hex shape, but an unkeyed `sha256(mrn)` has the same shape and is reversible by exhaustive search over a bounded identifier space. Open: construction, key scope, rotation and re-index, key custody, and whether a keyed hash of an MRN is de-identified at all (under a Safe Harbor reading it is not). **This blocks per-patient search in the message browser** (ADR-001 6.6) — a user-visible feature depends on it. Questions are enumerated in `contracts/events/v0/README.md` 3.1. |
| 5 | **Audit retention** (security finding 12) | **Answer before the `_tb.<env>.audit` contract is written.** Likely answer is architectural, not a larger number. | 30 days was picked to match the DLQ. HIPAA §164.316(b)(2)(i) requires six-year retention of records of required actions; whether that reaches this audit trail is an interpretation question and **no legal review has been done**. Six years at RF=3 points at ClickHouse or object storage as the system of record with Kafka as a buffer. Also puts `_tb.<env>.events` (3 d) and `raw` (7 d, the replay source) in scope. Written up in `contracts/topics/v0/README.md` 2.1. |
| 6 | **ADR-004 scope** | **Approve the scope: Kafka role/ACL model, credential rotation, key custody for encryption at rest and the `_hmac` key, claim-check object lifecycle.** Needed before the archiver or any production deployment; not before the Phase 0 gate. | `contracts/topics/v0` 3.4 and 3.5 fix the required mechanisms and the ACL shape, which is as far as an agent may go. The role model and anything touching key management is a human decision. |
| 7 | **Linux re-run of S1 and S2** | **Approve as the first Phase 1 CI task, before the receiver is built.** | It is the only way to convert the two most quotable numbers (parse throughput per core, ACK p99) from "measured on a laptop" into something a sizing guide can use, and S2's threshold cannot be called passed or failed without it. Cost: CI runner time and a dedicated 3-broker cluster; at least 5 repeats per cell. |
| 8 | **Revised Phase 1 scope** | **Approve section 8.** | The changes are: partition defaults land in the MVP; `retry-topic` and claim-check get spikes before implementation; `mirth-import` is timeboxed rather than committed; receiver head-of-line isolation is added as a design requirement. |
| 9 | **Mirth corpus** (blocks S6) | **Supply anonymized exports in `samples/mirth/`, or accept that the migration estimate stays a guess.** | Without a corpus, "5 to 20 percent importable" and "43 to 60+ person-days" are unvalidated by a factor that S6 itself puts at 2x or more in either direction. Migration cost is the highest-likelihood, highest-impact risk in ADR-001's risk table, and it is the one Phase 0 could not reduce. |

---

## 8. Recommended Phase 1 scope changes

Relative to ADR-001 section 14:

1. **Adopt the measured defaults on day one** — 3 partitions per channel topic, explicit
   `session.timeout.ms`, `commit_every` above 1. They are in `contracts/topics/v0` already; the runtime
   must read them rather than hardcode its own.
2. **Spike before implementing**, not during: `retry-topic` delay tiers (S5 follow-up 4) and
   claim-check (untested, and the largest PHI store in the design). Neither is a large spike; both are
   currently assumptions with a contract written against them.
3. **Receiver design gets two explicit requirements**: per-connection head-of-line isolation, verified
   by reproducing S2's `leader-kill` and comparing `ack_gap_s`; and multi-process receivers, since one
   process saturated below 5k msg/s.
4. **Synchronous reply is built on the batched-commit path**, with fail-fast plus DLQ rather than
   blocking retry for the reply worker, and the `AE` timeout semantics documented for integrators.
   S5's note to S3 is the design input.
5. **`mirth-import` is timeboxed, not committed.** Until a corpus exists, size it as an exploratory
   effort with a review point, not as a 43-to-60-day deliverable.
6. **Keep `tests/conformance` in CI** on every `contracts/**` change, and carry the Phase 0 test
   pattern forward: assert the fault fired, and keep a mutation control for every check that backs a
   headline claim.
7. **Do not size a production deployment from these numbers** until item 7 in section 7 is done.

---

## 9. Risk table, updated with evidence

Changes against ADR-001 section 13 are in bold.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka too heavy for small sites | Medium | High | none — no spike addressed footprint | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | ~~Medium~~ **Low** | Medium | S1: 2,446 msg/s/core (`python-hl7`) and 36,322 (lazy) against a 200 threshold, solo host; `hl7apy` fails at 100 | S1b **not needed**. Re-measure on Linux and on a production mapping with terminology lookups |
| Migration cost from JavaScript deters adopters | High | High | **none — S6 INCONCLUSIVE, no corpus** | Importer, compat shim, assisted porting, shadow diff; timebox Phase 1 effort |
| Buyers want references, certification, support SLAs | High | High | none | Design partners, security docs, paid support |
| Topic/partition explosion with many channels | ~~Medium~~ **Medium, now bounded** | Medium | **S4: 12,009 partitions healthy, about 21,000 fatal at 1G (single run, computed count); 500 channels FAIL at 12 partitions/topic, 1,000 channels PASS at 3** | **Partition default 3; sharing trigger in projected partitions; re-measure on real broker memory** |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles |
| **Receiver-wide stall on a single broker loss** | **Medium** | **Medium** | **S2: 7.5 to 12.7 s, median about 9.5 s, all connections, 20/20 trials (cause inferred)** | **Per-connection isolation, bounded fast NACK, broker session and election tuning** |
| **Sync-reply duplicates after a timeout NACK** | **High** | **Medium** | **S3: 1,026 of 9,000 requests timed out, all already committed and later processed** | **Downstream idempotency on `tb-msg-id` (invariant 4); document `AE` as "committed, outcome unknown"** |
| **Latency target unverified on production-like hardware** | **High** | **Medium** | **S2: p99 33 to 125 ms at 1k msg/s against a 50 ms threshold, on a shared Windows host; bare producer p99 131 ms on the same cluster** | **Linux re-run with a dedicated cluster before any sizing claim** |
| **Untested mechanisms carried into Phase 1** (`retry-topic` tiers, claim-check) | **Medium** | **Medium** | **none — neither was exercised** | **Spike both before implementation** |
| Contract schemas pass conformance but a security control is missing | Medium | High | Two review rounds, 14 findings, all fixed, escalated or referred; 74 conformance tests | Keep `tests/conformance` in CI; `security-reviewer` sees contracts, not only code |

---

## 10. Sign-off

| Item | Status |
|---|---|
| Phase 0 go/no-go | **GO WITH CHANGES — awaiting human sign-off** |
| Invariants 1-10 | **No change proposed** |
| ADR-001 updated with measured numbers | Yes (5.3, 6.3, 7.1, 7.3, 13) |
| ADR-002 `TODO(S4)` placeholders filled | Yes (D4, D5) |
| ADR-003 `TODO(Sn)` placeholders filled | Yes, except `retry-topic`, which is recorded as unmeasured |
| Human decisions outstanding | 9, listed in section 7 |
