# Wave 2 duplicate-rate report (exit criterion 2, W2-9 verify)

Author: qa-engineer, 2026-10-05, tree at `ec4b9d1` plus the counters described below.

## What was measured and how

Each per-key chaos scenario in `tests/chaos/` was run **3 times**, each run a separate `uv run pytest <file> -m chaos` process (every run exit code 0, `1 passed`). Every run emits one row through `tests.support.chaos_assertions.record_chaos_row` (new; mutation-controlled by `count_order_violations` in `test_chaos_assertion_mutation_controls.py`), appended to a JSON Lines file when `TB_CHAOS_ROWS_FILE` is set. Reproduce:

```
TB_CHAOS_ROWS_FILE=/tmp/rows.jsonl uv run pytest tests/chaos -m chaos -q
```

Definitions. **Sent**: records the scenario put on `raw` (or, for the broker kill, the 120 messages the MLLP sender attempted). **Delivered total / distinct**: records on the `out` topic (destination requests for the outage scenario) and distinct `tb-msg-id` (or key:seq identity) among them. **Duplicates** = total - distinct; **rate** = duplicates / sent. **Order violations**: adjacent inversions among the *first* deliveries of each key (the S5 rule; a redelivery is a duplicate, not a reorder), counted, not only asserted. **Fault fired**: the scenario's own `assert_fault_fired` conditions (listed below).

## Results (threshold from the plan: order violations 0, fault fired in every scenario, duplicate rates reported)

| Scenario | Runs | Sent | Distinct delivered | Duplicates | Duplicate rate | Order violations | Fault fired | Verdict |
|---|---|---|---|---|---|---|---|---|
| Broker kill (3-broker RF=3, min ISR 2, leader of `raw` SIGKILLed after send 60) | 3 | 120 (119, 120, 120 ACKed) | 120, 120, 120 | 0, 0, 0 | 0.00% | **n/a**: no per-key assertion (see caveat 1) | yes, 3/3 (non-zero exit, leader chosen from live metadata) | PASS: 0 acknowledged lost |
| Worker kill mid commit window (`commit_every` 50, SIGKILL at about record 85, static-membership restart) | 3 | 120, 120, 120 | 120, 120, 120 | 20, 20, 20 | **16.67%** (all three runs) | 0, 0, 0 | yes, 3/3 (exit != 0 and 0 < dup < sent) | PASS |
| Cooperative rebalance, commit refused mid-join (continuous publisher, joiners until `commit_refused` logged) | 3 (+1 observed with `-s`) | 4212, 4212, 4212 (+4224) | same as sent | 0, 0, 0 (+0) | 0.00% | 0, 0, 0 | yes, 3/3 (>= 1 refused commit; the `-s` run: 1 joiner, 2 refused commits) | PASS |
| Cooperative rebalance, stable join (traffic only between settled rebalances) | 3 | 240, 240, 240 | 240, 240, 240 | 0, 0, 0 | 0.00% (asserted to be 0) | 0, 0, 0 | yes, 3/3 (joiner owns > 0 partitions) | PASS |
| Destination outage, blocking retry (HTTP stub down, sustained second `partition_paused`) | 3 | 24, 24, 24 | 24, 24, 24 | 0, 0, 0 | 0.00% | 0, 0, 0 | yes, 3/3 (pre-outage stub got exactly batch A; sustained retry seen) | PASS |

Per-key order violations: **0 in every run of every scenario that asserts order** (12 of 15 runs; the other 3 are the broker kill, which has no key). Fault fired: **15 of 15 runs**. Raw rows: the scratch JSONL from this session (15 lines); the numbers above are copied from it.

## Reading the numbers

- The worker kill is the only scenario that produces duplicates, and the figure is **deterministic**: 20 per run. It equals the uncommitted tail of the commit window at the point of the kill (records after the last `commit_every = 50` commit that were already on `out`). It is an artefact of where the kill is placed, not an estimate of a production duplicate rate. In production the rate depends on `commit_every` and on where the death falls: bounded by `commit_every` records per partition.
- 0% in the two rebalance scenarios and the outage is a measured zero for these shapes, not a claim about all rebalances. The commit-refused scenario shows the refusal path fired (`commit_refused` logged) and still replayed nothing; the stable-join scenario asserts zero by design (commit before revoke).
- The broker-kill duplicate count is measured on `raw` (producer idempotence), not downstream. One run had 1 message not ACKed that nevertheless sat on `raw`: a sent-but-not-acknowledged record is allowed to land (invariant 1 only bounds the other direction).

## Environment caveats

1. **Broker kill has no per-key order check.** Records come from the real MLLP receiver with no partition key in the channel spec, so there is no per-key order to assert; the claim is invariant 1 only. STATUS already notes that receiver-originated keys are not under chaos: the other four scenarios publish keyed records directly to `raw`.
2. One shared 4 vCPU Linux container (Intel Xeon @ 2.80 GHz, 16 GB), Docker 29.3.1, `apache/kafka:4.0.0`, Python 3.12. The broker kill uses a private 3-broker RF=3 cluster; **the other four scenarios use a single broker, RF=1** (the lite profile), so they say nothing about durability under broker loss.
3. Small samples: 120 to 4,224 records per run, 3 runs per scenario. Three identical runs show the numbers are stable for these shapes; they are not a confidence interval. The `commit-refused` scenario sent 4,212 three times by coincidence of its fixed round interval (a fourth run sent 4,224).
4. Runs were serial with nothing else on the host. The broker-kill row's `sent` was changed after the first run from the ACK count to the attempted count (that run's row said 119 and a spurious 0.84%); the table uses the corrected definition and the raw fields.
5. Duplicates are counted at the `out` topic or the HTTP stub, before any loader. Loader idempotency (invariant 4) is covered separately by the eight redelivery tests (exit criterion 3).
6. No PHI: payloads are `{"synthetic": true}` or MSH-3 `TRIBUTARY-SYNTH` messages from `benchmarks/gen.py`; rows carry counts only.
