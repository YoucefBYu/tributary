# Spike reports

One file per spike: `S<n>-<name>.md`, plus `PHASE-0-DECISION.md`. See `docs/agent-team/phases/phase-0.md` for the plan and thresholds.

Each report states: question, method, environment (hardware and versions), raw results, threshold, verdict, follow-ups.

## Phase 0, as accepted

Synthesis: [`PHASE-0-DECISION.md`](PHASE-0-DECISION.md) — **GO WITH CHANGES**, no invariant changes,
9 decisions for the human. Evidence ratings: [`reviews/phase-0-evidence-review.md`](reviews/phase-0-evidence-review.md).

| Spike | Verdict as accepted |
|---|---|
| [S1](S1-hl7-throughput.md) HL7 parse and transform | **PASS** — `python-hl7` 2,446 msg/s/core (12.2x), lazy 36,322; `hl7apy` FAILS at 100 and stays off the hot path. Rust/PyO3 parser not needed |
| [S2](S2-mllp-kafka.md) MLLP ACK-after-commit | **Zero-loss and no-ACK-while-unavailable MET; ACK p99 NOT MET as measured, cause unresolved.** Design kept; needs a Linux re-run |
| [S3](S3-sync-reply.md) Synchronous reply | **Established against a stand-in worker.** Timed-out requests are already committed, so sync mode requires loader idempotency |
| [S4](S4-topic-scale.md) Topic-count scale | **PASS at 3 partitions per topic, FAIL at the plan's 12.** Partition default cut to 3 |
| [S5](S5-ordering-retry.md) Ordering, retry, DLQ | **PASS**, 0 order violations. `retry-topic` not implemented or measured |
| [S6](S6-mirth-import-corpus.md) Mirth import corpus | **INCONCLUSIVE** — no corpus. Blocked on the human |

**Every number in these reports comes from one Windows 11 / Docker Desktop laptop.** Latency figures
do not transfer; capacity figures are lower bounds. Nothing here should be published or used for
capacity planning before S1 and S2 are re-run on Linux.
