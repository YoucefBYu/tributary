# Spike reports

One file per spike: `S<n>-<name>.md`, plus `PHASE-0-DECISION.md`. See `docs/agent-team/phases/phase-0.md` for the plan and thresholds.

Each report states: question, method, environment (hardware and versions), raw results, threshold, verdict, follow-ups.

## Wave 2 Linux re-run of S1 and S2

[`wave-2-linux-rerun.md`](wave-2-linux-rerun.md), 2026-09-28: S1 parse plus the real W2-3 mapping **565 to 617 msg/s/core, PASS against 200** (about 60x below Phase 0's 36,322, which was not a like-for-like transform). S2 ACK p99 at ~1k msg/s **12.9 to 24.8 ms, PASS against 50 ms**, first-ACK outlier traced to a shared producer cold start. Shared 4 vCPU container, single-node Kafka, closed loop: **not** the dedicated 3-broker cluster; low confidence for RF=3.

## Wave 2 evidence reports (2026-10-05)

- [`wave-2-duplicate-rate.md`](wave-2-duplicate-rate.md): exit criterion 2. Five per-key chaos scenarios, 3 runs each: 0 order violations, fault fired 15 of 15, 0 duplicates except the worker kill (20 of 120, deterministic).
- [`wave-2-sync-reply-latency.md`](wave-2-sync-reply-latency.md): MLLP sync-reply round trip, 5 runs: p50 about 18 ms, p99 25 to 29 ms, none over 100 ms. The earlier 270 ms tail did not reproduce; cause is wave 3 (platform-engineer).

## Wave 3 evidence reports (2026-10-06)

- [`wave-3-outage-control-plane.md`](wave-3-outage-control-plane.md): W3-9a, exit criterion 2. 5 control plus 5 stopped runs, two sets: 0 order violations, 0 loss, fault fired 10 of 10 per set. Rule made one-sided after set 1 (stopped median >= control min): set 1 848 vs 484 to 847 (two-sided miss by 0.7 msg/s, stopped faster), set 2 888 vs 701 to 928, set 3 726 vs 395 to 735, all pass one-sided. Mutation control (archive in the message path) is red.
- [`wave-3-archive-search-replay.md`](wave-3-archive-search-replay.md): W3-9b. Archive path end to end (MLLP, worker, archivers, API): search by `tb-msg-id`, content view with audit record, reprocess with parent link, replay over a window with per-key order kept; 4 integration tests, 2 consecutive runs green. Search at 100k rows p50 about 100 ms, p95 about 132 ms (two runs; an authenticated request without the archive costs about 9 ms); replay 3.3k to 5.2k records/s over two runs. No replay cancel exists in the contract; no `received` event; no `audit_log` writer.
- [`wave-3-security-review.md`](wave-3-security-review.md): criterion 4 security pass on `tributary-api` at aba9ac5 (no Critical, High or Medium; Lows fixed; two items not verified).
- [`wave-3-sync-reply-tail.md`](wave-3-sync-reply-tail.md): W3-24, sync-reply tail, 12 runs x 3,250 requests. **Reproduced under contention; cause not isolated**: 17 requests over 200 ms (worst 317.5 ms) in 3 of 12 runs; only the 3 busy-loop runs are a control (1 of 3 over 200 ms); receiver, worker and broker logs hold no line in any slow window. Cause is platform-engineer's.
- [`wave-3-rf3-free-cluster.md`](wave-3-rf3-free-cluster.md): W3-33b, RF=3 chaos on a free 3-broker cluster; B1 ACK latency ratio at 750 msg/s: p50 about 1.2 to 1.3, p99 noisy (0.5 to 6.7).

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
