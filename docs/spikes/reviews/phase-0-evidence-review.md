# Phase 0 evidence review (S5, S2, S3 harnesses)

Reviewer: `code-reviewer` (read-only; ran nothing against Kafka or Docker; no tests run).
Filed by the lead from the reviewer's messages, since the reviewer has no Write tool.
Scope: are the headline claims supported by what the harness can actually detect?

## Findings

| # | Sev | Finding | Owner | Status |
|---|---|---|---|---|
| 1 | HIGH | S5 kill scenarios produced no duplicates or redeliveries (`runner.py:519`): `commit_every=1`, kill lands between commits, so replay-order is not demonstrated. Fix: `--commit-every 20` and assert duplicates > 0, or kill on a delivery-count trigger inside a batch | platform-engineer | open |
| 2 | HIGH | S5 `rebalance` scenario moved no partitions (`runner.py:589`): worker-c had 0 deliveries, 1 assign. Fix: assert worker-c delivered > 0 and a survivor's revoke count rose after c joined; start c earlier, larger `--count` | platform-engineer | open |
| 3 | HIGH | S3 `s3run.py` `restart()` calls `verify_loss.py` without `--id-from msh10`, so `docs/spikes/data/S3/s3.jsonl` records a false `lost: 7974`. The report's "0 lost" comes from a manual rerun (`s3-restart-verify.json`) | connector-engineer | fixed in ad5a655 (runner passes --id-from msh10; false record kept and annotated; rerun not done) |
| 4 | MED | Property-test faults are never asserted to have fired (`tests/test_properties.py:268-296`): faults at polls 1-30 on the first loader only; small examples can finish first | platform-engineer | open |
| 5 | MED | No mutation control on the loader (a deliberately broken loader must fail the property suite) | platform-engineer | open |
| 6 | MED | Cluster commit-after-delivery is a final-state check (`runner.py:290-306`); the online check exists only in the model | platform-engineer | open (fix or state in report) |
| 7 | MED | `txn_crash` does not prove a transaction was open at the kill (`runner.py:643-716`) | platform-engineer | open |
| 8 | MED | `RunResult.ok` (`runner.py:186`) does not include `exactly_once` for txn scenarios; require `duplicates == 0` for txn and txn_crash | platform-engineer | open |
| 9 | MED | `docs/spikes/S5-ordering-retry.md` was still a DRAFT ("results pending"); `runs/` holds two run sets (`*-14xxxx`, `*-142xxx`); the report must name which set it quotes | platform-engineer | open |
| 10 | MED | S2 leader-kill n=20: zero loss gives a 95% upper bound of roughly 14% per trial. Word it "no loss observed". Do not cite `aa_while_unavailable` for leader-kill (misdefined; trial 2 flagged 75) | connector-engineer / architect (memo wording) | fixed in 80c4f68 ("no loss observed", ~14% bound stated) |
| L1 | LOW | `spikes/common/src/tbcommon/checks.py:79`: a re-delivered seq is never counted as a reorder (correct under at-least-once, but 1,2,3,1,3,2 passes). Say so in the report | platform-engineer | open |
| L2 | LOW | `verify_loss.py` passes when `acked_ids` is empty; its fixed 30 s read deadline can only give a false "lost", never a false pass | connector-engineer | open |
| L3 | LOW | S3 latency cells are single-run (doc already says so) | | noted |
| L4 | LOW | S5 property tests hit one loader and successors run sequentially; concurrent-worker ordering is covered only by the cluster runs | platform-engineer | state in report |

## Verdict per headline claim

| Claim | Rating | Basis |
|---|---|---|
| S5 zero order violations, model, 2,000 schedules | WEAKLY SUPPORTED | Checker can fail (negative control in `spikes/common/tests/test_envelope_and_checks.py:82`). Fault firing unverified; no broken-loader control |
| S5 zero order violations, 12 cluster scenarios | WEAKLY SUPPORTED | Checks are sound (fsynced sink file, DLQ set compared exactly, kill proven by missing stats file and 10.0 s takeover gap), but no replays and `rebalance` moved nothing |
| S5 no loss under kill | WEAKLY SUPPORTED | Measured against produced ids and the sink, not worker bookkeeping; the kill had nothing to replay |
| S5 commit-after-delivery | WEAKLY SUPPORTED | Online check exists only in the model; the cluster check is final-state |
| S5 poison and DLQ | SUPPORTED | `dlq_ids == expected_dlq_ids` enforced; `halt` checked against a prefix read back from the topic |
| S5 txn exactly-once | WEAKLY SUPPORTED | 600 unique records, 0 duplicates via `read_committed`; open transaction unproven; `exactly_once` not in `ok` |
| S2 0 acked lost (20 leader-kill + 6 outage trials) | SUPPORTED | `verify_loss` compares acked ids with topic keys; ids unique per run; a missing id fails (shown by the mis-keyed S3 run). Limited statistical power |
| S2 no ACK during the two-broker outage | SUPPORTED | 0 ACKs in the window in 6/6 trials; first ACK 25-35 s after the kill, past the 12 s restart, with 11-13k NACKs; rules out a hung server |
| S3 latency, timeout, memory, restart numbers | SUPPORTED | Match `docs/spikes/data/S3/s3.jsonl`; stand-in worker labelled |
| S3 "0 lost" | SUPPORTED with an artefact | Only via the manually rerun `s3-restart-verify.json`; runner and `s3.jsonl` show a false 7,974 lost (finding 3) |

Nothing UNSUPPORTED. No Critical findings.
