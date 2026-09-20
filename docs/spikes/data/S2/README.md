# S2 raw data

Aggregates only: counts, latencies and synthetic control ids. No payloads, no PHI. The per-message
event dumps (1-4 MB each) are not kept; they regenerate from `chaos.py` and `bench.py`. Report:
[`../../S2-mllp-kafka.md`](../../S2-mllp-kafka.md).

| File | What it is | Quotable as evidence? |
|---|---|---|
| `matrix-full.jsonl` | Full rate × connections × TLS matrix, one pass | **Yes** — the source for the S2 latency tables |
| `matrix.jsonl` | Earlier priority-cell runs (1k msg/s, 10 connections) | **Yes**, as additional cell runs |
| `leader-kill.jsonl` | 20 leader-kill trials | **Yes.** Do **not** read its `aa_while_unavailable` column: the metric was misdefined for this scenario and is retired (report, "Broker failure") |
| `leader-kill-timeline.jsonl` | Per-message timeline rebuilt by `analyze_leader_kill.py` | **Yes** — the source for the 7.5-12.7 s ACK-gap finding |
| `outage.jsonl` | 6 two-broker outage trials | **Yes** |
| `smoke.jsonl` | Pre-run sanity check | Setup only |
| `linger.jsonl` | `linger.ms` sweep (0, 1, 5) at 1k/10 | **NO — unusable.** Taken while the host was noisy (p99 212-1,236 ms). Kept as raw data only; the `linger.ms` default remains a TODO |
| `dry-not-evidence.json` | Dry-run mode: the MLLP path with Kafka removed | **NO — NOT EVIDENCE for any threshold.** Dry-run ACKs the sender **without a Kafka commit**, so it is deliberately **not invariant-1 compliant** and the server prints a warning when started that way. Its only legitimate use is the attribution baseline in the report: receiver overhead alone is p50 0.65 ms / p99 2.7 ms, which is how we know the latency tail is broker commit time and not the MLLP design |

`dry-not-evidence.json` and `linger.jsonl` are the two files most likely to be misread. `code-reviewer` (gate
finding Low 8) asked for the rename of `dry.json` to `dry-not-evidence.json`; that is done, and this
table stays as the durable warning.
