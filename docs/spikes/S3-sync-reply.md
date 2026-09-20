# S3: synchronous reply pattern

Status: MEASURED 2026-09-20 by connector-engineer (receiver side) against a STAND-IN worker; awaiting architect verdict. The real pipeline worker (platform-engineer) did not exist in `packages/` at measurement time, so every number that includes worker time is for `spikes/S3-sync-reply/echo_worker.py`, which does no pipeline work (it echoes an ACK), commits offsets after the reply is delivered, and is otherwise a plain consumer/producer pair. Worker-side cost of real processing and the real worker's restart behavior are NOT measured here.

## Question

What does a synchronous reply (sender's ACK/response is produced by the worker and returned over the same MLLP connection) cost over the async ACK-after-commit path; what happens on timeout; how much memory does the pending map need at 10k in-flight requests; and is the result correct when the worker restarts? (phase-0.md S3.)

## Method

- Receiver: `spikes/S3-sync-reply/sync_server.py`, extending the S2 server. Per message: register a future under a fresh correlation id, publish to `raw` (`acks=all`, idempotent) with header `tb-correlation-id` (key = correlation id), wait for the reply record on the reply topic, write it to the sender. The message is committed before the wait begins (invariant 1); a timeout returns the configured NACK (`AE` in these runs) and means "committed, no answer", so a sender retry is a duplicate downstream (invariant 4). Pending map is capped (over the cap: immediate `AR`, nothing published) and every entry has a hard deadline.
- Reply consumption, two modes: `broadcast` (ADR-001 6.5 as written: one consumer per receiver reads all reply partitions, keyed by correlation id, drops replies not in its map) and `targeted` (the receiver owns one reply partition, advertised in header `tb-x-reply-partition`, the worker produces the reply to exactly that partition). The stand-in worker supports both. Cost of broadcast grows with (replies x receiver instances); with one receiver instance as here, the two are close.
- Stand-in worker contract (to share with the real worker): in = raw record with `tb-correlation-id`; out = reply record with the same header, payload = HL7 ACK with MSA-2 = MSH-10, key = correlation id, optional target partition; offsets committed only after the reply is delivered (at-least-once, so a restart can produce duplicate or late replies, which the receiver drops as unknown correlation ids).
- Load: the S2 `loadgen.py` and corpus (MSH-3 `TRIBUTARY-SYNTH`), open loop, 10 connections, 30 s, first 3 s excluded from steady statistics. The load generator only counts a reply as good if MSA-2 matches the request's MSH-10, so a reply for the wrong request would show as a NACK, not a success. Runner: `s3run.py latency|timeout|memory|restart`; one fresh topic pair (12 partitions, RF 3, `min.insync.replicas=2`) per scenario.
- Loss check after the restart scenario: every ACKed control id must exist in the raw topic (`verify_loss.py --id-from msh10`, because S3 keys records by correlation id, not control id).

## Environment

Same as S2: AMD Ryzen 7 5800H, 15.4 GB, Windows 11, Docker Desktop on WSL2, shared 3-broker KRaft cluster on the same host, Linux containers for server, worker and load generator. Not isolated; single run per cell; the bare-producer floor on this cluster at 1k msg/s is p50 23 ms, p99 131 ms with no MLLP (see S2), so differences under about 2x are not conclusive. The async baseline p99 at 1k msg/s in this window (152 ms) was itself worse than in the S2 matrix (47 to 125 ms).

## Results

### Added latency vs async ACK (steady state, ms, 10 connections; 0 lost, 0 NACK in all six cells)

| Rate | Mode | p50 | p99 | p99.9 |
|---|---|---|---|---|
| 100 | async ACK (baseline) | 8.4 | 21.8 | 29.0 |
| 100 | sync, broadcast | 48.5 | 94.3 | 107.5 |
| 100 | sync, targeted | 43.4 | 86.3 | 96.2 |
| 1000 | async ACK (baseline) | 36.1 | 151.8 | 230.0 |
| 1000 | sync, broadcast | 136.4 | 296.7 | 328.1 |
| 1000 | sync, targeted | 85.0 | 249.8 | 352.2 |

Added p50 is about +35 to +40 ms at 100 msg/s (one extra Kafka commit for the reply plus the worker's poll/commit cycle, and the reply consumer's fetch) and about +50 to +100 ms at 1k msg/s; targeted was lower than broadcast in both rates but the gap at 100 msg/s (5 ms) is inside the noise, and at 1k msg/s with a single receiver instance the difference is not conclusive on a single run. The absolute values are dominated by the environment (two Kafka commits per request, each with a p99 floor above 100 ms here). Pending-map high-water mark: 20 to 30 at 100 msg/s, 304 to 432 at 1k msg/s.

### Timeout (no worker; reply timeout 3.0 s; 50 msg/s, 10 s)

502 sent, 502 answered with `AE` at 2,998 to 3,003 ms (p50 3,000.5 ms): the timeout is accurate to about 5 ms. Pending map returned to 0; pending high-water 160 (about rate x timeout). No hung connections, none unanswered. A worker started afterwards produced 502 late replies, all counted as `late_replies` and dropped, no crash, no wrong delivery. The 502 messages were committed to `raw` before the NACK, so the sender's retry is a duplicate for downstream (design consequence: the NACK for a timeout must be documented as "committed, outcome unknown").

### Pending-map memory at 10k in-flight (no worker, reply timeout 60 s, 50 connections, 2,000 msg/s for 5 s)

Pending reached 10,006 and held (entries expire only at the 60 s deadline). Receiver RSS: 34.6 MB idle, 47 MB at 2.4k, 65 MB at 6.7k, 81 MB at 10k pending: **about +46 MB for 10k in-flight, roughly 4.6 KB per in-flight request** (this includes the future, correlation id, buffered request and the producer queue entry; the ~500 B corpus payloads are held once). No commit failures, no `busy` rejections at 10k (cap was 20,000). At the cap, new requests are refused immediately with `AR` and nothing is published (code path present; not exercised beyond the 10k point).

### Worker restart (200 msg/s, 10 connections, 45 s, reply timeout 5 s; worker SIGKILLed 10.8 s in, replacement started 6.5 s later)

9,000 sent: 7,974 replied `AA`, 1,026 `AE` (reply timeout), 0 unanswered. Reply flow stopped about 1 s after the kill (59 of 200 replies in the second of the kill, 0 for 4 s) and resumed 5 to 6 s later (114 of 199), then every message was answered, until the end of the run. **Loss check: 7,974 acked, 7,974 in topic, 0 lost.** The 1,026 timed-out messages were committed to `raw` (that is the 1,026 "unacked extras"); the replacement worker consumed them afterwards, producing 1,026 late replies which the receiver dropped (`replies_seen` 9,000 = all requests eventually got a reply; `late_replies` 1,026). No AA reply was delivered to the wrong request (the load generator matches MSA-2 to MSH-10). Pending map ended at 0.

Consistent with the stand-in's at-least-once design: work restarted from the last committed offset, and the reply path tolerates duplicates and late replies. Whether the real worker commits offsets after the reply is delivered, and how long its group rebalance takes, decides whether this holds; the stand-in uses cooperative-sticky with a 10 s session timeout.

## Threshold and verdict

phase-0.md S3 defines no numeric threshold; it lists four things to establish.

| Item | Verdict |
|---|---|
| Added latency vs async ACK | Measured: about +40 ms p50 at 100 msg/s, +50 to +100 ms at 1k msg/s on this host with a stand-in worker. Not compared against a real worker. |
| Behavior on timeout | Correct and bounded: accurate AE at the configured deadline, map drains, late replies dropped safely. |
| Pending-map memory at 10k | About 46 MB extra (81 MB total) for 10k in-flight; bounded by cap and deadlines. |
| Correctness under worker restart | Zero loss, no wrong replies, 5 to 6 s stall, timed-out requests become duplicates on retry. Stand-in worker only. |

Verdict: the receiver-side sync-reply mechanism is viable and safe under these tests. Not established: real-worker behavior and processing time, multi-receiver-instance broadcast cost, and results on Linux.

## Follow-ups

1. Rerun `s3run.py` against platform-engineer's real worker when it exists (contract above); change `start_worker` in `s3run.py` to launch it.
2. Broadcast vs targeted with 3+ receiver instances (broadcast cost scales with instances; the spike had one).
3. Duplicates on sender retry after a timeout NACK need an idempotent-receiver story (edge dedupe on sending application + MSH-10 is planned) and documentation of `AE` versus `AR` for timeout.
4. Exercise the pending-map cap (`AR` path) and a receiver restart with requests in flight.
5. Repeat latency cells at least 5 times on a quiet Linux host.

## Raw data

`spikes/S2-mllp-kafka/runs/s3.jsonl` plus the scenario logs `s3-*.log`, `s3-restart-verify.json` (the `runs/` directory is git-ignored, so these files exist only on the measuring host). Reproduce: `cd spikes/S2-mllp-kafka && uv run python ../S3-sync-reply/s3run.py latency` (also `timeout`, `memory`, `restart`); needs the `tb-s2` and `tb-s3` images.
