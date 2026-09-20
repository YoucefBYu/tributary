# S2: MLLP receiver publishing to Kafka, ACK after commit

Status: MEASURED 2026-09-20 by connector-engineer, awaiting architect verdict. Latency threshold NOT met in this environment (cause unresolved); zero-loss and no-ACK-while-unavailable thresholds met.

## Question

Can an asyncio + uvloop MLLP server that publishes to Kafka (`acks=all`, idempotent) and ACKs only after commit hold ACK p99 under 50 ms at 1k msg/s, and does it keep invariant 1 (no ACK for an uncommitted message, zero acknowledged-message loss) when brokers fail? (phase-0.md S2.)

## Method

- Server: `spikes/S2-mllp-kafka/server.py` (throwaway spike code). Per message: publish with `enable.idempotence=true`, `acks=all`, `linger.ms=2`, delivery timeout 5 s; ACK `AA` only from the delivery report success; any error, full local queue or timeout returns a NACK (`AR`); nothing is ACKed and dropped. Bounded everywhere (max connections, per-connection pipeline, global in-flight, ACK timeout 8 s, idle and write timeouts). TLS uses a self-signed cert; the client verifies against it (verification stays on).
- Load: `loadgen.py`, open loop at a fixed aggregate rate over N connections, synthetic messages from the P0-1 corpus (`benchmarks/results/corpus-1-10000.mllp`, MSH-3 `TRIBUTARY-SYNTH`) with MSH-10 replaced by a unique id. The load generator and server run in Linux containers on the compose network; topics have 12 partitions, RF=3, `min.insync.replicas=2`, one fresh topic per cell.
- Latency matrix: 30 s per cell, first 3 s excluded from the "steady" statistics (p50/p99/p99.9 below are steady). After each cell `verify_loss.py` reads the topic and checks that every AA'd control id is present.
- Broker failure, `chaos.py`: `leader-kill` kills (docker kill) the leader of a rotating partition 9 s into a 500 msg/s, 10-connection run and restarts it 12 s later (20 trials). `outage` kills two of three brokers (ISR falls below `min.insync.replicas`) for 12 s (6 trials). Every trial ends with the loss check. Host and container clocks differ by about -0.25 to -0.9 s (measured per trial); it is corrected for and a 0.5 s grace applies to the window checks.

## Environment

AMD Ryzen 7 5800H (8 cores / 16 threads), 15.4 GB RAM, Windows 11 (10.0.26220), Docker Desktop on WSL2. Shared 3-broker Apache Kafka KRaft cluster in Docker on that same host (all brokers, load generator and server compete for the same cores and the WSL2 VM); ephemeral storage. The cluster was used exclusively during these runs but Windows and Docker Desktop background load was not controlled. Idle broker CPU before the runs was 2.3 to 2.7 percent per broker. This is not a Linux CI machine or production-like hardware.

Attribution baseline: a bare idempotent `acks=all` producer at 1k msg/s, no MLLP, on this cluster gives commit p50 23 ms, p99 131 ms (`produce_floor.py`). With Kafka removed (dry-run mode, not invariant-1 compliant, not verdict evidence) the MLLP path alone gives p50 0.65 ms, p99 2.7 ms at 1k msg/s, 10 connections. So the receiver's own overhead is small and the tail is dominated by broker commit latency in this environment.

## Results

### Latency matrix (steady-state ACK latency, ms; all cells: 0 lost)

Priority cell first (1k msg/s, 10 connections; every run listed):

| Cell | p50 | p99 | p99.9 |
|---|---|---|---|
| plain, early run A (`matrix.jsonl`) | 5.7 | 63.1 | 113.0 |
| plain, run B | 16.6 | 100.6 | 130.6 |
| plain, run C (matrix pass) | 8.0 | 51.3 | 87.2 |
| TLS, run B | 12.1 | 85.2 | 122.1 |
| TLS, run C (matrix pass) | 8.5 | 125.1 | 176.5 |

Full matrix, one pass (`runs/matrix-full.jsonl`; `sent`/`acked`/`nack`/`unanswered` in the raw file):

| Rate | Conns | Plain p50 / p99 / p99.9 | TLS p50 / p99 / p99.9 |
|---|---|---|---|
| 100 | 1 | 4.8 / 18.9 / 34.2 | 5.9 / 9.6 / 21.7 |
| 100 | 10 | 7.4 / 15.1 / 30.3 | 5.9 / 16.5 / 21.1 |
| 100 | 100 | 21.9 / 63.6 / 68.8 | 8.2 / 33.8 / 51.4 |
| 1000 | 1 | 25.0 / 120.6 / 162.9 | 9.8 / 110.7 / 159.2 |
| 1000 | 10 | 8.0 / 51.3 / 87.2 | 8.5 / 125.1 / 176.5 |
| 1000 | 100 | 13.0 / 47.1 / 80.2 | 15.3 / 124.8 / 172.6 |
| 5000 | 1 | 3,885 / 5,925 / 5,939 | 3,039 / 5,364 / 5,394 |
| 5000 | 10 | 1,763 / 2,328 / 2,417 | 1,360 / 2,392 / 2,507 |
| 5000 | 100 | 2,538 / 12,013 / 12,555 (11,212 of 150,008 unanswered at drain) | 2,467 / 9,986 / 10,575 |

5k msg/s is beyond what one server process sustains here (latency is queueing that grows with the run, not steady service time); this is a capacity observation for the single-process spike server on this host, not a threshold miss (S2 sets its threshold at 1k msg/s). No loss and no NACKs at 5k except the 11,212 drain-window leftovers, which were never ACKed.

The `linger.ms` sweep (0, 1, 5) at 1k/10 (`linger.jsonl`) was taken while the host was noisy and is not usable (p99 212 to 1,236 ms); it is kept only as raw data.

### Broker failure

leader-kill, 20 trials, 500 msg/s, 10 connections: 244,710 messages acknowledged, **0 lost** in 20 of 20 trials. NACKs during and after the failover (1,415 to 4,400 per trial) are `AR` responses, which is the intended behavior; senders retry, and those messages appear in the topic as unacknowledged extras or not at all.

Reading the `aa_while_unavailable` column of `leader-kill.jsonl` correctly: the metric was **misdefined for this scenario and is retired**. It counted any AA sent after the kill and ACKed before the victim restarted (trials 2, 9, 12: 75, 3,221, 2). But with RF=3 and `min.insync.replicas=2`, one dead broker stops writes only until the new leader is elected; once elected, `acks=all` succeeds with two in-sync replicas. The per-message timeline (`analyze_leader_kill.py`, `runs/leader-kill-timeline.jsonl`) shows what happens instead: in every trial ACKs stop entirely from the kill (about 7.6 to 8.1 s into the run) for 7.5 to 12.7 s (median about 9.5 s), then resume. The three flagged trials are the three with the shortest gap (7.5 to 8.2 s), where election finished before the 12 s restart; their flagged ACKs all land after the gap ends and their loss check is clean. Trials with gaps of about 12 s recovered only when the victim restarted. So the flagged ACKs were legitimate post-election commits, not ACKs during unavailability.

The real availability finding: a single broker kill stalls every connection of the receiver for 7.5 to 12.7 s, not just the connections whose partition lost its leader. Cause (INFERRED from the code and timeline; not confirmed by any experiment): each connection's next message hits a partition without a leader within a few messages, and the receiver answers in order per connection, so all connections stall behind it; meanwhile the producer queue fills and new messages are refused quickly with AR (correct, but a visible outage). The gap length matches the Kafka default broker session timeout of 9 s plus election; a tuned cluster (faster session timeout, or ISR-based shrink) may shorten it; not tested.

outage (2 of 3 brokers killed, ISR below `min.insync.replicas`), 6 trials: **0 ACKs landed inside the outage window** in all 6 (strict metric: any AA whose ACK time falls between kill completion plus 0.5 s and the restart, regardless of send time; the looser metric "sent after the kill" is 0 as well), **0 lost**, first ACK after the kill only 25 to 35 s later (the controller quorum needs the brokers back and a rebalance; 12 s of that was the outage itself). 6,345 to 9,390 messages were ACKed per trial before the kill, 10,613 to 13,660 were NACKed.

## Threshold and verdict

| Threshold | Verdict |
|---|---|
| ACK p99 under 50 ms at 1k msg/s | **NOT MET as measured; cause unresolved.** 1k-cell steady p99 ranged 33 to 125 ms across 11 cell runs (2 of 11 under 50 ms: 1k/100 plain at 32.6 in the early run and 47.1 in the matrix pass; none of the five 1k/10 runs), with p50 5 to 25 ms. The bare producer with no MLLP has p99 131 ms on the same cluster, and receiver overhead without Kafka is under 3 ms p99, so the misses track broker commit latency on a shared Windows/WSL2 host, not the MLLP design. That is an argument that the number is environment-dominated, not evidence that it would pass elsewhere. |
| Zero acknowledged-message loss (20 leader-kill trials) | **MET.** 0 lost in 20/20 (244,710 acked), 0 lost in 6/6 outage trials and in every matrix cell. |
| No ACK while Kafka is unavailable | **MET** for the two-broker outage (0 in 6 trials). The leader-kill metric was misdefined and is retired; see above. |

Recommendation for the architect: keep the design (ACK after commit is proven safe), record the p99 threshold as unresolved until it is re-measured on Linux with a dedicated 3-broker cluster (Phase 1 or CI), and treat the 7.5 to 12.7 s whole-receiver stall on a single broker failure as a design item for the production receiver (per-partition or per-connection isolation, bounded fast NACK with a Retry-After hint, and Kafka session and election tuning).

## Follow-ups

1. Re-measure the 1k msg/s cells on a Linux host with a dedicated cluster (at least 5 repeats per cell) before treating the 50 ms threshold as pass or fail.
2. Product receiver: design per-connection head-of-line isolation so one leaderless partition does not stall all connections; reproduce with leader-kill and compare `ack_gap_s`.
3. Multi-process receivers to lift the single-process ceiling below 5k msg/s.
4. Persist raw run files outside the ignored `runs/` directory (the root `.gitignore` is damaged; the raw files are therefore not in git).
5. Broker tuning (session timeout, `replica.lag.time.max.ms`) sweep for the failover gap.

## Raw data

Aggregate results are tracked in `docs/spikes/data/S2/`: `matrix-full.jsonl` (full matrix pass and priority cell), `matrix.jsonl`, `linger.jsonl`, `smoke.jsonl`, `leader-kill.jsonl`, `leader-kill-timeline.jsonl`, `outage.jsonl`, `dry.json`. They hold counts, latencies and synthetic control ids only. The per-message event dumps (1 to 4 MB each) are not kept; they regenerate from `chaos.py` and `bench.py` (the `runs/` directory is git-ignored). Reproduce: `python bench.py --rates 100,1000,5000 --conns 1,10,100 --tls both --seconds 30`; `python chaos.py leader-kill --trials 20`; `python chaos.py outage --trials 6`; `python analyze_leader_kill.py runs/leader-kill.jsonl` (rebuilds the timeline summary from the event dumps).
