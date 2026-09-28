# Wave 2 exit criterion 4: Linux re-run of S1 and S2

Date: 2026-09-28. Branch wave/1-2. Owner: qa-engineer. Raw JSON: `benchmarks/results/linux-wave2-rerun/` (force-added; the directory is git-ignored).

## Evidence rating (read first)

This is a **shared, non-dedicated container**, not the dedicated 3-broker cluster the Phase 0 memo priced. Other agents ran tests on the same 4 vCPUs during these runs. Kafka is a **single node, RF=1** (`apache/kafka:4.0.0`, testcontainers, the lite profile), whereas Phase 0's S2 used a 3-broker cluster. S2 here is **closed-loop** (send, await ACK, repeat), not Phase 0's open-loop fixed-rate generator. Treat the numbers as an easier-topology, noisy-host data point. They do not settle the RF=3 ACK p99 question and must not be used for capacity planning.

Host: Linux 6.18.44-fc-v37 (Firecracker VM), x86_64, 4 vCPU Intel Xeon @ 2.10 GHz (1 thread/core), 15 GiB RAM, no swap. Docker 29.3.1, CPython 3.12.3, uv 0.8.17, confluent-kafka 2.15.1 (librdkafka 2.15.1), tributary-hl7v2 0.0.0. Kafka image digest `sha256:3f7b9391...dedc75`. Synthetic payloads only (MSH-3 `TRIBUTARY-SYNTH`).

## S1: HL7 parse throughput (threshold: at least 200 msg/s/core parse plus transform)

Command: `uv run python -m benchmarks.s1_hl7_parse_throughput --count 10000 --repeats 5 --procs 1,2,4 --out <file>`, 3 runs.

| Run | parse msg/s (1 core) | parse p50 / p99 us | parse+extract msg/s | parse+extract p50 / p99 us |
|---|---|---|---|---|
| 1 | 55,741 | 16.5 / 39.1 | 14,179 | 51.5 / 216.5 |
| 2 | 46,486 | 18.2 / 56.8 | 14,001 | 51.5 / 223.2 |
| 3 | 43,878 | 18.6 / 64.6 | 14,815 | 49.6 / 201.7 |

Multi-process total msg/s (parse+extract, 1 / 2 / 4 procs): run1 11,299 / 29,645 / 50,809; run2 13,851 / 29,176 / 34,202; run3 14,200 / 27,557 / 50,941. Scaling at 4 procs is noisy on a shared host (34k to 51k).

Caveat that matters: **no HL7-to-FHIR transform exists yet (W2-3)**, so "parse+extract" is a substitute, not "parse plus transform". Phase 0 baseline for the lazy parser was 36,322 msg/s/core (Windows, includes its transform) and 2,446 for `python-hl7`; the two are not like-for-like.

Result: parse+extract is 14.0k-14.8k msg/s/core, about 70x the 200 msg/s/core threshold, and this holds even at 1 proc under contention. **PASS against the threshold, with the transform caveat above.** Comparison to the 36,322 baseline is not valid until W2-3 lands.

## S2: MLLP ACK-after-commit through the real receiver (threshold: ACK p99 under 50 ms at 1k msg/s)

Command: `uv run python -m benchmarks.s2_mllp_ack_latency --count N --conns C --seed S --out <file>`, 3 runs per cell. Phase 0 specified 1, 10 and 100 connections; the harness cannot set an offered rate, so throughput is whatever C connections sustain. C=10 lands at about 1.05k msg/s, which is the plan's 1k msg/s point.

| Conns | Count | Run | ACKs/s | p50 ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|---|
| 1 | 3000 | 1 / 2 / 3 | 133 / 133 / 133 | 7.0 / 7.0 / 6.9 | 7.7 / 7.6 / 7.7 | 9.6 / 9.6 / 8.9 | 1175 / 1053 / 1061 |
| **10** | 5000 | 1 / 2 / 3 | **1051 / 1043 / 1064** | 8.0 / 8.1 / 8.0 | 9.0 / 9.4 / 8.9 | **14.3 / 12.9 / 13.0** | 662 / 623 / 607 |
| 100 | 5000 | 1 / 2 / 3 | 2791 / 2968 / 3006 | 20.3 / 21.8 / 20.4 | 29.6 / 27.3 / 29.9 | 586.5 / 587.4 / 595.7 | 598 / 589 / 612 |
| 100 | 20000 | 1 / 2 / 3 | 4406 / 4252 / 4073 | 19.3 / 20.0 / 21.0 | 25.0 / 26.5 / 28.2 | 35.1 / 34.5 / 52.9 | 592 / 585 / 587 |

Observations:
- At the plan's 1k msg/s point (C=10): p99 12.9-14.3 ms, **PASS against 50 ms by about 3.5x**, on an RF=1 single broker.
- C=1 (p99 under 10 ms) also passes, but at only 133 msg/s it is not the 1k msg/s condition.
- C=100 at 5000 msgs fails on p99 (about 590 ms), but this is a warm-up artifact, not steady state: every run has one ~600 ms outlier per cell (max column, also present at C=1 and C=10), which I infer is the first produce (metadata and topic warm-up). With 100 connections that is at least 100 slow first ACKs, about 2% of 5000, so it lands in p99. At 20000 msgs the p99 falls to 34.5-52.9 ms; one of three runs (52.9 ms) is over 50 ms. Cause of the ~600 ms first-ACK is inferred, not confirmed. Throughput at C=100 is also above 1k msg/s, so that cell is not the 1k msg/s threshold condition anyway.
- Not measured: RF=3 latency, TLS, open-loop fixed-rate load, and the broker-kill stall (Phase 0 finding, 7.5-12.7 s). These need the dedicated cluster.

Verdict: **S2 ACK p99 at about 1k msg/s: PASS on this host and topology (12.9-14.3 ms vs 50 ms), rated low-confidence for the RF=3 question.** Phase 0's "NOT MET, cause unresolved" (33-125 ms on a Windows laptop with a 3-broker cluster) is neither confirmed nor refuted; a bare-producer baseline on the same host was not run, so the environment-dominated argument is still not measured. The 64 MiB receiver queue default ("measure in the Linux S2 re-run") was **not measured** by this harness.

## Reproduce

`uv run python -m benchmarks.s1_hl7_parse_throughput --count 10000 --repeats 5 --procs 1,2,4` and `uv run python -m benchmarks.s2_mllp_ack_latency --count 5000 --conns 10`. Requires a local Docker daemon.
