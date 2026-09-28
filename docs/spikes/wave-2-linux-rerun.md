# Wave 2 exit criterion 4: Linux re-run of S1 and S2

Date: 2026-09-28. Branch wave/1-2. Owner: `qa-engineer`. Raw JSON is on disk in `benchmarks/results/linux-wave2-rerun/` (git-ignored on purpose, not committed); the tables below summarise it. Every number is regenerable with the commands in "Reproduce".

## Evidence rating (read first)

This is a **shared, non-dedicated container**, not the dedicated 3-broker cluster the Phase 0 memo priced. Other agents ran tests on the same 4 vCPUs during these runs, so run-to-run spread is wide. Kafka is a **single node, RF=1** (`apache/kafka:4.0.0`, testcontainers, lite profile); Phase 0's S2 used 3 brokers. S2 here is **closed loop** (send, await ACK, repeat), not Phase 0's open-loop fixed-rate generator. Treat the numbers as an easier-topology, noisy-host data point. They do not settle the RF=3 ACK p99 question and are not a capacity-planning input.

Host: Linux 6.18.44-fc-v37 (Firecracker VM), x86_64, 4 vCPU Intel Xeon @ 2.10 GHz (1 thread/core), 15 GiB RAM, no swap. Docker 29.3.1, CPython 3.12.3, uv 0.8.17, confluent-kafka 2.15.1 (librdkafka 2.15.1), tributary-hl7v2, tributary-content-hl7-fhir and tributary-fhir 0.0.0 (workspace). Kafka image digest `sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75`. Synthetic payloads only (MSH-3 `TRIBUTARY-SYNTH`).

## S1: HL7 parse plus transform (threshold: at least 200 msg/s/core; Phase 0 baseline 36,322)

The W2-3 mapping exists, so this measures the real thing. **parse+map** = the SDK `parse` step then the real `adt_to_fhir` / `oru_r01_to_fhir` step (packages `tributary-content-hl7-fhir`, `tributary-fhir`), producing a FHIR R4 Bundle as JSON bytes, the same path as the golden tests' `map_fixture`. Corpus: the six golden inputs in `tests/fixtures/hl7/` (5 ADT: A01 x3, A03, A08; 1 ORU R01), cycled to 3000 messages. Per-message `make_message` and `fake_fhir_context()` are included, as the golden path does; "harness-only" measures exactly that overhead. Command: `uv run python -m benchmarks.s1_hl7_parse_throughput --corpus fixtures --count 3000 --repeats 5 --procs 1,2,4`, 3 runs (median of 5 repeats per run).

| Run | parse+map msg/s (1 core) | p50 us | p99 us | harness-only msg/s | parse msg/s | parse+extract msg/s |
|---|---|---|---|---|---|---|
| 1 | 617 | 1,507 | 4,285 | 88,822 | 60,112 | 17,161 |
| 2 | 584 | 1,554 | 4,484 | 89,777 | 60,550 | 17,508 |
| 3 | 565 | 1,608 | 4,661 | 80,424 | 58,456 | 14,394 |

Repeat spread inside a run: parse+map 520 to 630 msg/s. Harness overhead is about 0.7% of a parse+map message, so the mapping itself is about 1.6 ms/message.

Multi-process, parse+map total msg/s (1 / 2 / 4 procs): run 1 588 / 1,226 / 2,305; run 2 607 / 1,184 / 1,413; run 3 579 / 1,065 / 1,320. Scaling is near-linear to 2 procs and noisy at 4 on 4 shared vCPUs.

Extra rows from the earlier synthetic-mix corpus (`--corpus synth`, `benchmarks.gen`, 10k messages, 3 runs): parse 43.9k to 55.7k msg/s, parse+extract 14.0k to 14.8k msg/s. These carry no transform.

**Against the threshold: 565 to 617 msg/s/core versus at least 200: PASS by 2.8x to 3.1x, the worst run included.**

**Against the Phase 0 baseline (36,322 msg/s, solo Windows laptop, `docs/spikes/S1-hl7-throughput.md`): 565 to 617 is about 1.6% of it, 59x to 64x lower. This is not a like-for-like regression.** Phase 0's "lazy parse+extract+fhir" built a plain dict and serialised it with `orjson`. The real mapping constructs validated `fhir.resources` pydantic models and serialises them with the model serialiser. A `cProfile` pass over the same corpus (profiler overhead inflates absolute times, so read the shares only): model serialisation (`model_dump_json`) is about 41% of cumulative time, model construction (`safe_build`) about 37%, and HL7 parsing is negligible. So the Phase 0 36k figure was an optimistic upper bound on what a Bundle-producing transform costs; the Phase 0 python-hl7 figure (2,446, same "fhir" step in dict form) is likewise not comparable. The hardware differs too (laptop vs shared 2.1 GHz Xeon vNCPU), and that share is not separable here.

Consequence for planning: a single-core Bundle-producing mapping is about 600 msg/s, roughly 3x the threshold, not 180x. That is still adequate for the plan's target, but the margin is far thinner than the Phase 0 report implied, and a shared host makes it a lower-confidence figure. Pydantic model dump is the obvious first optimisation target if headroom is needed (a finding for the owner of `tributary-fhir`, not acted on here).

## S2: MLLP ACK-after-commit through the real receiver (threshold: ACK p99 under 50 ms at 1k msg/s)

Command: `uv run python -m benchmarks.s2_mllp_ack_latency --count N --conns C --seed S`, 3 runs per cell. The harness cannot set an offered rate; C=10 lands at 0.9k to 1.06k ACKs/s, which is the plan's 1k msg/s point. **"All ACKs" figures include every ACK, warm-up included, and are the numbers compared against the threshold.** "Excluding first" is a diagnostic only, defined as all ACKs except the first ACK on each connection.

| Conns | Count | ACKs/s | p50 ms | p99 ms, all ACKs (3 runs) | max ms |
|---|---|---|---|---|---|
| 1 | 3000 | 133 | 6.9 to 7.0 | 8.9 / 9.6 / 9.6 | 1,053 to 1,175 |
| **10** | 5000 (runs A) | 1,043 to 1,064 | 8.0 to 8.1 | **14.3 / 12.9 / 13.0** | 607 to 662 |
| **10** | 5000 (runs B, later, with diagnostics) | 893 to 926 | 8.2 to 8.7 | **20.3 / 24.8 / 17.6** | 622 to 1,058 |
| 100 | 5000 | 2,791 to 3,136 | 19.8 to 21.8 | 586.5 / 587.4 / 595.7 (runs A); 580.7 / 587.5 / 585.3 (runs B) | 585 to 612 |
| 100 | 20000 | 4,073 to 4,406 | 19.3 to 21.0 | 35.1 / 34.5 / 52.9 | 585 to 592 |

At the 1k msg/s point (C=10): p99 12.9 to 24.8 ms across 6 runs, **PASS against 50 ms**, best-to-worst margin 2.0x to 3.9x, on an RF=1 single broker. Runs A and B differ by up to 2x on the same code and host, which is the shared-host noise. C=100 at 5000 messages **fails on p99 (about 590 ms, all ACKs)**, and at 20000 messages one of three runs (52.9 ms) is over 50 ms; but C=100 runs at 2.5k to 4.4k msg/s, so it is not the 1k msg/s threshold condition.

### The first-ACK outlier: what it is (measured, with one part still open)

Every run has one ~0.6 to 1.05 s outlier per connection's first ACK. Diagnostics added to the harness (`benchmarks/s2_mllp_ack_latency.py`: first ACK per connection, its completion time, connect time, and `--prewarm`), 3 runs per cell, 5000 messages:

| Cell | p99 all ACKs | p99 excluding first-per-conn | first-ACK per conn p50 / max ms | first ACKs completed at (ms since start) | connect p50 / max ms |
|---|---|---|---|---|---|
| C=100 cold, runs 1-3 | 580.7 / 587.5 / 585.3 | 29.6 / 30.1 / 32.1 | 581 to 588 / 586 to 597 | 604-616, 600-616, 615-628 | 6.6 to 7.1 / 17 to 31 |
| C=100 **prewarmed**, runs 1-3 | **41.1 / 35.9 / 33.2** | 35.3 / 29.1 / 32.9 | 36 to 42 / 44 to 52 | 55-75, 60-84, 55-73 | 7 to 13 / 22 to 29 |
| C=10 cold, runs 1-3 | 20.3 / 24.8 / 17.6 | 18.9 / 23.1 / 14.7 | 621 to 1,054 / 622 to 1,058 | one window of 0.3 to 37 ms per run | 1.9 to 5.9 / 2 to 8.5 |

Findings:
1. **Not connection setup.** TCP connect is 2 to 13 ms at the median (max 31 ms), against a ~600 ms first ACK.
2. **Not per-connection.** All 100 first ACKs complete inside a 12 to 16 ms window, about 600 ms after the run starts: one shared event that all connections queue behind, not 100 independent setups.
3. **Not topic auto-create.** The topic is created up front and the broker runs with `auto.create.topics.enable=false`.
4. **Shared producer cold start, confirmed by elimination.** Sending one throwaway message on a separate connection before the clock starts removes the outlier entirely: first-ACK per connection falls from ~585 ms to ~40 ms, and all-ACK p99 falls from ~585 ms to 33 to 41 ms, which passes 50 ms. The receiver's producer is built at startup, but its first `produce` pays for whatever librdkafka defers (metadata fetch for the topic, idempotent producer id initialisation, broker connection ramp).
5. **Still open:** which of those deferred steps dominates. Separating them needs instrumentation inside the receiver (`packages/tributary-receivers`, not a QA-owned path); the timestamped first delivery callback the lead suggested cannot be taken from outside the receiver process. I did not infer a split.

Labelling: the threshold verdict below uses **all ACKs, warm-up included**. The "prewarmed" and "excluding first" columns show what the steady state is and are never substituted for it.

Finding for `connector-engineer`: a receiver that starts accepting connections before its producer has done one produce hands the first client a 0.6 to 1 s ACK, on this host. Warming the producer before opening the listener (or before reporting ready) would remove it. Not a delivery-safety issue (the ACK still follows the commit, invariant 1 holds), a latency one.

### S2 verdict

**ACK p99 at about 1k msg/s: PASS on this host and topology (12.9 to 24.8 ms against 50 ms), low confidence for the RF=3 question.** Steady-state p99 at 100 connections is 29 to 41 ms with the warm-up removed or done beforehand; the 5000-message all-ACK p99 at 100 connections is about 590 ms and is reported as such. Phase 0's "NOT MET, cause unresolved" (33 to 125 ms on a Windows laptop, 3 brokers) is neither confirmed nor refuted, and no bare-producer baseline was run on this host. Not measured: RF=3, TLS, open-loop fixed-rate load, the broker-kill stall (Phase 0: 7.5 to 12.7 s), and the 64 MiB receiver queue default (this harness does not observe receiver memory).

## Reproduce

```
uv run python -m benchmarks.s1_hl7_parse_throughput --corpus fixtures --count 3000 --repeats 5 --procs 1,2,4
uv run python -m benchmarks.s1_hl7_parse_throughput --count 10000 --repeats 5 --procs 1,2,4   # synthetic mix, no transform
uv run python -m benchmarks.s2_mllp_ack_latency --count 5000 --conns 10
uv run python -m benchmarks.s2_mllp_ack_latency --count 5000 --conns 100 [--prewarm]
```

Requires a local Docker daemon for S2. Output goes to `benchmarks/results/` (git-ignored); pass `--out` to redirect.
