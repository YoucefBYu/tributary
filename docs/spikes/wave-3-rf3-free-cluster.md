# Wave 3: RF=3 on a free 3-broker cluster (W3-33b)

Status: measured locally 2026-10-06, 3 full runs. The CI job (W3-33a) has not run these yet, so there is no
`ubuntu-latest` row. Everything below is single-host evidence.

## Environment

Linux 6.18.44 (Firecracker VM), 4 vCPU, 15 GiB RAM, x86_64, CPython 3.12.3, `apache/kafka:4.0.0`,
`confluent-kafka` via `uv`, Docker via testcontainers. Brokers: heap `-Xms256m -Xmx512m`, no `mem_limit`.
Peak summed container memory of the `tb-rf3-*` containers, sampled every 15 s with `docker stats` during the
the earlier set of 3 runs: about 2.0 GiB (sampling can miss short peaks).

## Method

- Fixture `tests/support/kafka_cluster.py`: 3 KRaft brokers on a private network, unique names
  `tb-rf3-<run>-k<n>`, teardown stops only its own containers. Topologies: `split` (3 brokers + 1 controller,
  so two brokers can stop while the quorum survives) and `combined` (3 nodes, each broker and controller).
  It does not depend on the devops Compose file.
- Replication factor, `min.insync.replicas`, unclean election, auto-create and the `raw` topic configs and
  partitions are read from `contracts/topics/v0/topic-defaults.json`. Local values are only heap and a 6 s
  `replica.lag.time.max.ms` (so ISR shrink is visible quickly).
- Real MLLP receiver (`python -m tributary.receivers.mllp`), channel `ordering: per-key` on `hl7:PID.3.1`,
  8 synthetic patients, MSH-3 `TRIBUTARY-SYNTH`. Loss = ACKed control ids absent from `raw` after recovery.
  Order = first deliveries per key increasing, each key on one partition. Duplicates counted.
- Command, per run: `uv run pytest -m "chaos and rf3" tests/chaos` (5 tests). Rows go to `TB_CHAOS_ROWS_FILE`.

## Results (3 runs, all 5 tests green each run)

Final code, 3 full runs. Pytest summary lines: run 1 `5 passed, 18 deselected in 258.53s`; run 2
`5 passed, 18 deselected in 265.37s`; run 3 `5 passed, 18 deselected in 275.07s`. (An earlier set of 3 runs, before
review fixes, was also green; B1 numbers from it are discarded because its latency was not measured from the
scheduled send time.)

| Scenario | Fault fired | ACKed lost | Order violations | Duplicates | Notes |
|---|---|---|---|---|---|
| C1 two of three brokers down (`split`) | 3/3: both exit 137, ISR=[1] on `raw` and on the probe topic, receiver-profile producer got `_MSG_TIMED_OUT` | 0 | n/a | 0 | 12 of 12 sends during the outage were not ACKed within 30 s: 12 receiver NACKs (MSA AR/AE), 0 client timeouts or resets, in all 3 runs. The receiver ACKed again after recovery, and those ACKs are in the loss check. The 12 un-ACKed sends all landed on `raw` later (allowed by at-least-once) |
| C1 control, `acks=1` receiver | 3/3 (same kill) | n/a | n/a | n/a | ACKed during the outage: 9, 12, 10 of 12; the headline assertion raised, so the control is red as intended |
| C2 rolling restart of all 3 (`split`) | 3/3: each broker stopped (exit 143), out of every ISR, 2.6k to 3.1k ACKs while one was down (run 1) | 0 | 0 | 0 | 18.0k, 18.5k, 18.6k messages |
| C3 leader SIGKILL (`combined`) | 3/3: exit 137, 1 key hit on both sides of the kill | 0 | 0 | 0 | distinct `tb-msg-id` equals record count each run (4700, 3985, 4422) |

Each topic and broker is checked with `describe_configs` before a run (min ISR 2, RF 3, unclean election off).
Duplicates observed: 0 in every run; a measured zero on this load, not a guarantee.

### B1 ACK latency ratio, RF=3 over RF=1 (same cluster, same host, interleaved)

Offered rate over 8 connections, 8 s phases, one discarded warm-up pair, 3 measured pairs per run. Latency runs from
each message's scheduled send time (no coordinated omission). Ratios per pair, RF=3 over RF=1.

At 1000 msg/s (3 runs): RF=1 held the offered rate in every phase, RF=3 did not (achieved 872 to 999 msg/s), so
queueing, not replication, dominates the ratios (p50 1.6 to 135.7, p99 4.4 to 59.8). No ratio is defensible there.

Rerun at 750 msg/s (3 runs, `TB_RF3_B1_RATE=750`; every phase of both arms achieved the offered rate, one phase
748; 3 passed, 80.19 s, 80.56 s, 81.29 s):

| Run | p50 ratio per pair | p99 ratio per pair |
|---|---|---|
| 1 | 2.66, 1.24, 1.24 | 4.76, 1.89, 4.19 |
| 2 | 1.29, 1.09, 1.51 | 6.44, 0.54, 6.73 |
| 3 | 1.32, 1.22, 1.24 | 1.89, 2.62, 3.18 |

Reading: with both arms holding the rate, the p50 ratio is stable at about 1.1 to 1.5 (one warm-up-adjacent 2.66).
The p99 ratio is not stable (0.54 to 6.73, run medians 4.19, 6.44, 2.62): 8 s phases give about 6000 samples per
phase, so the p99 rests on roughly 60 tail messages and is dominated by host scheduling noise on 4 shared vCPUs.
A single-sweep check at 250 and 500 msg/s (one run each, not part of the 3 above) gave p50 ratios 1.13 to 1.59 and
p99 ratios 1.06 to 3.79. Conclusion: RF=3 costs about 1.2x to 1.3x at p50 at 750 msg/s; the p99 cost is between
about 1x and 7x and cannot be narrowed on this host without longer phases. No plan threshold exists for the ratio,
so no pass or fail is claimed. Peak summed `tb-rf3-*` container memory during the 750 msg/s runs: about 1.2 GiB
(`docker stats`, 5 s sampling).

## What this proves

- With ISR below `min.insync.replicas=2` the real receiver does not ACK within 30 s (12 of 12 NACKed, 0 ACKed;
  invariant 1 at RF=3), and the control shows the test detects `acks=1`.
- No ACKed loss and no per-key order violation across a rolling restart and a leader SIGKILL, with the
  contract's unclean-election setting; the idempotent producer left no duplicate `tb-msg-id` on `raw`.
- A p50 ACK-latency ratio of about 1.24 (RF=3 over RF=1) at 750 msg/s on this host; the p99 ratio is only "a few times, noisy" (B1).

## What it cannot prove

- Absolute ACK p99 or msg/s for sizing: 4 shared vCPUs, brokers and load generator on one host.
- Real replication cost: one kernel, one disk, no inter-broker network RTT; fsync contention unrealistic.
- A ratio at 1000 msg/s: RF=3 does not sustain it on 4 shared vCPUs here.
- Cross-zone latency, partition capacity at scale, long soaks, real network partitions.
- C1 observed produce refusal through a probe producer using the receiver's contract profile, not the
  receiver's own handle (it does not log produce errors); the receiver-side evidence is 0 ACKs, 12 AR/AE NACKs.
- Only 3 runs with a handful of key/partition combinations; C3 hit as few as 1 key per run.

## Open items

- CI runner row (runner type, vCPU, RAM, peak memory) waits for W3-33a's `chaos-rf3` job.
- The RF=3 tests are deliberately not marked `integration` (lead's decision): they need 2 to 5 GiB and about 5 minutes
  per run and belong only in the weekly `chaos-rf3` job, so `integration_expected.json` does not change.
