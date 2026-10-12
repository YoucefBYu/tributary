# Wave 3: workers at full rate with ClickHouse, PostgreSQL and the API stopped (W3-9a)

Status: measured locally 2026-10-06 and 2026-10-12, three full sets of 5 control plus 5 stopped runs. Exit criterion 2. No CI row yet.

**Rule change after set 1 (lead's ruling).** The plan's rule was two-sided (stopped median inside the control min-max). Set 1 missed it by 0.7 msg/s on the high side. The lead then made it one-sided: stopped median >= control minimum, plus 0 loss, 0 order violations, 0 duplicates, because invariant 3 asks that the data plane not degrade and a faster stopped arm is not a failure. The two-sided miss of set 1 stays in the table below; the two-sided result is still printed in every row (`two-sided: inside/outside`).

## Environment

Linux 6.18.44 (Firecracker VM), 4 vCPU, 15 GiB RAM, x86_64, CPython 3.12.3, `apache/kafka:4.0.0` (1 broker),
`clickhouse/clickhouse-server:24.8`, `postgres:16-alpine` with TLS, real `tributary.archiver`, `tributary.api` and a real
pipeline worker process. All on one host, so the archiver, API, brokers, containers and the load generator compete for
the same 4 vCPUs.

## Method

`uv run pytest -m chaos tests/chaos/test_outage_control_plane.py -k keeps_full_rate` (about 5 min). Per run, fresh
containers and a fresh env: warm-up of 48 keyed messages, shown to reach ClickHouse (archiver wired) and the API
`/readyz` 200. Stopped arm: the API process, PostgreSQL and ClickHouse are stopped, each shown down with
`assert_fault_fired` (port refuses; API process exited); the archiver stays running. Control arm: nothing stopped, all
three shown up. Then 10,000 synthetic keyed messages (16 keys, 4 partitions, `ordering: per-key`) are
published in one burst and throughput is 10,000 / (time to read the last record from `out.<dest>`), from just before
the publish. Runs alternate control, stopped, control, ... Pass rule (one-sided, lead's ruling after set 1): stopped median >= control
minimum. "Inside the control min-max" (the plan's original two-sided wording) is reported as informational. Each run also asserts 0 ACKed loss, 0 per-key order violations, one partition per key, the same worker
pid alive, no `partition_halted`.

## Results (msg/s, one worker, 10,000 messages per run)

| Set | Control runs | Stopped runs | Stopped median | Control min-max (median) | Rule |
|---|---|---|---|---|---|
| 1 | 484, 737, 817, 847, 810 | 742, 890, 848, 895, 834 | 848 | 484 to 847 (810) | **FAIL**, 0.7 msg/s (0.09%) above the control maximum |
| 2 | 701, 928, 863, 828, 882 | 840, 932, 888, 903, 886 | 888 | 701 to 928 (863) | PASS, inside |
| 3 (one-sided rule, rerun 2026-10-12) | 395, 697, 714, 711, 735 | 594, 758, 745, 726, 725 | 726 | 395 to 735 (711) | PASS one-sided (726 >= 395); also inside the range |

Order violations 0, ACKed loss 0, duplicates 0 in all 30 runs; fault fired in all 15 stopped runs. In the control arm the
archiver wrote about 19,000 rows to ClickHouse during the window (18,632 to 20,096).

Set 3 also changed the harness to match the archiver's new startup grants check: ClickHouse is migrated with the admin credential (`--migrate-only`) and the archiver runs as an INSERT/SELECT-only user with `--skip-ddl`; ops listeners use `TB_OPS_PORT=0`. Throughput is about 8% lower in set 3 (control median 711 vs 810-863). **Set 3 is not comparable to sets 1 and 2 in absolute msg/s**; only its within-set comparison counts.

Set 1 is reported as a failure of the rule as written: the stopped median exceeds the control maximum. The deviation
is upward, not a shortfall. In sets 1 to 3 the stopped median is 1.03x to 1.05x the control median: with ClickHouse
stopped the archiver stops doing insert work, so the stopped arm has more CPU for the worker on a 4 vCPU host. That is a
same-host effect (the archiver shares the CPUs), not a sign of any dependency of the worker on the stores. The first
control run of each set was the slowest (484 and 701), which also widens the control range. The "inside the range" rule
is two-sided and so can fail on a faster stopped arm; a one-sided "stopped is not below the control minimum" reading
holds in all three sets (reported as `stopped_not_below_control_min`). The lead adopted it (see the top). Sets 1 to 3 all satisfy the one-sided rule (848 >= 484, 888 >= 701, 726 >= 395).

## Mutation control

Same harness, with a worker step that waits on ClickHouse per message (the archive in the message path, a violation of
invariant 9), 2 control and 2 stopped runs of 300 messages: control 237 and 258 msg/s, stopped 9 and 9 msg/s, verdict
FAIL under the one-sided rule (set 3 rerun: control 236 and 237, stopped 9 and 9; still red).

## What it cannot prove

- Dedicated-hardware throughput: the absolute 450 to 930 msg/s is a single worker on shared vCPUs and is not a sizing number.
- Run-to-run spread is about 10% inside an arm, larger than any effect of the outage; so the test detects coupling of
  the order seen in the mutant (about 27x), not a few percent.
- Only a stopped (connection refused) fault: no hung or slow store (a blackholed port); no restart-and-catch-up of the
  archiver after ClickHouse returns; the API had no request load.
- Not marked `integration`: ten runs with fresh containers take about 5 minutes, so it belongs in the on-demand and
  nightly chaos job. `integration_expected.json` is unchanged.
