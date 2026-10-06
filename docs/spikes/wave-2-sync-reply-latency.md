# Wave 2 sync-reply latency distribution (STATUS backlog item 9, wave 2 part)

Author: qa-engineer, 2026-10-05, tree at `ec4b9d1`. **Filed as measured. No root cause is claimed**; platform-engineer owns the cause in wave 3.

## What was measured and how

`packages/tributary-receivers/tests/integration/test_mllp_sync_reply.py::test_sync_reply_aa_round_trip_latency_is_reported`: a real MLLP receiver process and a real pipeline worker on a single-broker Kafka (testcontainers, `apache/kafka:4.0.0`); 10 warm-up exchanges, then 5 senders x 60 messages with 80 ms think time (about 50 msg/s in total), n = 300 per run. The sender-observed time is the full request to AA round trip through `raw`, worker, `reply` and the replier. The test only prints p50, p99, max and the count over 200 ms, so a QA-owned pytest plugin (`tests/support/latency_tail_plugin.py`) records every `Sender.exchange` time with the same `perf_counter` bracket and the report computes the rest. Percentile index = `int(n * q)` on the sorted samples, as in the test. Reproduce one run:

```
TB_LATENCY_SAMPLES_FILE=/tmp/samples.json PYTHONPATH=. uv run pytest \
  packages/tributary-receivers/tests/integration/test_mllp_sync_reply.py -k latency_is_reported \
  -m integration -p tests.support.latency_tail_plugin -q -s
```

## Results

Threshold: **none**. The test is "not a gate" and STATUS asks only for the distribution. All runs exit 0, `1 passed`.

| Run | n | p50 ms | p90 ms | p99 ms | max ms | > 100 ms | > 200 ms |
|---|---|---|---|---|---|---|---|
| 1 | 300 | 18.4 | 21.4 | 27.0 | 40.8 | 0.0% | 0.0% |
| 2 | 300 | 18.1 | 21.5 | 28.1 | 42.5 | 0.0% | 0.0% |
| 3 | 300 | 17.4 | 20.7 | 26.5 | 40.2 | 0.0% | 0.0% |
| 4 | 300 | 18.4 | 22.5 | 29.4 | 36.0 | 0.0% | 0.0% |
| 5 | 300 | 18.3 | 21.2 | 25.0 | 36.0 | 0.0% | 0.0% |

Over the five runs: p50 17.4 to 18.4 ms, p99 25.0 to 29.4 ms, worst single request 42.5 ms. **The tail reported in STATUS (p99 about 270 ms in one run of four) did not reproduce in 5 of 5 runs (1,500 requests, none above 100 ms).** The receiver's own raw-commit p50 was 7.1 to 7.4 ms and p99 10.2 to 12.0 ms in these runs.

Probe, not part of the five (labelled as such): two more runs with four busy-loop processes saturating the 4 vCPUs.

| Probe | n | p50 | p90 | p99 | max | > 100 ms | > 200 ms |
|---|---|---|---|---|---|---|---|
| 6 (CPU saturated) | 300 | 23.7 | 35.8 | 55.2 | 62.3 | 0.0% | 0.0% |
| 7 (CPU saturated) | 300 | 19.3 | 29.6 | 42.6 | 46.7 | 0.0% | 0.0% |

## What this does and does not say

- Run in isolation on this host, the round trip is stable and about 18 ms at the median with p99 under 30 ms. CPU contention moves p99 to 43 to 55 ms but does not produce a 270 ms tail here.
- So the earlier outlier is **not** reproduced by either quiet or CPU-saturated conditions. Candidates still open (hypotheses, none measured): a Kafka broker or group-coordinator event at that moment (the earlier runs were inside a full integration session with other containers coming and going), disk or Docker network contention on that host, a consumer rebalance of the `replier` group, or producer cold start (the same cause found in the S2 re-run). The way to find out is wave 3's cause work: capture the receiver and broker logs for a slow run, which this test does not.
- Not measured: tail under the full integration suite (the context in which the outlier was seen), more than 300 requests per run (p99 is 3 samples; a rare 1-in-1,000 event would usually not appear), and higher concurrency.

## Environment caveats

One shared 4 vCPU Linux container (Intel Xeon @ 2.80 GHz, 16 GB), Docker 29.3.1, single broker RF=1, Python 3.12, loopback, closed-loop low-rate senders. The Kafka container starts fresh for each run. Latency figures do not transfer to other hardware or to the RF=3 topology. The plugin adds one function call per exchange; its p99 differed from the test's own printed p99 by 0.1 ms in the one comparison made (run 6: 55.2 vs 55.3).
