# Wave 3: the sync-reply tail (W3-24, STATUS rank 9)

Author: qa-engineer, 2026-10-12. **Result: reproduced under contention; cause not isolated.**
`platform-engineer` takes the cause (see "Hand-off"). Raw per-run summaries and every request over 200 ms:
[`data/wave-3-sync-reply-tail-runs.json`](data/wave-3-sync-reply-tail-runs.json).

## What was measured and how

`tests/integration/test_sync_reply_tail.py` (marked `bench`, not `integration`: about 85 s, no gate; `-m "integration or bench"` runs it at the end of the full
integration session, where the original outlier was seen). A real MLLP receiver process and a real pipeline worker on a single-broker
Kafka (`apache/kafka:4.0.0`, own container per run), the real `raw`, `reply` and `replier` group. 10 warm-up exchanges,
then 5 senders x 650 requests = **3,250 per run** (at least 3,000), 80 ms think time, about 50 msg/s; latency is the
sender-observed request-to-AA time (`perf_counter` bracket). For every request over 200 ms the test keeps the
receiver and worker log lines and the broker container log lines (`docker logs --timestamps`) from 3 s before to 1 s after,
and host CPU (busy, iowait, steal, load average) sampled every 250 ms for the whole run. It asserts only that all answers
are AA and n >= 3,000. Reproduce one run, then analyse:

```
TB_TAIL_REPORT_DIR=/tmp/tail uv run pytest -m bench tests/integration/test_sync_reply_tail.py -q -s
uv run python benchmarks/analyze_sync_reply_tail.py /tmp/tail
```

Environment: Linux 6.18 (Firecracker VM), 4 vCPU shared with other agents' work, Docker 29.3.1, single broker RF=1, CPython
3.12.3, loopback. Threshold: none (the plan asks for the distribution, or "not reproduced in N").

## Results (12 runs, 39,000 requests, all answered AA)

| Arm | Runs | p50 ms | p99 ms | max ms | Runs with a request > 200 ms | Requests > 200 ms |
|---|---|---|---|---|---|---|
| Isolated, host shared with other work (host busy median 26 to 72%) | 8 | 16.2 to 18.5 | 25.8 to 78.2 | 56 to 317.5 | **2 of 8** | 8 + 3 |
| Full `pytest -m integration` session, host otherwise quiet (busy median 17%) | 1 | 16.0 | 28.3 | 97.6 | 0 of 1 | 0 |
| Isolated + 4 busy-loop processes (all 4 vCPUs at 100%) | 3 | 16.1 to 17.1 | 56.2 to 89.4 | 145.8 to 278.3 | **1 of 3** | 6 |

Per run (p99 / max / count > 100 ms / count > 200 ms), isolated: 25.8/56.2/0/0; 55.5/125.2/7/0; 78.2/317.5/24/8;
74.5/116.9/12/0; 59.7/108.5/4/0; 63.1/247.3/9/3; 41.7/110.7/1/0; 46.8/85.3/0/0. Loaded: 56.2/176.0/4/0; 79.4/145.8/19/0;
89.4/278.3/19/6. Full session: 28.3/97.6/0/0. All 17 requests over 200 ms are listed in the data file.

## What the evidence says

- **The tail reproduces**: 17 requests over 200 ms (worst 317.5 ms) in 3 of 12 runs, and requests over 100 ms in 9 of 12.
  p50 is stable at 16 to 18.5 ms in every run, so the common path does not change; only the tail does.
- **Only the busy-loop arm is a control, and it is small.** The three runs with four busy-loop processes (all vCPUs at
  100%) are the one manipulation I made: requests over 100 ms in 3 of 3, over 200 ms in 1 of 3 (6 requests, worst 278 ms).
  That shows CPU saturation can produce the tail; with n = 3 it does not show it is the only way.
- **The rest is observational.** The "isolated" runs shared the VM with other agents' unrecorded work, so their host-busy
  figures (26 to 72% median) are a correlation, not a manipulation: the runs with requests over 200 ms sat at 64.5% and
  72.1%, the two runs with nothing over 100 ms at 25.5% and 51.0%, and the quiet full-session run (17%) had none over
  100 ms. Twelve runs cannot separate CPU contention from anything else that was also busy at the time (disk, memory
  bandwidth, Docker network).
- **Disk wait is neither necessary nor sufficient.** iowait peaks of 73 to 89% occurred in the first three isolated runs,
  including run 1 with no request over 100 ms; in the two stalls of run 3 it was 61% and 81%, but in the later stalls it was
  0 to 6%. So iowait does not explain the tail and I do not rule it out as one contributor; the later runs had a median of
  0% iowait throughout.
- **Stalls hit all senders at once.** Requests over 100 ms come in clusters inside one or two seconds (for example 8 requests
  over 200 ms within 14 ms of each other at one instant, from 3 and then 5 of the 5 senders in the two stalls of that run). Concurrent stalls of every sender point at a
  shared component (receiver, worker, broker or the reply consumer) being descheduled or delayed, not one slow connection.
- **The logs show nothing.** For all 17 requests over 200 ms, the receiver log, the worker log and the broker log held
  **zero** lines in the window (the receiver and worker log 7 lines each at start-up and none per request; the broker logs
  nothing at those moments). No rebalance, no leader change, no error. So no component announces the stall, and the
  cause cannot be read from these logs; it needs per-stage timestamps.

## What this does not establish

- Which stage stalls: sender-observed time only. The receiver reports a raw-commit latency histogram (wave 2: p99 about
  10 to 12 ms on a quiet host), but not the time the reply spends in the worker or the replier.
- Whether production sizing is affected: the VM is shared and 4 vCPUs also carry the broker, receiver, worker and 5 senders.
- One full-session run only (30 min each), on a quiet host; it did not reproduce there. The original outlier ("p99 about 270 ms
  in one run of four") fits the contended runs here, but it cannot be proven to be the same event.
- Rate and concurrency were fixed at about 50 msg/s and 5 senders.

## Hand-off to platform-engineer

Add per-stage timestamps (receiver accepted, raw committed, worker routed, reply produced, replier consumed, ACK written) behind
debug or a metric, then rerun this test under the busy-loop probe (`python3 -c "while True: pass"` x4); the stage whose time
inflates in the 17 slow requests is the cause. Questions to answer: do the receiver's reply consumer poll interval and the
worker's `poll_timeout_s` (0.1 s in the test harness) add scheduling-sensitive waits; is the replier group's fetch wait
(`fetch.wait.max.ms`) in the reply path; does the reply path pay a producer or consumer wake-up that a descheduled thread
delays by a whole timeslice.
