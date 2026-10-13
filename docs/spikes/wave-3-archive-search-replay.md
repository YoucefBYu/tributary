# Wave 3: archive search, reprocess and replay end to end, and a benchmark (W3-9b)

Measured locally 2026-10-13. Evidence for the message-browser path (exit criterion 1 inputs). Plan threshold: none for search latency or
replay throughput, so no pass or fail is claimed for the numbers; the end-to-end test is pass/fail.

## End to end (`tests/e2e/test_archive_search_reprocess_replay_e2e.py`, 4 tests, marked `integration`)

`uv run pytest -m integration tests/e2e/test_archive_search_reprocess_replay_e2e.py` -> `4 passed in 56.69 s` and `4 passed in 54.95 s`
(two consecutive runs after the review fixes). Real processes: single-broker Kafka, ClickHouse 24.8, TLS PostgreSQL, the events archiver and the content
archiver (INSERT/SELECT-only user under the pinned settings profile), the API (SELECT-only ClickHouse user, signed bearer token),
an MLLP receiver and a pipeline worker started from the spec the API deployed. 9 synthetic ADT messages (3 keys x 3, MSH-3
`TRIBUTARY-SYNTH`) over MLLP; content storage on for the channel; `ordering: per-key`, 3 partitions.

- Search finds all 9 `tb-msg-id`s (the ids on `raw`) and the detail shows the `transformed` and `routed` stages; an unknown id is 404.
- Content view returns bytes whose SHA-256 equals the sent payload's; the `message.content.view` audit record (phase `requested`)
  is on `_tb.<env>.audit` with the actor, roles, channel, stage and message id, and carries no payload bytes.
- Reprocess: refused 409 `ORDERING_WOULD_BREAK` without `allow_reorder`; with it, 200 with a new id; `raw` holds that id with
  `tb-parent-id` = the original on the original's key; the same `operation_id` returns the same new id (and so the retried reprocess is on `raw` twice under that one id: at-least-once, absorbed by loader idempotency on `tb-msg-id`); the SSE stream shows the new
  message with `parent_id`.
- Replay over the window of the 9 originals: refused without `allow_reorder` (409, nothing published); `max_records` 2 ends `failed`
  `RANGE_TOO_LARGE` with nothing published; the real replay is `completed`, `republished` 9 (the reprocessed record is outside the
  window and is not replayed), 0 undecodable; a repeated POST of the same body is 200 and publishes nothing (record counts on `raw` are read to end-of-file after a pause and compared exactly); the stream shows 9 new
  ids with `parent_id` in the originals; on `raw` each key's replayed copies are in the originals' order (0, 1, 2) and each key is on one
  partition.

### Findings for other owners

- **No replay cancel.** `openapi/v0` has no cancel operation (a replay is `running`, `completed` or `failed`), so "can be cancelled" is not
  tested. The bounds that exist are tested. If cancel is wanted it needs a contract change (architect).
- **No `received` event.** The receiver emits no `received` stage event today; the archive holds `transformed` and `routed`, and `received`
  exists only as stored content (from `raw`). A search for `stages: ["received"]` returns nothing for a healthy message.
- **`audit_log` has no writer in the tree.** The audit contract names the events archiver as consumer of `_tb.<env>.audit` (W3-30 part 2), but
  no process writes `audit_log` yet, so the test asserts the audit record on the topic. When the consumer lands, add the ClickHouse
  assertion.

## Benchmark (`uv run python -m benchmarks.w3_archive_search_replay`, raw results in `docs/spikes/data/wave-3-archive-search-replay.json`; the script also writes the git-ignored `benchmarks/results/`)

Environment: Linux 6.18 (Firecracker VM), 4 vCPU shared with the load generator and other agents' work, CPython 3.12.3, ClickHouse 24.8,
Kafka 4.0.0 (1 broker, RF=1), PostgreSQL 16. No worker, receiver or archiver runs during the measurement. 100,000 `message_events` rows
inserted directly (half `transformed`, half `routed`, 1% `status=error`), 200 timed requests per case after 10 discarded, client-observed
latency through the real API over HTTP, sequential. Two full runs, both shown (run 2's raw rows are in the data file; run 1 predates the
baseline case, so it has none). At n = 200 the p99 is the second-largest sample, so read it as approximately the maximum, not a stable tail estimate.

| Case | Run 1 p50 / p95 / p99 ms | Run 2 p50 / p95 / p99 ms |
|---|---|---|
| `page1` (1 stage, 50 rows, whole 100k window) | 102.0 / 131.5 / 156.1 | 101.7 / 133.4 / 152.5 |
| `page1_narrow` (5-minute window) | 72.0 / 93.1 / 106.6 | 63.3 / 86.1 / 98.5 |
| `error_filter` (status + error code) | 107.5 / 129.4 / 146.0 | 104.7 / 132.9 / 143.5 |
| `paginate` (20 cursor pages; n = 20, so p95 and p99 are the two largest pages) | 110.3 / 133.4 / 156.3 | 120.9 / 136.8 / 138.1 |
| baseline: authenticated `GET /v1/environments` (PostgreSQL only) | not measured | 9.3 / 12.5 / 16.5 |

Reading: the page-1 search at 100k rows is about 100 ms at p50 and about 132 ms at p95 in both runs. An authenticated request that does not
touch the archive costs about 9 ms, so most of a search's time is outside the API's own fixed cost. **Hypothesis, not measured:** that remainder is
the ClickHouse query plus the query library's deduplication, with the narrow window saving about 30 to 40 ms because it scans less. There is no
server-side timing (no ClickHouse `query_log` read, no API span), so the split between ClickHouse, the library and the HTTP client is not
established. The paginate case over 20 pages is no evidence about depth: 20 pages is not deep, and its p50 differs by 11 ms between runs.

Replay throughput (records produced to `raw` first, then one replay over exactly their window, `allow_reorder` true, polled every 50 ms):

| Records | Run 1 records/s | Run 2 records/s | Difference |
|---|---|---|---|
| 5,000 | 4,061 (1.23 s) | 3,291 (1.52 s) | run 2 is 19% lower |
| 20,000 | 5,216 (3.83 s) | 4,559 (4.39 s) | run 2 is 13% lower |

All records republished, 0 skipped in both runs. Two runs are too few to give a spread; they differ by 13% to 19%, and the larger run is faster in
both. **Hypothesis, not measured:** a fixed per-replay cost (consumer assignment, `offsetsForTimes`, producer start-up) is a larger share of
the 5,000-record case; the data cannot separate it from run-to-run noise on a shared host.

### What this cannot show

- Search at 100k rows is one table on one node with a warm cache; no concurrent writers (the archiver was idle) and no concurrent searches.
- Replay throughput is the library's single producer on loopback with RF=1; real durability (RF=3, `min.insync.replicas` 2) will be lower.
- The numbers are from a shared 4 vCPU VM and are not sizing numbers.
