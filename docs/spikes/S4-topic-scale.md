# S4: topic-count scale (channels x topics x partitions)

Status: MEASURED by qa-engineer 2026-09-20, awaiting architect acceptance. Feeds ADR-002 D4 (partition default) and D5 (topic-sharing fallback).

## Question

Threshold (phase-0.md S4): 500 channels operate normally, rebalance under 30 s. Where does the cluster degrade first, and what per-channel partition default and topic-sharing trigger follow?

## Method

- Script `spikes/S4-topic-scale/s4.py`. Each channel is 4 topics (`s4.cNNNN.{raw,out,retry,dlq}`), RF=3, min.insync=2, N partitions per topic (`--partitions`). Channels are created cumulatively in batches of 25; at each tier the script records create time, `list_topics` latency (median and max of 5), per-broker container memory, JVM RSS and open fds, and a produce-ACK probe (300 messages, about 150 to 200 msg/s, on a 3-partition topic).
- `groups`: 100 consumer groups (cooperative, static ids), one per channel, each subscribed to its `raw` topic; time to full assignment. `regex-group`: one group subscribed by pattern to all `raw` topics; time to stable after each member join, and after a static member is closed.
- Cluster: 3 Apache Kafka 4.0.0 KRaft brokers (combined broker+controller) in Docker Desktop. **The Docker VM limit is 7.436 GiB on a 15.7 GB Windows 11 host (Ryzen 7 5800H, 8c/16t). Every number below is a lower bound for a real broker host, not a cluster limit.** Nothing else was running during the S4 runs. I did not touch VM settings.
- Every record is tagged with heap (1G = `-Xmx1G -Xms1G`, 2G = `-Xmx2G -Xms2G`; 3G was not run: 3 brokers x 3 GB exceeds the VM). Raw JSON lines in `docs/spikes/data/S4/`.
- Reproduce (fresh cluster; TB_KAFKA_HEAP unset for 1G): `uv run --project spikes/common python spikes/S4-topic-scale/s4.py --heap 1G --partitions 3 --keep topics --tiers 100,250,500,750,1000`, then `groups --channels 1000 --consumers 100`, `regex-group --channels 1000`, then `cleanup`.

## Results

### 1G heap, 12 partitions per topic (48 per channel), run 1 (`s4-1g-12p*`)

Baseline had 986 leftover partitions from earlier spikes (added to the counts below).

| Channels | Total partitions (x3 replicas) | Cumulative create | list_topics median | ACK p50 / p99 | Broker container mem | Result |
|---|---|---|---|---|---|---|
| 100 | 5,786 | 31 s | 0.09 s | 95 / 558 ms | 1.3 to 1.6 GiB | fine |
| 250 | 11,789 | 48 s (batches about 9.7 s / 25 ch) | 0.21 s | 195 / 621 ms | 1.5 to 1.7 GiB | fine |
| 450 (batch 425-450) | about 21,000 | n/a | metadata request timed out (a MetadataRequest in flight timed out after 60.5 s; the later cleanup `list_topics` timed out at 120 s) | n/a | n/a | **cluster died** |

Evidence limits: the tracked log (`s4-1g-12p-topics.log`) shows only the failed create batch and those timeouts. The cause (JVM heap exhaustion) was read from the broker logs at the time. Those container logs are lost (containers destroyed afterwards), so it is **not independently reproducible from tracked data**; `data/S4/s4-1g-12p-broker-log-excerpt.txt` is a hand transcription of the lines I saw, marked as such. The partition counts are computed (425 channels x 48 partitions plus 986 leftover baseline partitions, about 21,000) from a single run, not read from a metadata call at the failure point.

From the transcribed logs: at 12:37 kafka3 lost raft quorum contact (election timeouts, ISR shrinks), OOM at 12:39:25 (`java.lang.OutOfMemoryError: Java heap space` in the raft IO thread, fatal fault); kafka2 OOM at 12:42:48 (metadata image publisher); kafka1 exited at 12:43:15 with an orderly-looking shutdown, and I did not see an OOM line for it (inferred: it lost quorum). `docker inspect` showed OOMKilled=false for all three, consistent with a JVM-heap failure, not the kernel OOM killer. Roughly 48 fds per channel per broker; 22k fds at 250 channels against a 1M limit, so fds are not the constraint.

**500 channels at 12 partitions on a 1G heap: FAIL (0.9x, cluster lost at about 425 to 450).** Ceiling on this signal (one run, computed count): about 20k partitions (60k replicas) across 3 brokers with 1 GB heap each, about 20k replicas per broker.

### 1G heap, 3 partitions per topic (12 per channel), (`s4-1g-3p*`)

Clean cluster (9 baseline partitions).

| Channels | Total partitions | Cumulative create | list_topics median | ACK p50 / p99 | Broker container mem |
|---|---|---|---|---|---|
| 100 | 1,206 | 13 s | 0.09 s | 138 / 1,466 ms | 0.5 to 0.6 GiB |
| 250 | 3,009 | 13 s | 0.09 s | 106 / 550 ms | 0.7 GiB |
| 500 | 6,009 | 36 s | 0.09 s | 308 / 1,091 ms | 1.15 GiB |
| 750 | 9,009 | 28 s | 0.11 s | 188 / 579 ms | 1.3 to 1.4 GiB |
| 1,000 | 12,009 | 31 s | 0.17 s | 200 / 676 ms | 1.4 GiB |

1,000 channels (4,000 topics, 12k partitions) is healthy on 1G. Threshold at 500 channels: **PASS**, and it holds to 1,000.

Rebalance at 3 partitions, 1G:

| Test | Result | Threshold |
|---|---|---|
| 100 groups (one per channel), 1,000 channels existing: time until all hold a full assignment | 4.3 s (p50 2.5 s, p99 4.1 s) | under 30 s, PASS |
| Pattern-subscribed group over 1,000 `raw` topics (3,000 partitions): first member join | 0.9 s | PASS |
| Add 2nd / 3rd member (cooperative) | 5.5 s / 5.3 s (run 2: 0.3 s / 7.5 s) | PASS |
| Remove a static member | 43.8 s (run 2) | **FAIL, but bounded by the 45 s default session.timeout.ms of static membership, not by topic count.** Not verified that it is independent of topic count. |

Caveats: the first regex-group run's remove step hit a bug in my script (pump thread died on a concurrently closed consumer, fixed in `s4.py`), and its rerun's first join timed out (stale static ids from the killed earlier process, plus a segfault on exit inside confluent-kafka teardown). I used run 1 for join and add, run 2 for remove; both are in `s4-1g-3p-groups-regex.jsonl` and `s4-1g-3p-regex-run2.log`.

### 2G heap, 12 partitions per topic (two runs, inconsistent)

Run A (`s4-2g-12p*`, fresh cluster): 100 ch 4.8k partitions create 36 s, ACK p99 1.1 s; 250 ch 12.0k partitions create 45 s, list 0.11 s, **ACK p99 11.5 s** (new producers timed out on ApiVersion after 10 s); 500 ch 24.0k partitions create 120 s, list_topics 0.58 s, **ACK p50 17.3 s**. The tier-750 create then completed, but every metadata call timed out, the brokers stayed at 100 to 260% CPU for over 10 minutes, and raft leadership flapped (epoch 56 to 57, repeated election timeouts). No OOM at 2G (containers about 2.1 GiB). Cluster at about 35k partitions: list_topics 1.5 s median, 3.3 s max, produce ACK probe 0 of 300 acked.

Run B (`s4-2g-12p-runB*`, fresh cluster, same heap): 250 ch (9.6k partitions) took 174 s to create and ACK p50 was 6.3 s; the next batch failed (`Broker transport failure`) at channels 250-275 (13.2k partitions). The regex-group measurement in run B is invalid (only about 275 channels existed but 500 were expected); `groups` with 100 groups completed in 14.4 s.

Runs A and B disagree by more than 3x at similar partition counts (run A created 12k partitions in 45 s; run B 9.6k in 174 s), and both differ from the 1G run at 12k partitions (48 s, ACK p99 0.6 s). The cause is unresolved. One unverified hypothesis: 3 x 2 GB heap leaves the 7.4 GiB VM with little page cache. A further unexplained item: in run B the tier-250 `list_topics` reported 9,612 partitions although 250 channels (12,000 partitions) had been created without error, so the metadata view was incomplete or the create was partly not applied; I did not investigate. What the data supports: at 2G and 12 partitions per topic, **500 channels did not operate normally in either run** (run A created 500 channels but with 17 s ACK latency, run B failed at 250-275), and 2G behaviour was unstable and not reproducible across two runs. No JVM OOM was seen at 2G; the symptoms were slow creates, high ACK latency, raft leader churn and metadata timeouts. Two runs are not enough to say whether 2G is better or worse than 1G at the same partition count.

### Not measured

- `shared` (topic-sharing fallback throughput and skew): not run, cluster time went to the failure characterisation. The command exists in `s4.py`.
- 3G heap, heap sweeps on a real Linux host with more memory, and any Linux CI number.
- 12 partitions per topic with 500 channels on a host that gives the VM enough memory.

## Ceiling, by which signal degrades first

Measured under 3 brokers, RF=3, 7.4 GiB Docker VM:

1. **Broker JVM heap (1G)** is the first hard limit: OOM and loss of the raft quorum at about 20k to 21k total partitions (about 20k replicas per broker). 12k partitions (about 12k replicas per broker) was healthy.
2. **Create latency** grows with partitions per batch (about 9.7 s per 25 channels at 48 partitions per channel, versus 2 to 4 s at 12 per channel) and is the first soft signal.
3. **list_topics latency** is the mildest: 0.09 s (1.2k to 6k partitions), 0.17 to 0.21 s (12k), 0.58 s (24k), 1.5 s median (35k). It does not degrade before the others, but it matters because every client pays it at startup and `ensure_topics` uses a fixed 10 s timeout.
4. **Produce-ACK latency** degrades once the controller is saturated, independent of data volume (p99 11 to 17 s in the 2G runs at 12k to 24k partitions).
5. **Rebalance time** did not degrade in any tested state (4 to 8 s; static-member removal is dominated by session timeout).

In short: on this VM the ceiling is about 12k partitions healthy and about 20k fatal at 1G (single run, computed count), and 2G was unstable in two runs that disagree, so no heap-scaling claim is made. That is 250 channels at 48 partitions per channel, and at least 1,000 channels at 12 partitions per channel.

## Recommendations for ADR-002

- **D4 partition default: 3 partitions per topic** for channel topics (`raw`, `out`, `retry`, `dlq`), overridable per channel in the spec where throughput needs more. 3 gives 12 partitions per channel and meets the 500-channel threshold with margin up to 1,000 channels. Consider 1 partition for `retry` and `dlq` (not tested).
- **D5 sharing trigger, stated in partitions, not channels:** define the fallback trigger on projected total partitions per cluster, `channels x 4 x partitions`. On 1 GB broker heap and 3 brokers: warn at 8,000 and switch new channels to shared topics at 12,000 (the highest tested healthy value). Above about 20,000 the cluster is lost. Do not scale these with heap without re-measuring on Linux; the 2G runs were inconsistent.
- Topic sharing must not cross a tenant boundary (per contracts): the trigger applies per tenant partition group, so a large multi-tenant deployment reaches the ceiling before the fallback can help, and needs more brokers rather than more sharing.
- Ask platform-engineer: `ensure_topics` used a fixed 10 s `list_topics` timeout in its own calls (now configurable, platform-engineer commit 8ff13ef), and the compose stack should set a 1G or larger heap knowingly (default 1G is the tested case).
- Static membership: set `session.timeout.ms` explicitly (default 45 s makes failure detection slower than 30 s).

## Environment gotchas

- The first `tbcommon.smoke` immediately after a cluster start fails with `UNKNOWN_TOPIC_OR_PART` (coordinator load); it passes on the second try.
- `docker compose down` while the cluster is thrashing can hang past the tool timeout; use `down -t 5`.
- Cluster left as: default 1G heap, healthy, smoke green, no `s4.*` topics (fresh cluster after `down`).
