# tributary-archiver

`python -m tributary.archiver --env <env>` consumes `_tb.<env>.events` (group `tb.<env>.archiver`) into ClickHouse `message_events`.

- Config: Kafka from `TB_ENGINE_KAFKA_FILE`; ClickHouse from `TB_ARCHIVE_CLICKHOUSE_HOST|PORT|DATABASE|SECURE|CA_CERT|SECRET_REF` (secret dir holds `user`, `password`); `TB_ARCHIVE_TTL_DAYS` (default 90, a placeholder, not policy).
- Idempotency: `ReplacingMergeTree(ts)` keyed `(channel, message_id, stage, destination, attempt, scope)`. Count with `FINAL`. A re-emit landing in a different calendar month is not collapsed (partition is `toYYYYMM(ts)`).
- Consistency: the browser is eventually consistent. Steady-state lag is about `flush_interval` (1 s) plus event linger (50 ms); exposed as `ingest_lag_ms` and `kafka_lag_messages` in the `archiver_stats` log line (no metrics endpoint until `contracts/observability/v0`).
- ClickHouse down: partitions are paused, the same batch is retried with capped backoff, nothing is committed. Message flow is unaffected (invariant 9).
- If the archive is down longer than `_tb.<env>.events` retention (3 days), those events are lost from the archive for good. Kafka remains the message store; `message_events` for that window cannot be rebuilt from `_tb.events`. Backfill from `raw`/`out` (W3-2b) can recreate only the stages that topics still hold.
- PHI: no `_hmac` or identifier-derived attributes are created. Any `*_hmac` or deny-listed `attrs` entry in an incoming event is dropped and counted (`attrs_dropped`).
- Transport: TLS on by default (`TB_ARCHIVE_CLICKHOUSE_SECURE` is `true` or `false`, anything else is refused); plain HTTP only to loopback; proxy environment variables are ignored. Driver resend-once is blocked by a custom pool manager plus `Connection: close` (see `sink.py`).
- Known follow-up (W3-10): the runtime credential needs CREATE and ALTER because the archiver owns its schema. A separate migration credential would let the runtime one be INSERT-only.
- Insert failures that cannot succeed on retry (unknown table, access refused) halt the batch without dropping or committing it; alert on `insert_failures_permanent` and `batches_halted`.
