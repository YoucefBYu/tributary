"""Archiver counters and the lag figures the message browser needs (names and meaning, not an exporter).

There is no metrics endpoint yet: `contracts/observability/v0` (W3-12) is not published. Until it is,
`snapshot()` is logged as the `archiver_stats` line. Names below are the ones to map. No label carries
a message id, channel or any event field.

Lag, two figures with different meanings:
* `ingest_lag_ms`: wall clock minus the `ts` of the newest event in the last written batch. This is how
  stale the message browser is. Expected steady state: `flush_interval_ms` plus Kafka and event-emitter
  linger (events producer `linger.ms=50`), i.e. about 1 to 2 seconds; the browser is eventually consistent.
* `kafka_lag_messages`: log end offset minus committed offset, summed over assigned partitions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True)
class ArchiverMetrics:
    rows_written: int = 0
    bytes_consumed: int = 0
    batches_written: int = 0
    insert_failures: int = 0
    insert_failures_permanent: int = 0
    batches_halted: int = 0
    stray_records_rewound: int = 0
    events_rejected: int = 0
    attrs_dropped: int = 0
    fields_blanked: int = 0
    commit_failures: int = 0
    ingest_lag_ms: float = 0.0
    kafka_lag_messages: int = 0
    last_insert_ms: float = 0.0

    def snapshot(self) -> dict[str, int | float]:
        return {
            "rows_written": self.rows_written,
            "bytes_consumed": self.bytes_consumed,
            "batches_written": self.batches_written,
            "insert_failures": self.insert_failures,
            "insert_failures_permanent": self.insert_failures_permanent,
            "batches_halted": self.batches_halted,
            "stray_records_rewound": self.stray_records_rewound,
            "events_rejected": self.events_rejected,
            "attrs_dropped": self.attrs_dropped,
            "fields_blanked": self.fields_blanked,
            "commit_failures": self.commit_failures,
            "ingest_lag_ms": round(self.ingest_lag_ms, 1),
            "kafka_lag_messages": self.kafka_lag_messages,
            "last_insert_ms": round(self.last_insert_ms, 1),
        }


def now_ms() -> float:
    return time.time() * 1000.0
