"""The archiver's `/metrics` (`contracts/observability/v0/metrics.json`, every metric whose `emitters` names
`archiver`) and where each value comes from.

| Metric | Source |
|---|---|
| `tb_build_info{component, version}` | 1, set once |
| `tb_commits_total{component, channel, outcome}` | `ArchiverMetrics.commits_ok` / `commit_failures`; `refused` never occurs |
| `tb_archiver_events_total{outcome}` | `rows_written` (written), `events_rejected` (invalid) |
| `tb_archiver_attrs_dropped_total{reason}` | `attrs_dropped_hmac` (hmac); the rest of `attrs_dropped` (denied_name) |
| `tb_archiver_batches_total{outcome}` | `batches_written` (ok), `insert_failures` (failed: one per failed attempt) |
| `tb_archiver_insert_duration_seconds` | observed per successful insert batch (`ArchiverMetrics.observe_insert`) |

Counters are mirrored from the in-memory `ArchiverMetrics` by a collector at the start of each scrape (O6: no
I/O). Ingest lag and Kafka lag are not in the v0 registry; they stay in the `archiver_stats` log line until the
contract gains them (an additive change for the architect).

**Channel label.** The archiver is not per channel, but `tb_commits_total` requires the `channel` label and
pattern-checks it. `ARCHIVER_CHANNEL` is the fixed stand-in, so the series is exported rather than dropped. A
real channel could in principle share the name; the contract should say what the archiver uses (open item).
"""

from __future__ import annotations

from typing import Final

from tributary.runtime.metrics import MetricsContract, MetricsRegistry, build_version
from tributary.sdk.context import Logger

from .metrics import ArchiverMetrics

COMPONENT: Final = "archiver"
ARCHIVER_CHANNEL: Final = "tb-archiver"


class ArchiverExposition:
    """Registers every archiver metric on construction, so a registry that drifted from the contract refuses
    the start, not the first scrape."""

    def __init__(
        self,
        metrics: ArchiverMetrics,
        *,
        contracts_dir: str | None = None,
        version: str | None = None,
        log: Logger | None = None,
        strict: bool = False,
    ) -> None:
        self.source = metrics
        self.registry = MetricsRegistry(
            MetricsContract.load(contracts_dir), component=COMPONENT, strict=strict, log=log
        )
        registry = self.registry
        self._build_info = registry.gauge("tb_build_info")
        self._commits = registry.counter("tb_commits_total")
        self._events = registry.counter("tb_archiver_events_total")
        self._attrs = registry.counter("tb_archiver_attrs_dropped_total")
        self._batches = registry.counter("tb_archiver_batches_total")
        self._insert = registry.histogram("tb_archiver_insert_duration_seconds")
        self._build_info.set(1, component=COMPONENT, version=version or build_version("tributary-archiver"))
        registry.collector(self._collect)
        metrics.observe_insert = self._insert.observe

    def _collect(self) -> None:
        m = self.source
        for outcome, total in (("ok", m.commits_ok), ("refused", 0), ("failed", m.commit_failures)):
            self._commits.set_total(total, component=COMPONENT, channel=ARCHIVER_CHANNEL, outcome=outcome)
        self._events.set_total(m.rows_written, outcome="written")
        self._events.set_total(m.events_rejected, outcome="invalid")
        self._attrs.set_total(m.attrs_dropped_hmac, reason="hmac")
        self._attrs.set_total(max(0, m.attrs_dropped - m.attrs_dropped_hmac), reason="denied_name")
        self._batches.set_total(m.batches_written, outcome="ok")
        self._batches.set_total(m.insert_failures, outcome="failed")

    def render(self) -> bytes:
        return self.registry.render()
