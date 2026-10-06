"""The pipeline worker's metrics (`contracts/observability/v0/metrics.json`, every metric whose `emitters` names
`worker`), and where each value comes from.

| Metric | Source |
|---|---|
| `tb_build_info{component, version}` | 1, set once |
| `tb_spec_version{component, channel}` | the version the handler is **applying** (a refused newer version does not move it) |
| `tb_spec_source_unproven{component, channel}` | `LiveSpecSource.available` is false |
| `tb_messages_total{component, channel, stage, status, destination}` | every lifecycle event, at emit time (`CountingEmitter`), so it works while the archiver is down (invariant 9) |
| `tb_stage_duration_seconds{component, channel, stage="transformed", destination=""}` | one pipeline execution: head, splitter, children and destination steps of one consumed record, observed when its `transformed` event is emitted |
| `tb_step_duration_seconds{channel, scope, step, outcome}` | every step call, retries included (`StepStats.observer`) |
| `tb_worker_records_total{channel, outcome}` | `LoopStats` handled / retried / dead_lettered / quarantined / rejected |
| `tb_commits_total{component, channel, outcome}` | `LoopStats` commits / commits_refused / commits_failed |
| `tb_partitions_paused` / `tb_partitions_halted{component, channel, destination=""}` | `ConsumerLoop.partition_counts()`, as of the last loop iteration. The worker's retry is the produce side (`PRODUCE_RETRY`), which has no destination |
| `tb_worker_replies_total{channel, outcome}` | `ReplyStats` |

Every label value is a spec-defined name or a contract enum; no id, key, partition or offset is a label
(README 3.2). The registry refuses anything else (`metrics.py`).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from tributary.sdk.context import Logger

from .consumer import LoopStats
from .events import EventEmitter, LifecycleEvent
from .executor import StepOutcome
from .metrics import MetricsContract, MetricsRegistry, build_version
from .reply import ReplyStats

COMPONENT: Final = "worker"

#: `tb_worker_records_total` outcome -> `LoopStats` field.
RECORD_OUTCOMES: Final = {
    "handled": "handled",
    "retried": "retried",
    "dead_lettered": "dead_lettered",
    "quarantined": "quarantined",
    "rejected": "rejected",
}
#: `tb_commits_total` outcome -> `LoopStats` field.
COMMIT_OUTCOMES: Final = {"ok": "commits", "refused": "commits_refused", "failed": "commits_failed"}
#: `tb_worker_replies_total` outcomes are exactly the `ReplyStats` integer fields.
REPLY_OUTCOMES: Final = tuple(f.name for f in dataclasses.fields(ReplyStats) if f.type in ("int", int))


@dataclass(slots=True)
class WorkerSources:
    """Callables that read in-memory state for a scrape. None of them may do I/O (O6)."""

    loop_stats: Callable[[], LoopStats | None]
    partition_counts: Callable[[], tuple[int, int]]
    reply_stats: Callable[[], ReplyStats | None]
    applied_version: Callable[[], int | None]
    spec_available: Callable[[], bool | None]


class WorkerMetrics:
    """Registers every worker metric on construction, so a registry that drifted from the contract refuses the
    start, not the first scrape."""

    def __init__(self, registry: MetricsRegistry, *, channel: str, version: str | None = None) -> None:
        if registry.component != COMPONENT:
            raise ValueError("WorkerMetrics needs a registry for component 'worker'")
        self.registry = registry
        self.channel = channel
        self.build_info = registry.gauge("tb_build_info")
        self.spec_version = registry.gauge("tb_spec_version")
        self.spec_unproven = registry.gauge("tb_spec_source_unproven")
        self.messages = registry.counter("tb_messages_total")
        self.stage_duration = registry.histogram("tb_stage_duration_seconds")
        self.step_duration = registry.histogram("tb_step_duration_seconds")
        self.records = registry.counter("tb_worker_records_total")
        self.commits = registry.counter("tb_commits_total")
        self.paused = registry.gauge("tb_partitions_paused")
        self.halted = registry.gauge("tb_partitions_halted")
        self.replies = registry.counter("tb_worker_replies_total")
        self._sources: WorkerSources | None = None

        self.build_info.set(1, component=COMPONENT, version=version or build_version())
        # Zero series exist from the start, so an alert on `tb_partitions_halted > 0` sees 0, not "absent".
        self.spec_unproven.set(0, component=COMPONENT, channel=channel)
        self.paused.set(0, component=COMPONENT, channel=channel, destination="")
        self.halted.set(0, component=COMPONENT, channel=channel, destination="")
        for outcome in RECORD_OUTCOMES:
            self.records.set_total(0, channel=channel, outcome=outcome)
        for outcome in COMMIT_OUTCOMES:
            self.commits.set_total(0, component=COMPONENT, channel=channel, outcome=outcome)
        for outcome in REPLY_OUTCOMES:
            self.replies.set_total(0, channel=channel, outcome=outcome)
        registry.collector(self._collect)

    @classmethod
    def create(
        cls, *, channel: str, contracts_dir: str | None = None, strict: bool = False, log: Logger | None = None
    ) -> WorkerMetrics:
        registry = MetricsRegistry(MetricsContract.load(contracts_dir), component=COMPONENT, strict=strict, log=log)
        return cls(registry, channel=channel)

    # -- push side (data-plane thread) -------------------------------------------------------------

    def counted(self, inner: EventEmitter) -> EventEmitter:
        """Wrap the event emitter so every event emitted increments `tb_messages_total`."""
        return CountingEmitter(inner, self.on_event)

    def on_event(self, event: LifecycleEvent) -> None:
        self.messages.inc(
            component=COMPONENT,
            channel=event.channel,
            stage=event.stage,
            status=event.status,
            destination=getattr(event, "destination", None) or "",
        )

    def observe_step(self, scope: str, step: str, outcome: StepOutcome, duration_ms: float) -> None:
        self.step_duration.observe(duration_ms / 1000.0, channel=self.channel, scope=scope, step=step, outcome=outcome)

    def observe_pipeline(self, seconds: float) -> None:
        self.stage_duration.observe(
            seconds, component=COMPONENT, channel=self.channel, stage="transformed", destination=""
        )

    # -- pull side (scrape thread) -----------------------------------------------------------------

    def bind(self, sources: WorkerSources) -> None:
        self._sources = sources

    def _collect(self) -> None:
        sources = self._sources
        if sources is None:
            return
        channel = self.channel
        version = sources.applied_version()
        if version is not None:
            self.spec_version.set(version, component=COMPONENT, channel=channel)
        available = sources.spec_available()
        if available is not None:
            self.spec_unproven.set(0 if available else 1, component=COMPONENT, channel=channel)
        paused, halted = sources.partition_counts()
        self.paused.set(paused, component=COMPONENT, channel=channel, destination="")
        self.halted.set(halted, component=COMPONENT, channel=channel, destination="")
        stats = sources.loop_stats()
        if stats is not None:
            for outcome, attribute in RECORD_OUTCOMES.items():
                self.records.set_total(getattr(stats, attribute), channel=channel, outcome=outcome)
            for outcome, attribute in COMMIT_OUTCOMES.items():
                self.commits.set_total(getattr(stats, attribute), component=COMPONENT, channel=channel, outcome=outcome)
        replies = sources.reply_stats()
        if replies is not None:
            for outcome in REPLY_OUTCOMES:
                self.replies.set_total(getattr(replies, outcome), channel=channel, outcome=outcome)

    def render(self) -> bytes:
        return self.registry.render()


class CountingEmitter:
    """An `EventEmitter` that counts each event after the inner emitter accepted it."""

    __slots__ = ("_count", "_inner")

    def __init__(self, inner: EventEmitter, count: Callable[[LifecycleEvent], None]) -> None:
        self._inner = inner
        self._count = count

    @property
    def inner(self) -> EventEmitter:
        return self._inner

    def emit(self, event: LifecycleEvent) -> None:
        self._inner.emit(event)
        self._count(event)

    def close(self) -> None:
        self._inner.close()
