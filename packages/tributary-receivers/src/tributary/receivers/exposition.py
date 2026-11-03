"""The receiver's `/metrics` (`contracts/observability/v0/metrics.json`, every metric whose `emitters` names
`receiver`), and where each value comes from.

The data plane keeps writing its plain `Metrics` counters. A scrape mirrors them into a `MetricsRegistry`
(`collector`, in-memory reads only, O6); only the ACK latency is pushed, one histogram observation per sample
(`Metrics.latency_sink`).

| Metric | Source |
|---|---|
| `tb_build_info`, `tb_ready`, `tb_ready_check`, `tb_spec_version`, `tb_spec_source_unproven` | the readiness `Report` |
| `tb_receiver_connections_*`, `_received_total`, `_publish_failures_total`, `_poll_errors_total` | `Metrics` |
| `tb_receiver_responses_total{outcome, reason}` | the `acked`, `nacked_*` and `rejected_*` counters (`RESPONSES`) |
| `tb_receiver_sync_answers_total{answer, code}` | `reply_aa`, `Metrics.sync_answers` (AE), the reply-refusal counters (AR) |
| `tb_receiver_reply_pending`, `tb_receiver_replies_discarded_total` | reply feed and pending map counters |
| `tb_receiver_files_skipped_total`, `tb_receiver_offset_commits_total` | `Metrics` (sftp, kafka sources) |
| `tb_ack_latency_seconds` | `Metrics.latency_sink` |

Labels are the channel name, the receiver type and contract enums: no id, address, header or payload value
(invariant 6). The registry refuses anything else.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

from tributary.runtime.metrics import MetricsContract, MetricsRegistry, build_version
from tributary.sdk.context import Logger

from . import readiness as rd
from .metrics import Metrics

COMPONENT: Final = "receiver"

#: `tb_receiver_responses_total` `(outcome, reason)` -> the `Metrics` field it mirrors.
RESPONSES: Final = {
    ("acked", "committed"): "acked",
    ("nacked", "parse_error"): "nacked_parse_error",
    ("nacked", "oversize"): "nacked_oversize",
    ("nacked", "queue_full"): "nacked_queue_full",
    ("nacked", "unavailable"): "nacked_unavailable",
    ("nacked", "not_accepting"): "nacked_not_accepting",
    ("nacked", "reply_busy"): "nacked_reply_busy",
    ("nacked", "reply_connection_cap"): "nacked_reply_connection_cap",
    ("nacked", "reply_unavailable"): "nacked_reply_unavailable",
    ("rejected", "auth"): "rejected_auth",
    ("rejected", "peer_not_allowed"): "rejected_peer_not_allowed",
    ("rejected", "content_type"): "rejected_content_type",
    ("rejected", "envelope"): "rejected_envelope",
}
REFUSED_REASONS: Final = ("max_conns", "not_accepting", "peer_not_allowed", "connect_failed")
DISCARDED: Final = {
    "unmatched": "replies_unmatched",
    "unreadable": "replies_unreadable",
    "route_lost": "reply_route_lost",
}
#: Readiness check names are the contract's `check` enum.
CHECKS: Final = ("spec", "component", "raw_producer", "reply_consumer")


class ReceiverMetrics:
    """Registers every receiver metric on construction, so a registry that drifted from the contract refuses the
    start, not the first scrape."""

    def __init__(
        self,
        registry: MetricsRegistry,
        metrics: Metrics,
        report: Callable[[], rd.Report],
        *,
        channel: str,
        receiver_type: str,
        version: str | None = None,
    ) -> None:
        if registry.component != COMPONENT:
            raise ValueError("ReceiverMetrics needs a registry for component 'receiver'")
        self.registry = registry
        self._m = metrics
        self._report = report
        self._channel = channel
        self._type = receiver_type
        r = registry
        self.build_info = r.gauge("tb_build_info")
        self.ready = r.gauge("tb_ready")
        self.ready_check = r.gauge("tb_ready_check")
        self.spec_version = r.gauge("tb_spec_version")
        self.spec_unproven = r.gauge("tb_spec_source_unproven")
        self.conns_open = r.gauge("tb_receiver_connections_open")
        self.conns_total = r.counter("tb_receiver_connections_total")
        self.conns_refused = r.counter("tb_receiver_connections_refused_total")
        self.received = r.counter("tb_receiver_received_total")
        self.responses = r.counter("tb_receiver_responses_total")
        self.latency = r.histogram("tb_ack_latency_seconds")
        self.publish_failures = r.counter("tb_receiver_publish_failures_total")
        self.sync_answers = r.counter("tb_receiver_sync_answers_total")
        self.reply_pending = r.gauge("tb_receiver_reply_pending")
        self.discarded = r.counter("tb_receiver_replies_discarded_total")
        self.poll_errors = r.counter("tb_receiver_poll_errors_total")
        self.files_skipped = r.counter("tb_receiver_files_skipped_total")
        self.offset_commits = r.counter("tb_receiver_offset_commits_total")

        self.build_info.set(1, component=COMPONENT, version=version or build_version())
        metrics.latency_sink = self.observe_latency
        r.collector(self._collect)

    @classmethod
    def create(
        cls,
        metrics: Metrics,
        report: Callable[[], rd.Report],
        *,
        channel: str,
        receiver_type: str,
        contracts_dir: str | None = None,
        strict: bool = False,
        log: Logger | None = None,
    ) -> ReceiverMetrics:
        registry = MetricsRegistry(MetricsContract.load(contracts_dir), component=COMPONENT, strict=strict, log=log)
        return cls(registry, metrics, report, channel=channel, receiver_type=receiver_type)

    def observe_latency(self, seconds: float) -> None:
        self.latency.observe(seconds, channel=self._channel, receiver_type=self._type)

    def render(self) -> bytes:
        return self.registry.render()

    def _collect(self) -> None:
        m, ch, ty = self._m, self._channel, self._type
        scope = {"channel": ch, "receiver_type": ty}
        self.conns_open.set(m.connections_open, **scope)
        self.conns_total.set_total(m.connections_total, **scope)
        for reason in REFUSED_REASONS:
            total = m.rejected_peer_not_allowed if reason == "peer_not_allowed" else m.refused_by_reason.get(reason, 0)
            self.conns_refused.set_total(total, reason=reason, **scope)
        self.received.set_total(m.messages_received, **scope)
        for (outcome, reason), attribute in RESPONSES.items():
            self.responses.set_total(getattr(m, attribute), outcome=outcome, reason=reason, **scope)
        self.publish_failures.set_total(m.publish_failures, **scope)
        self.poll_errors.set_total(m.poll_errors, **scope)
        self.files_skipped.set_total(m.files_skipped, channel=ch)
        self.offset_commits.set_total(m.offset_commits, channel=ch, outcome="ok")
        self.offset_commits.set_total(m.commit_failures, channel=ch, outcome="failed")
        self._collect_sync(m, scope)
        self._collect_readiness()

    def _collect_sync(self, m: Metrics, scope: dict[str, str]) -> None:
        self.reply_pending.set(m.reply_pending, channel=self._channel)
        for reason, attribute in DISCARDED.items():
            self.discarded.set_total(getattr(m, attribute), channel=self._channel, reason=reason)
        answers: dict[tuple[str, str], int] = dict(m.sync_answers)
        answers[("AA", "NONE")] = m.reply_aa
        answers[("AR", "REPLY_BUSY")] = m.nacked_reply_busy + m.nacked_reply_connection_cap
        answers[("AR", "REPLY_UNAVAILABLE")] = m.nacked_reply_unavailable
        for (answer, code), total in answers.items():
            if total or answer in ("AA", "AR"):
                self.sync_answers.set_total(total, answer=answer, code=code, **scope)

    def _collect_readiness(self) -> None:
        ch = self._channel
        report = self._report()
        self.ready.set(1 if report.ready else 0, component=COMPONENT, channel=ch)
        present = {c.name: c.reason is None for c in report.checks}
        for name in CHECKS:
            if name in present:
                self.ready_check.set(1 if present[name] else 0, component=COMPONENT, channel=ch, check=name)
        if report.channel_version is not None:
            self.spec_version.set(report.channel_version, component=COMPONENT, channel=ch)
        unproven = rd.SPEC_SOURCE_UNPROVEN in report.degraded
        self.spec_unproven.set(1 if unproven else 0, component=COMPONENT, channel=ch)
