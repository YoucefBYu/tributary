"""In-process counters and ACK-latency samples for the "connector-engineer" charter's
"Expose connection and ACK-latency metrics."

v0 exposes a plain snapshot (`Metrics.snapshot()`), not a Prometheus or OpenTelemetry exporter: no
metrics pipeline exists yet in this repo to wire into (devops-engineer owns that decision, and the ops
stack table names both OpenTelemetry and Prometheus as candidates), and guessing the exporter shape
now risks the same "invented detail with nothing to check it" the channel-spec schema explicitly
avoids for unbuilt receiver types. `snapshot()` is the seam: whatever exporter is chosen reads from
here.

Invariant 6: no PHI in a metric or a label. Nothing in this module ever takes a header value, a
record key or payload bytes as an argument — only counts and durations.
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: Latency samples kept per histogram-ish counter. Bounded so a long-running process cannot grow this
#: without limit; a real exporter (Prometheus, OTel) replaces this with proper bucketing.
MAX_SAMPLES = 4096


@dataclass
class Metrics:
    connections_total: int = 0
    connections_refused: int = 0
    connections_open: int = 0
    messages_received: int = 0
    acked: int = 0
    nacked_parse_error: int = 0
    nacked_oversize: int = 0
    nacked_queue_full: int = 0
    nacked_unavailable: int = 0
    nacked_not_accepting: int = 0  # channel undeployed or receiver shutting down
    rejected_auth: int = 0
    rejected_peer_not_allowed: int = 0  # TLS_PEER_NOT_ALLOWED
    rejected_content_type: int = 0
    poll_errors: int = 0
    partitions_halted: int = 0
    files_skipped: int = 0
    rejected_envelope: int = 0
    commit_failures: int = 0
    offset_commits: int = 0
    publish_failures: int = 0
    _ack_latency_ms: list[float] = field(default_factory=list, repr=False)

    def record_ack_latency_ms(self, value: float) -> None:
        if len(self._ack_latency_ms) >= MAX_SAMPLES:
            self._ack_latency_ms.pop(0)
        self._ack_latency_ms.append(value)

    def snapshot(self) -> dict[str, float | int]:
        samples = sorted(self._ack_latency_ms)
        out: dict[str, float | int] = {
            "connections_total": self.connections_total,
            "connections_refused": self.connections_refused,
            "connections_open": self.connections_open,
            "messages_received": self.messages_received,
            "acked": self.acked,
            "nacked_parse_error": self.nacked_parse_error,
            "nacked_oversize": self.nacked_oversize,
            "nacked_queue_full": self.nacked_queue_full,
            "nacked_unavailable": self.nacked_unavailable,
            "nacked_not_accepting": self.nacked_not_accepting,
            "rejected_auth": self.rejected_auth,
            "rejected_peer_not_allowed": self.rejected_peer_not_allowed,
            "rejected_content_type": self.rejected_content_type,
            "poll_errors": self.poll_errors,
            "partitions_halted": self.partitions_halted,
            "files_skipped": self.files_skipped,
            "rejected_envelope": self.rejected_envelope,
            "commit_failures": self.commit_failures,
            "offset_commits": self.offset_commits,
            "publish_failures": self.publish_failures,
            "ack_latency_ms_count": len(samples),
        }
        if samples:
            out["ack_latency_ms_p50"] = samples[len(samples) // 2]
            out["ack_latency_ms_p99"] = samples[min(len(samples) - 1, int(len(samples) * 0.99))]
            out["ack_latency_ms_max"] = samples[-1]
        return out
