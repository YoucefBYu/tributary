"""The readiness document a receiver serves on `GET /readyz` (`contracts/observability/v0` 2.3).

Pure: a function of a state snapshot, no I/O. Every value is a fixed code or a spec-defined name
(invariant 6): the reasons below are the closed list in `readiness.schema.json`, so a broker error string,
a peer address or exception text cannot reach the body.
"""

from __future__ import annotations

from dataclasses import dataclass

import orjson

# `spec`
NO_SPEC = "no_spec"
SPEC_REFUSED = "spec_refused"
UNDEPLOYED = "undeployed"
# `component`
STARTING = "starting"
BIND_FAILED = "bind_failed"
STOPPING = "stopping"
COMPONENT_FAILED = "component_failed"
# `raw_producer`
NOT_WARMED = "not_warmed"
ALL_BROKERS_DOWN = "all_brokers_down"
FATAL = "fatal"
# `reply_consumer`
NO_PARTITION = "no_partition"
NOT_ESTABLISHED = "not_established"
DISCONNECTED = "disconnected"

SPEC_SOURCE_UNPROVEN = "spec_source_unproven"
LITE_PROFILE = "lite_profile"


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    reason: str | None  # None means ok

    def as_dict(self) -> dict[str, object]:
        if self.reason is None:
            return {"name": self.name, "ok": True}
        return {"name": self.name, "ok": False, "reason": self.reason}


@dataclass(frozen=True, slots=True)
class Report:
    channel: str
    receiver_type: str
    channel_version: int | None
    sync_reply: bool
    checks: tuple[Check, ...]
    degraded: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return all(c.reason is None for c in self.checks)

    @property
    def http_status(self) -> int:
        return 200 if self.ready else 503

    def document(self) -> dict[str, object]:
        doc: dict[str, object] = {
            "v": 0,
            "component": "receiver",
            "status": "ready" if self.ready else "not_ready",
            "channel": self.channel,
            "receiver_type": self.receiver_type,
        }
        if self.channel_version is not None:
            doc["channel_version"] = self.channel_version
        if self.sync_reply:
            doc["sync_reply"] = True
        doc["checks"] = [c.as_dict() for c in self.checks]
        if self.degraded:
            doc["degraded"] = list(self.degraded)
        return doc

    def body(self) -> bytes:
        return orjson.dumps(self.document())
