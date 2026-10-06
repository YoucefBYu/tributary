"""Synthetic event builder for tests (no PHI: every value is a fixed synthetic token)."""

from __future__ import annotations

from typing import Any


def sample_event(**over: Any) -> dict[str, Any]:
    """A minimal valid `kind=message` event; keyword overrides replace fields."""
    event: dict[str, Any] = {
        "v": 0,
        "kind": "message",
        "ts": "2026-01-01T00:00:00.123Z",
        "channel": "c",
        "channel_version": 1,
        "stage": "received",
        "status": "ok",
        "instance": "w-1",
        "message_id": "0194f5a0-0000-7000-8000-000000000001",
    }
    event.update(over)
    return event
