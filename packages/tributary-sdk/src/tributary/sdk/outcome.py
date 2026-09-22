"""`Drop`: the one non-`Message` outcome a step may return.

A dropped message is a deliberate filter decision, not a failure: the runtime emits a `filtered` event
with `status: "dropped"` and commits the offset. Nothing is retried and nothing reaches a destination.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import DropReasonError

MAX_REASON_CHARS = 512


@dataclass(frozen=True, slots=True)
class Drop:
    """Filter this message out, with a reason that is safe to publish.

    The reason reaches `_tb.events.error_message`, which flows to ClickHouse, Grafana and the browser,
    so it must be **static text chosen by the step author** — `"unsupported trigger event"`, not an
    f-string built from a field value. The events contract calls an `error_message` interpolated from an
    exception a Critical finding, and a trigger code pasted into a reason is the same mistake one step
    earlier. Only length and control characters can be checked here; the rest is code review.
    """

    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise DropReasonError("a Drop needs a non-empty reason: it is what the browser shows")
        if len(self.reason) > MAX_REASON_CHARS:
            raise DropReasonError(
                f"Drop reason is {len(self.reason)} characters, at most {MAX_REASON_CHARS} "
                "(_tb.events.error_message is capped at 512)"
            )
        if any(ord(char) < 0x20 for char in self.reason):
            raise DropReasonError(
                "Drop reason must not contain control characters: it is written to a log and an event"
            )
