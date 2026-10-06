"""`Drop` and `Reject`: the two outcomes of a step besides "here is the message".

A dropped message is a deliberate filter decision, not a failure: the runtime emits a `filtered` event
with `status: "dropped"` and commits the offset. Nothing is retried and nothing reaches a destination.

A rejected message is a deliberate *failure* decision: the step knows that retrying cannot help (the
message is structurally wrong for this channel), so the runtime dead-letters it at once with the step's
code, skipping `pipeline.stepRetries` (channel-spec README 3.3.2). Any other exception stays retryable.
"""

from __future__ import annotations

from dataclasses import dataclass

from .codes import REJECT_CODE, is_engine_code
from .errors import DropReasonError, RejectCodeError, TributaryError, safe_token

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


class Reject(TributaryError):
    """Fail this message non-retryably with a stable code: `raise Reject("ORDER_WITHOUT_VISIT")`.

    The code becomes `tb-error-code` on the `dlq` record and `error_code` on the `dlq` event, so it must
    match `^[A-Z][A-Z0-9_]{2,63}$` and must not be one the engine reserves (`tributary.sdk.codes`). A bad
    code raises `RejectCodeError` at the `raise` site, which is an ordinary (retryable) exception: the
    mistake shows up as a red unit test, not as a mislabelled dead letter.

    There is deliberately no free-text reason. The exception text of a step never reaches a log, an
    event or a record (invariant 6), so a reason field would be a place to put PHI that nobody reads.
    """

    code: str

    def __init__(self, code: str) -> None:
        _check_code(code)
        if is_engine_code(code):
            raise RejectCodeError(
                f"Reject code {safe_token(code)} is reserved by the engine (tributary.sdk.codes); choose a channel code"
            )
        super().__init__(f"rejected with {code}")
        self.code = code

    @classmethod
    def engine(cls, code: str) -> Reject:
        """A `Reject` carrying an engine code. For the engine's built-in steps only.

        Refuses any code that is *not* reserved, so the engine cannot mint an unregistered code either.
        """
        _check_code(code)
        if not is_engine_code(code):
            raise RejectCodeError(f"{safe_token(code)} is not an engine code")
        reject = cls.__new__(cls)
        TributaryError.__init__(reject, f"rejected with {code}")
        reject.code = code
        return reject


def _check_code(code: object) -> None:
    if not isinstance(code, str) or not REJECT_CODE.match(code):
        raise RejectCodeError(f"Reject code {safe_token(code)} does not match ^[A-Z][A-Z0-9_]{{2,63}}$")
