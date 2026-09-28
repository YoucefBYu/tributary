"""`MllpLoader`: one delivery attempt of one `Message`, sent as one MLLP frame over the destination's
one persistent connection. `contracts/channel-spec/v0/README.md` section 3.4.6 is the source of truth.

## The payload is sent as-is

README 3.4.6: "the loader does not rewrite `MSH`". `client.py`'s `frame()` only adds the MLLP envelope
bytes; nothing in this module inspects, reshapes or re-encodes the payload. There is no
`PAYLOAD_SHAPE_INVALID` for this destination type — a step upstream is responsible for handing this
loader well-formed HL7 v2, and this loader has no v2 parser to check that with anyway (`ack.py`'s
module docstring: not general HL7 v2 parsing, `tributary-hl7v2` owns that).

## Idempotency (README 5.1) — only `sink-native` or `none`

`config.py`'s module docstring already makes the case at startup-config time: an MLLP receiver gets one
payload and gives back one MSA reply, with no field this loader can attach a second identifier to
without rewriting `MSH`, which the spec forbids. This loader cannot start-up-verify either technique
the way `postgresql`/`clickhouse` do — say so, don't imply a check that does not exist.

## One connection, one attempt, the shared timeout budget

`client.py`'s `MllpConnection` holds the one persistent socket; `deliver()` calls
`send_and_receive_ack` exactly once per attempt and lets `timeoutMs` (connect + send + ACK, one shared
deadline) bound the whole thing. `retry.policy: blocking` (the only value the schema allows) means the
caller — the runtime's `ConsumerLoop`, through `MllpRecordHandler` — holds the partition and calls
`deliver` again with the same message on a retryable failure; this loader does not retry internally.
`config.circuit_breaker`, when configured, is consulted first, same as every other loader in this
package.

## AA/CA is success; the source offset commits only then

`success=True` is returned only for `AA`/`CA` **and** a matching `MSA-2`/sent-`MSH-10` control id
(below). Everything else is `success=False`, so `MllpRecordHandler.handle` raises `DeliveryError` and
the runtime's `ConsumerLoop` never commits the `out.<dest>` offset for that record — invariant 1's
"acknowledge (here: commit) only after the destination's own answer is in" applies at this boundary the
same way it applies to a receiver's own ACK to its sender.

## Validating `MSA-2` against the sent `MSH-10`

`framing.control_id` reads the control id this loader actually sent; `ack.parse_ack` reads what the
reply claims to be replying to. A mismatch means this reply cannot be trusted to be *this* message's
answer — a slow or misbehaving peer that replies out of order, or a stale ACK left over from a previous,
already-timed-out attempt on the same reused connection. Treated as `MLLP_NO_ACK` (the same code an
absent `MSA` segment gets, `ack.py`'s docstring): the fixed error-code family in README 3.4 has no
"the ACK does not match" code of its own, and semantically this loader does not have a validated ACK
for this message either way. Retryable, matching a genuinely absent ACK, and the connection is closed
first (`client.py`'s `send_and_receive_ack` already does this) so the next attempt starts clean.

## `onNegativeAck` (README 3.4.6)

`AE`/`CE` and `AR`/`CR` both count as a negative acknowledgement — HL7 v2's commit-level codes (`CE`,
`CR`) are not named in the README 3.4 error-code-family table, which lists only `MLLP_AE`/`MLLP_AR`, so
this loader folds them into the closest of the two: `CE` ("commit error") alongside `AE`
("application error") as `MLLP_AE`, `CR` ("commit reject") alongside `AR` ("application reject") as
`MLLP_AR`. `onNegativeAck: "retry"` makes either retryable; `onNegativeAck: "fail"` makes either
non-retryable, handing the message straight to `retry.onExhaust` (README 3.4.6: "no default ... `fail`
hands the message straight to `retry.onExhaust`").

## Error codes (README 3.4 table)

* `MLLP_TIMEOUT` -- `client.py` raised `TimeoutError`: no ACK within `timeoutMs` (README 3.4.6: "No ACK
  in time is `MLLP_TIMEOUT`, retryable, never success" -- the MLLP-specific code, not the generic
  cross-type `TIMEOUT` the "all" row of README 3.4's table lists for every other destination type).
  Retryable.
* `CONNECT_FAILED` -- `client.py`'s `TransportError`: the connection could not be made or was lost.
  Retryable.
* `TLS_VERIFY_FAILED` -- `client.py`'s `TlsVerificationError`: certificate or hostname verification
  failed. Not retryable -- a bad CA or hostname mismatch fails identically on every attempt.
* `MLLP_NO_ACK` -- `client.py`'s `NoAckError` (connection closed before a complete ACK frame arrived),
  or a parsed ACK with no `MSA` segment, or one whose `MSA-2` does not match the sent `MSH-10`.
  Retryable.
* `MLLP_AE` -- `MSA-1` is `AE` or `CE`. Retryable iff `onNegativeAck: "retry"`.
* `MLLP_AR` -- `MSA-1` is `AR` or `CR`. Retryable iff `onNegativeAck: "retry"`.

No payload or ACK text ever reaches a log line (invariant 6): only `message_id`, byte lengths, and
fixed reason/result tokens (`ack_code`, `match`/`mismatch`).
"""

from __future__ import annotations

from dataclasses import dataclass

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .ack import parse_ack
from .client import MllpConnection, NoAckError, TlsVerificationError, TransportError
from .config import MllpDestinationConfig
from .framing import control_id

DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

#: `MSA-1` values this loader ever sees (`ack.py`'s docstring). `AA`/`CA` is success; the rest are
#: negative acknowledgements folded into the two named error codes (module docstring).
_SUCCESS_CODES = frozenset({"AA", "CA"})
_AE_CODES = frozenset({"AE", "CE"})
_AR_CODES = frozenset({"AR", "CR"})


@dataclass(frozen=True, slots=True)
class MllpDeliveryOutcome(DeliveryOutcome):
    ack_code: str = ""  # MSA-1, for tests and metrics only -- never logged as free text (invariant 6)


class MllpLoader:
    def __init__(self, config: MllpDestinationConfig, connection: MllpConnection, *, log: Logger | None = None) -> None:
        self.config = config
        self._connection = connection
        self._log = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        breaker_spec = config.circuit_breaker
        self.breaker = CircuitBreaker(
            breaker_spec.failure_threshold if breaker_spec else DEFAULT_FAILURE_THRESHOLD,
            breaker_spec.open_seconds if breaker_spec else DEFAULT_OPEN_SECONDS,
        )

    def deliver(self, message: Message) -> MllpDeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return MllpDeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        payload = message.payload or b""
        sent_control_id = control_id(payload)

        try:
            ack_bytes = self._connection.send_and_receive_ack(payload, timeout_s=self.config.timeout_s)
        except TimeoutError:
            return self._record_failure(msg_id, "MLLP_TIMEOUT", retryable=True)
        except TlsVerificationError:
            return self._record_failure(msg_id, "TLS_VERIFY_FAILED", retryable=False)
        except NoAckError:
            return self._record_failure(msg_id, "MLLP_NO_ACK", retryable=True)
        except TransportError:
            return self._record_failure(msg_id, "CONNECT_FAILED", retryable=True)

        ack = parse_ack(ack_bytes)
        if not ack.code:
            return self._record_failure(msg_id, "MLLP_NO_ACK", retryable=True)
        if ack.control_id != sent_control_id:
            # Cannot trust this reply to be this message's own answer (module docstring): close the
            # connection so the next attempt cannot read a second, still-buffered stale reply. Never
            # log `ack.control_id`/`sent_control_id` as text (invariant 6) -- `_record_failure` only
            # ever logs the fixed reason and `ack.code`.
            self._connection.close()
            return self._record_failure(msg_id, "MLLP_NO_ACK", retryable=True, ack_code=ack.code)

        if ack.code in _SUCCESS_CODES:
            return self._record_success(msg_id, ack.code)
        if ack.code in _AE_CODES:
            return self._record_failure(
                msg_id, "MLLP_AE", retryable=self.config.on_negative_ack == "retry", ack_code=ack.code
            )
        if ack.code in _AR_CODES:
            return self._record_failure(
                msg_id, "MLLP_AR", retryable=self.config.on_negative_ack == "retry", ack_code=ack.code
            )
        # An MSA-1 value outside HL7's own enumeration (README 3.4's rule 1: "an outcome the loader
        # does not recognise is a retryable failure, not a success").
        return self._record_failure(msg_id, "MLLP_NO_ACK", retryable=True, ack_code=ack.code)

    def _record_success(self, msg_id: str, ack_code: str) -> MllpDeliveryOutcome:
        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record("success")
        self._log.info("delivery_succeeded", destination=self.config.destination, message_id=msg_id, ack_code=ack_code)
        return MllpDeliveryOutcome(success=True, reason="success", retryable=False, ack_code=ack_code)

    def _record_failure(self, msg_id: str, reason: str, *, retryable: bool, ack_code: str = "") -> MllpDeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        self.breaker.record_failure()
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning(
            "delivery_failed", destination=self.config.destination, message_id=msg_id, reason=reason, ack_code=ack_code
        )
        return MllpDeliveryOutcome(success=False, reason=reason, retryable=retryable, ack_code=ack_code)

    def close(self) -> None:
        self._connection.close()
