"""MLLP framing: pure functions and a stateful de-framer, no I/O (invariant 7 spirit — everything
here is unit-testable with no socket and no Kafka).

Frame: `0x0B <payload> 0x1C 0x0D`. Adapted from the Phase 0 S2 spike
(`spikes/S2-mllp-kafka/mllp.py`), reimplemented here rather than imported: `spikes/` code stays out of
`packages/` per the Phase 0 ground rule (gate report, requirement 10 note).
"""

from __future__ import annotations

from dataclasses import dataclass

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
EB_CR = EB + CR


@dataclass(frozen=True, slots=True)
class Oversize:
    """A complete frame arrived but exceeded `max_size`. Never published; the control id is still
    extracted (it lives in the first bytes) so the NACK can reference it."""

    cid: str


class Fatal:
    """A frame grew past `max_size` without ever terminating: unbounded or hostile sender. The
    buffer is dropped and the caller must close the connection — no control id is available because
    the frame never completed."""


FATAL = Fatal()

Frame = bytes | Oversize | Fatal


class FrameParser:
    """Incremental MLLP de-framer with a hard size bound.

    `feed` returns a list of `Frame`: complete payloads as `bytes`, oversize complete frames as
    `Oversize` (NACK, do not publish), or `FATAL` once, for a frame that outgrew `max_size` before
    terminating (caller must close the connection). Bytes before a start-block are discarded. A
    partial frame still accumulating when the connection ends is simply never returned, so it can
    never be ACKed or lost silently — the sender sees the connection close and can resend the whole
    frame (invariant 4: a resend is a duplicate, never a loss).
    """

    def __init__(self, max_size: int) -> None:
        self._buf = bytearray()
        self._max = max_size
        self.fatal = False

    def buffered(self) -> int:
        return len(self._buf)

    def feed(self, data: bytes) -> list[Frame]:
        out: list[Frame] = []
        self._buf += data
        while True:
            start = self._buf.find(SB)
            if start == -1:
                self._buf.clear()
                return out
            if start:
                del self._buf[:start]
            end = self._buf.find(EB_CR, 1)
            if end == -1:
                if len(self._buf) - 1 > self._max:
                    self.fatal = True
                    self._buf.clear()
                    out.append(FATAL)
                return out
            frame = bytes(self._buf[1:end])
            del self._buf[: end + 2]
            out.append(frame if len(frame) <= self._max else Oversize(control_id(frame)))


def control_id(msg: bytes) -> str:
    """MSH-10 only, bounded to 64 characters and stripped of HL7 field/component/sub-component
    separators so it can never be used to forge extra fields in a hand-built ACK. This is the only
    part of the message this package ever reads; it never logs the result — only its length, or the
    fact that it was empty, reach a log line."""
    end = msg.find(b"\r")
    msh = msg[: end if end != -1 else len(msg)].split(b"|")
    if len(msh) <= 9:
        return ""
    raw = msh[9].decode("ascii", "replace")[:64]
    return "".join(ch for ch in raw if ch.isprintable() and ch not in "|^~\\&")


def looks_like_hl7(msg: bytes) -> bool:
    """A cheap structural sanity check, not HL7 validation: starts with `MSH|` and has at least the
    nine field separators needed to reach MSH-10. Real parsing is `tributary-hl7v2`'s (not built in
    this wave); this only exists to back the `ack.nackOn: [parse-error]` a channel spec can opt into
    (see `mllp/server.py`'s handling of it — this check is gated on that opt-in, never automatic, so a
    channel that wants everything forwarded to the pipeline as-is still gets that)."""
    end = msg.find(b"\r")
    head = msg[: end if end != -1 else len(msg)]
    return head.startswith(b"MSH|") and head.count(b"|") >= 9
