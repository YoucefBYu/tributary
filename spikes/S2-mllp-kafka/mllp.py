"""MLLP framing helpers (S2 spike, throwaway). Pure functions and a stateful parser, no I/O,
so every edge case is unit-testable without Kafka or sockets (invariant 7 spirit).

Frame: 0x0B <payload> 0x1C 0x0D. Nothing here logs payload content; only MSH-10
(sender-chosen control id) is ever surfaced, and only as a short sanitised string.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
EB_CR = EB + CR


def build_ack(code: str, control_id: str, text: str = "") -> bytes:
    """ACK with MSA-1 = AA | AE | AR. ``text`` must be a fixed, PHI-free reason string."""
    ts = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    body = (
        f"MSH|^~\\&|TRIBUTARY|SPIKE|||{ts}||ACK|{uuid.uuid4().hex[:12]}|P|2.5\r"
        f"MSA|{code}|{control_id}|{text}\r"
    )
    return SB + body.encode("ascii", "replace") + EB_CR


def control_id(msg: bytes) -> str:
    """MSH-10 only. Bounded to 64 chars, stripped of field/component separators so it
    can never be used to forge extra ACK fields."""
    end = msg.find(b"\r")
    msh = msg[: end if end != -1 else len(msg)].split(b"|")
    if len(msh) <= 9:
        return ""
    raw = msh[9].decode("ascii", "replace")[:64]
    return "".join(ch for ch in raw if ch.isprintable() and ch not in "|^~\\&")


@dataclass(frozen=True, slots=True)
class Oversize:
    """A complete frame arrived but exceeded ``max_size``. Never forwarded to Kafka;
    the control id is still extracted (it lives in the first bytes) so the AR can
    reference it."""

    cid: str


class Fatal:
    """A frame grew past ``max_size`` without ever terminating: unbounded/hostile
    sender. The buffer is dropped and the caller must close the connection -- no
    control id is available because the frame never completed."""


FATAL = Fatal()

Frame = bytes | Oversize | Fatal


class FrameParser:
    """Incremental MLLP de-framer with a hard size bound.

    ``feed`` returns a list of ``Frame``: complete payloads as ``bytes``, oversize
    complete frames as ``Oversize`` (answer AR, do not publish), or ``FATAL`` once, for
    a frame that outgrew ``max_size`` before terminating (caller must close the
    connection). Bytes before a start-block are discarded. A partial frame still
    accumulating at EOF is simply never returned, so it can never be ACKed or lost
    silently -- the sender will see the connection close and can resend the whole frame.
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
            s = self._buf.find(SB)
            if s == -1:
                self._buf.clear()
                return out
            if s:
                del self._buf[:s]
            e = self._buf.find(EB_CR, 1)
            if e == -1:
                if len(self._buf) - 1 > self._max:
                    self.fatal = True
                    self._buf.clear()
                    out.append(FATAL)
                return out
            frame = bytes(self._buf[1:e])
            del self._buf[: e + 2]
            out.append(frame if len(frame) <= self._max else Oversize(control_id(frame)))
