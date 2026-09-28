"""MLLP framing for the **client** side: wrapping an outbound payload, and an incremental de-framer
for the ACK reply. Frame: `0x0B <payload> 0x1C 0x0D`.

**Reimplemented here, not imported from `tributary.receivers.mllp.framing`.** That module already
reimplements the same framing rather than importing the Phase 0 spike (its own docstring: "spikes/ code
stays out of packages/ per the Phase 0 ground rule"), and this loader has the same reason to avoid a new
edge in the other direction: `tributary-loaders` importing `tributary-receivers` would make the outbound
half of MLLP depend on the inbound half for four lines of byte-slicing, and CLAUDE.md's contract-first
ownership model has no `tributary-mllp-common` package for either side to import instead. **Proposed
for the lead/architect**: if a third MLLP consumer needs this (a receiver connection-per-sender pool
change, a chaos harness, ...), a small `tributary-mllp-wire` (or similar) package holding just framing
and ACK parsing — pure functions, no I/O, no Kafka — would be the shared home; until then this is a
second, independently tested copy of four small functions, the same trade `postgresql/config.py`'s
`_is_loopback` already documents for a one-line duplicate.
"""

from __future__ import annotations

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
EB_CR = EB + CR


def frame(payload: bytes) -> bytes:
    """Wrap `payload` for the wire. The loader never rewrites the payload itself (README 3.4.6:
    "the loader does not rewrite MSH") — this only adds the MLLP envelope bytes."""
    return SB + payload + EB_CR


def control_id(msg: bytes) -> str:
    """`MSH-10` of an **outbound** message (identical extraction to
    `tributary.receivers.mllp.framing.control_id`, reimplemented per the module docstring), so
    `loader.py` knows what `MSA-2` the reply should echo. Bounded and stripped of HL7 separators for
    the same reason the receiver's copy is: this value only ever reaches a log line as a length or a
    match/mismatch boolean, never as text (invariant 6)."""
    end = msg.find(b"\r")
    msh = msg[: end if end != -1 else len(msg)].split(b"|")
    if len(msh) <= 9:
        return ""
    raw = msh[9].decode("ascii", "replace")[:64]
    return "".join(ch for ch in raw if ch.isprintable() and ch not in "|^~\\&")


class FrameParser:
    """Incremental de-framer for the ACK reply, with a hard size bound. Only ever expects at most one
    complete frame per `deliver()` attempt (this loader sends one message and waits for one ACK before
    sending the next — no pipelining), but reads may arrive in arbitrarily small chunks, so this still
    has to accumulate rather than assume one `recv()` is one frame.

    Simpler than the receiver's `FrameParser` on purpose: no `Oversize`/`Fatal` distinction (a receiver
    NACKs a hostile sender and keeps its listener alive for the next one; this client only ever has
    this one attempt to give up on, and an oversize or unterminated ACK is `loader.py`'s `MLLP_NO_ACK`,
    not a protocol state to recover from mid-connection).
    """

    def __init__(self, max_size: int) -> None:
        self._buf = bytearray()
        self._max = max_size

    def feed(self, data: bytes) -> bytes | None:
        """Returns the complete frame's payload once one has arrived, else `None`. Raises `ValueError`
        if the buffer exceeds `max_size` before a frame terminates — the caller closes the connection."""
        self._buf += data
        if len(self._buf) > self._max:
            raise ValueError("ACK frame exceeded max_size before terminating")
        start = self._buf.find(SB)
        if start == -1:
            return None
        end = self._buf.find(EB_CR, start + 1)
        if end == -1:
            return None
        return bytes(self._buf[start + 1 : end])
