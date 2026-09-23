"""A minimal MLLP client for tests: frame a message, send it, read the ACK back.

No production code depends on this — wave 1 has no MLLP sender, only the receiver
(`packages/tributary-receivers`). Framing matches `tributary.receivers.mllp.framing` (`0x0B <payload>
0x1C 0x0D`), reimplemented here rather than imported: this package tests the receiver from the outside,
as a sender would see it, not through its internals.
"""

from __future__ import annotations

import socket

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def send_mllp(host: str, port: int, message: bytes, *, timeout_s: float = 10.0) -> bytes:
    """Send one MLLP-framed message on a fresh connection and return the raw ACK frame.

    Blocks until the peer sends a complete `EB CR`-terminated frame back, the peer closes the
    connection, or `timeout_s` elapses — whichever comes first. An empty return means the connection
    closed (or timed out) with no ACK, which is a NACK-shaped outcome a caller must check for, not an
    error this function raises: invariant 1 allows "refuse" as well as "NACK."
    """
    with socket.create_connection((host, port), timeout=timeout_s) as sock:
        sock.sendall(SB + message + EB + CR)
        sock.settimeout(timeout_s)
        buf = b""
        try:
            while not buf.endswith(EB + CR):
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
        except TimeoutError:
            pass
        return buf


def is_application_accept(ack: bytes) -> bool:
    """`MSA|AA` anywhere in the frame — good enough for a test ACK check without a full HL7 parser
    (`tributary-hl7v2` does not exist until wave 2)."""
    return b"MSA|AA" in ack
