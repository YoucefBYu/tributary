"""The MLLP client transport: a plain `socket` (optionally TLS-wrapped), one persistent connection per
destination, reopened lazily on failure. Standard library only — no new dependency, matching the human
decision for the ClickHouse loader's `urllib`/`ssl` shape (`clickhouse/client.py`'s module docstring) and
the wave-2 rule against adding a client library where the protocol is this small.

## One connection per destination, bounded timeouts

`MllpConnection` holds at most one open socket, built lazily on the first `send_and_receive_ack` call and
reused across messages — "keep a connection per destination" (task): MLLP has no equivalent of HTTP's
one-request-per-socket statelessness, and reopening a TLS connection for every single message would be
needless handshake cost against a receiver this loader talks to repeatedly. `timeoutMs` is **connect plus
ACK wait, together** (README 3.4.6), enforced as one `deadline = time.monotonic() + timeout_s` covering
connect, send and every `recv` until a complete ACK frame arrives or the deadline passes — never a fresh
`timeoutMs` budget per phase, which would let a slow connect plus a slow ACK together exceed what the
channel spec configured.

## TLS

`ssl.create_default_context()` — the stdlib default, verifying certificate and hostname against the OS
trust store (task: "TLS uses the stdlib ssl default context"). An additional trust bundle from
`tls.secretRef` is loaded on top of the default store (unlike the ClickHouse loader's *pinning* choice —
there is no `auth.type: client-certificate` equivalent in `$defs/mllpParams`/`clientTls` to make pinning
the documented behaviour here, so this follows `http/client.py`'s "trust store plus an addition" reading
instead). An optional client certificate is loaded only if both `tls.crt` and `tls.key` exist under that
same `secretRef` (schema comment on `clientTls`: "optionally a client certificate and key").

## No payload or ACK text in a log line (invariant 6)

Every log call in this package passes `message_id`, byte lengths, and fixed reason/result tokens
(`ack_code`, `match`/`mismatch`) — never `message.payload`, the raw ACK bytes, or `AckResult.control_id`
as text. `control_id` only ever reaches a log as a boolean (matched the sent `MSH-10` or not).
"""

from __future__ import annotations

import contextlib
import socket
import ssl
import time
from dataclasses import dataclass

from ..secrets import SecretResolver
from .config import MllpDestinationConfig, MllpTlsSpec
from .framing import FrameParser, frame

#: An ACK is a few hundred bytes at most; this bounds a misbehaving or hostile peer from growing this
#: loader's buffer without limit while "waiting for a terminator" (same reasoning as
#: `clickhouse/client.py`'s `_MAX_BODY_BYTES`).
MAX_ACK_SIZE = 65536

#: One `recv()` call's chunk size.
_RECV_CHUNK = 4096


class TransportError(Exception):
    """The connection could not be made, or was lost while sending or reading a reply: DNS failure,
    connection refused, connection reset. Always retryable (`loader.py`'s classification)."""


class TlsVerificationError(Exception):
    """The TLS handshake failed: certificate or hostname verification. Never retryable — a bad CA or
    hostname mismatch fails identically every time, matching `clickhouse/client.py`'s identical split
    from `TransportError`."""


class NoAckError(Exception):
    """The connection closed (a clean EOF, or the frame parser's size bound tripped) before a complete
    ACK frame arrived. Distinct from a timeout: the peer answered by hanging up, not by staying silent."""


def _build_ssl_context(tls: MllpTlsSpec, resolver: SecretResolver) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.minimum_version = {"1.2": ssl.TLSVersion.TLSv1_2, "1.3": ssl.TLSVersion.TLSv1_3}[tls.min_version]
    if tls.secret_ref is None:
        return ctx
    ctx.load_verify_locations(cafile=str(resolver.resolve_file(tls.secret_ref, "ca.crt")))
    try:
        cert = resolver.resolve_file(tls.secret_ref, "tls.crt")
        key = resolver.resolve_file(tls.secret_ref, "tls.key")
    except FileNotFoundError:
        return ctx  # no client certificate under this secretRef: optional (schema comment on clientTls)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    return ctx


@dataclass
class MllpConnection:
    host: str
    port: int
    ssl_context: ssl.SSLContext | None

    def __post_init__(self) -> None:
        self._sock: socket.socket | ssl.SSLSocket | None = None

    def is_connected(self) -> bool:
        return self._sock is not None

    def close(self) -> None:
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.close()
            self._sock = None

    def _connect(self, deadline: float) -> None:
        remaining = max(0.001, deadline - time.monotonic())
        try:
            raw = socket.create_connection((self.host, self.port), timeout=remaining)
        except (TimeoutError, OSError) as exc:
            raise TransportError(str(exc)) from exc
        if self.ssl_context is None:
            self._sock = raw
            return
        try:
            self._sock = self.ssl_context.wrap_socket(raw, server_hostname=self.host)
        except ssl.SSLError as exc:
            raw.close()
            raise TlsVerificationError(str(exc)) from exc
        except OSError as exc:
            raw.close()
            raise TransportError(str(exc)) from exc

    def send_and_receive_ack(self, payload: bytes, *, timeout_s: float) -> bytes:
        """One request/reply cycle: connect if needed, send the framed payload, read until a complete
        ACK frame arrives. Raises `TransportError`, `TlsVerificationError`, `TimeoutError` or
        `NoAckError`; always closes the connection first on any of those (a connection that failed
        mid-protocol cannot be trusted to still be in a clean request/reply state for the next attempt —
        the caller reconnects on the next `deliver()` call)."""
        deadline = time.monotonic() + timeout_s
        if self._sock is None:
            self._connect(deadline)
        sock = self._sock
        if sock is None:  # pragma: no cover - _connect either sets this or raises
            raise TransportError("connection was not established")

        try:
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            sock.sendall(frame(payload))
        except TimeoutError as exc:
            # `timeoutMs` is one shared budget for connect+send+ACK (module docstring): a stall
            # sending to an unresponsive peer is not operationally different from a stall waiting for
            # its reply, so both become the same MLLP_TIMEOUT (loader.py), not a transport error.
            self.close()
            raise TimeoutError("no ACK within timeoutMs") from exc
        except OSError as exc:
            self.close()
            raise TransportError(str(exc)) from exc

        parser = FrameParser(MAX_ACK_SIZE)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.close()
                raise TimeoutError("no ACK within timeoutMs")
            try:
                sock.settimeout(remaining)
                chunk = sock.recv(_RECV_CHUNK)
            except TimeoutError as exc:
                self.close()
                raise TimeoutError("no ACK within timeoutMs") from exc
            except OSError as exc:
                self.close()
                raise TransportError(str(exc)) from exc
            if not chunk:
                self.close()
                raise NoAckError("connection closed before a complete ACK arrived")
            try:
                ack = parser.feed(chunk)
            except ValueError as exc:
                self.close()
                raise NoAckError(str(exc)) from exc
            if ack is not None:
                return ack


def connection_from_config(config: MllpDestinationConfig, resolver: SecretResolver) -> MllpConnection:
    ssl_context = _build_ssl_context(config.tls, resolver) if config.tls is not None else None
    return MllpConnection(config.host, config.port, ssl_context)
