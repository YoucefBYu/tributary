"""The HTTP/FHIR `asyncio` receiver: parse one `POST` request, publish to `<env>.<channel>.raw`,
respond only after the commit (invariant 1).

One connection serves exactly one request-response, then closes (`Connection: close`). Set 1.1's
`http` source is "an ingest endpoint, not a FHIR server" (README 3.2.3) with a single fixed path, so
there is no benefit yet to the extra state a keep-alive, pipelining HTTP/1.1 server would need, and
not building it means one fewer place invariant 1 could be gotten wrong under pipelining -- the same
trade-off the MLLP receiver's docstring calls out for a different protocol. Each connection is fully
independent (head-of-line isolation): a slow or hostile peer on one socket cannot block another's
request from being read, parsed or published, because there is exactly one `asyncio` task per
connection and the receiver-wide `ByteBudget` is the only thing they share (Phase 0 memo 8.3).

Size and shape are enforced strictly before any bytes past the headers are read: `Content-Length` is
required (no chunked transfer-encoding -- refused outright, see `_read_head`), and a request whose
`Content-Length` exceeds `maxMessageBytes` gets `413` without this receiver ever reading its body
(README 3.2.2's "before the body is buffered past the limit").
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hmac
import json
import logging
import re
import ssl
import sys
import time
from typing import Any

from tributary.sdk import Message
from tributary.sdk import headers as h

from ..backpressure import ByteBudget
from ..engine_kafka import engine_security_properties
from ..envelope import build_raw_envelope
from ..errors import ConfigError, PermanentPublishError
from ..keys import resolve_key
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher, topic_name
from ..net import is_loopback, require_safe_bind
from ..payload import well_formed
from ..secrets import DirSecretResolver, SecretResolver
from ..tls import AEAD_CIPHER_PIN, build_ssl_context, log_peer_refused, peer_san_allowed
from .config import HttpSettings, http_settings_from_channel_spec

__all__ = ["HttpServer", "HttpSettings", "serve_from_channel_spec"]

READ_CHUNK = 65536

#: A sender-supplied `charset` is copied into `tb-content-type` only if it is this plain a token;
#: every other media-type parameter is dropped (README 3.2.3), never copied into a header.
_CHARSET = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_KNOWN_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})

#: After an error response sent without reading the body, read and discard at most this much, for at
#: most `_LINGER_S`: closing a socket with unread inbound data makes the kernel send RST, which can
#: destroy the response before the sender reads it. Bounded, so it is not a way to make us buffer.
_LINGER_BYTES = 65536
_LINGER_S = 0.5

_access_log = logging.getLogger("tributary.receivers.http.access")


class _HttpError(Exception):
    """One synchronous outcome: send this status and stop reading, no publish attempted."""

    def __init__(
        self, status: int, reason: str, *, retry_after_s: float | None = None, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason
        self.retry_after_s = retry_after_s
        self.headers = headers or {}


def _encode_response(status: int, reason: str, body: bytes, *, extra_headers: dict[str, str] | None = None) -> bytes:
    headers = {"Content-Length": str(len(body)), "Connection": "close", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    head = f"HTTP/1.1 {status} {reason}\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode("ascii") + body


def _tb_content_type(header_value: str, accepted: frozenset[str]) -> str | None:
    """The matched `accept` entry, plus `charset` if the sender gave a plain one; `None` if the media
    type is not admitted. Every other parameter is dropped (README 3.2.3)."""
    media, *params = (part.strip() for part in header_value.split(";"))
    media = media.lower()
    if media not in accepted:
        return None
    for param in params:
        name, sep, value = param.partition("=")
        if sep and name.strip().lower() == "charset":
            value = value.strip().strip('"')
            if _CHARSET.match(value):
                return f"{media}; charset={value.lower()}"
    return media


class HttpServer:
    def __init__(
        self,
        cfg: HttpSettings,
        publisher: RawPublisher,
        *,
        metrics: Metrics | None = None,
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        require_safe_bind(cfg.bind, tls_configured=cfg.tls is not None)  # `tls: {}` still parses to a TlsSpec
        if cfg.auth.mode == "none" and not is_loopback(cfg.bind):
            raise ConfigError("auth mode 'none' is only permitted on a loopback bind (README 3.2.3)")
        self.cfg = cfg
        self.publisher = publisher
        self.metrics = metrics if metrics is not None else Metrics()
        self.budget = ByteBudget(cfg.queue_max_bytes, cfg.queue_max_messages)
        self._inflight = asyncio.Semaphore(cfg.max_inflight)
        resolver = secret_resolver or DirSecretResolver()
        self._ssl_context: ssl.SSLContext | None = None
        if cfg.tls is not None:
            self._ssl_context = build_ssl_context(cfg.tls, resolver, cipher_pin=AEAD_CIPHER_PIN)
        self._bearer_tokens: tuple[bytes, ...] = ()
        if cfg.auth.mode == "bearer":
            if cfg.auth.secret_ref is None:
                raise ConfigError("auth mode 'bearer' requires secretRef")
            # One token per line: the secret may name several so a rotation has an overlap window.
            text = resolver.resolve_text(cfg.auth.secret_ref, "token")
            self._bearer_tokens = tuple(line.strip().encode("utf-8") for line in text.splitlines() if line.strip())
            if not self._bearer_tokens:
                raise ConfigError("bearer token secret is empty; refusing to start an endpoint any request could pass")

    @staticmethod
    async def _read(r: asyncio.StreamReader, n: int, idle_s: float, deadline: float) -> bytes:
        """One read bounded by both the idle timeout and what is left of the whole-request deadline."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        return await asyncio.wait_for(r.read(n), min(idle_s, remaining))

    async def _read_head(self, r: asyncio.StreamReader, deadline: float) -> tuple[bytes, bytes]:
        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            if len(buf) > self.cfg.max_header_bytes:
                raise _HttpError(431, "Request Header Fields Too Large")
            chunk = await self._read(r, 4096, self.cfg.idle_timeout_s, deadline)
            if not chunk:
                raise ConnectionError("peer closed before headers completed")
            buf += chunk
        head, _, rest = bytes(buf).partition(b"\r\n\r\n")
        if len(head) > self.cfg.max_header_bytes:
            raise _HttpError(431, "Request Header Fields Too Large")
        return head, rest

    @staticmethod
    def _parse_head(head: bytes) -> tuple[str, str, list[tuple[str, str]]]:
        lines = head.split(b"\r\n")
        request_line = lines[0].split(b" ")
        if len(request_line) != 3 or not request_line[2].startswith(b"HTTP/1."):
            raise _HttpError(400, "Bad Request")
        method_b, raw_path, _version = request_line
        try:
            method = method_b.decode("ascii")
            path = raw_path.split(b"?", 1)[0].decode("ascii")  # the query string is never read
        except UnicodeDecodeError:
            raise _HttpError(400, "Bad Request") from None
        headers: list[tuple[str, str]] = []
        for line in lines[1:]:
            name, sep, value = line.partition(b":")
            if not sep or not name or name != name.strip():
                raise _HttpError(400, "Bad Request")  # also refuses obs-fold and whitespace before the colon
            try:
                headers.append((name.decode("ascii"), value.decode("ascii").strip()))
            except UnicodeDecodeError:
                raise _HttpError(400, "Bad Request") from None
        return method, path, headers

    @staticmethod
    def _header(headers: list[tuple[str, str]], name: str) -> str | None:
        matches = [v for k, v in headers if k.lower() == name]
        if len(matches) > 1:
            raise _HttpError(400, "Bad Request")  # ambiguous, never "the first one"
        return matches[0] if matches else None

    def _check_auth(self, headers: list[tuple[str, str]]) -> None:
        mode = self.cfg.auth.mode
        if mode in ("none", "mtls"):  # mtls: the TLS layer already required and verified the certificate
            return
        value = self._header(headers, "authorization")
        scheme, _, token = (value or "").partition(" ")
        presented = token.strip().encode("utf-8")
        # Every candidate is compared (no early exit), so timing does not reveal which one matched.
        matches = [hmac.compare_digest(presented, known) for known in self._bearer_tokens]
        ok = scheme.lower() == "bearer" and any(matches) and bool(self._bearer_tokens)
        if not ok:
            self.metrics.rejected_auth += 1
            raise _HttpError(401, "Unauthorized", headers={"WWW-Authenticate": "Bearer"})

    async def _read_body(self, r: asyncio.StreamReader, already: bytes, content_length: int, deadline: float) -> bytes:
        body = bytearray(already[:content_length])
        remaining = content_length - len(body)
        while remaining > 0:
            chunk = await self._read(r, min(READ_CHUNK, remaining), self.cfg.idle_timeout_s, deadline)
            if not chunk:
                raise ConnectionError("peer closed before the body completed")
            body += chunk
            remaining -= len(chunk)
        return bytes(body)

    async def _publish(self, message: Message, deadline: float) -> bool:
        async with self._inflight:
            timeout = max(deadline - time.monotonic(), 0.001)
            t0 = time.monotonic()
            try:
                ok = await self.publisher.publish(message, timeout_s=timeout)
            except PermanentPublishError:
                self.metrics.rejected_envelope += 1
                raise _HttpError(422, "Unprocessable Content") from None  # retrying cannot help: no Retry-After
            except Exception:  # any failure is "not committed"; its text is never surfaced
                ok = False
            self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
            return ok

    async def _handle_request(self, r: asyncio.StreamReader, remote: str, ctx: dict[str, Any]) -> bytes:
        request_deadline = time.monotonic() + self.cfg.request_timeout_s
        head, rest = await self._read_head(r, request_deadline)
        method, path, headers = self._parse_head(head)
        ctx["method"] = method if method in _KNOWN_METHODS else "OTHER"
        if path != self.cfg.path:
            raise _HttpError(404, "Not Found")
        if method != "POST":
            raise _HttpError(405, "Method Not Allowed", headers={"Allow": "POST"})
        self._check_auth(headers)  # before the body is read or anything is reserved

        content_type = _tb_content_type(self._header(headers, "content-type") or "", self.cfg.accept)
        if content_type is None:
            self.metrics.rejected_content_type += 1
            raise _HttpError(415, "Unsupported Media Type")
        if self._header(headers, "transfer-encoding") is not None:
            raise _HttpError(400, "Bad Request")  # chunked bodies are not supported in v0
        if self._header(headers, "expect") is not None:
            raise _HttpError(417, "Expectation Failed")
        length_text = self._header(headers, "content-length")
        if length_text is None:
            raise _HttpError(411, "Length Required")
        if not length_text.isascii() or not length_text.isdigit() or len(length_text) > 12:
            raise _HttpError(400, "Bad Request")
        content_length = int(length_text)
        if content_length > self.cfg.max_message_bytes:
            self.metrics.nacked_oversize += 1
            raise _HttpError(413, "Payload Too Large")  # decided from the header: no body byte is read
        if content_length == 0:
            self.metrics.nacked_parse_error += 1
            raise _HttpError(400, "Bad Request")

        # Reserved before the body is read, so N slow connections cannot buffer N x maxMessageBytes
        # outside the byte budget. A wait longer than the ACK timeout is a `queue-full` refusal.
        if not await self.budget.reserve(content_length, self.cfg.ack_timeout_s):
            self.metrics.nacked_queue_full += 1
            raise _HttpError(503, "Service Unavailable", retry_after_s=1.0)
        try:
            body = await self._read_body(r, rest, content_length, request_deadline)
            ctx["bytes"] = len(body)
            self.metrics.messages_received += 1
            if "parse-error" in self.cfg.nack_on and not well_formed(content_type, body):
                self.metrics.nacked_parse_error += 1
                raise _HttpError(400, "Bad Request")
            outcome = resolve_key(
                self.cfg.key_expression,
                payload=body,
                content_type=content_type,
                headers=[(name, value.encode("utf-8")) for name, value in headers],
                header_source="http",
            )
            if outcome.reason is not None:
                self.metrics.nacked_parse_error += 1
                raise _HttpError(400, "Bad Request")  # unresolved per-key: a parse-error NACK (README 4.3)
            message = build_raw_envelope(
                channel=self.cfg.channel,
                channel_version=self.cfg.channel_version,
                source=f"http:{self.cfg.instance}@{remote}",
                payload=body,
                key=outcome.key,
                content_type=content_type,
            )
            msg_id = message.require(h.TB_MSG_ID)
            ctx["msg_id"] = msg_id
            # The ACK budget starts now, once the body is in: a slow upload must not eat it and turn a
            # healthy Kafka into a 503 (whose retry would duplicate the message).
            ack_deadline = time.monotonic() + self.cfg.ack_timeout_s
            ok = await self._publish(message, ack_deadline)
        finally:
            await self.budget.release(content_length)
        if not ok:
            self.metrics.publish_failures += 1
            self.metrics.nacked_unavailable += 1
            raise _HttpError(503, "Service Unavailable", retry_after_s=1.0)
        self.metrics.acked += 1
        return _encode_response(202, "Accepted", json.dumps({"tb-msg-id": msg_id}).encode("ascii"))

    @staticmethod
    async def _linger(r: asyncio.StreamReader) -> None:
        deadline = time.monotonic() + _LINGER_S
        remaining = _LINGER_BYTES
        with contextlib.suppress(TimeoutError, ConnectionError, OSError, ssl.SSLError):
            while remaining > 0 and (left := deadline - time.monotonic()) > 0:
                chunk = await asyncio.wait_for(r.read(min(4096, remaining)), left)
                if not chunk:
                    return
                remaining -= len(chunk)

    def _log_access(self, ctx: dict[str, Any], status: int) -> None:
        # Method (allow-listed), status, byte count and tb-msg-id: never the request line, a header or a body.
        _access_log.info(
            json.dumps(
                {
                    "event": "http_access",
                    "channel": self.cfg.channel,
                    "method": ctx.get("method", "OTHER"),
                    "status": status,
                    "bytes": ctx.get("bytes", 0),
                    "tb_msg_id": ctx.get("msg_id"),
                }
            )
        )

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        peer = w.get_extra_info("peername")
        remote = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        if self.metrics.connections_open >= self.cfg.max_conns:
            self.metrics.connections_refused += 1
            w.close()
            return
        if self.cfg.tls is not None and self.cfg.tls.allowed_client_sans:
            ssl_object = w.get_extra_info("ssl_object")
            if not peer_san_allowed(ssl_object, self.cfg.tls.allowed_client_sans):
                # README 3.2.7: a bare 403, sent without reading the request; never 401, never 5xx.
                self.metrics.rejected_peer_not_allowed += 1
                log_peer_refused(self.cfg.channel, ssl_object)
                try:
                    w.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                    await asyncio.wait_for(w.drain(), self.cfg.write_timeout_s)
                except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
                    pass
                finally:
                    w.close()
                return
        self.metrics.connections_total += 1
        self.metrics.connections_open += 1
        ctx: dict[str, Any] = {}
        lingering = False
        try:
            try:
                response = await self._handle_request(r, remote, ctx)
                status = 202
            except _HttpError as exc:
                extra = dict(exc.headers)
                if exc.retry_after_s:
                    extra["Retry-After"] = str(max(1, int(exc.retry_after_s)))
                body = json.dumps({"error": exc.reason}).encode("ascii")
                response = _encode_response(exc.status, exc.reason, body, extra_headers=extra)
                status = exc.status
                lingering = exc.status in (400, 401, 404, 405, 411, 413, 415, 417, 431)
            except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
                return  # idle/slow/reset peer: no response can safely be written
            except Exception:  # a bug here must not leak text to the peer or wedge the connection
                response = _encode_response(400, "Bad Request", b'{"error": "Bad Request"}')
                status = 400
            self._log_access(ctx, status)
            try:
                w.write(response)
                await asyncio.wait_for(w.drain(), self.cfg.write_timeout_s)
                if lingering:
                    await self._linger(r)
            except (TimeoutError, ConnectionError, OSError):
                pass
        finally:
            self.metrics.connections_open -= 1
            w.close()

    async def start(self) -> asyncio.base_events.Server:
        """Start listening. Known bound, not counted: a TLS handshake in progress is not yet a connection
        (asyncio calls the handler after the handshake), so it does not count against `max_conns`. Half-open
        handshakes are bounded by `tls_handshake_timeout_s` and the listen backlog; a plaintext-to-TLS flood
        is the operator's edge-firewall concern. Counting them needs a custom protocol factory, not done."""
        return await asyncio.start_server(
            self.handle,
            self.cfg.bind,
            self.cfg.port,
            ssl=self._ssl_context,
            backlog=1024,
            ssl_handshake_timeout=self.cfg.tls_handshake_timeout_s if self._ssl_context else None,
        )


async def serve_from_channel_spec(
    spec: dict[str, Any],
    *,
    env: str,
    bootstrap_servers: str,
    instance: str = "http-0",
    secret_resolver: SecretResolver | None = None,
    metrics_file: str | None = None,
    engine_security_file: str | None = None,
) -> None:
    cfg = http_settings_from_channel_spec(spec, env=env, bootstrap_servers=bootstrap_servers, instance=instance)
    engine_security = engine_security_properties(
        cfg.bootstrap_servers, engine_security_file, secret_resolver or DirSecretResolver()
    )
    topic = topic_name(cfg.env, cfg.channel)
    publisher = RawPublisher(
        cfg.bootstrap_servers,
        topic,
        security=engine_security,
        linger_ms=cfg.linger_ms,
        queue_max_messages=cfg.max_inflight * 2,
        ordering="per-key" if cfg.key_expression is not None else "none",
        client_id_prefix="http-receiver",
    )
    publisher.start(asyncio.get_running_loop())
    metrics = Metrics()
    server = HttpServer(cfg, publisher, metrics=metrics, secret_resolver=secret_resolver)
    try:
        asyncio_server = await server.start()
        bound_port = asyncio_server.sockets[0].getsockname()[1]
        print(
            json.dumps(
                {
                    "event": "listening",
                    "channel": cfg.channel,
                    "topic": topic,
                    "port": bound_port,
                    "tls": cfg.tls is not None,
                }
            ),
            flush=True,
        )
        async with asyncio_server:
            while True:
                await asyncio.sleep(5)
                if metrics_file:
                    with open(metrics_file, "w") as f:
                        json.dump(metrics.snapshot(), f)
    finally:
        publisher.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel-spec", required=True)
    p.add_argument("--env", required=True)
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--instance", default="http-0")
    p.add_argument("--secrets-dir")
    p.add_argument("--metrics-file")
    p.add_argument(
        "--engine-security",
        help="JSON file (channel-spec $defs/kafkaSecurity shape) for the engine Kafka link; absent means "
        "plaintext, which is refused unless every --bootstrap host is loopback (PROVISIONAL, engine_kafka.py)",
    )
    a = p.parse_args()
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    with open(a.channel_spec, encoding="utf-8") as f:
        spec = json.load(f)
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else None
    coro = serve_from_channel_spec(
        spec,
        env=a.env,
        bootstrap_servers=a.bootstrap,
        instance=a.instance,
        secret_resolver=resolver,
        metrics_file=a.metrics_file,
        engine_security_file=a.engine_security,
    )
    try:
        import uvloop  # type: ignore[import-not-found]  # no Windows wheel; optional on this platform
    except ImportError:
        asyncio.run(coro)
    else:
        uvloop.run(coro)


if __name__ == "__main__":
    main()
