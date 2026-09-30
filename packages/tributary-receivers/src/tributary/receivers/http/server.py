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

import asyncio
import contextlib
import hmac
import json
import logging
import re
import ssl
import sys
import time
from dataclasses import dataclass
from typing import Any

from tributary.runtime import load_engine_kafka_or_exit
from tributary.runtime.logs import JsonLogger
from tributary.sdk import Message
from tributary.sdk import headers as h

from ..backpressure import BudgetExceeded, ByteBudget
from ..envelope import build_raw_envelope
from ..errors import ConfigError, PermanentPublishError
from ..keys import resolve_key
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher, topic_name
from ..net import is_loopback, require_safe_bind
from ..payload import well_formed
from ..secrets import DirSecretResolver, SecretResolver
from ..tls import AEAD_CIPHER_PIN, build_ssl_context, log_peer_refused, peer_san_allowed
from .config import HttpSettings
from .reload import restart_required_fields

__all__ = ["HttpServer", "HttpSettings"]

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


@dataclass(frozen=True, slots=True)
class _Live:
    """What one request runs under: the settings and the bearer tokens resolved for them. Replaced whole by a
    live update, never mutated, and read once at the start of a request (one spec per request)."""

    cfg: HttpSettings
    bearer_tokens: tuple[bytes, ...] = ()


@dataclass(slots=True)
class _Conn:
    """Per-connection state a live update needs: the TLS object to re-check against a new allow-list, whether
    the peer was revoked, and whether the request is already handed to the publisher (its answer is owed)."""

    ssl_object: Any
    revoked: bool = False
    publishing: bool = False


def _load_bearer_tokens(cfg: HttpSettings, resolver: SecretResolver) -> tuple[bytes, ...]:
    if cfg.auth.mode != "bearer":
        return ()
    if cfg.auth.secret_ref is None:
        raise ConfigError("auth mode 'bearer' requires secretRef")
    # One token per line: the secret may name several so a rotation has an overlap window.
    text = resolver.resolve_text(cfg.auth.secret_ref, "token")
    tokens = tuple(line.strip().encode("utf-8") for line in text.splitlines() if line.strip())
    if not tokens:
        raise ConfigError("bearer token secret is empty; refusing to start an endpoint any request could pass")
    return tokens


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
        self.publisher = publisher
        self.metrics = metrics if metrics is not None else Metrics()
        self.budget = ByteBudget(cfg.queue_max_bytes, cfg.queue_max_messages)
        self._inflight = asyncio.Semaphore(cfg.max_inflight)
        self._resolver = secret_resolver or DirSecretResolver()
        self._ssl_context: ssl.SSLContext | None = None
        if cfg.tls is not None:
            self._ssl_context = build_ssl_context(cfg.tls, self._resolver, cipher_pin=AEAD_CIPHER_PIN)
        self._live = _Live(cfg, _load_bearer_tokens(cfg, self._resolver))
        self._listener: asyncio.base_events.Server | None = None
        self._conns: dict[asyncio.StreamWriter, _Conn] = {}
        self._refusal: str | None = None  # set by `stop_accepting`: later requests are refused, none published

    @property
    def cfg(self) -> HttpSettings:
        return self._live.cfg

    @property
    def open_connections(self) -> int:
        return len(self._conns)

    @property
    def accepting(self) -> bool:
        return self._refusal is None

    async def apply_settings(self, new: HttpSettings) -> None:
        """Apply a live spec update (`http/reload.py` lists what may change). Everything that can fail (the
        restart-only check, re-reading a bearer secret) runs first; nothing is applied unless it all passed. The
        rest is one step on the event loop, so no request sees half of an update."""
        if restart_required_fields(self.cfg, new):
            raise ValueError("settings changed a field that needs a restart")
        tokens = _load_bearer_tokens(new, self._resolver)
        self.budget.set_limits(new.queue_max_bytes, new.queue_max_messages)
        self._live = _Live(new, tokens)
        self._revoke_disallowed_peers(new)
        await self.budget.wake()

    def _revoke_disallowed_peers(self, cfg: HttpSettings) -> None:
        """Re-check every open connection's peer certificate against the new allow-list. The list is otherwise
        only checked at accept, so a SAN removed from it would keep sending on a connection that was already
        open. A revoked connection that has not yet handed its request to the publisher is closed now; one that
        has is answered (its record is committed or failing), and it is refused if it tries anything else."""
        allowed = cfg.tls.allowed_client_sans if cfg.tls is not None else ()
        if not allowed:
            return
        for w, conn in list(self._conns.items()):
            if conn.revoked or peer_san_allowed(conn.ssl_object, allowed):
                continue
            conn.revoked = True
            self.metrics.rejected_peer_not_allowed += 1
            log_peer_refused(cfg.channel, conn.ssl_object)  # TLS_PEER_NOT_ALLOWED, certificate hash only
            if not conn.publishing:
                w.close()

    def stop_accepting(self, reason: str) -> None:
        """Close the listener and refuse (503, nothing published) every request that arrives afterwards on a
        connection still open. A request already handed to the publisher is untouched: its answer is still
        written (invariant 1). Idempotent; the first reason wins."""
        if self._refusal is None:
            self._refusal = reason
        if self._listener is not None:
            self._listener.close()

    async def drain(self, timeout_s: float) -> bool:
        """Wait, bounded, until every request this server admitted has been answered, then close every
        connection. True when nothing was left in flight."""
        deadline = time.monotonic() + timeout_s
        while any(c.publishing for c in self._conns.values()) and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        drained = not any(c.publishing for c in self._conns.values())
        for w in list(self._conns):
            w.close()
        return drained

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

    def _check_auth(self, live: _Live, headers: list[tuple[str, str]]) -> None:
        mode = live.cfg.auth.mode
        if mode in ("none", "mtls"):  # mtls: the TLS layer already required and verified the certificate
            return
        value = self._header(headers, "authorization")
        scheme, _, token = (value or "").partition(" ")
        presented = token.strip().encode("utf-8")
        # Every candidate is compared (no early exit), so timing does not reveal which one matched.
        matches = [hmac.compare_digest(presented, known) for known in live.bearer_tokens]
        ok = scheme.lower() == "bearer" and any(matches) and bool(live.bearer_tokens)
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

    def _check_open(self, conn: _Conn) -> None:
        """Refuse work for a peer whose certificate was revoked, or after the channel was undeployed. Nothing of
        the request has been published when this raises, so the sender is told and may retry (invariant 1)."""
        if conn.revoked:
            raise _HttpError(403, "Forbidden")
        if self._refusal is not None:
            self.metrics.nacked_not_accepting += 1
            raise _HttpError(503, "Service Unavailable", retry_after_s=1.0)

    async def _handle_request(self, r: asyncio.StreamReader, remote: str, ctx: dict[str, Any], conn: _Conn) -> bytes:
        live = self._live  # one spec for this whole request, from the head to the stamped envelope
        cfg = live.cfg
        request_deadline = time.monotonic() + cfg.request_timeout_s
        head, rest = await self._read_head(r, request_deadline)
        method, path, headers = self._parse_head(head)
        ctx["method"] = method if method in _KNOWN_METHODS else "OTHER"
        self._check_open(conn)
        if path != cfg.path:
            raise _HttpError(404, "Not Found")
        if method != "POST":
            raise _HttpError(405, "Method Not Allowed", headers={"Allow": "POST"})
        self._check_auth(live, headers)  # before the body is read or anything is reserved

        content_type = _tb_content_type(self._header(headers, "content-type") or "", cfg.accept)
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
        if content_length > cfg.max_message_bytes:
            self.metrics.nacked_oversize += 1
            raise _HttpError(413, "Payload Too Large")  # decided from the header: no body byte is read
        if content_length == 0:
            self.metrics.nacked_parse_error += 1
            raise _HttpError(400, "Bad Request")

        # Reserved before the body is read, so N slow connections cannot buffer N x maxMessageBytes
        # outside the byte budget. A wait longer than the ACK timeout is a `queue-full` refusal.
        try:
            reserved = await self.budget.reserve(content_length, cfg.ack_timeout_s)
        except BudgetExceeded:
            # Only reachable if a live update shrank the queue below a request already admitted by the size
            # check; `apply_settings` makes that a single step, so treat it as oversize.
            self.metrics.nacked_oversize += 1
            raise _HttpError(413, "Payload Too Large") from None
        if not reserved:
            self.metrics.nacked_queue_full += 1
            raise _HttpError(503, "Service Unavailable", retry_after_s=1.0)
        try:
            body = await self._read_body(r, rest, content_length, request_deadline)
            ctx["bytes"] = len(body)
            self.metrics.messages_received += 1
            if "parse-error" in cfg.nack_on and not well_formed(content_type, body):
                self.metrics.nacked_parse_error += 1
                raise _HttpError(400, "Bad Request")
            outcome = resolve_key(
                cfg.key_expression,
                payload=body,
                content_type=content_type,
                headers=[(name, value.encode("utf-8")) for name, value in headers],
                header_source="http",
            )
            if outcome.reason is not None:
                self.metrics.nacked_parse_error += 1
                raise _HttpError(400, "Bad Request")  # unresolved per-key: a parse-error NACK (README 4.3)
            message = build_raw_envelope(
                channel=cfg.channel,
                channel_version=cfg.channel_version,
                source=f"http:{cfg.instance}@{remote}",
                payload=body,
                key=outcome.key,
                content_type=content_type,
            )
            msg_id = message.require(h.TB_MSG_ID)
            ctx["msg_id"] = msg_id
            # Tombstoned or revoked while this request waited for the queue or for its body: not published.
            self._check_open(conn)
            conn.publishing = True  # from here its answer is owed: revocation and drain wait for it
            # The ACK budget starts now, once the body is in: a slow upload must not eat it and turn a
            # healthy Kafka into a 503 (whose retry would duplicate the message).
            ack_deadline = time.monotonic() + cfg.ack_timeout_s
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
        cfg = self.cfg
        if self._refusal is not None or self.metrics.connections_open >= cfg.max_conns:
            self.metrics.connections_refused += 1
            w.close()
            return
        if cfg.tls is not None and cfg.tls.allowed_client_sans:
            ssl_object = w.get_extra_info("ssl_object")
            if not peer_san_allowed(ssl_object, cfg.tls.allowed_client_sans):
                # README 3.2.7: a bare 403, sent without reading the request; never 401, never 5xx.
                self.metrics.rejected_peer_not_allowed += 1
                log_peer_refused(cfg.channel, ssl_object)
                try:
                    w.write(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                    await asyncio.wait_for(w.drain(), cfg.write_timeout_s)
                except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
                    pass
                finally:
                    w.close()
                return
        self.metrics.connections_total += 1
        self.metrics.connections_open += 1
        conn = _Conn(w.get_extra_info("ssl_object"))
        self._conns[w] = conn
        ctx: dict[str, Any] = {}
        lingering = False
        try:
            try:
                response = await self._handle_request(r, remote, ctx, conn)
                status = 202
            except _HttpError as exc:
                extra = dict(exc.headers)
                if exc.retry_after_s:
                    extra["Retry-After"] = str(max(1, int(exc.retry_after_s)))
                body = json.dumps({"error": exc.reason}).encode("ascii")
                response = _encode_response(exc.status, exc.reason, body, extra_headers=extra)
                status = exc.status
                lingering = exc.status in (400, 401, 403, 404, 405, 411, 413, 415, 417, 431)
            except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
                return  # idle/slow/reset peer, or a connection closed by a revocation: no response can be written
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
            conn.publishing = False
            self.metrics.connections_open -= 1
            self._conns.pop(w, None)
            w.close()

    async def start(self) -> asyncio.base_events.Server:
        """Start listening. Known bound, not counted: a TLS handshake in progress is not yet a connection
        (asyncio calls the handler after the handshake), so it does not count against `max_conns`. Half-open
        handshakes are bounded by `tls_handshake_timeout_s` and the listen backlog; a plaintext-to-TLS flood
        is the operator's edge-firewall concern. Counting them needs a custom protocol factory, not done."""
        self._listener = await asyncio.start_server(
            self.handle,
            self.cfg.bind,
            self.cfg.port,
            ssl=self._ssl_context,
            backlog=1024,
            ssl_handshake_timeout=self.cfg.tls_handshake_timeout_s if self._ssl_context else None,
        )
        return self._listener


def main() -> None:
    """`python -m tributary.receivers.http --channel <name> --env <env>`.

    The channel spec is read from the compacted `_tb.<env>.channels` topic over the engine Kafka link
    (invariant 3); there is no file input. The listener opens only after a valid spec for `--channel` has
    been read. Exit codes: 0 after SIGTERM/SIGINT or after a spec change that needs a restart (the
    supervisor restarts on 0), 2 for a refused startup configuration."""
    from tributary.runtime.config import load_topic_defaults
    from tributary.runtime.errors import RuntimeConfigurationError

    from ..cli import QuietArgumentParser, run_until_stopped, spec_feed_opener
    from ..startup import refuse, refuse_retired_channel_spec_flag, spec_reader_group_or_exit
    from .runner import HttpRunner, RunnerOptions

    argv = sys.argv[1:]
    log = JsonLogger("receiver-http")
    # Before parse_args: argparse echoes an unknown flag's value to stderr, so a retired
    # --bootstrap / --engine-security / --channel-spec must reach its refusal first.
    kafka = load_engine_kafka_or_exit(argv=argv, log=log)
    refuse_retired_channel_spec_flag(argv, log)
    p = QuietArgumentParser(log, description=__doc__)
    p.add_argument("--channel", required=True, help="the deployed channel name (spec.metadata.name)")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--instance", default="http-0", help="this process's identity, for tb-source")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
    p.add_argument("--metrics-file")
    a = p.parse_args(argv)
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    # Operator input is validated (and refused without being echoed) before it becomes a log field.
    spec_group = spec_reader_group_or_exit(a.env, a.channel, a.instance, log)
    log = JsonLogger("receiver-http", base={"channel": a.channel, "instance": a.instance})
    try:
        defaults = load_topic_defaults(a.contracts_dir)
    except RuntimeConfigurationError as exc:
        refuse(log, error=type(exc).__name__, code=exc.code)
    try:
        topic = topic_name(a.env, a.channel)
    except ValueError:
        refuse(log, error="UsageError", code="ARGUMENTS_INVALID", reason="invalid --env or --channel")
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else None

    def make_publisher(cfg: HttpSettings) -> RawPublisher:
        publisher = RawPublisher(
            kafka,
            topic,
            linger_ms=cfg.linger_ms,
            queue_max_messages=cfg.max_inflight * 2,
            topic_defaults=defaults,
            ordering="per-key" if cfg.key_expression is not None else "none",
            client_id_prefix="http-receiver",
        )
        publisher.start(asyncio.get_running_loop())
        return publisher

    def make_server(cfg: HttpSettings, publisher: RawPublisher, metrics: Metrics) -> HttpServer:
        return HttpServer(cfg, publisher, metrics=metrics, secret_resolver=resolver)

    runner = HttpRunner(
        open_feed=spec_feed_opener(
            kafka=kafka, defaults=defaults, env=a.env, channel=a.channel, group=spec_group, log=log
        ),
        make_publisher=make_publisher,
        make_server=make_server,
        options=RunnerOptions(env=a.env, instance=a.instance, topic=topic),
        log=log,
        metrics_file=a.metrics_file,
    )
    run_until_stopped(runner)


if __name__ == "__main__":
    main()
