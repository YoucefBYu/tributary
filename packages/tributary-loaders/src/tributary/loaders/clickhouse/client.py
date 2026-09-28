"""The ClickHouse transport: standard library only (`urllib.request` + `ssl`), same shape as
`http/client.py` and for the same reason (that module's docstring: no `httpx`/`aiohttp`, both of which
pull in `certifi`, MPL-2.0, which needs its own architect/human approval as a *runtime* dependency —
`clickhouse-connect` was tried and reverted for exactly this, `config.py`'s module docstring).

ClickHouse's **HTTP interface**: a query or `INSERT` is one `POST` with the SQL as the request body
(`?database=<db>&wait_end_of_query=1` on the query string — never a credential; see below), and the
server reports success as a `2xx` with the result body, or a query error as a **non-2xx status** (`404`
for `UNKNOWN_TABLE`/`UNKNOWN_DATABASE`, `500` for most others — ClickHouse's own status mapping, not
this loader's) carrying `X-ClickHouse-Exception-Code` (the numeric code `loader.py` turns into
`CH_<code>`) and a body starting `Code: <n>. DB::Exception: ... (<SYMBOLIC_NAME>) (version ...)`.

**`wait_end_of_query=1` (code review, Critical).** Without it, ClickHouse can send the `200` status line
and headers *before* the query has actually finished — the query then fails after the response has
already started, and the failure can only be seen by scanning the body for the `Code: <n>.` prefix even
on a `2xx`. `loader.py` does that scan unconditionally, on every response, as defence in depth: this
setting is a request to the server, not a guarantee this loader controls, and treating it as sufficient
on its own is the exact silent-success shape that finding described (invariant 1/4 — a row that was
never inserted must never look like a success).

**No connection pooling, no automatic retries, no redirects, no environment proxy** — the runtime's
retry policy (`retry.policy: blocking`, `ConsumerLoop`) is the only retry, matching `http/client.py`'s
rule and the human decision that named this shape explicitly. Every request carries `Connection: close`
so a wedged or since-rotated connection is never silently reused across delivery attempts. **`ProxyHandler({})`
(code review, High)**: `urllib.request.build_opener` otherwise installs a `ProxyHandler` that reads
`HTTP_PROXY`/`HTTPS_PROXY` from the environment, so an operator's proxy configuration — meant for this
process's *outbound internet* traffic — would silently also apply to this loader's PHI-carrying,
credentialed requests to a destination that is never supposed to leave the operator's own network. An
empty mapping disables that lookup entirely; `http/client.py`'s `_OPENER` gets the identical fix.

**Auth is header-based, never in the URL.** `X-ClickHouse-User`/`X-ClickHouse-Key` (ClickHouse's own
HTTP auth headers) carry the resolved username/password; the query string carries only `database` and
`wait_end_of_query`. A proxy or access log records the request line and headers separately, and neither
this loader nor the channel spec puts a secret where the request LINE would show it.

**TLS.** `tls.secret_ref` **pins** trust to that one CA bundle: a fresh `ssl.SSLContext(ssl.PROTOCOL_
TLS_CLIENT)` loaded with `load_verify_locations(cafile=...)` and nothing else, so a certificate that
chains to the OS trust store but not to the named bundle is rejected — the deliberate reading of
`$defs/clientTls`'s "secretRef, when present, names a trust bundle" as *the* trust anchor for this
destination, not an addition to every publicly-trusted CA. No `tls.secret_ref` means
`ssl.create_default_context()`, the stdlib default (OS trust store, certificate and hostname
verification on). Either way hostname verification is always on; there is no field that can turn it off.

A TLS failure raises `TlsVerificationError`, kept **distinct from `TransportError`** so `loader.py` can
classify it `TLS_VERIFY_FAILED`/non-retryable instead of `CONNECT_FAILED`/retryable (code review, Low —
folding it into `TransportError` made a certificate problem retry forever, identically, on every attempt).

**Read caps and an overall deadline (code review, Medium).** `resp.read()`/`exc.read()` with no argument
reads until EOF with no size bound; `_read_capped` stops at `_MAX_BODY_BYTES` (64 KiB — this loader only
ever needs the `Code: <n>.` prefix and a short summary, never the full body) and also stops if
`time.monotonic()` passes the deadline computed from `timeout_s` at the start of `send()`, so a peer that
trickles bytes just under the per-`recv` socket timeout cannot hold this call open past the request's own
budget by resetting that timer forever. An `OSError` raised while reading an *error* body (`exc.read()`)
is caught and re-raised as `TransportError` rather than escaping untyped (code review, Low).
"""

from __future__ import annotations

import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from ..secrets import SecretResolver
from .config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig

#: This loader only ever needs a short summary or the `Code: <n>.` prefix (`loader.py`); anything
#: beyond this is a size an operator investigates on the server, not text this process should buffer.
_MAX_BODY_BYTES = 65536


@dataclass(frozen=True, slots=True)
class Response:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


class TransportError(Exception):
    """The request never got a response at all: DNS failure, connection refused, a timeout, or an
    I/O error while reading one. Always retryable (`loader.py`'s classification), never the server's
    own answer."""


class TlsVerificationError(Exception):
    """The TLS handshake failed: certificate or hostname verification. Kept distinct from
    `TransportError` (module docstring) — never retryable, since a bad CA or hostname mismatch fails
    identically on every attempt."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Declines every redirect, for the same reason `http/client.py`'s copy does: a 3xx must surface
    as an ordinary, classified response, never a silently followed hop that could drop a body or leak
    the `X-ClickHouse-Key` header to an unchecked target."""

    def redirect_request(self, req: object, fp: object, code: int, msg: str, headers: object, newurl: str) -> None:
        return None


def _build_ssl_context(tls: ChTlsSpec, auth: ChAuthSpec, resolver: SecretResolver) -> ssl.SSLContext | None:
    if tls.mode == "disable":
        return None
    if tls.secret_ref is not None:
        # Pin trust to this one bundle (module docstring) rather than adding to the OS trust store.
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cafile=str(resolver.resolve_file(tls.secret_ref, "ca.crt")))
    else:
        ctx = ssl.create_default_context()
    if auth.type == "client-certificate":
        ctx.load_cert_chain(
            certfile=str(resolver.resolve_file(auth.secret_ref, "tls.crt")),
            keyfile=str(resolver.resolve_file(auth.secret_ref, "tls.key")),
        )
    return ctx


def _read_capped(readable: object, *, limit: int, deadline: float) -> bytes:
    """Reads at most `limit` bytes, stopping early once `deadline` (a `time.monotonic()` value) has
    passed even if the peer keeps trickling data (module docstring)."""
    # `read1`, not `read`: `HTTPResponse.read(n)` loops until it has `n` bytes or EOF, so a trickling
    # peer would hold one call open and the deadline check below would never run.
    read = getattr(readable, "read1", None) or readable.read  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    total = 0
    while total < limit:
        if time.monotonic() > deadline:
            break
        chunk = read(min(65536, limit - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


@dataclass(frozen=True, slots=True)
class ClickhouseTransport:
    """One opener plus the fixed parts of every request to one destination: base URL, TLS context and
    the auth header this destination's `secretRef` resolved to at startup. Built once per
    `ClickhouseLoader` (like `postgresql`'s pool, `http`'s executor) — not reopened per message.

    `auth_headers` carries `X-ClickHouse-Key` (a password): `repr=False` keeps it out of any accidental
    `repr()`/log of this object (code review, Medium — the default dataclass `repr` would otherwise
    print it verbatim, e.g. from an uncaught exception's traceback locals)."""

    base_url: str
    auth_headers: dict[str, str] = field(repr=False)
    opener: urllib.request.OpenerDirector = field(repr=False)

    def send(self, sql: bytes, *, timeout_s: float) -> Response:
        deadline = time.monotonic() + timeout_s
        headers = {**self.auth_headers, "Connection": "close", "Content-Type": "text/plain; charset=utf-8"}
        request = urllib.request.Request(self.base_url, data=sql, headers=headers, method="POST")  # noqa: S310
        try:
            with self.opener.open(request, timeout=timeout_s) as resp:
                body = _read_capped(resp, limit=_MAX_BODY_BYTES, deadline=deadline)
                return Response(resp.status, dict(resp.headers), body)
        except urllib.error.HTTPError as exc:
            # A non-2xx IS a response (ClickHouse's own error reporting, module docstring): read it for
            # classification rather than let it surface as a bare exception.
            try:
                body = _read_capped(exc, limit=_MAX_BODY_BYTES, deadline=deadline)
            except OSError as read_exc:
                raise TransportError(str(read_exc)) from read_exc
            return Response(exc.code, dict(exc.headers or {}), body)
        except urllib.error.URLError as exc:
            # `urlopen` wraps a handshake failure in `URLError(reason=...)` rather than letting the
            # `ssl.SSLError` escape directly (security review, Low — the `except ssl.SSLError` below
            # never actually ran against a real connection, so a hostname mismatch or an unknown CA
            # was misclassified `CONNECT_FAILED`/retryable). Only a **certificate-verification**
            # failure is `TlsVerificationError`/non-retryable: `SSLCertVerificationError` covers both
            # an unknown CA and a hostname mismatch (both confirmed against a real listener,
            # `tests/clickhouse/test_tls_failures.py`).
            # Every other `ssl.SSLError` (`SSLEOFError`, `SSLZeroReturnError`, ...), raised mid-read on
            # a reset connection, is transient and stays a retryable `TransportError`.
            if isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise TlsVerificationError(str(exc.reason)) from exc
            raise TransportError(str(exc)) from exc
        except ssl.SSLCertVerificationError as exc:
            # In case a certificate failure is ever raised directly rather than wrapped in URLError
            # (not observed against a real connection, but cheap defence in depth).
            raise TlsVerificationError(str(exc)) from exc
        except (ssl.SSLError, TimeoutError, OSError) as exc:
            raise TransportError(str(exc)) from exc


def transport_from_config(config: ClickhouseDestinationConfig, resolver: SecretResolver) -> ClickhouseTransport:
    ssl_context = _build_ssl_context(config.tls, config.auth, resolver)
    scheme = "http" if config.tls.mode == "disable" else "https"
    query = urllib.parse.urlencode({"database": config.database, "wait_end_of_query": "1"})
    base_url = f"{scheme}://{config.host}:{config.port}/?{query}"

    auth_headers: dict[str, str] = {"X-ClickHouse-User": resolver.resolve_text(config.auth.secret_ref, "user")}
    if config.auth.type == "password":
        auth_headers["X-ClickHouse-Key"] = resolver.resolve_text(config.auth.secret_ref, "password")

    opener = urllib.request.build_opener(
        _NoRedirectHandler,
        urllib.request.ProxyHandler({}),  # never pick up HTTP_PROXY/HTTPS_PROXY (code review, High)
        urllib.request.HTTPSHandler(context=ssl_context) if ssl_context is not None else urllib.request.HTTPHandler(),
    )
    return ClickhouseTransport(base_url, auth_headers, opener)
