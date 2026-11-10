"""The HTTP transport: standard library only (`urllib.request` + `ssl`), run off the event loop via
`asyncio.to_thread`.

Not `httpx` or `aiohttp`: `httpx` pulls in `certifi` (MPL-2.0), which needs architect/human approval
as an added dependency (CLAUDE.md: "MPL only as an unmodified dependency approved by the architect")
and this loader does not need a whole HTTP client library to POST/PUT/PATCH a payload and read a
status code. `ssl.create_default_context()` already verifies against the OS trust store — the
behaviour "TLS verification on by default" needs — with no extra dependency at all.

`send` is a free function taking a `Request` and returning a `Response`, injected into `HttpLoader`
so tests can swap in a fake without a real socket; the default is `stdlib_send`, wrapped for
`asyncio` by `HttpLoader` itself.

**Redirects are refused, never followed** (security review, gate finding, High). `urlopen`'s default
opener has an active `HTTPRedirectHandler`, and following a redirect silently is a chain of three
problems: `redirect_request` turns a POST + 301/302/303 into a bodyless GET, so a destination that
moved gets nothing while this loader still sees a 200 and calls the record delivered — the record's
PHI payload never actually arrives anywhere (invariant 4, silent message loss, not just a duplicate);
`Authorization` is not stripped across a redirect, so a bearer token or basic-auth credential leaks
to whatever the `Location` header names; and that target is not checked against the channel spec's
`http://` loopback-only rule (`contracts/channel-spec/v0`), so a 302 can point a credential at a
plaintext, non-loopback URL the spec would never have allowed as the configured destination. `_OPENER`
below replaces the default redirect handler with one that declines every redirect, so a 3xx surfaces
as an ordinary, classified response (`loader.classify_status`'s `HTTP_REDIRECT_REFUSED`) instead of a followed hop.
"""

from __future__ import annotations

import http.client
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from ..deadline import DeadlineHTTPHandler, DeadlineHTTPSHandler, DeadlineRequest, Watchdog
from ..tls import default_client_context

#: Default cap on how much of a response body `stdlib_send` reads (`Request.max_response_bytes`). A
#: destination's answer is a status line plus, at most, a small acknowledgement; nothing here needs
#: an unbounded read, and a hostile or broken peer must not be able to make this process buffer one.
DEFAULT_MAX_RESPONSE_BYTES = 1 << 20


@dataclass(frozen=True, slots=True)
class Request:
    method: str
    url: str
    #: `repr=False`: this dict carries the `Authorization` header, and a dataclass repr would print it
    #: (security review, Medium) in any traceback, debug line or assertion message.
    headers: dict[str, str] = field(repr=False)
    body: bytes
    timeout_s: float
    #: Stop reading the response body here (`Response.truncated` says whether the cap was hit).
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES


@dataclass(frozen=True, slots=True)
class Response:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    #: True if the body was longer than `Request.max_response_bytes` (or the overall deadline passed
    #: before it ended) and `body` is only a prefix. A caller that must inspect the whole body has to
    #: treat this as "cannot verify".
    truncated: bool = False


class TransportError(Exception):
    """The request never got a response at all: DNS failure, connection refused, a non-certificate TLS
    failure (a reset mid-handshake, `SSLEOFError`, ...), a timeout. Always retryable (`loader.py`'s
    classification), never a destination's own answer."""


class TransportTimeout(TransportError):
    """`TransportError` whose cause was a timeout (connect, read, or the overall deadline). Still
    retryable; a subclass so existing `except TransportError` call sites are unchanged."""


class InvalidRequestError(TransportError):
    """The request could not be built or sent because a part of it is malformed (for example a header
    value with a line break). The message is fixed: the offending value is never in it (`http.client`'s
    own `ValueError` quotes the whole header value, which for `Authorization` is the credential)."""


class TlsVerificationError(Exception):
    """The TLS **certificate verification** failed: an unknown CA or a hostname mismatch
    (`ssl.SSLCertVerificationError`, matching the `clickhouse` loader's identical split, security
    review). Never retryable — a bad CA or hostname mismatch fails identically on every attempt, unlike
    other `ssl.SSLError` subclasses (a reset connection mid-handshake), which stay a retryable
    `TransportError`."""


#: Verifies against the OS trust store; TLS on by default; TLS 1.2+ with AEAD suites only (`tls.py`).
_SSL_CONTEXT = default_client_context()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Declines every redirect: `redirect_request` returning `None` tells urllib not to follow it,
    so the 3xx response propagates (as an `HTTPError`, caught below) instead of a second request
    ever being made — no dropped body, no leaked `Authorization` header, no unchecked target URL."""

    def redirect_request(self, req: object, fp: object, code: int, msg: str, headers: object, newurl: str) -> None:
        return None


#: One opener, built once: `HTTPSHandler(context=...)` carries the same trust-store verification
#: `urlopen`'s `context=` argument would have applied; `_NoRedirectHandler` replaces the default
#: (redirect-following) handler because `build_opener` drops any default handler a passed-in one
#: subclasses. `ProxyHandler({})` (code review, High, found on the clickhouse loader's identical
#: opener): without it, `build_opener` installs a `ProxyHandler` that reads `HTTP_PROXY`/`HTTPS_PROXY`
#: from the environment, so an operator's proxy configuration for this process's outbound internet
#: traffic would silently also apply to this loader's credentialed, PHI-carrying destination requests.
#: An empty mapping disables that environment lookup entirely.
_OPENER = urllib.request.build_opener(
    _NoRedirectHandler, urllib.request.ProxyHandler({}), DeadlineHTTPHandler(), DeadlineHTTPSHandler(_SSL_CONTEXT)
)


def stdlib_send(request: Request, *, opener: urllib.request.OpenerDirector = _OPENER) -> Response:
    """Synchronous; callers on an event loop must run this via `asyncio.to_thread`.

    `opener` is **test-only** (code review): it defaults to the module-level `_OPENER` singleton, and
    no production call site ever passes a different one. It exists solely so a test can inject a
    different `SSLContext` (a throwaway CA, to prove the TLS failure classification against a real
    handshake: `tests/http/test_tls_failures.py`) without reloading this module — production code must
    never build a second opener per call, exactly what `_OPENER` being a module-level singleton is
    for."""
    # `timeout_s` is a per-socket-operation bound in urllib (connect, each recv). `Watchdog` makes it an
    # overall bound on everything after the connection is up (status line, headers, body): a peer
    # trickling bytes cannot hold a delivery open. `_read_capped` still checks the deadline between reads.
    watchdog = Watchdog(request.timeout_s)
    try:
        response = _send_once(request, opener, watchdog)
        if watchdog.fired_in_headers:
            raise TransportTimeout("timed out")  # a header block cut short is not a response
        return response
    except TransportError as exc:
        if watchdog.fired and not isinstance(exc, TransportTimeout):
            raise TransportTimeout("timed out") from None
        raise
    finally:
        watchdog.cancel()


def _send_once(request: Request, opener: urllib.request.OpenerDirector, watchdog: Watchdog) -> Response:
    urllib_request = DeadlineRequest(  # url is channel-spec-authored, not from the message
        request.url,
        data=request.body or None,
        headers=request.headers,
        method=request.method,
        watchdog=watchdog,
    )
    deadline = watchdog.deadline
    try:
        with opener.open(urllib_request, timeout=request.timeout_s) as resp:
            watchdog.headers_complete()
            body, truncated = _read_capped(resp, limit=request.max_response_bytes, deadline=deadline)
            return Response(resp.status, dict(resp.headers), body, truncated)
    except urllib.error.HTTPError as exc:
        # a non-2xx (or, now, non-redirected 3xx) response IS a response: read it (for status/body
        # classification) rather than let it surface as an exception, which is what urllib does by
        # default for anything outside 200-299 that no handler resolves.
        watchdog.headers_complete()
        try:
            body, truncated = _read_capped(exc, limit=request.max_response_bytes, deadline=deadline)
        except (ssl.SSLError, TimeoutError, OSError) as read_exc:
            raise _transport_error(read_exc) from None
        except http.client.HTTPException as read_exc:
            raise _malformed_response(read_exc) from None
        return Response(exc.code, dict(exc.headers or {}), body, truncated)
    except urllib.error.URLError as exc:
        # `urlopen` wraps a handshake failure in `URLError(reason=...)` rather than letting the
        # `ssl.SSLError` escape directly (security review, Low, found on the clickhouse loader's
        # identical pattern): a bare `except ssl.SSLError` here would never actually run against a
        # real connection, silently misclassifying a hostname mismatch or an unknown CA as
        # `TransportError`/retryable. Only `SSLCertVerificationError` (an unknown CA or a hostname
        # mismatch, both) is `TlsVerificationError`/non-retryable.
        if isinstance(exc.reason, ssl.SSLCertVerificationError):
            raise TlsVerificationError("certificate verification failed") from None
        raise _transport_error(exc.reason if isinstance(exc.reason, BaseException) else exc) from None
    except ssl.SSLCertVerificationError:
        # In case a certificate failure is ever raised directly rather than wrapped in URLError (not
        # observed against a real connection, but cheap defence in depth).
        raise TlsVerificationError("certificate verification failed") from None
    except (ssl.SSLError, TimeoutError, OSError) as exc:
        raise _transport_error(exc) from None
    except http.client.HTTPException as exc:
        # A malformed status line (`BadStatusLine` quotes the peer's line verbatim), a bad header block,
        # a short body (`IncompleteRead`): peer text or peer behaviour, never echoed (security review).
        raise _malformed_response(exc) from None
    except ValueError:
        # `http.client` rejects a header name or value containing a control character with a
        # `ValueError` that quotes the whole value, an `Authorization` credential included.
        raise InvalidRequestError("request is malformed") from None


def _read_capped(readable: object, *, limit: int, deadline: float) -> tuple[bytes, bool]:
    """At most `limit` bytes, stopping at `deadline` even if the peer keeps trickling. Returns the
    bytes and whether more was (or may have been) left unread."""
    # `read1`, not `read`: `HTTPResponse.read(n)` keeps reading until it has `n` bytes or EOF, so one
    # trickling peer would sit inside a single call for as long as it liked and the deadline below would
    # never be looked at. `read1(n)` returns after one underlying socket read (itself bounded by the
    # socket timeout), so the overall bound is the deadline plus at most one socket timeout.
    read = getattr(readable, "read1", None) or readable.read  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    total = 0
    while total < limit:
        if time.monotonic() > deadline:
            return b"".join(chunks), True
        chunk = read(min(65536, limit - total))
        if not chunk:
            # An EOF at or after the deadline is the watchdog shutting the socket down, not the peer
            # finishing: the body is a prefix.
            return b"".join(chunks), time.monotonic() > deadline
        chunks.append(chunk)
        total += len(chunk)
    # cap reached: is there anything after it?
    more = read(1)
    return b"".join(chunks), bool(more)


def _malformed_response(cause: BaseException) -> TransportError:
    """Fixed phrase and the exception class only: never the peer's status line or header text."""
    return TransportError(f"malformed response ({type(cause).__name__})")


def _transport_error(cause: BaseException) -> TransportError:
    """A fixed-phrase error naming only the exception *class*: an `OSError`'s text can carry the peer
    address or a path, and a `URLError`'s carries the URL (invariant 6: no peer text in exceptions)."""
    if isinstance(cause, TimeoutError):
        return TransportTimeout("timed out")
    return TransportError(f"transport failure ({type(cause).__name__})")
