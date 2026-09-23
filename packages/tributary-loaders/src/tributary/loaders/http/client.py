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
as an ordinary, classified response (`loader.classify_status`'s `http-3xx`) instead of a followed hop.
"""

from __future__ import annotations

import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Request:
    method: str
    url: str
    headers: dict[str, str]
    body: bytes
    timeout_s: float


@dataclass(frozen=True, slots=True)
class Response:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


class TransportError(Exception):
    """The request never got a response at all: DNS failure, connection refused, TLS failure, a
    timeout. Always retryable (`loader.py`'s classification), never a destination's own answer."""


_SSL_CONTEXT = ssl.create_default_context()  # verifies against the OS trust store; TLS on by default


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Declines every redirect: `redirect_request` returning `None` tells urllib not to follow it,
    so the 3xx response propagates (as an `HTTPError`, caught below) instead of a second request
    ever being made — no dropped body, no leaked `Authorization` header, no unchecked target URL."""

    def redirect_request(self, req: object, fp: object, code: int, msg: str, headers: object, newurl: str) -> None:
        return None


#: One opener, built once: `HTTPSHandler(context=...)` carries the same trust-store verification
#: `urlopen`'s `context=` argument would have applied; `_NoRedirectHandler` replaces the default
#: (redirect-following) handler because `build_opener` drops any default handler a passed-in one
#: subclasses.
_OPENER = urllib.request.build_opener(_NoRedirectHandler, urllib.request.HTTPSHandler(context=_SSL_CONTEXT))


def stdlib_send(request: Request) -> Response:
    """Synchronous; callers on an event loop must run this via `asyncio.to_thread`."""
    urllib_request = urllib.request.Request(  # noqa: S310 - url is channel-spec-authored, not from the message
        request.url, data=request.body or None, headers=request.headers, method=request.method
    )
    try:
        with _OPENER.open(urllib_request, timeout=request.timeout_s) as resp:
            return Response(resp.status, dict(resp.headers), resp.read())
    except urllib.error.HTTPError as exc:
        # a non-2xx (or, now, non-redirected 3xx) response IS a response: read it (for status/body
        # classification) rather than let it surface as an exception, which is what urllib does by
        # default for anything outside 200-299 that no handler resolves.
        return Response(exc.code, dict(exc.headers or {}), exc.read())
    except (urllib.error.URLError, TimeoutError, OSError, ssl.SSLError) as exc:
        raise TransportError(str(exc)) from exc
