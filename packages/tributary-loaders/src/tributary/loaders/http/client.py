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


def stdlib_send(request: Request) -> Response:
    """Synchronous; callers on an event loop must run this via `asyncio.to_thread`."""
    urllib_request = urllib.request.Request(  # noqa: S310 - url is channel-spec-authored, not from the message
        request.url, data=request.body or None, headers=request.headers, method=request.method
    )
    try:
        with urllib.request.urlopen(  # noqa: S310 - same as above; scheme is checked by the channel spec's own pattern
            urllib_request, timeout=request.timeout_s, context=_SSL_CONTEXT
        ) as resp:
            return Response(resp.status, dict(resp.headers), resp.read())
    except urllib.error.HTTPError as exc:
        # a non-2xx response IS a response: read it (for status/body-based classification) rather
        # than let it surface as an exception, which is what urllib does for every HTTPError.
        return Response(exc.code, dict(exc.headers or {}), exc.read())
    except (urllib.error.URLError, TimeoutError, OSError, ssl.SSLError) as exc:
        raise TransportError(str(exc)) from exc
