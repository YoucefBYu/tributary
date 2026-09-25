"""The ClickHouse transport: standard library only (`urllib.request` + `ssl`), same shape as
`http/client.py` and for the same reason (that module's docstring: no `httpx`/`aiohttp`, both of which
pull in `certifi`, MPL-2.0, which needs its own architect/human approval as a *runtime* dependency —
`clickhouse-connect` was tried and reverted for exactly this, `config.py`'s module docstring).

ClickHouse's **HTTP interface**: a query or `INSERT` is one `POST` with the SQL as the request body
(`?database=<db>` on the query string — never a credential; see below), and the server reports success
as any `2xx` with the result body, or a query error as a **non-2xx status** (`404` for
`UNKNOWN_TABLE`/`UNKNOWN_DATABASE`, `500` for most others — ClickHouse's own status mapping, not this
loader's) carrying `X-ClickHouse-Exception-Code` (the numeric code `loader.py` turns into `CH_<code>`)
and a body starting `Code: <n>. DB::Exception: ... (<SYMBOLIC_NAME>) (version ...)`. Both are read here;
`loader.py` decides what they mean.

**No connection pooling, no automatic retries, no redirects** — the runtime's retry policy
(`retry.policy: blocking`, `ConsumerLoop`) is the only retry, matching `http/client.py`'s rule and the
human decision that named this shape explicitly. Every request carries `Connection: close` so a
wedged or since-rotated connection is never silently reused across delivery attempts (also what makes
`tests/clickhouse/test_integration_clickhouse_loader.py`'s connection-count-based genuine-redelivery
fault deterministic, the way `http/loader.py`'s executor-per-request already is for HTTP).

**Auth is header-based, never in the URL.** `X-ClickHouse-User`/`X-ClickHouse-Key` (ClickHouse's own
HTTP auth headers) carry the resolved username/password; the query string carries only `database`. A
proxy or access log records the request line and headers separately, and neither this loader nor the
channel spec puts a secret where the request LINE would show it (the same rule `postgresql`'s
`auth.secretRef` and `http`'s `Authorization` header both already follow).

**TLS**: `ssl.create_default_context()` — the stdlib default, verifying certificate and hostname against
the OS trust store, exactly like `http/client.py`'s `_SSL_CONTEXT`, unlike that module built **per
destination** here (not a single module-level constant) because `tls.secretRef` names an
additional trust bundle and `auth.type: client-certificate` loads a client cert, both of which vary per
`ClickhouseDestinationConfig`.
"""

from __future__ import annotations

import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from ..secrets import SecretResolver
from .config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig


@dataclass(frozen=True, slots=True)
class Response:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


class TransportError(Exception):
    """The request never got a response at all: DNS failure, connection refused, a timeout. Always
    retryable (`loader.py`'s classification), never the server's own answer."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Declines every redirect, for the same reason `http/client.py`'s copy does: a 3xx must surface
    as an ordinary, classified response, never a silently followed hop that could drop a body or leak
    the `X-ClickHouse-Key` header to an unchecked target."""

    def redirect_request(self, req: object, fp: object, code: int, msg: str, headers: object, newurl: str) -> None:
        return None


def _build_ssl_context(tls: ChTlsSpec, auth: ChAuthSpec, resolver: SecretResolver) -> ssl.SSLContext | None:
    if tls.mode == "disable":
        return None
    ctx = ssl.create_default_context()
    if tls.secret_ref is not None:
        ctx.load_verify_locations(cafile=str(resolver.resolve_file(tls.secret_ref, "ca.crt")))
    if auth.type == "client-certificate":
        ctx.load_cert_chain(
            certfile=str(resolver.resolve_file(auth.secret_ref, "tls.crt")),
            keyfile=str(resolver.resolve_file(auth.secret_ref, "tls.key")),
        )
    return ctx


@dataclass(frozen=True, slots=True)
class ClickhouseTransport:
    """One opener plus the fixed parts of every request to one destination: base URL, TLS context and
    the auth header this destination's `secretRef` resolved to at startup. Built once per
    `ClickhouseLoader` (like `postgresql`'s pool, `http`'s executor) — not reopened per message."""

    base_url: str
    auth_headers: dict[str, str]
    opener: urllib.request.OpenerDirector

    def send(self, sql: bytes, *, timeout_s: float) -> Response:
        headers = {**self.auth_headers, "Connection": "close", "Content-Type": "text/plain; charset=utf-8"}
        request = urllib.request.Request(self.base_url, data=sql, headers=headers, method="POST")  # noqa: S310
        try:
            with self.opener.open(request, timeout=timeout_s) as resp:
                return Response(resp.status, dict(resp.headers), resp.read())
        except urllib.error.HTTPError as exc:
            # A non-2xx IS a response (ClickHouse's own error reporting, module docstring): read it for
            # classification rather than let it surface as a bare exception.
            return Response(exc.code, dict(exc.headers or {}), exc.read())
        except (urllib.error.URLError, TimeoutError, OSError, ssl.SSLError) as exc:
            raise TransportError(str(exc)) from exc


def transport_from_config(config: ClickhouseDestinationConfig, resolver: SecretResolver) -> ClickhouseTransport:
    ssl_context = _build_ssl_context(config.tls, config.auth, resolver)
    scheme = "http" if config.tls.mode == "disable" else "https"
    query = urllib.parse.urlencode({"database": config.database})
    base_url = f"{scheme}://{config.host}:{config.port}/?{query}"

    auth_headers: dict[str, str] = {"X-ClickHouse-User": resolver.resolve_text(config.auth.secret_ref, "user")}
    if config.auth.type == "password":
        auth_headers["X-ClickHouse-Key"] = resolver.resolve_text(config.auth.secret_ref, "password")

    opener = urllib.request.build_opener(
        _NoRedirectHandler,
        urllib.request.HTTPSHandler(context=ssl_context) if ssl_context is not None else urllib.request.HTTPHandler(),
    )
    return ClickhouseTransport(base_url, auth_headers, opener)
