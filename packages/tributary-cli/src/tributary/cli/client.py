"""A thin client over `contracts/openapi/v0`. It returns decoded JSON or raises a typed error; it never
echoes a response body that is not a contract Problem, and never puts the token anywhere but the
`Authorization` header."""

from __future__ import annotations

import json as jsonlib
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any
from urllib.parse import quote, urlencode

from .config import ClientConfig
from .errors import ApiError, UnreachableError

_PROBLEM_KEYS = ("code", "title")
MAX_RESPONSE_BYTES = 8 * 1024 * 1024

#: `(method, url, body, headers, timeout_s) -> (status, body)`. Raises `UnreachableError`. Tests pass a scripted one.
Transport = Callable[[str, str, bytes | None, dict[str, str], float], tuple[int, bytes]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect must not carry the token to another host: refuse to follow, and surface the 3xx."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def urllib_transport(
    method: str, url: str, body: bytes | None, headers: dict[str, str], timeout_s: float
) -> tuple[int, bytes]:
    """Standard library only: no third-party HTTP client (httpx would pull `certifi`, MPL-2.0, which the
    licence gate approves for the archiver alone). TLS verification uses the system trust store
    (`SSL_CERT_FILE` is honoured by OpenSSL); it is never turned off."""
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(url, data=body, method=method, headers=headers)  # noqa: S310 (scheme checked in config)
    try:
        with opener.open(request, timeout=timeout_s) as response:
            return response.status, bytes(response.read(MAX_RESPONSE_BYTES))
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, bytes(exc.read(MAX_RESPONSE_BYTES))
    except TimeoutError:
        raise UnreachableError("the API did not answer in time") from None
    except (urllib.error.URLError, OSError):
        raise UnreachableError("cannot reach the API") from None


def _seg(value: str | int) -> str:
    return quote(str(value), safe="")


class ApiClient:
    def __init__(self, config: ClientConfig, *, transport: Transport = urllib_transport) -> None:
        self._base = config.base_url
        self._timeout_s = config.timeout_s
        self._transport = transport
        self._headers = {"Authorization": f"Bearer {config.token}", "Accept": "application/json"}

    def close(self) -> None:
        """Nothing to release; kept so callers can use `with`."""

    def __enter__(self) -> ApiClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _call(self, method: str, path: str, *, json: Any = None, params: dict[str, Any] | None = None) -> Any:
        url = self._base + path + (f"?{urlencode(params)}" if params else "")
        headers = dict(self._headers)
        body: bytes | None = None
        if json is not None:
            body = jsonlib.dumps(json).encode()
            headers["Content-Type"] = "application/json"
        status, raw = self._transport(method, url, body, headers, self._timeout_s)
        if status >= 300:  # 3xx included: redirects are never followed
            raise _problem(status, raw)
        try:
            return jsonlib.loads(raw)
        except ValueError:
            raise ApiError(status, "BAD_RESPONSE", "the API answered with a body that is not JSON") from None

    # -- operations ------------------------------------------------------------------------------

    def validate(self, document: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = self._call("POST", "/v1/validate", json=document)
        return out

    def list_channels(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return self._pages("/v1/channels", limit)

    def get_channel(self, name: str) -> dict[str, Any]:
        out: dict[str, Any] = self._call("GET", f"/v1/channels/{_seg(name)}")
        return out

    def list_versions(self, name: str, *, limit: int = 200) -> list[dict[str, Any]]:
        return self._pages(f"/v1/channels/{_seg(name)}/versions", limit)

    def get_version(self, name: str, version: int) -> dict[str, Any]:
        out: dict[str, Any] = self._call("GET", f"/v1/channels/{_seg(name)}/versions/{_seg(version)}")
        return out

    def create_channel(self, document: dict[str, Any], origin: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = self._call("POST", "/v1/channels", json={"document": document, "origin": origin})
        return out

    def add_version(self, name: str, document: dict[str, Any], origin: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = self._call(
            "POST", f"/v1/channels/{_seg(name)}/versions", json={"document": document, "origin": origin}
        )
        return out

    def environments(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self._call("GET", "/v1/environments")["items"]
        return out

    def current_deployments(self, env: str, *, limit: int = 200) -> list[dict[str, Any]]:
        return self._pages(f"/v1/environments/{_seg(env)}/deployments", limit)

    def channel_deployments(self, env: str, channel: str, *, limit: int = 20) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = self._call(
            "GET", f"/v1/environments/{_seg(env)}/channels/{_seg(channel)}/deployments", params={"limit": limit}
        )["items"]
        return out

    def deploy(self, env: str, channel: str, version: int, *, expected: int | str | None = "absent") -> dict[str, Any]:
        body: dict[str, Any] = {"version": version}
        if expected != "absent":
            body["expected_current_version"] = expected
        out: dict[str, Any] = self._call(
            "POST", f"/v1/environments/{_seg(env)}/channels/{_seg(channel)}/deploy", json=body
        )
        return out

    def rollback(self, env: str, channel: str, *, expected: int, to_version: int | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"expected_current_version": expected}
        if to_version is not None:
            body["to_version"] = to_version
        out: dict[str, Any] = self._call(
            "POST", f"/v1/environments/{_seg(env)}/channels/{_seg(channel)}/rollback", json=body
        )
        return out

    def undeploy(self, env: str, channel: str, *, expected: int) -> dict[str, Any]:
        out: dict[str, Any] = self._call(
            "POST",
            f"/v1/environments/{_seg(env)}/channels/{_seg(channel)}/undeploy",
            json={"expected_current_version": expected},
        )
        return out

    def _pages(self, path: str, limit: int) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(1000):  # a bound, so a misbehaving server cannot loop the client
            params: dict[str, Any] = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            page = self._call("GET", path, params=params)
            items.extend(page["items"])
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return items


def _problem(status: int, raw: bytes) -> ApiError:
    code, title = "UNKNOWN", "the API refused the request"
    try:
        body = jsonlib.loads(raw)
        if isinstance(body, dict) and all(isinstance(body.get(k), str) for k in _PROBLEM_KEYS):
            code, title = body["code"][:64], body["title"][:120]
    except ValueError:
        pass
    return ApiError(status, code, title)
