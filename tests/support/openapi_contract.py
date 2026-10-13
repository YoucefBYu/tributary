"""Validate API responses against `contracts/openapi/v0/openapi.json`: the declared status, the declared content type
and the response schema, for every call a test makes. A QA-owned copy of the idea in the API package's test kit (which
is another package's test tree and not importable by name). An undeclared status fails, except 5xx, which must still be
a `Problem`.

`CheckedClient` wraps an `httpx.Client` and validates each `get`/`post` response on the way out; `check_event` validates
the `data` of a server-sent event against a named component schema (the SSE body itself is not JSON).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from jsonschema import Draft202012Validator, FormatChecker

OPENAPI = Path(__file__).resolve().parents[2] / "contracts" / "openapi" / "v0" / "openapi.json"


class OpenApiContract:
    def __init__(self) -> None:
        self.doc: dict[str, Any] = json.loads(OPENAPI.read_text(encoding="utf-8"))
        self._templates = [
            (re.compile("^" + re.sub(r"\\\{[^/]+?\\\}", "[^/]+", re.escape(template)) + "$"), template)
            for template in self.doc["paths"]
        ]
        self.checked = 0

    def template_for(self, path: str) -> str:
        for pattern, template in self._templates:
            if pattern.fullmatch(path):
                return template
        raise AssertionError(f"{path} matches no path in openapi.json")

    def _resolve(self, node: dict[str, Any]) -> dict[str, Any]:
        while "$ref" in node:
            target: Any = self.doc
            for part in node["$ref"].removeprefix("#/").split("/"):
                target = target[part]
            node = target
        return node

    def _validate(self, schema_ref: str, instance: Any) -> None:
        schema = {"$ref": schema_ref, "components": self.doc["components"]}
        errors = list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(instance))
        assert not errors, [e.message[:160] for e in errors][:3]  # messages name fields, not values we hold

    def check(self, method: str, response: httpx.Response) -> None:
        template = self.template_for(urlsplit(str(response.request.url)).path)
        operation = self.doc["paths"][template][method.lower()]
        declared = operation["responses"].get(str(response.status_code))
        if declared is None:
            assert response.status_code >= 500, f"{response.status_code} is not declared for {method} {template}"
            declared = {"content": {"application/problem+json": {"schema": {"$ref": "#/components/schemas/Problem"}}}}
        declared = self._resolve(declared)
        content = declared.get("content", {})
        if not content:
            return
        media = response.headers.get("content-type", "").split(";")[0]
        assert media in content, f"{media} not declared for {response.status_code} {method} {template}"
        if media == "text/event-stream":
            return  # events are checked one by one with `check_event`
        self._validate(content[media]["schema"]["$ref"], response.json())
        self.checked += 1

    def check_event(self, schema_name: str, data: Any) -> None:
        self._validate(f"#/components/schemas/{schema_name}", data)
        self.checked += 1


class CheckedClient:
    """`get` and `post` that validate every response against the contract before returning it."""

    def __init__(self, client: httpx.Client, *, validate: bool = True, contract: OpenApiContract | None = None) -> None:
        self._client = client
        self._contract = contract or OpenApiContract()
        self._validate = validate

    @property
    def contract(self) -> OpenApiContract:
        return self._contract

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self._seen("GET", self._client.get(url, **kwargs))

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self._seen("POST", self._client.post(url, **kwargs))

    def _seen(self, method: str, response: httpx.Response) -> httpx.Response:
        if self._validate:
            self._contract.check(method, response)
        return response

    def __enter__(self) -> CheckedClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()
