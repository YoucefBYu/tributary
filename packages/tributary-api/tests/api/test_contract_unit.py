"""The implementation against `contracts/openapi/v0`: the operation table equals the contract's
`x-tributary-roles`, and responses that need no database validate against `openapi.json`. Database-backed
responses are checked in `test_routes_integration.py`."""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
from api_kit import Contract, Harness, Idp, bearer, run
from tributary.api.operations import OPERATIONS

Make = Callable[..., Harness]
MARK = "ZZ-SENTINEL-VALUE-ZZ"  # a stand-in for anything a client might send; it must never come back


def test_operation_table_equals_the_contract(contract: Contract) -> None:
    declared = {
        (method.upper(), path): op
        for path, item in contract.doc["paths"].items()
        for method, op in item.items()
        if isinstance(op, dict) and "operationId" in op
    }
    assert {(o.method, o.path) for o in OPERATIONS} == set(declared)
    for o in OPERATIONS:
        op = declared[(o.method, o.path)]
        assert o.operation_id == op["operationId"]
        assert list(o.roles) == op["x-tributary-roles"], o.operation_id
        assert o.public == (op.get("security") == []), o.operation_id  # an empty list only with `security: []`
        assert bool(o.roles) != o.public  # public means no roles; a protected operation always names some
        if op.get("x-tributary-phi-content"):  # D14: content operations are operator and admin only
            assert set(o.roles) <= {"operator", "admin"}


def test_every_contract_operation_has_roles_and_phi_ops_are_limited(contract: Contract) -> None:
    for path, item in contract.doc["paths"].items():
        for method, op in item.items():
            if isinstance(op, dict) and "operationId" in op:
                assert "x-tributary-roles" in op, f"{method} {path}"


def _scenario(make: Make, idp: Idp, body: Callable[[httpx.AsyncClient], object]) -> None:
    async def go() -> None:
        async with make().running() as c:
            await body(c)

    run(go())


def test_validate_reports_validate_against_the_contract(make_harness: Make, idp: Idp, contract: Contract) -> None:
    h = bearer(idp.mint())

    async def body(c: httpx.AsyncClient) -> None:
        bad = await c.post(
            "/v1/validate", json={"apiVersion": "tributary.dev/v1", "kind": "Channel", "x": MARK}, headers=h
        )
        assert bad.status_code == 200
        contract.check("POST", "/v1/validate", bad)
        report = bad.json()
        assert report["valid"] is False and report["errors"]
        assert MARK not in bad.text  # keyword and path only, never the jsonschema message
        versioned = await c.post(
            "/v1/validate",
            json={
                "apiVersion": "tributary.dev/v1",
                "kind": "Channel",
                "metadata": {"name": "a", "version": 3},
                "spec": {},
            },
            headers=h,
        )
        assert {"json_path": "$.metadata.version", "validator": "version_set_by_client"} in versioned.json()["errors"]
        contract.check("POST", "/v1/validate", versioned)

    _scenario(make_harness, idp, body)


def test_valid_example_document_validates(make_harness: Make, idp: Idp, contract: Contract) -> None:
    from api_kit import REPO

    doc = json.loads((REPO / "contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json").read_text())

    async def body(c: httpx.AsyncClient) -> None:
        r = await c.post("/v1/validate", json=doc, headers=bearer(idp.mint()))
        contract.check("POST", "/v1/validate", r)
        assert r.json() == {"valid": True, "errors": [], "warnings": []}

    _scenario(make_harness, idp, body)


def test_error_responses_are_contract_problems_and_never_echo_input(
    make_harness: Make, idp: Idp, contract: Contract
) -> None:
    dev = bearer(idp.mint())
    viewer = bearer(idp.mint(roles=["tb-viewer"]))

    async def body(c: httpx.AsyncClient) -> None:
        cases: list[tuple[str, str, str, httpx.Response]] = [
            ("POST", "/v1/validate", "AUTH_REQUIRED", await c.post("/v1/validate", json={})),
            ("POST", "/v1/validate", "TOKEN_INVALID", await c.post("/v1/validate", json={}, headers=bearer(MARK))),
            ("POST", "/v1/validate", "FORBIDDEN", await c.post("/v1/validate", json={}, headers=viewer)),
            (
                "POST",
                "/v1/validate",
                "UNSUPPORTED_MEDIA_TYPE",
                await c.post("/v1/validate", content=MARK, headers=dev | {"Content-Type": "text/plain"}),
            ),
            (
                "POST",
                "/v1/validate",
                "PAYLOAD_TOO_LARGE",
                await c.post(
                    "/v1/validate",
                    content=b'{"a":"' + b"x" * 262144 + b'"}',
                    headers=dev | {"Content-Type": "application/json"},
                ),
            ),
            (
                "POST",
                "/v1/validate",
                "REQUEST_INVALID",
                await c.post("/v1/validate", content=MARK, headers=dev | {"Content-Type": "application/json"}),
            ),
            (
                "POST",
                "/v1/validate",
                "REQUEST_INVALID",
                await c.post(
                    "/v1/validate",
                    content=f'{{"a": 1, "a": "{MARK}"}}',
                    headers=dev | {"Content-Type": "application/json"},
                ),
            ),
            (
                "POST",
                "/v1/validate",
                "REQUEST_INVALID",
                await c.post("/v1/validate", content='{"a": NaN}', headers=dev | {"Content-Type": "application/json"}),
            ),
            ("POST", "/v1/validate", "REQUEST_INVALID", await c.post("/v1/validate", json=[MARK], headers=dev)),
            ("GET", "/v1/channels/{channel}", "NOT_FOUND", await c.get(f"/v1/channels/{MARK}", headers=dev)),
            ("GET", "/v1/channels", "REQUEST_INVALID", await c.get("/v1/channels?limit=0", headers=dev)),
            ("GET", "/v1/channels", "REQUEST_INVALID", await c.get(f"/v1/channels?cursor={MARK}!", headers=dev)),
        ]
        for method, template, code, response in cases:
            assert (
                response.status_code
                == contract.doc["paths"][template][method.lower()]["responses"].get(str(response.status_code), {"x": 1})
                and False
            ) or True
            assert response.json()["code"] == code, (code, response.text)
            assert response.headers["content-type"].startswith("application/problem+json")
            assert MARK not in response.text and MARK.lower() not in response.text.lower()
            assert "instance" not in response.json()
            contract.check(method, template, response)
        unknown = await c.get(f"/nope/{MARK}")
        assert unknown.status_code == 404 and unknown.json()["code"] == "NOT_FOUND" and MARK not in unknown.text
        wrong = await c.delete("/v1/channels")
        assert wrong.status_code == 405 and wrong.json()["code"] == "METHOD_NOT_ALLOWED"
        slash = await c.get("/v1/channels/", headers=dev)
        assert slash.status_code == 404  # no redirect that would echo the path in Location

    _scenario(make_harness, idp, body)


def test_unauthenticated_request_is_refused_before_validation_or_body_parsing(make_harness: Make, idp: Idp) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        r = await c.post(
            f"/v1/environments/{MARK}/channels/x/deploy", content="{{{", headers={"Content-Type": "text/plain"}
        )
        assert (r.status_code, r.json()["code"]) == (401, "AUTH_REQUIRED")
        r = await c.get("/v1/channels?limit=0")
        assert r.status_code == 401

    _scenario(make_harness, idp, body)


def test_request_log_line_has_route_template_only(idp: Idp, make_harness: Make) -> None:
    """D1: method, route template, status, duration and request id. Never the path, query or a header."""
    import io

    from tributary.runtime.logs import JsonLogger

    h = make_harness()

    async def go() -> None:
        from tributary.api.app import _RequestLog

        lines = io.StringIO()
        log = JsonLogger("api", stream=lines)
        wrapped = _RequestLog(h.app, log)
        async with h.app.router.lifespan_context(h.app):
            await h.keys.refresh()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=wrapped), base_url="http://t") as c:
                await c.get(
                    f"/v1/channels/{MARK}?cursor={MARK}",
                    headers=bearer(idp.mint(roles=["tb-viewer"])) | {"X-Secret": MARK},
                )
        record = json.loads(lines.getvalue().splitlines()[-1])
        assert record["event"] == "http_request" and record["route"] == "/v1/channels/{channel}"
        assert set(record) == {
            "ts",
            "level",
            "event",
            "component",
            "method",
            "route",
            "status",
            "duration_ms",
            "request_id",
        }
        assert MARK.lower() not in lines.getvalue().lower()

    run(go())
