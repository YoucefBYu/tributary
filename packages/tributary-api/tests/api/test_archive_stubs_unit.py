"""Part 2 (archive) operations before W3-6: authn and RBAC are real and enforced per role; a permitted caller
gets the failure the contract declares (503 ARCHIVE_UNAVAILABLE, or 404 where the contract declares no 503),
validated against `openapi.json`. Nothing from the request comes back, and no content is ever returned."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from api_kit import ROLE_MAP, Contract, Harness, Idp, bearer, run
from tributary.api.operations import OPERATIONS

MSG = "0191a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5b"
REPLAY = "0191a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5c"
PART2 = [
    o for o in OPERATIONS if "/messages" in o.path or "/replays" in o.path or o.path.endswith(("/stats", "/events"))
]
NOT_FOUND_STUBS = {"listReplays", "getReplay"}
Make = Callable[..., Harness]


def url(path: str) -> str:
    return path.format(env="dev", channel="adt", message_id=MSG, replay_id=REPLAY)


def test_the_nine_remaining_part_2_stubs_are_in_the_table() -> None:
    assert len(PART2) == 9  # getRetention is real (test_retention_integration)


@pytest.mark.parametrize("op", PART2, ids=lambda o: o.operation_id)
def test_every_role_gets_exactly_what_the_contract_grants(make_harness: Make, idp: Idp, contract: Contract, op) -> None:  # type: ignore[no-untyped-def]
    async def go() -> None:
        async with make_harness().running() as c:
            for token_role, role in ROLE_MAP.items():
                r = await c.request(
                    op.method,
                    url(op.path),
                    json={} if op.method == "POST" else None,
                    headers=bearer(idp.mint(roles=[token_role])),
                )
                contract.check(op.method, op.path, r)
                if role in op.roles:
                    want = (404, "NOT_FOUND") if op.operation_id in NOT_FOUND_STUBS else (503, "ARCHIVE_UNAVAILABLE")
                    assert (r.status_code, r.json()["code"]) == want, (op.operation_id, role)
                else:
                    assert (r.status_code, r.json()["code"]) == (403, "FORBIDDEN"), (op.operation_id, role)
                assert MSG not in r.text and REPLAY not in r.text  # nothing from the request is echoed
            anonymous = await c.request(op.method, url(op.path), json={} if op.method == "POST" else None)
            assert anonymous.status_code == 401
            bad = await c.request(op.method, url(op.path), headers=bearer("not.a.jwt"))
            assert bad.status_code == 401

    run(go())


def test_content_and_reprocess_are_operator_and_admin_only() -> None:
    by_id = {o.operation_id: o for o in OPERATIONS}
    for name in ("getMessageContent", "reprocessMessage", "startReplay"):
        assert set(by_id[name].roles) == {"operator", "admin"}, name


def test_a_malformed_id_is_not_found_not_a_stub_answer(make_harness: Make, idp: Idp) -> None:
    async def go() -> None:
        async with make_harness().running() as c:
            h = bearer(idp.mint(roles=["tb-admin"]))
            r = await c.get("/v1/environments/dev/channels/adt/messages/not-a-uuid/content", headers=h)
            assert r.status_code == 404
            r = await c.get("/v1/environments/dev/channels/adt/replays/" + MSG.replace("-7", "-4", 1), headers=h)
            assert r.status_code == 404

    run(go())


def test_authn_runs_before_path_validation(make_harness: Make) -> None:
    async def go() -> None:
        async with make_harness().running() as c:
            r = await c.get("/v1/environments/dev/channels/adt/messages/not-a-uuid/content")
            assert r.status_code == 401  # not 404: an anonymous caller learns nothing about id grammar

    run(go())


def test_other_methods_and_trailing_slashes_on_part_2_routes(make_harness: Make, idp: Idp) -> None:
    async def go() -> None:
        async with make_harness().running() as c:
            h = bearer(idp.mint(roles=["tb-admin"]))
            for op in PART2:
                for method in ("HEAD", "OPTIONS"):
                    r = await c.request(method, url(op.path), headers=h)
                    assert r.status_code == 405, (op.operation_id, method, r.status_code)
                slash = await c.request(op.method, url(op.path) + "/", headers=h)
                assert slash.status_code == 404 and "location" not in slash.headers, op.operation_id

    run(go())


def test_problem_codes_are_a_subset_of_the_contracts_enum(contract: Contract) -> None:
    from tributary.api.problems import CODES

    enum = set(contract.doc["components"]["schemas"]["Problem"]["properties"]["code"]["enum"])
    assert set(CODES) <= enum, sorted(set(CODES) - enum)
