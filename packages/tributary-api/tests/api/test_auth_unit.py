"""ADR-009 rev 2, every rule, through the real app with a local HTTPS IdP and synthetic keys.

Each rejection test first proves the control: a good token reaches the validator and gets 200 on the same
route, so a 401 here means the rule fired, not that the setup was broken. `test_control_*` shows that a
validator with the audience check removed accepts the token the rule exists to refuse.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Callable
from typing import Any

import httpx
import jwt
import pytest
from api_kit import AUDIENCE, ROLE_MAP, Contract, Harness, Idp, bearer, oidc_settings, public_jwk, run
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROUTE = "/v1/validate"  # developer, operator, admin; no database needed
Make = Callable[..., Harness]


async def good(c: httpx.AsyncClient, idp: Idp) -> None:
    r = await c.post(ROUTE, json={}, headers=bearer(idp.mint()))
    assert r.status_code == 200, r.text


async def refused(
    c: httpx.AsyncClient,
    idp: Idp,
    *,
    headers: dict[str, str] | list[tuple[str, str]],
    url: str = ROUTE,
    status: int = 401,
    code: str = "TOKEN_INVALID",
    **kw: Any,
) -> httpx.Response:
    await good(c, idp)
    r = await c.post(url, headers=headers, **({"json": {}} | kw))
    assert r.status_code == status, r.text
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["code"] == code
    return r


def scenario(make: Make, idp: Idp, body: Callable[[httpx.AsyncClient], Any], **kw: Any) -> None:
    async def go() -> None:
        async with make(**kw).running() as c:
            await body(c)

    run(go())


def test_valid_token_and_no_token(make_harness: Make, idp: Idp) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        await good(c, idp)
        r = await c.post(ROUTE, json={})
        assert (r.status_code, r.json()["code"], r.headers["www-authenticate"]) == (401, "AUTH_REQUIRED", "Bearer")

    scenario(make_harness, idp, body)


@pytest.mark.parametrize(
    "name",
    [
        "alg_none",
        "hs256_with_public_key",
        "wrong_aud",
        "wrong_iss",
        "expired",
        "no_exp",
        "no_sub",
        "no_iss",
        "no_aud",
        "nbf_future",
        "iat_future",
        "id_token_typ",
        "no_typ",
        "nonce",
        "at_hash",
        "typ_claim_id",
        "no_kid",
        "unknown_kid",
        "crit",
        "kty_mismatch",
        "ps256_on_ec_key",
        "es256_on_rsa_key",
        "bad_sub",
        "long_sub",
        "roles_string",
        "roles_mixed",
        "roles_object",
        "attacker_jwk_header",
        "attacker_x5c_header",
        "wrong_signature",
        "alg_rs384",
        "garbage",
    ],
)
def test_token_rejections(make_harness: Make, idp: Idp, name: str) -> None:
    attacker = idp.rsa2
    pub_pem = idp.rsa.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    def forged_hs256() -> str:
        header = b64(json.dumps({"alg": "HS256", "kid": "rsa-1", "typ": "at+jwt"}).encode())
        claims = b64(
            json.dumps(
                {"iss": idp.issuer, "aud": AUDIENCE, "sub": "u", "exp": 4102444800, "roles": ["tb-admin"]}
            ).encode()
        )
        mac = hmac.new(pub_pem, f"{header}.{claims}".encode(), hashlib.sha256).digest()
        return f"{header}.{claims}.{b64(mac)}"

    jwk_attacker = public_jwk(attacker, "rsa-1")
    tokens: dict[str, Callable[[], str]] = {
        "alg_none": lambda: idp.mint(alg="none"),
        "hs256_with_public_key": forged_hs256,
        "wrong_aud": lambda: idp.mint(claims={"aud": "someone-else"}),
        "wrong_iss": lambda: idp.mint(claims={"iss": "https://evil.example"}),
        "expired": lambda: idp.mint(claims={"exp": 1}),
        "no_exp": lambda: idp.mint(drop=("exp",)),
        "no_sub": lambda: idp.mint(drop=("sub",)),
        "no_iss": lambda: idp.mint(drop=("iss",)),
        "no_aud": lambda: idp.mint(drop=("aud",)),
        "nbf_future": lambda: idp.mint(claims={"nbf": 4102444800}),
        "iat_future": lambda: idp.mint(claims={"iat": 4102444800}),
        "id_token_typ": lambda: idp.mint(typ="JWT"),
        "no_typ": lambda: idp.mint(typ=None),
        "nonce": lambda: idp.mint(claims={"nonce": "n"}),
        "at_hash": lambda: idp.mint(claims={"at_hash": "h"}),
        "typ_claim_id": lambda: idp.mint(claims={"typ": "ID"}),
        "no_kid": lambda: idp.mint(kid=None),
        "unknown_kid": lambda: idp.mint(kid="nope"),
        "crit": lambda: idp.mint(headers={"crit": ["exp"]}),
        "kty_mismatch": lambda: idp.mint(kid="ec-1", alg="RS256", key=idp.rsa),
        "ps256_on_ec_key": lambda: idp.mint(kid="ec-1", alg="PS256", key=idp.rsa),
        "es256_on_rsa_key": lambda: idp.mint(kid="rsa-1", alg="ES256", key=idp.ec),
        "bad_sub": lambda: idp.mint(claims={"sub": "has space"}),
        "long_sub": lambda: idp.mint(claims={"sub": "a" * 256}),
        "roles_string": lambda: idp.mint(roles="tb-admin"),
        "roles_mixed": lambda: idp.mint(roles=["tb-admin", 5]),  # type: ignore[list-item]
        "roles_object": lambda: idp.mint(claims={"roles": {"tb-admin": True}}),
        "attacker_jwk_header": lambda: idp.mint(key=attacker, headers={"jwk": jwk_attacker}),
        "attacker_x5c_header": lambda: idp.mint(key=attacker, headers={"x5c": ["MIIB"]}),
        "wrong_signature": lambda: idp.mint(key=attacker),
        "alg_rs384": lambda: idp.mint(alg="RS384"),
        "garbage": lambda: "not.a.jwt",
    }
    token = tokens[name]()

    async def body(c: httpx.AsyncClient) -> None:
        r = await refused(c, idp, headers=bearer(token))
        assert r.headers["www-authenticate"] == 'Bearer error="invalid_token"'
        assert token not in r.text and AUDIENCE not in r.text and idp.issuer not in r.text

    scenario(make_harness, idp, body)


def test_header_x5u_and_jku_are_ignored_and_never_fetched(make_harness: Make, idp: Idp) -> None:
    """D13: a token whose jku/x5u points at a test server is 401 (signed by an attacker key) and the server
    records no request for it. A good token that merely carries the headers is judged on its signature alone."""
    seen: list[str] = []
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Spy(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            seen.append(self.path)
            self.send_response(404)
            self.end_headers()

        def log_message(self, *a: Any) -> None:
            pass

    spy = HTTPServer(("127.0.0.1", 0), Spy)
    threading.Thread(target=spy.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{spy.server_address[1]}/keys"
    try:

        async def body(c: httpx.AsyncClient) -> None:
            for hdr in ({"jku": url}, {"x5u": url}):
                await refused(c, idp, headers=bearer(idp.mint(key=idp.rsa2, headers=hdr)))
            r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(headers={"jku": url, "x5u": url})))
            assert r.status_code == 200
            before = len(idp.requests)
            await refused(c, idp, headers=bearer(idp.mint(key=idp.rsa2, kid="unknown-1", headers={"jku": url})))
            assert len(idp.requests) <= before + 2  # at most the one rate-limited key refresh (discovery + jwks)

        scenario(make_harness, idp, body)
    finally:
        spy.shutdown()
    assert seen == []


@pytest.mark.parametrize(
    "variant", ["query", "query_and_header", "two_headers", "form_and_header", "basic", "empty", "no_space"]
)
def test_one_method_only(make_harness: Make, idp: Idp, variant: str) -> None:
    token = idp.mint()

    async def body(c: httpx.AsyncClient) -> None:
        if variant == "query":
            await refused(c, idp, headers={}, url=f"{ROUTE}?access_token={token}")
        elif variant == "query_and_header":
            await refused(c, idp, headers=bearer(token), url=f"{ROUTE}?access_token={token}")
        elif variant == "two_headers":
            await refused(c, idp, headers=[("Authorization", f"Bearer {token}"), ("Authorization", f"Bearer {token}")])
        elif variant == "form_and_header":
            r = await c.post(
                ROUTE,
                headers=bearer(token) | {"Content-Type": "application/x-www-form-urlencoded"},
                content=f"access_token={token}",
            )
            assert (r.status_code, r.json()["code"]) == (401, "TOKEN_INVALID")
        elif variant == "basic":
            await refused(c, idp, headers={"Authorization": "Basic dXNlcjpwdw=="})
        elif variant == "empty":
            await refused(c, idp, headers={"Authorization": "Bearer "})
        else:
            await refused(c, idp, headers={"Authorization": f"Bearer{token}"})

    scenario(make_harness, idp, body)


def test_oversized_authorization_header_is_refused(make_harness: Make, idp: Idp) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        await refused(c, idp, headers={"Authorization": "Bearer " + "a" * 9000})

    scenario(make_harness, idp, body)


def test_roles_no_mapped_role_and_wrong_role(make_harness: Make, idp: Idp) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        await good(c, idp)
        for roles in ([], ["unknown-role"]):
            r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(roles=roles)))
            assert (r.status_code, r.json()["code"]) == (403, "FORBIDDEN")
        r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(drop=("roles",))))  # absent claim: no roles
        assert r.status_code == 403
        r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(roles=["tb-viewer"])))  # viewer may not validate
        assert r.status_code == 403
        r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(roles=["tb-viewer", "tb-operator"])))
        assert r.status_code == 200

    scenario(make_harness, idp, body)


def test_nested_roles_claim_path(make_harness: Make, idp: Idp) -> None:
    oidc = oidc_settings(idp, roles_claim=("realm_access", "roles"))

    async def body(c: httpx.AsyncClient) -> None:
        ok = idp.mint(claims={"realm_access": {"roles": ["tb-admin"]}})
        assert (await c.post(ROUTE, json={}, headers=bearer(ok))).status_code == 200
        absent = idp.mint(claims={"realm_access": "x"})
        assert (await c.post(ROUTE, json={}, headers=bearer(absent))).status_code == 403

    scenario(make_harness, idp, body, oidc=oidc)


def test_accept_jwt_typ_flag_and_its_three_extra_checks(make_harness: Make, idp: Idp) -> None:
    oidc = oidc_settings(idp, accept_jwt_typ=True, ui_client_id="tb-ui")

    async def body(c: httpx.AsyncClient) -> None:
        assert (await c.post(ROUTE, json={}, headers=bearer(idp.mint(typ="JWT")))).status_code == 200
        assert (await c.post(ROUTE, json={}, headers=bearer(idp.mint(typ="at+jwt")))).status_code == 200
        for claims in ({"nonce": "x"}, {"at_hash": "x"}, {"typ": "ID"}, {"aud": "tb-ui"}):
            r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(typ="JWT", claims=claims)))
            assert r.status_code == 401, claims
        assert (await c.post(ROUTE, json={}, headers=bearer(idp.mint(typ=None)))).status_code == 401

    scenario(make_harness, idp, body, oidc=oidc)


def test_control_validator_without_aud_check_accepts_the_wrong_audience(
    make_harness: Make, idp: Idp, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The broken-implementation control: with `aud` verification removed the wrong-audience token gets in, so
    `test_token_rejections[wrong_aud]` is testing the real rule."""
    import tributary.api.auth.validator as validator_module

    real = jwt.decode

    def no_aud(token: str, **kw: Any) -> Any:
        kw["options"] = {**kw["options"], "verify_aud": False}
        kw.pop("audience")
        return real(token, **kw)

    monkeypatch.setattr(validator_module.jwt, "decode", no_aud)

    async def body(c: httpx.AsyncClient) -> None:
        r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(claims={"aud": "someone-else"})))
        assert r.status_code == 200

    scenario(make_harness, idp, body)


def test_deny_by_default_operation_without_roles_entry(make_harness: Make, idp: Idp) -> None:
    """D6: a route missing from the table is 403 for every caller (and the startup check refuses it)."""
    from tributary.api.errors import ConfigurationError

    h = make_harness()

    @h.app.get("/v1/unlisted")
    async def unlisted() -> dict[str, str]:
        return {"x": "y"}

    async def go() -> None:
        with pytest.raises(ConfigurationError) as exc:
            async with h.app.router.lifespan_context(h.app):
                pass
        assert exc.value.code == "ROUTE_WITHOUT_ROLES"
        # The runtime layer, with the startup check out of the way:
        await h.keys.refresh()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=h.app), base_url="http://t") as c:
            for role in ("tb-admin", "tb-developer"):
                r = await c.get("/v1/unlisted", headers=bearer(idp.mint(roles=[role])))
                assert (r.status_code, r.json()["code"]) == (403, "FORBIDDEN")
            assert (await c.get("/v1/unlisted")).status_code == 401  # authenticated first

    run(go())


def test_health_is_unauthenticated_and_ignores_tokens(make_harness: Make, idp: Idp, contract: Contract) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        live = await c.get("/livez")
        assert live.status_code == 200
        contract.check("GET", "/livez", live)
        ready = await c.get("/readyz?access_token=junk", headers={"Authorization": "garbage"})
        assert ready.status_code == 503  # the database is not reachable in this unit test
        contract.check("GET", "/readyz", ready)
        assert {"name": "database", "ok": False, "reason": "unreachable"} in ready.json()["checks"]
        assert {"name": "oidc_keys", "ok": True} in ready.json()["checks"]

    scenario(make_harness, idp, body)


def test_unknown_kid_refresh_is_rate_limited_even_after_a_failure(make_harness: Make, idp: Idp) -> None:
    async def body(c: httpx.AsyncClient) -> None:
        await good(c, idp)
        idp.fail = True
        try:
            before = len(idp.requests)
            for _ in range(5):
                r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(kid="nope")))
                assert r.status_code == 401
            # one refresh (discovery fails first, so a single request), never one per token
            assert len(idp.requests) - before == 1
        finally:
            idp.fail = False

    scenario(make_harness, idp, body)


def test_stale_snapshot_refuses_tokens_and_flips_readyz(make_harness: Make, idp: Idp) -> None:
    now = [1000.0]
    oidc = oidc_settings(idp, max_stale_s=300.0)

    async def body(c: httpx.AsyncClient) -> None:
        await good(c, idp)
        now[0] += 301  # past the maximum cache age, and the IdP unreachable
        idp.fail = True
        try:
            r = await c.post(ROUTE, json={}, headers=bearer(idp.mint()))
            assert (r.status_code, r.json()["code"]) == (401, "TOKEN_INVALID")
            ready = await c.get("/readyz")
            assert {"name": "oidc_keys", "ok": False, "reason": "keys_unavailable"} in ready.json()["checks"]
        finally:
            idp.fail = False

    scenario(make_harness, idp, body, oidc=oidc, clock=lambda: now[0])


def test_rsa_key_below_2048_bits_is_not_loaded() -> None:
    from tributary.api.auth.jwks import JwksError, parse_jwks

    small = rsa.generate_private_key(public_exponent=65537, key_size=1024)  # noqa: S505 - the point of the test
    with pytest.raises(JwksError):
        parse_jwks(json.dumps({"keys": [public_jwk(small, "small")]}).encode())


def test_role_map_is_used_not_the_raw_role_name(make_harness: Make, idp: Idp) -> None:
    """A token carrying the Tributary role name itself, unmapped, gets nothing."""

    async def body(c: httpx.AsyncClient) -> None:
        r = await c.post(ROUTE, json={}, headers=bearer(idp.mint(roles=["admin"])))
        assert r.status_code == 403

    assert "admin" not in ROLE_MAP
    scenario(make_harness, idp, body)
