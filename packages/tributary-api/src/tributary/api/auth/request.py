"""Token extraction (ADR-009 D1) and the one authorization layer (D6). Deny by default."""

from __future__ import annotations

from urllib.parse import parse_qsl

from fastapi import Request

from ..problems import ApiProblem, unauthorized
from .validator import MAX_TOKEN_BYTES, Principal, TokenInvalid, TokenValidator

FORM_PROBE_BYTES = 64 * 1024
_FORM = "application/x-www-form-urlencoded"


async def _form_has_token(request: Request) -> bool:
    """RFC 6750 2.2: an `access_token` form field is refused, even beside a header. Only a urlencoded body
    is probed, bounded; any other body is rejected later as 415 and never read."""
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return False
    if not request.headers.get("content-type", "").lower().startswith(_FORM):
        return False
    body = b""
    async for chunk in request.stream():
        body += chunk
        if len(body) > FORM_PROBE_BYTES:
            break
    return any(k == "access_token" for k, _ in parse_qsl(body.decode("latin-1"), keep_blank_values=True))


async def authenticate(request: Request, validator: TokenValidator) -> Principal:
    headers = request.headers.getlist("authorization")
    query_token = any(k == "access_token" for k, _ in parse_qsl(request.url.query, keep_blank_values=True))
    if query_token or await _form_has_token(request) or len(headers) > 1:
        raise unauthorized("TOKEN_INVALID", token_sent=True)
    if not headers:
        raise unauthorized("AUTH_REQUIRED", token_sent=False)
    value = headers[0]
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or len(value) > MAX_TOKEN_BYTES or not token or " " in token:
        raise unauthorized("TOKEN_INVALID", token_sent=True)
    try:
        return await validator.validate(token)
    except TokenInvalid:
        raise unauthorized("TOKEN_INVALID", token_sent=True) from None


async def authorize(request: Request) -> None:
    """Global dependency: authenticate, then check the operation's `x-tributary-roles`. An operation with
    no entry, or an empty role list that is not marked public, is `403` for every caller."""
    route = request.scope.get("route")
    if route is None:
        return
    operations = request.app.state.operations
    op = operations.get((request.method, route.path))
    if op is not None and op.public:
        return
    principal = await authenticate(request, request.app.state.validator)
    request.state.principal = principal
    if op is None or not op.roles or principal.roles.isdisjoint(op.roles):
        raise ApiProblem("FORBIDDEN")
