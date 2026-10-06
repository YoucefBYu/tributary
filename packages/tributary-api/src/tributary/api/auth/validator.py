"""Access-token validation (ADR-009 D2-D4, D6, D13). PyJWT parses and verifies; every policy decision
is made here. Every failure is one `TokenInvalid`: callers never learn which check failed (D7)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import jwt

from ..config import OidcSettings
from .jwks import ALLOWED_ALGORITHMS, KeySet

MAX_TOKEN_BYTES = 8 * 1024
MAX_ROLES = 256
REQUIRED_CLAIMS = ["exp", "iss", "aud", "sub"]
_SUBJECT = re.compile(r"^[\x21-\x7e]{1,255}$")  # OIDC Core: sub is at most 255 ASCII characters
_AT_JWT = {"at+jwt", "application/at+jwt"}


class TokenInvalid(Exception):
    """Deliberately carries nothing."""


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    roles: frozenset[str]

    def __repr__(self) -> str:  # a subject is personal data: never in a traceback or log by accident
        return "Principal(...)"


class TokenValidator:
    def __init__(self, settings: OidcSettings, keys: KeySet) -> None:
        self._s = settings
        self._keys = keys

    async def validate(self, token: str) -> Principal:
        if not token or len(token) > MAX_TOKEN_BYTES:
            raise TokenInvalid
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            raise TokenInvalid from None
        alg = header.get("alg")
        kid = header.get("kid")
        # Well formed, on the allow-list, no `crit` (no extension is supported). jku, x5u, x5c and jwk are
        # never read: the key comes from the cached key set by kid, nothing else (D3, D13).
        if not isinstance(alg, str) or alg not in ALLOWED_ALGORITHMS or "crit" in header:
            raise TokenInvalid
        if not isinstance(kid, str) or not kid or len(kid) > 256 or not self._typ_ok(header.get("typ")):
            raise TokenInvalid
        if not self._keys.usable:
            raise TokenInvalid
        entry = self._keys.get(kid)
        if entry is None:
            await self._keys.refresh_for_unknown_kid()
            entry = self._keys.get(kid)
        if entry is None or not entry.permits(alg):
            raise TokenInvalid
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                key=entry.key,
                algorithms=[alg],  # the single algorithm of the selected key, never the whole list (D3)
                audience=self._s.audience,
                issuer=self._s.issuer,
                leeway=self._s.leeway_s,
                options={"require": REQUIRED_CLAIMS, "verify_signature": True},
            )
        except jwt.PyJWTError:
            raise TokenInvalid from None
        subject = claims.get("sub")
        if not isinstance(subject, str) or not _SUBJECT.fullmatch(subject):
            raise TokenInvalid
        # ID-token-only claims (D4 b, c). Applied in both modes: no access token legitimately has them.
        if "nonce" in claims or "at_hash" in claims or claims.get("typ") == "ID":
            raise TokenInvalid
        return Principal(subject=subject, roles=self._roles(claims))

    def _typ_ok(self, typ: object) -> bool:
        if not isinstance(typ, str):
            return False
        lowered = typ.lower()
        return lowered in _AT_JWT or (self._s.accept_jwt_typ and lowered == "jwt")

    def _roles(self, claims: Mapping[str, Any]) -> frozenset[str]:
        node: Any = claims
        for part in self._s.roles_claim:
            if not isinstance(node, dict) or part not in node:
                return frozenset()  # an absent claim means no roles (D6)
            node = node[part]
        if not isinstance(node, list) or len(node) > MAX_ROLES or not all(isinstance(r, str) for r in node):
            raise TokenInvalid
        return frozenset(self._s.role_map[r] for r in node if r in self._s.role_map)
