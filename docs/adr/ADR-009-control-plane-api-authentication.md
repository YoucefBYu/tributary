# ADR-009: Control-Plane API Authentication (OIDC Bearer, Resource Server)

- **Status:** **Proposed** (2026-10-05). No invariant changes. Not adopted until `security-reviewer`
  has read it, because it governs auth and a network listener (DoD item 6). Nothing here designs
  cryptography: it selects a standard protocol (OAuth 2.0 bearer tokens, OIDC discovery, JWT access
  tokens) and a vetted library (`phases/wave-3.md` section 7, "Not escalated").
- **Date:** 2026-10-05
- **Deciders:** `architect` (W3-1a). Reviewers owed: `security-reviewer`; `control-plane-engineer`
  implements in W3-4b.
- **Contracts:** `openapi/v0` (`oidcBearer` security scheme, `x-tributary-roles`); `observability/v0`
  (`oidc_keys` readiness check for the API)

## 1. Context

ADR-001 section 8 names OIDC (Keycloak or similar), five roles (`admin`, `developer`, `operator`,
`auditor`, `viewer`), channel-level scopes, and "API tokens with scopes and expiry". W3-4b builds the
FastAPI skeleton now, and exit criterion 4 needs `security-reviewer` clean on authentication. What is
unsettled:

- **Who issues credentials.** If the API issues tokens or sessions, Tributary owns password storage,
  token signing keys and rotation. That is key management, which CLAUDE.md escalates.
- **What the UI and the CLI do.** The UI is a browser single-page app. The CLI runs on developer
  machines and in CI.
- **Invariant 3.** The data plane must not depend on the API or the identity provider. It does not
  today: receivers, workers and loaders authenticate to Kafka only (ADR-008).
- **Invariant 6.** Tokens and claims must not reach logs, metrics or error bodies.

## 2. Options

| Option | For | Against | Complexity / cost / familiarity |
|---|---|---|---|
| **A. API as an OAuth 2.0 resource server: validate JWT access tokens from the operator's OIDC provider (chosen)** | No credential store and no signing key in Tributary. SSO, MFA and lifecycle live in the IdP the hospital already runs. Standard flows for UI (Authorization Code + PKCE) and CI (client credentials). Stateless API, so no CSRF surface from cookies | Needs an IdP even for the lite stack. JWT validation has well-known pitfalls (algorithm confusion, `aud` not checked, JWKS refresh storms), so the rules below are explicit | Low: one library, configuration only. Familiar to FastAPI users |
| B. API-issued sessions and API tokens (own user table, password hashing, token minting) | Works without an IdP | Tributary stores credentials and holds a signing or hashing key: designing key management, which is escalated. Duplicates the IdP that sites have | High; security-critical code to own forever |
| C. Token introspection (RFC 7662) on every request | Revocation is immediate | One IdP round trip per request; an IdP outage takes the API down even for reads; per-IdP client secrets | Medium; adds latency and a hard dependency |
| D. mTLS client certificates only | Strong for CI | No browser story; certificate issuance is key management | Medium-high |

**Rejected: an "auth disabled" flag**, even for development. It ships, and someone runs it in
production. The lite stack gets a local issuer instead (D9).

## 3. Decision

Option A. The API is a resource server only.

| # | Rule |
|---|---|
| D1 | **Bearer tokens in the `Authorization` header only.** RFC 6750's form-body and query-string forms are refused (`401 TOKEN_INVALID`): a query string lands in access logs and proxies. Header value at most 8 KiB |
| D2 | **JWT validation** with a vetted library (D8): signature against the issuer's JWKS; `iss` equals the configured issuer exactly; `aud` contains the configured audience; `exp` required and in the future; `nbf` and `iat` honoured when present; leeway at most 60 s |
| D3 | **Algorithm allow-list: `RS256`, `PS256`, `ES256`.** Never `none`, never an `HS*` algorithm, never the algorithm the token header asks for unless it is on the list. The key is selected by `kid` from the JWKS, and its `kty` must match the algorithm |
| D4 | **Token type.** `typ: at+jwt` (RFC 9068) is required by default. `TB_OIDC_ACCEPT_JWT_TYP=true` also accepts `JWT`, for IdPs that do not set `at+jwt` (Keycloak does not by default; **unverified for the current Keycloak release**). An ID token is never accepted as an access token: `aud` must be the API's audience, not the UI's client id |
| D5 | **JWKS handling.** Discovered from `<issuer>/.well-known/openid-configuration` (or `TB_OIDC_JWKS_URI`), over verified TLS, cached. On an unknown `kid`: one refresh, at most once per 30 s, then refuse. On a refresh failure the cached keys keep working for at most `TB_OIDC_JWKS_MAX_STALE_S` (default 86400). After that, every token is refused, and `/readyz` reports `oidc_keys: keys_unavailable`. **Give up:** a key the IdP revoked is trusted until the next successful refresh or the stale limit |
| D6 | **Roles.** Read from a configured claim path (`TB_OIDC_ROLES_CLAIM`, default `roles`; Keycloak typically `realm_access.roles` or `resource_access.<client>.roles`), mapped through an operator-supplied table (`TB_OIDC_ROLE_MAP`, IdP role to Tributary role). Unknown roles are ignored. No mapped role means `403`. Roles are **global in v0**; channel and environment scopes are later and additive. Each operation's allowed roles are `x-tributary-roles` in `openapi/v0`, checked by a contract test |
| D7 | **What is never logged or returned:** the token, the `Authorization` header, any claim except `sub`. `sub` goes to deployment records (`requested_by`, `created_by`) and, in part 2, the audit topic. It is staff identity, not PHI, but it is personal data, so application logs carry the request id and route template instead. A `401` names `AUTH_REQUIRED` or `TOKEN_INVALID` and never which check failed |
| D8 | **Library: PyJWT with its `crypto` extra** (`PyJWKClient` for JWKS). PyJWT 2.15.1 is MIT; `cryptography` 50.0.2 is `Apache-2.0 OR BSD-3-Clause` and needs `cffi` (MIT). `PyJWKClient` fetches with `urllib`, so no `requests` and no `certifi`. Read from PyPI metadata 2026-10-05; **`licence_check.py` has not run on them.** Alternatives: Authlib (BSD-3-Clause, larger surface, includes server-side flows we do not want) and joserfc (BSD-3-Clause, younger). `python-jose` rejected (pulls `ecdsa`, which documents itself as not side-channel safe) |
| D9 | **Lite stack and tests: a local issuer, not a bypass.** Options for `devops-engineer` and `control-plane-engineer` to choose between in W3-4b: (a) Keycloak (Apache-2.0) in an optional Compose profile, the realistic path; (b) a static JWKS file (`TB_OIDC_JWKS_FILE`), with tokens minted by a test helper using PyJWT and a key generated at test start, never committed. (b) is allowed only with profile `lite` and the API bound to loopback, and logs a startup WARN. Both validate through the same D2-D6 code; only the key source differs. **Generating a test key with a standard library call is not key management. A committed key would be, so no key is ever committed** |
| D10 | **CORS off by default.** The UI is served same-origin behind the same proxy. A cross-origin deployment sets an explicit origin allow-list; `*` is refused with credentials |
| D11 | **Health is unauthenticated** (`/livez`, `/readyz`). Bodies carry fixed codes only (`observability/v0` 2) |
| D12 | **"API tokens with scopes and expiry"** (ADR-001 8) are **IdP client-credentials clients**, not tokens the API mints. CI gets a confidential client in the IdP with a role mapping. Revocation and expiry are the IdP's |

## 4. Consequences

- **Invariant 3 holds:** no data-plane process calls the IdP or the API. An IdP outage affects API
  logins only, for up to `TB_OIDC_JWKS_MAX_STALE_S`, and never message flow.
- **Invariant 6:** D1 and D7 keep tokens and claims out of logs, URLs and error bodies.
  `security-reviewer` checks FastAPI's default validation-error body (it echoes input), the access log
  format (it logs query strings), and exception handlers.
- **Gives up:** immediate revocation (C), which is bounded by the access-token lifetime the IdP sets.
  Recommend 5-15 minutes, an operator setting. Also gives up a zero-dependency lite stack, because
  D9 adds an issuer.
- **New dependencies** (PyJWT, `cryptography`, `cffi`) go through `licence_check.py` and
  `security-reviewer` in W3-4b.

## 5. Follow-ups

1. `control-plane-engineer` (W3-4b): implement D1-D7 behind one FastAPI dependency. Tests for each
   rejection: `alg: none`, `HS256` signed with the public key bytes (algorithm confusion), a wrong
   `aud`, a wrong `iss`, expired, `nbf` in the future, an ID token, a token in the query string, an
   unknown `kid` (one refresh only), no mapped role (`403`). Each test proves the token reached the
   validator, and a broken control that skips `aud` goes red.
2. `control-plane-engineer` and `devops-engineer`: choose D9 (a) or (b) for Compose; (b) for pytest.
3. `security-reviewer`: review this ADR before W3-4b merges, and the implementation at exit
   (criterion 4).
4. `architect`: roles per operation are in `openapi/v0`. Channel and environment scopes come with RBAC
   (ADR-001 roadmap item 13).
5. Human, **only if** a site requires API-minted tokens without an IdP: that is option B, which is
   key management and escalated. Not expected for v0.
