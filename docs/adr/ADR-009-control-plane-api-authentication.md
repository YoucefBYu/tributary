# ADR-009: Control-Plane API Authentication (OIDC Bearer, Resource Server)

- **Status:** **Proposed** (2026-10-05). No invariant changes. Not adopted until `security-reviewer`
  has read it, because it governs auth and a network listener (DoD item 6). Nothing here designs
  cryptography: it selects a standard protocol (OAuth 2.0 bearer tokens, OIDC discovery, JWT access
  tokens) and a vetted library (`phases/wave-3.md` section 7, "Not escalated").
  **Amended 2026-10-05 (rev 2)** after `security-reviewer` read rev 1: no Critical or High; the
  Mediums on D1, D2, D4, D5, D6 and D8 are settled below, with new rules D13 (discovery and
  untrusted key headers) and D14 (PHI content roles). `control-plane-engineer` builds W3-4b on rev 2.
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
| D1 | **Bearer tokens in the `Authorization` header only, sent once.** RFC 6750's form-body and query-string forms are refused (`401 TOKEN_INVALID`): a query string lands in access logs and proxies. **One method only** (RFC 6750 2): a request with an `access_token` query parameter or form field is refused even when it also carries a valid header, and so is a request with more than one `Authorization` header. Header value at most 8 KiB. **Rule, not advice: the server access log is off** (uvicorn `access_log=False`). The API's own request log line carries method, **route template**, status, duration and request id only, never the raw path, the query string or any header. A startup self-check fails the process if the uvicorn access logger has a handler. Reverse-proxy log formats in `deploy/` follow the same rule (follow-up 6) |
| D2 | **JWT validation** with a vetted library (D8): signature against the issuer's JWKS; `iss` equals the configured issuer exactly; `aud` contains the configured audience; `exp` in the future; `nbf` and `iat` honoured when present; leeway at most 60 s. **Required claims, stated to the library: `require=["exp", "iss", "aud", "sub"]`** (PyJWT `options={"require": [...]}`). PyJWT does not require `exp` unless told, so this list is load-bearing. `sub` must be a non-empty string of at most 255 characters |
| D3 | **Algorithm allow-list: `RS256`, `PS256`, `ES256`.** Never `none`, never an `HS*` algorithm, never the algorithm the token header asks for unless it is on the list. The key is selected by `kid` from the cached JWKS only (D5), and its `kty` (and `crv` for EC) must match the algorithm. `algorithms=` is passed to the library as the single allowed algorithm of the selected key, never the whole list. Untrusted key headers: D13 |
| D4 | **Token type.** `typ: at+jwt` (RFC 9068; `application/at+jwt` accepted, case-insensitive) is required by default. `TB_OIDC_ACCEPT_JWT_TYP=true` also accepts `JWT`, for IdPs that do not set `at+jwt` (Keycloak does not by default; **unverified for the current Keycloak release**). An ID token is never accepted as an access token: `aud` must be the API's audience, not the UI's client id. **With `TB_OIDC_ACCEPT_JWT_TYP=true`, three extra checks:** (a) `TB_OIDC_UI_CLIENT_ID` must be set, and the API **refuses to start** if `TB_OIDC_AUDIENCE` equals it; (b) a token carrying a `nonce` or `at_hash` claim is refused (ID-token-only claims, OIDC Core 2 and 3.1.3.6); (c) a token whose Keycloak `typ` **claim** (payload, not header) equals `"ID"` is refused |
| D5 | **JWKS handling: a Tributary key-set wrapper, not `PyJWKClient`'s fetch.** `security-reviewer` read PyJWT 2.15.1: `PyJWKClient` keeps no stale keys, fetches with blocking `urllib` and a 30 s timeout under a lock, and starts its cooldown only after a successful fetch, so a failing IdP is retried on every request. The wrapper: **(1) No network I/O on the request path.** Requests read an immutable snapshot (`kid` to key, fetch time) that one background task replaces atomically. The fetch runs off the event loop (`asyncio.to_thread`). **(2) Short timeout:** `TB_OIDC_HTTP_TIMEOUT_S`, default 5, range 1-10; response body capped at 256 KiB and 32 keys; anything larger fails the fetch. **(3) Schedule:** proactive refresh every `TB_OIDC_JWKS_REFRESH_S` (default 300, range 60-3600) after a success. **(4) Backoff after failures:** exponential from 5 s, doubling, capped at 300 s, full jitter. A failed fetch never replaces the snapshot. **(5) Unknown `kid`:** the request asks the background task for a refresh and waits for it at most `TB_OIDC_HTTP_TIMEOUT_S`. Refreshes triggered this way are single-flight and **at most one per 60 s, counting failed attempts**; during backoff no extra refresh is triggered. Still unknown afterwards: `401 TOKEN_INVALID`. A miss only triggers a refresh if the header is well formed and its `alg` is on the D3 list. **(6) Maximum cache age:** a snapshot is usable for at most `TB_OIDC_JWKS_MAX_STALE_S` after its fetch, **default 3600 (1 h), range 300-14400 (4 h)**; outside the range the API refuses to start. Past that age every token is refused and `/readyz` reports `oidc_keys: keys_unavailable`. Before the first successful fetch the API runs but is not ready and refuses every token. **Give up:** a key the IdP revoked stays trusted until the next successful refresh (normally 5 min) or, if the IdP is unreachable, for at most the stale limit. An IdP outage longer than the limit stops API logins, which is the intent |
| D6 | **Roles.** Read from a configured claim path (`TB_OIDC_ROLES_CLAIM`, default `roles`; Keycloak typically `realm_access.roles` or `resource_access.<client>.roles`), mapped through an operator-supplied table (`TB_OIDC_ROLE_MAP`, IdP role to Tributary role). **The API refuses to start** if the map is empty, unparsable, or maps to a name outside the five roles. The claim at the path must be **a JSON array of strings** (at most 256 entries); any other type (a string, an object, a mixed array) is `401 TOKEN_INVALID`. An absent claim means no roles. Unknown roles are ignored. No mapped role means `403`. **Deny by default at runtime:** an operation without an `x-tributary-roles` entry returns `403` for every caller; an empty list is allowed only together with `security: []` (health). A startup check fails the process if a registered route has no entry, and a contract test checks the same against `openapi.json`. Roles are **global in v0**; channel and environment scopes are later and additive. PHI content: D14 |
| D7 | **What is never logged or returned:** the token, the `Authorization` header, any claim except `sub`. `sub` goes to deployment records (`requested_by`, `created_by`) and, in part 2, the audit topic. It is staff identity, not PHI, but it is personal data, so application logs carry the request id and route template instead. A `401` names `AUTH_REQUIRED` or `TOKEN_INVALID` and never which check failed |
| D8 | **Library: PyJWT with its `crypto` extra, for parsing and verification only.** `jwt.PyJWKSet` / `jwt.PyJWK` parse the JWKS bytes the D5 wrapper fetched, and `jwt.decode` verifies. **`PyJWKClient` is not used** (D5). The wrapper fetches discovery and JWKS with stdlib `urllib.request` and the default verified `ssl` context (system trust store), so no `requests` and no `certifi`. PyJWT 2.15.1 is MIT; `cryptography` 50.0.2 is `Apache-2.0 OR BSD-3-Clause` and needs `cffi` (MIT). Read from PyPI metadata 2026-10-05; **`licence_check.py` has not run on them.** Alternatives: Authlib (BSD-3-Clause, larger surface, includes server-side flows we do not want) and joserfc (BSD-3-Clause, younger). `python-jose` rejected (pulls `ecdsa`, which documents itself as not side-channel safe) |
| D9 | **Lite stack and tests: a local issuer, not a bypass.** Options for `devops-engineer` and `control-plane-engineer` to choose between in W3-4b: (a) Keycloak (Apache-2.0) in an optional Compose profile, the realistic path; (b) a static JWKS file (`TB_OIDC_JWKS_FILE`), with tokens minted by a test helper using PyJWT and a key generated at test start, never committed. (b) is allowed only with profile `lite` and the API bound to loopback, and logs a startup WARN. Both validate through the same D2-D6 code; only the key source differs. **Generating a test key with a standard library call is not key management. A committed key would be, so no key is ever committed** |
| D10 | **CORS off by default.** The UI is served same-origin behind the same proxy. A cross-origin deployment sets an explicit origin allow-list; `*` is refused with credentials |
| D11 | **Health is unauthenticated** (`/livez`, `/readyz`). Bodies carry fixed codes only (`observability/v0` 2) |
| D12 | **"API tokens with scopes and expiry"** (ADR-001 8) are **IdP client-credentials clients**, not tokens the API mints. CI gets a confidential client in the IdP with a role mapping. Revocation and expiry are the IdP's |
| D13 | **Discovery and untrusted key headers.** The discovery document's `issuer` must equal `TB_OIDC_ISSUER` exactly (OIDC Discovery 1.0 section 4.3), and `jwks_uri` must be an `https` URL; otherwise the fetch fails (D5 backoff) and the previous snapshot is kept. A configured `TB_OIDC_ISSUER` or `TB_OIDC_JWKS_URI` that is not `https` makes the API **refuse to start** (the only non-https key source is D9 (b)'s file, under its own conditions). Token headers **`jku`, `x5u`, `x5c` and `jwk` are ignored**: never fetched, never used as a key, never a reason to refresh. A token carrying `crit` is refused (no extension is supported; whether PyJWT enforces `crit` itself is **unverified**, so the wrapper checks). **Required test:** a token signed with an attacker key embedded in `jwk` (and one with `x5c`) is `401`; a token with `jku` or `x5u` pointing at a test server is `401` and that server records no request |
| D14 | **PHI content roles.** Roles are global (D6), so a content grant reaches every environment, production included. **Only `operator` and `admin` may read message content** (payload bytes, per-stage content, claim-check fetches) **or act on it** (reprocess, replay). `viewer`, `developer` and `auditor` see metadata only (ids, channel, stage, status, timings, error codes). Every content read emits an audit event (ADR-004 A5, W3-6). `openapi/v0` part 2 marks such operations `x-tributary-phi-content: true`, and a contract test checks that their roles are a subset of `["operator", "admin"]`. **Give up:** a developer debugging a transform in `dev` needs `operator`, which is global and also grants production content. Environment-scoped roles (ADR-001 roadmap item 13) remove this; until then sites that need it run separate control planes per environment. **Rejected:** content for `developer` (production PHI to everyone who writes channels) and for `auditor` (the auditor reviews who saw content, not the content) |

## 4. Consequences

- **Invariant 3 holds:** no data-plane process calls the IdP or the API. An IdP outage affects API
  logins only after `TB_OIDC_JWKS_MAX_STALE_S` (default 1 h, at most 4 h), and never message flow.
- **Invariant 6:** D1 and D7 keep tokens and claims out of logs, URLs and error bodies. The server
  access log is off by rule (D1). `security-reviewer` checks FastAPI's default validation-error body (it
  echoes input), the D1 startup self-check, and exception handlers.
- **Gives up:** immediate revocation (C), which is bounded by the access-token lifetime the IdP sets.
  Recommend 5-15 minutes, an operator setting. Also gives up a zero-dependency lite stack, because
  D9 adds an issuer.
- **New dependencies** (PyJWT, `cryptography`, `cffi`) go through `licence_check.py` and
  `security-reviewer` in W3-4b.

## 5. Follow-ups

1. `control-plane-engineer` (W3-4b): implement D1-D7, D13 and D14 behind one FastAPI dependency,
   and the D5 key-set wrapper as its own module, unit-tested with a fake clock and a fake fetcher (no
   network). Tests for each rejection: `alg: none`, `HS256` signed with the public key bytes
   (algorithm confusion), a wrong `aud`, a wrong `iss`, expired, no `exp`, no `sub`, `nbf` in the
   future, an ID token (and, with `TB_OIDC_ACCEPT_JWT_TYP=true`, `nonce`, `at_hash`, claim `typ: ID`),
   a token in the query string, a token in header and query string together, two `Authorization`
   headers, an unknown `kid` (one refresh, then none for 60 s even after a failure), the D13 header
   cases, a roles claim that is a string, no mapped role (`403`), an operation without
   `x-tributary-roles` (`403`). Startup refusals: empty role map, audience equal to UI client id with
   the flag on, non-https issuer or JWKS URI, max-stale outside 300-14400. Key-set tests: a failing
   fetcher is retried on the backoff schedule, not per request; a slow fetcher does not block the
   event loop; a snapshot older than the limit refuses tokens and flips `/readyz`. Each test proves
   the token reached the validator, and a broken control that skips `aud` goes red.
2. `control-plane-engineer` and `devops-engineer`: choose D9 (a) or (b) for Compose; (b) for pytest.
3. `security-reviewer`: review this ADR before W3-4b merges, and the implementation at exit
   (criterion 4).
4. `architect`: roles per operation are in `openapi/v0`. Channel and environment scopes come with RBAC
   (ADR-001 roadmap item 13).
5. Human, **only if** a site requires API-minted tokens without an IdP: that is option B, which is
   key management and escalated. Not expected for v0.
6. `devops-engineer` (W3-4b Compose, Helm later): proxy and ingress log formats carry no query
   string and no `Authorization` header (D1).
7. `architect` (set 1.8 part 2): `observability/v0` metric for key-set refreshes by outcome, so the D5
   failure path is visible (DoD item 4); `x-tributary-phi-content` and its contract test (D14).
