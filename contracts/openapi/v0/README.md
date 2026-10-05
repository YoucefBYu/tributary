# Contract: control-plane API (OpenAPI) — v0 (draft)

**Contract id:** `tributary.openapi/v0`
**Status:** Draft, **new in contract set 1.8 (2026-10-05)**, part 1 of 2 (W3-1a). Part 2 (W3-1b) adds
message search and detail, reprocess, replay, stats, the SSE event stream and the
`_tb.<env>.audit` event shape.
**Owner:** `architect`. Propose changes by message with the exact diff and the reason.
**Source:** ADR-001 6.6-6.8 and 8; ADR-009 (authentication, Proposed); `channel-spec/v0` 6 (the
`_tb.<env>.channels` record); `observability/v0` 2 (health).
**Document:** [`openapi.json`](openapi.json) (OpenAPI 3.1.0, JSON so `validate.py` needs no YAML parser).
**Examples:** [`examples/manifest.json`](examples/manifest.json).

Consumers: `tributary-api` (W3-4b, contract tests), the UI's generated client, the CLI (W3-7),
`qa-engineer`'s conformance suite.

---

## 1. What part 1 covers

| Area | Operations |
|---|---|
| Health | `GET /livez`, `GET /readyz` (unauthenticated; body per `observability/v0`) |
| Environments | `GET /v1/environments` |
| Channels and versions | `GET/POST /v1/channels`; `GET /v1/channels/{channel}`; `GET/POST /v1/channels/{channel}/versions`; `GET /v1/channels/{channel}/versions/{version}` |
| Validation | `POST /v1/validate` |
| Deployments | `GET /v1/environments/{env}/deployments`; `GET /v1/environments/{env}/channels/{channel}/deployments`; `POST .../deploy`, `.../rollback`, `.../undeploy` |

Every operation names its roles in `x-tributary-roles`. Authentication and role mapping are
[ADR-009](../../../docs/adr/ADR-009-control-plane-api-authentication.md).

## 2. Invariants this API must keep

1. **Invariant 3.** Nothing in the data plane calls this API. Deploy writes `_tb.<env>.channels` and
   returns. It never waits for a worker, and a stopped API stops no worker (W3-4b verify).
2. **Invariant 6.** No response echoes a request body, a header or a token claim. Validation issues
   are `json_path` plus keyword, never the jsonschema message, which embeds the failing value. Problem
   bodies have no `instance` member, because it would echo the path. `detail` is fixed text per code.
   Metrics label the route **template** (`observability/v0` 3.2). Application logs never contain the
   `Authorization` header, a token or a claim.
3. **Strict producers.** Request bodies are `additionalProperties: false`, except the channel document,
   which `channel-spec/v0` types (3.1).

## 3. Decisions in this document

### 3.1 The channel document is typed loosely here, strictly by its own schema

`ChannelDocument` checks `apiVersion`, `kind` and `metadata.name`, and links the authority in
`x-tributary-schema`. The server validates against `channel-spec/v0/channel.schema.json` and answers
`422 SPEC_INVALID` with `errors`. **Rejected:** a cross-file `$ref` to the channel schema. Its `$id` is
an `https://tributary.dev/` URI that nothing serves. A validator that tries to fetch it fails or needs
network access, and two copies would drift. **Give up:** generated clients get `spec` as an untyped
object. Clients that want types generate them from the channel schema.

### 3.2 Versions are immutable and numbered by the server

`POST .../versions` stores the next version number (`latest + 1`, starting at 1) and sets
`metadata.version` to that number in the stored document. A client that sends `metadata.version` gets `422 VERSION_SET_BY_CLIENT`. A path and document
name mismatch is `422 CHANNEL_NAME_MISMATCH`. `spec_sha256` is the SHA-256 of the stored document's
**published bytes**, the exact value written to `_tb.<env>.channels`. A stored version is never edited.

### 3.3 Rollback republishes the old document unchanged

Rollback publishes the target version's stored document **as it is, `metadata.version` included**.
Workers then stamp `tb-channel-version` with the old number, which is true: the old code is running.
The runtime accepts a lower version (`ChannelCache` applies whatever the latest record is; read in
`runtime/channels.py`, not run). **Rejected:** minting `n+1` as a copy of the old version. The archive
would then attribute old code to a new version number. **Give up:** `tb-channel-version` is not
monotonic over time on a channel. The deployment history says why.

### 3.4 Deploy is asynchronous, and `published` means committed

`202` with a `pending` record. The publisher (W3-4b inline, then the W3-5 reconciler) moves it to
`published` once the `_tb.<env>.channels` produce is acknowledged (`acks=all`), or to `failed` with
`PUBLISH_FAILED` or `PUBLISH_TIMEOUT`. `published` says nothing about which workers have applied the
version. That is observed through `tb_spec_version` (`observability/v0` 3.3) and, in part 2, stats.
A newer request for the same channel and environment supersedes an older `pending` one.

### 3.5 Optimistic concurrency, not locks

`expected_current_version` is optional on deploy and required on rollback and undeploy. A mismatch is
`409 CURRENT_VERSION_MISMATCH`. Two operators cannot roll back twice and land two versions back by
accident. Re-deploying the current version is allowed and republishes, which is the repair action
after a lost or compacted record.

### 3.6 Management modes (ADR-001 6.8)

`git-managed` environments accept deploy and rollback only for versions whose `origin.kind` is `git`;
otherwise `409 MANAGEMENT_MODE`. `origin` is client-asserted. It enforces the workflow, not
security. RBAC and the audit trail (part 2) are the controls. The reconciler and both modes are W3-5.

### 3.7 Paths use sub-resources, not `:verb`

ADR-001 6.7 sketched `POST /v1/channels/{id}:deploy`. Part 1 uses
`/v1/environments/{env}/channels/{channel}/deploy`, because deployment is per environment and a
version is not. A colon after a path template is also handled unevenly by routers and generators.
`POST /v1/validate` sits outside `/v1/channels/` so that a channel named `validate` stays addressable.

### 3.8 Pagination and limits

Cursor pagination (`limit` 1-200, default 50, opaque `cursor`, `next_cursor` null at the end). Request
bodies up to 262144 bytes (`413`). Unknown query parameters are ignored.

## 4. Errors

RFC 9457 `application/problem+json`, restricted (`Problem` schema): `code` is a closed list. 401
distinguishes only `AUTH_REQUIRED` from `TOKEN_INVALID`. Which token check failed is not disclosed,
and `WWW-Authenticate` carries no `error_description`.

## 5. Conformance

`validate.py` checks `openapi.json` with `openapi-spec-validator` (a structural OpenAPI 3.1 check,
counted as one check), checks each component schema against the JSON Schema 2020-12 metaschema, and
validates every example against its named component. The whole document is the root, so internal
references resolve. One entry is the `observability/v0` api readiness example, so the two contracts
cannot drift on that body.

```
uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py
```

**`openapi-spec-validator` licence (checked 2026-10-05 from PyPI metadata, not by `licence_check.py`):**
0.9.0 Apache-2.0. Closure: `jsonschema` MIT, `jsonschema-path` Apache-2.0 (`requests` is only an
optional extra since 0.5), `pathable` Apache-2.0, `lazy-object-proxy` BSD-2-Clause,
`openapi-schema-validator` BSD-3-Clause, `rfc3339-validator` MIT, `pydantic` and `pydantic-settings`
MIT, `python-dotenv` BSD-3-Clause, `PyYAML` MIT, `referencing` MIT. No MPL, so no `certifi`. It runs
only via `uv run --with` in a contract check. It is not in the lockfile and not distributed.

Contract tests in W3-4b (`control-plane-engineer`) assert that real responses validate against these
components, and that a 4xx body never contains the request's own values.

## 6. Open items

- Part 2 (W3-1b): messages, reprocess, replay, stats, events (SSE), audit event shape.
- Channel-level role scopes (ADR-001 8: "channel-level scopes"): roles are global in v0 (ADR-009).
- Environment-scoped roles (for example, operator in `dev` only): not in v0.
