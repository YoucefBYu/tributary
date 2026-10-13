# Contract: control-plane API (OpenAPI) — v0 (draft)

**Contract id:** `tributary.openapi/v0`
**Status:** Draft, **new in contract set 1.8**. Part 1 (W3-1a, 2026-10-05) and **part 2 (W3-1b,
2026-10-06)**: message search, detail and content, reprocess, replay, stats, the SSE event stream and
the effective-retention view. The `_tb.<env>.audit` record shape is its own contract,
[`audit/v0`](../../audit/v0/README.md). `info.version` 0.2.0.
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

### 1.1 What part 2 covers

All under `/v1/environments/{env}/channels/{channel}/` unless stated. `P` marks
`x-tributary-phi-content: true` (roles `operator`, `admin`; audited, fail closed).

| Area | Operations | Roles |
|---|---|---|
| Search | `POST .../messages/search` | all five |
| Detail | `GET .../messages/{message_id}` (timeline, destination statuses, content index) | all five |
| Content **P** | `GET .../messages/{message_id}/content?stage=&destination=` | operator, admin |
| Reprocess **P** | `POST .../messages/{message_id}/reprocess` | operator, admin |
| Replay **P** | `POST .../replays`; `GET .../replays`, `GET .../replays/{replay_id}` (metadata) | P: operator, admin; GET: all five |
| Stats | `GET .../stats?window=` | all five |
| Live events | `GET /v1/environments/{env}/events?channel=...` (SSE) | all five |
| Retention | `GET /v1/environments/{env}/retention` (read-only) | all five |

Every mutating or content operation also names its audit action in `x-tributary-audit-action`
(`audit/v0` 2). Part 1's create, deploy, rollback and undeploy carry it too (additive extension).

Every operation names its roles in `x-tributary-roles`. Authentication and role mapping are
[ADR-009](../../../docs/adr/ADR-009-control-plane-api-authentication.md) (rev 2). Rules this contract
carries from it:

- **Deny by default (D6).** An operation without `x-tributary-roles` is refused to every caller. An
  empty list is valid only with `security: []` (health). A contract test checks every operation.
- **PHI content (D14).** Only `operator` and `admin` read message content or act on it (reprocess,
  replay). Part 2 marks those operations `x-tributary-phi-content: true`; their roles must be a subset
  of `["operator", "admin"]`. `viewer`, `developer` and `auditor` get metadata only. Part 1 has no
  content operation. **Contract test (W3-6):** every operation with `x-tributary-phi-content: true` has
  roles within `["operator", "admin"]` and an `x-tributary-audit-action`; no response schema of an
  operation without the marker contains `payload_base64`.

## 2. Invariants this API must keep

1. **Invariant 3.** Nothing in the data plane calls this API. Deploy writes `_tb.<env>.channels` and
   returns. It never waits for a worker, and a stopped API stops no worker (W3-4b verify).
2. **Invariant 6.** No response echoes a request body, a header or a token claim. Validation issues
   are `json_path` plus keyword, never the jsonschema message, which embeds the failing value. Problem
   bodies have no `instance` member, because it would echo the path. `detail` is fixed text per code.
   Metrics label the route **template** (`observability/v0` 3.2). Application logs never contain the
   `Authorization` header, a token or a claim. The server access log is off; the request log carries
   the route template, never the raw path or query string (ADR-009 D1).
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

**Default target (W3-1b clarification, wording only).** Rollback without `to_version` targets the most
recently published version that differs from the current one. After a rollback, the version it
replaced is therefore the default target of the next rollback, so repeated default rollbacks alternate
between two versions. Use `to_version` to go further back.

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

### 3.9 Part 2 is scoped to one environment and one channel

Every message operation sits under `/v1/environments/{env}/channels/{channel}/`. The query library
requires a channel (it is the sorting-key prefix, and search also requires `since`), reprocess
authorises the channel before any payload is read, and replay never leaves its channel. A future
channel-level role scope (ADR-009 D6) then attaches to the path. **Rejected:** ADR-001 6.7's
`/v1/messages/{id}` (no channel, so an unbounded archive scan for a non-v7 id, and no place for a
channel scope). **Give up:** a cross-channel search; the UI searches one channel at a time.

### 3.10 Search is a POST, and its filters never sit in a URL

`POST .../messages/search` takes `MessageSearchRequest`. URLs reach proxy and access logs (ADR-009 D1),
and identifier search, when it comes, carries values treated as PHI (ADR-004 H3). Fields map to the
library's `SearchFilter` one to one. `limit` is 1-200, default 50 (part 1's bounds; the library allows
500). The cursor is `SearchCursor`, up to 512 characters: the library's cursor encodes timestamp,
channel, id, stage, destination and attempt, which can exceed part 1's 256. Results are deduplicated
events, newest first. A search attribute named for a direct identifier, or ending `_hmac`, is refused
by the schema and by the library (`QUERY_INVALID` → `422 REQUEST_INVALID`).

### 3.11 Detail and content are separate operations

**Options.** (a) One detail operation that includes content for `operator` and `admin` and omits it
for the other roles. (b) Metadata detail for every role, plus a per-stage content operation for
`operator` and `admin` (**chosen**). **Criteria:** ADR-009 D14 least privilege; one audit record per
content view (W3-32 verify); bounded responses. (a) varies the response shape by role and writes an
audit record whenever an operator opens a timeline. **Give up:** showing a stage's bytes costs a
second call, and a diff between two stages costs two audited reads.

`MessageDetail` is exit criterion 1's response without bytes: the timeline (library `timeline`,
oldest first, at most 1000 rows, `truncated` beyond that), one `DestinationStatus` per destination
the message was routed to (`state` is the stage of its newest event, `attempt` the highest
`tb-attempt`) and the content index (`ContentRef`: stage, destination, type, size, storage).

### 3.12 Content (PHI)

`GET .../content?stage=received|routed|dlq[&destination=]` returns one `message_content` row (ADR-004
A2): inline payloads as base64 of the exact committed bytes, `Cache-Control: no-store`, and the stored
envelope headers, never `tb-usr-*` or `tb-x-*`. Before the read the API produces one `audit/v0`
`message.content.view` record and waits for the acknowledgement; if it fails, `503
AUDIT_UNAVAILABLE` and nothing is read. Content storage off (`TB_ARCHIVE_CONTENT`, ADR-004 A4): `409
CONTENT_STORAGE_OFF`. No row for that stage: `404 CONTENT_NOT_FOUND`. **Claim-checked payloads return
the locator only.** Fetching the object through the API would put bucket credentials in the API
process, which is a custody decision (ADR-004 H1, A3), so it is an open item, not v0.

**W3-6 rulings (2026-10-13).**

- **`422 REQUEST_INVALID`, not `400`,** for a well-formed query whose combination is refused: a
  `destination` with `stage=received`, or none with `stage=routed`. This matches `DestinationQuery`
  and search's "`until` not after `since`". `400` stays for a malformed value (a stage outside the
  enum, a non-UUID id). The order of checks is validation, then audit, then read: a refused request
  writes no audit record and reads nothing. Declared in 0.2.2.
- **Where "content storage off" comes from: `archive_settings`, not the table's absence.** The migrate
  step records content as enabled or disabled on every run (`archive-engineer`, with W3-30). Table
  absence is ambiguous: a failed or partial migrate looks the same, and a table left behind after
  content was switched off would make new messages answer `CONTENT_NOT_FOUND`, which hides the cause.
  The API answers `409 CONTENT_STORAGE_OFF` when the latest setting says disabled. When the setting
  says enabled but the table is missing, it answers `503 ARCHIVE_UNAVAILABLE`. Rows already stored
  while content was on stay readable until their TTL (audited as any view). **Interim:** until the
  flag exists, table absence may stand in for "off". It is removed when the flag lands.
- **Archive SQL has one owner: `tributary-archiver`'s query library (`archive-engineer`).** Stats and the
  content read move there beside `search` and `timeline`, and the API calls them. Two copies of the
  deduplication rule (`events/v0` 4.1 rule 2) drift; that is a recurring defect here. W3-6 may merge
  with its own parameterised readonly SQL plus a test that pins it, provided the move lands before the
  wave 3 gate. The tamper check stays mirrored in the API with an equality test, because the API
  owning that query is the point (ADR-004 R7 rev 3).

### 3.13 Reprocess

`POST .../messages/{message_id}/reprocess` with `{operation_id, allow_reorder}` calls
`ReprocessService.reprocess(message_id, channel=channel, request_id=operation_id, actor=sub,
allow_reorder=...)`. Synchronous: `200` once the broker acknowledged the record (`acks=all`).

Order of checks: role (`403`); channel exists (`404 NOT_FOUND`); **channel deployed in `{env}`, else
`409 NOT_DEPLOYED`** (the ordering traits come from the deployed document's `spec.ordering`, and the
library skips the traits lookup when `allow_reorder` is true, so the API checks deployment itself);
then the library's refusals, the audit `requested` record (fail closed), the fetch and the publish
(mapping in 4.1). A failure after `requested` puts the acknowledged count in the problem's
`republished`.

**Identity (pinned here, as the set 1.8 W3-3 ruling promised).** The new `tb-msg-id` is a
deterministic UUIDv7-shaped value: the 48-bit timestamp of `operation_id`; version 7; then, from
`d` = the first 10 bytes of `SHA-256("tributary.reprocess.v0" || 0x00 || original id (16 bytes) ||
operation_id (16 bytes))` read big-endian, `rand_a = (d >> 68) & 0xFFF` and `rand_b = d & (2^62 - 1)`;
variant `0b10` (`archiver/republish.py`). A retry with the same `operation_id` re-publishes the same
id, which loader idempotency absorbs (invariant 4). A new `operation_id` is a new, deliberate
reprocess. `tb-parent-id` is the original id and `tb-received-at` is copied (`envelope/v0` 6 rule 5).

**Refused, by the library's rules:** a message with a destination-scoped `dlq` event
(`DESTINATION_SCOPED`, until wave 4's re-entry points, `wave-3.md` 2.6 H5); a message with a parent
(`CHILD_MESSAGE`: reprocess the root); no `raw` coordinates in the events (`NO_PAYLOAD_COORDINATES`);
a per-key channel without `allow_reorder` (`ORDERING_WOULD_BREAK`, invariant 5).

### 3.14 Replay

`POST .../replays` with `ReplayRequest` calls `ReplayService.replay(channel, start=, end=,
replay_id=, actor=sub, max_records=, deadline_s=, allow_reorder=)` in the background and answers
`202` with a `Replay` record (state in PostgreSQL, the API's own; invariant 3 is untouched).

- **Why asynchronous:** the library's deadline is 900 s by default and up to 3600 s, longer than any
  HTTP intermediary waits. **Give up:** the caller polls `GET .../replays/{replay_id}`.
- **Synchronous refusals, before `202` and before any audit record:** `422 REQUEST_INVALID` (`end` not
  after `start`), `409 NOT_DEPLOYED`, `409 ORDERING_WOULD_BREAK`, `409 REQUEST_ID_REUSED`. Everything
  after the `requested` record ends the replay `failed` with a `ReplayFailureCode` and the partial
  `republished` count (at-least-once: acknowledged records stay on `raw`).
- **`replay_id` is the idempotency key.** Same id and identical body: `200` with the existing record if
  it is `running` or `completed`; a `failed` one runs again (`202`) and regenerates the same ids.
  Same id, different body: `409 REQUEST_ID_REUSED`.
- **Interrupted:** if the API process stops mid-run, the record stays `running` until `deadline_at`,
  then reads as `failed`, `INTERRUPTED`. Re-submitting resumes safely (same ids).
- **No target.** The request has no target field and refuses unknown fields: replay writes only to
  the source channel's `raw` (lead's ruling, `wave-3.md` 2.6 H8). "Shadow" keeps its `topics/v0` 3.6
  meaning, a consumer group, and no replay feature uses the word.
- **Range** `[start, end)` on the Kafka record timestamp; `max_records` bounds the offset span
  (`RANGE_TOO_LARGE`, nothing published). The API runs the replay under the channel's `reprocessor`
  principal (`topics/v0` 3.4.1), so the API process holds that credential per channel.

### 3.15 Stats come from the archive, without consumer lag

`GET .../stats?window=` counts deduplicated events over the window, per stage and per destination,
with `duration_ms` p50/p95 on `sent`, the top 20 error codes, and the channel versions stamped on events
with their instance counts. The last is how a deploy or rollback is seen applied (3.4). **No consumer
lag:** the API holds no Kafka grant to read offsets (`lag_reader` is KEDA's and the exporter's), and
lag is already on Prometheus. **Give up:** stats lag the data plane by the archiver's batch delay, and
the UI shows lag only through Grafana.

### 3.16 Live events (SSE) tail the archive

`GET /v1/environments/{env}/events?channel=a&channel=b` (1-16 channels) streams `text/event-stream`.

- **Source: the archive, polled** (recommended every 2 s), with the search deduplication.
  **Rejected:** consuming `_tb.<env>.events` in the API, which needs READ on every channel's events and
  one consumer per stream. **Give up:** latency is the archiver's batch delay plus the poll; with the
  archiver down the stream sends heartbeats only.
- **Events:** `message` (data `MessageEvent`, `id` an opaque cursor), `heartbeat` every 15 s (data
  `StreamHeartbeat`), `reset` (`cursor_expired`, `cursor_invalid`, and since 0.2.3 `backlog_too_dense`:
a window still too dense to read completely after 10 halvings; the stream never skips events
silently, it resets and continues live, and the client fills the gap with search) and `end`
  (`token_expiring`, `max_duration`, `server_shutdown`; the server closes). Delivery is at-least-once;
  clients deduplicate on `(message_id, stage, destination, attempt)`.
- **Resume:** `Last-Event-ID` within 15 minutes; older, `reset` and live.
- **Authentication:** the bearer header, so clients use a fetch-based stream reader. `EventSource`
  cannot send headers, and a query-string token is refused (ADR-009 D1). The server ends a stream no
  later than the token's `exp`, and after 60 minutes at most. At most 4 open streams per subject
  (`429 TOO_MANY_STREAMS`).
- Metadata only: every role. Partition events (`partition_halted`) are not streamed in v0; the
  `partition_halted` alert covers them (`wave-3.md` 2.5 H1).

### 3.17 Retention is read-only

`GET /v1/environments/{env}/retention` reads the `archive_settings` rows the migrate step writes
(ADR-004 R3, R6): per store, `days`, `previous_days`, `applied_at`, `release`. There is no write
operation: the API holds SELECT only on ClickHouse (ADR-004 A5, option c rejected). Kafka topic
retention is not shown (R8).

## 4. Errors

RFC 9457 `application/problem+json`, restricted (`Problem` schema): `code` is a closed list. 401
distinguishes only `AUTH_REQUIRED` from `TOKEN_INVALID`. Which token check failed is not disclosed,
and `WWW-Authenticate` carries no `error_description`.

### 4.1 Part 2 codes and the archiver library

Library codes keep their names. The API never adds a library exception's text to `detail`.

| Library (`tributary.archiver.errors`) | HTTP, `code` |
|---|---|
| `QueryError` `QUERY_INVALID` / `CURSOR_INVALID` | `422` / `400`, `REQUEST_INVALID` |
| `QueryError` `QUERY_FAILED`; ClickHouse unreachable | `503 ARCHIVE_UNAVAILABLE` |
| `MessageNotFound` | `404 MESSAGE_NOT_FOUND` (the archive may lag) |
| `ReprocessRefused` `DESTINATION_SCOPED`, `CHILD_MESSAGE`, `NO_PAYLOAD_COORDINATES`, `ORDERING_WOULD_BREAK` | `409`, same code |
| `ReprocessRefused` / `ReplayRefused` `INVALID_REQUEST` | `422 REQUEST_INVALID` |
| `ReplayRefused` `ORDERING_WOULD_BREAK` | `409`, same code (before `202`) |
| `PayloadUnavailable` `RETENTION_LAPSED` | `410`, same code |
| `PayloadUnavailable` `OFFSET_NOT_FOUND`, `RECORD_MISMATCH`, `RECORD_UNDECODABLE` | `409`, same code |
| `PayloadUnavailable` / `ReplayRefused` `SOURCE_ERROR`, `SOURCE_TIMEOUT` | `503`, same code (replay: `failure_code`) |
| `PublishError` `PRODUCE_FAILED`, `PRODUCE_TIMEOUT`, `PRODUCE_REFUSED` | `503`, same code, with `republished` (replay: `failure_code`) |
| `PublishError` `ENVELOPE_INVALID` | `409`, same code (replay: `failure_code`) |
| `ReplayRefused` `TOPIC_NOT_FOUND`, `RANGE_TOO_LARGE`, `DEADLINE_EXCEEDED` | replay `failure_code` only (they occur after `requested`) |
| `AuditHook` raised on `requested` | `503 AUDIT_UNAVAILABLE` (replay: `failure_code`); nothing read or published |

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

- ~~Part 2 (W3-1b)~~ published 2026-10-06 (above; audit shape in `audit/v0`).
- Claim-check fetch through the API (3.12): needs a custody decision on bucket credentials in the API.
- Destination-scoped reprocess: wave 4 (`wave-3.md` 2.6 H5).
- Cross-channel search, partition events on the stream, an auditor read API over `audit_log`.
- `observability/v0` metric for OIDC key-set refreshes by outcome (ADR-009 follow-up 7): not in this
  part; still owed.
- Channel-level role scopes (ADR-001 8: "channel-level scopes"): roles are global in v0 (ADR-009).
- Environment-scoped roles (for example, operator in `dev` only): not in v0.
