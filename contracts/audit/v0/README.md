# Contract: audit records (`_tb.<env>.audit`) — v0 (draft)

**Contract id:** `tributary.audit/v0`
**Status:** Draft, **new in contract set 1.8 part 2 (W3-1b, 2026-10-06)**.
**Owner:** `architect`. Propose changes by message with the exact diff and the reason.
**Source:** ADR-004 R2, R7 (system of record, retention); ADR-009 D7, D14 (who, which roles, content
rule); `topics/v0` 2.1 and 3.4.1; `tributary-archiver` `audit.py`, `reprocess.py`, `replay.py` (W3-3).
**Schema:** [`audit-event.schema.json`](audit-event.schema.json). **Examples:**
[`examples/manifest.json`](examples/manifest.json).

Producer: the control-plane API (`AuditHook`, W3-32), under the control-plane publisher principal
(WRITE `_tb.<env>.audit`). Consumer: the events archiver (READ LITERAL `_tb.<env>.audit`, group
`tb.<env>.archiver`), into ClickHouse `audit_log` (W3-30 part 2).

---

## 1. Transport

| Item | Value |
|---|---|
| Topic | `_tb.<env>.audit`: 6 partitions, 30 d, delete (`topics/v0` 2, `topic-defaults.json` `classes.audit`). A **buffer**, not the store |
| Key | `actor` as UTF-8 bytes (`topics/v0` 1: "actor id"), so one subject's records stay in order |
| Value | One JSON object, UTF-8, at most 8192 bytes, valid against the schema |
| Headers | None required. An audit record is not a message: no `tb-*` envelope (invariant 8 does not apply) |
| Producer | `acks=all`, `enable.idempotence=true`, the invariant 1 profile (`topic-defaults.json` `clients.receiver_producer` values) |

## 2. Fields

Required on every record: `v`, `audit_id`, `ts`, `environment`, `action`, `phase`, `actor`,
`actor_roles`. The rest depend on the action (schema `allOf`):

| Action | Phases | Also required |
|---|---|---|
| `message.content.view` | `requested` only | `channel`, `message_id`, `stage` (`destination` when the row has one) |
| `message.reprocess` | `requested`, then `completed` or `failed` | `channel`, `message_id`, `operation_id`; `allow_reorder` on `requested`; `count` on `completed`/`failed` |
| `channel.replay` | `requested`, then `completed` or `failed` | `channel`, `operation_id`, `target_topic`, `range_start`, `range_end`; `allow_reorder` on `requested`; `count` on `completed`/`failed` |
| `channel.version.create` | `requested` only | `channel`, `version` |
| `deployment.deploy`, `deployment.rollback` | `requested` only | `channel`, `deployment_id`, `version` (`previous_version` when one was current) |
| `deployment.undeploy` | `requested` only | `channel`, `deployment_id`; no `version` |

Any `failed` record also carries `error_code` (the library's or the API's problem code).

## 3. Rules

1. **`requested` fails closed.** It is produced and acknowledged **before** content is read, a record
   is re-published, a version is stored or a channel record is published. If the produce is not
   acknowledged, the action does not happen: synchronous operations answer `503 AUDIT_UNAVAILABLE`, a
   replay ends `failed` with `failure_code: AUDIT_UNAVAILABLE` and has read nothing. This is
   `archiver/audit.py`'s rule, extended to every action.
2. **One record per content view** (`getMessageContent` call), phase `requested`. A view whose read
   then fails is still on the record: the audit says who asked, which is what an auditor needs.
3. **Reprocess and replay** follow the library: `requested` (fail closed), then `completed` or
   `failed` **best effort**. A follow-up that cannot be produced is logged by class name
   (`audit_followup_failed`) and does not turn a delivered re-publish into an error.
4. **`allow_reorder` is on every reprocess and replay `requested` record**, `true` or `false`
   (invariant 5; `wave-3.md` 2.6 H8). A per-key replay without it is refused before any record.
5. **`target_topic` equals `<environment>.<channel>.raw`** of the same record. The schema checks the
   class (`.raw`), not the equality; the W3-32 contract test checks the equality.
6. **No PHI (invariant 6).** Identifiers, codes and counts only. Never payload bytes, a record key, a
   header value, a token, the `Authorization` header, a claim other than `sub`, a URL or a query
   string. `actor` is `sub`: staff identity and personal data, not PHI (ADR-009 D7).
7. **`audit_id` is minted once per record** (UUIDv7) and reused on every retry of that send, so a
   duplicate delivery collapses at query time in `audit_log` (section 4).
8. **Refused requests (`401`, `403`) are not audited in v0.** They are counted by
   `tb_api_requests_total` (`observability/v0`). Open item (section 6).

### 3.1 Mapping from the library's `AuditEvent`

`tributary.archiver.audit.AuditEvent` fields map one to one: `action`, `phase`, `actor`, `channel`,
`message_id`, `request_id` → **`operation_id`**, `count`, `allow_reorder`, `target_topic`,
`range_start`, `range_end`, `ts` (rendered `YYYY-MM-DDTHH:MM:SS.mmmZ`). The API adds `v`,
`audit_id`, `environment`, `actor_roles`, `request_id` (its HTTP request id) and `error_code`.

**Two library gaps, sent to `archive-engineer` (W3-3 owner), needed before W3-32 can conform:**

- `AuditEvent` has no error code, so the hook cannot fill `error_code` on `failed`. Requested: an
  `error_code: str | None` field set from the raised error's `code` on every `failed` event.
- `ACTOR` (`[A-Za-z0-9][A-Za-z0-9._:@/=-]{0,127}`) is narrower than ADR-009's `sub` (`[\x21-\x7e]{1,255}`).
  A valid subject such as `idp|1234` is refused as `INVALID_REQUEST`. Requested: accept the `Subject`
  grammar, the one this schema and `openapi/v0` use.

## 4. `audit_log` (ClickHouse, the system of record; ADR-004 R7)

One row per record. Column names equal the field names; absent fields are `NULL` or `''`.

| Column | Type |
|---|---|
| `audit_id` | `UUID` |
| `ts` | `DateTime64(3, 'UTC')` |
| `environment`, `action`, `phase` | `LowCardinality(String)` |
| `actor` | `String` |
| `actor_roles` | `Array(LowCardinality(String))` |
| `request_id`, `operation_id`, `message_id`, `deployment_id` | `Nullable(UUID)` |
| `channel`, `destination`, `target_topic` | `String` (default `''`) |
| `stage`, `error_code` | `LowCardinality(String)` (default `''`) |
| `version`, `previous_version` | `Nullable(UInt32)` |
| `allow_reorder` | `Nullable(Bool)` |
| `range_start`, `range_end` | `Nullable(DateTime64(3, 'UTC'))` |
| `count` | `Nullable(UInt64)` |
| `source_partition`, `source_offset` | `UInt32`, `UInt64` (provenance on `_tb.<env>.audit`) |
| `inserted_at` | `DateTime64(3, 'UTC') DEFAULT now64(3)` |

- **Engine (ADR-004 R7 rev 3):** plain append-only `MergeTree`, **never `ReplacingMergeTree`**, which
  would let the INSERT-only archiver principal overwrite a row by inserting the same sorting key.
  `PARTITION BY toYYYYMM(ts)`; sorting key for reads (recommended `(actor, ts, audit_id)`).
- **Query-time dedup:** every read collapses redeliveries on `audit_id`, keeping the earliest-archived
  copy (`ORDER BY inserted_at, source_partition, source_offset LIMIT 1 BY audit_id`). No `FINAL`.
- **Tamper signal:** two rows with one `audit_id` and different fields mean something other than a
  redelivery wrote the table. The API's periodic check counts them as `tb_audit_tamper_suspect_ids`,
  with `tb_audit_tamper_check_timestamp_seconds` beside it (`observability/v0` 3.4), and both are
  alerted on.
- **TTL:** `toDateTime(ts) + INTERVAL <TB_ARCHIVE_AUDIT_TTL_DAYS> DAY`, default 2190, range 1-36500,
  `ttl_only_drop_parts = 1`, applied by the migrate step only, decrease guarded (ADR-004 R2, R6).
- **Access:** archiver users INSERT; the API user SELECT (ADR-004 A5). A ClickHouse admin can alter
  rows: tamper evidence is an operational control in v0 (ADR-004 R7, give up).
- **Invalid records:** a value that does not parse or fails the schema is not stored. The archiver
  counts it under its rejected-record counter with a fixed code and logs topic, partition and offset
  only, so an operator can inspect it with Kafka tools within the 30 d buffer.

## 5. Conformance

`validate.py` validates the examples (`FILE_CONTRACTS` includes `audit`). The W3-32 integration test
asserts exactly one record per content view, no payload bytes in any record, and refusal with
`AUDIT_UNAVAILABLE` and no content read while Kafka is stopped (`wave-3.md` W3-32 verify).

## 6. Open items

- An auditor read API over `audit_log` (`auditor`, `admin`): not in wave 3.
- Auditing refused requests (`401`, `403`): v0 counts them only.
- Tamper evidence beyond ClickHouse access control (object lock, hash chain): ADR-004 R7 give up.
