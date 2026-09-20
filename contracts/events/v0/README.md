# Contract: lifecycle events on `_tb.events` — v0 (draft)

**Contract id:** `tributary.events/v0`
**Status:** Draft.
**Owner:** `architect`.
**Source:** ADR-001 sections 5.5, 6.6 and 6.10.
**Schema:** [`event.schema.json`](event.schema.json) · **Examples:** [`examples/manifest.json`](examples/manifest.json)

Realises invariant 9 (the archive is off the critical path) and is the main place where invariant 6
(no PHI) can be violated by accident.

Consumers: `tributary-archiver` (ClickHouse and the message browser), metrics and alerting, the
live UI stream, replay tooling.

---

## 1. Record shape

| Part | Value |
|---|---|
| Topic | `_tb.<env>.events` |
| Key | `tb-msg-id` for message events; `<topic>:<partition>` for partition events |
| Value | Single-line JSON, UTF-8, per this schema |
| Headers | `traceparent` only |

JSON rather than Avro or Protobuf: no schema registry to operate, `kcat | jq` debugging, and
ClickHouse ingests JSON natively. The cost is size and parse time — measured against nothing yet,
so **TODO: confirm the archiver keeps up at the S1/S2 event rate**. If it does not, the escape hatch
is Protobuf on the same field set, which is why field names are flat and typed.

### 1.1 Two kinds, one topic

`kind` discriminates:

- `kind: "message"` — a message crossed a lifecycle stage (ADR-001 5.5). `message_id` required.
- `kind: "partition"` — a partition was paused, resumed or halted by the blocking retry policy
  (ADR-001 6.4). There is no single message this is about, so `message_id` is optional and carries
  the triggering message when there is one.

One topic keeps invariant 2's topic list unchanged, keeps operator and message timelines in one
ordered stream, and lets the browser show "delivery stopped here, and why" next to the message.
**This differs from the S5 spike**, which emits partition events with the same shape as message
events; that made `message_id` meaninglessly non-null.

---

## 2. Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `v` | integer | yes | Schema major version. `0` in v0. A consumer that sees a higher `v` must not crash; it may ignore unknown fields. |
| `kind` | `message` \| `partition` | yes | Section 1.1. |
| `ts` | RFC 3339 UTC, 3 to 9 fractional digits | yes | When the transition happened, not when the event was produced. Same format as `tb-received-at`, so one time parser covers both contracts. |
| `channel` | string | yes | |
| `channel_version` | integer ≥ 0 | yes | |
| `stage` | enum | yes | Section 2.1. |
| `status` | `ok` \| `error` \| `timeout` \| `dropped` | yes | `dropped` is for a deliberate drop such as a filter. |
| `instance` | string | yes | Emitting component instance, for example `worker:pipeline-2@worker-4`. |
| `message_id` | UUID | when `kind=message` | |
| `parent_id` | UUID | no | From `tb-parent-id`. |
| `correlation_id` | string | no | |
| `destination` | string | no | Destination name; absent for pre-routing stages. |
| `attempt` | integer 0-9999 | no | |
| `content_type` | string | no | |
| `size_bytes` | integer ≥ 0 | no | Payload size. `0` with a claim-check. |
| `claim_check` | string | no | Object-store URI, so the browser can fetch large content. |
| `topic`, `partition`, `offset` | string, integer, integer | required for `kind=partition` | Also useful on message events for replay by offset. |
| `error_code` | `^[A-Z][A-Z0-9_]{0,63}$` | no | Same vocabulary as `tb-error-code`. Required when `status` is `error` or `timeout`. |
| `error_message` | string ≤ 512 | no | **Redacted, static text only.** Section 3. |
| `duration_ms` | number ≥ 0 | no | Time spent in the stage the event closes. Lets the archiver compute stage latency without joining two events. |
| `attrs` | object, string→string | no | Searchable metadata. Section 3. At most 32 entries. |
| `traceparent` | W3C traceparent | no | Links the event to the trace. |

### 2.1 Stages

`kind=message`: `received`, `queued`, `filtered`, `transformed`, `routed`, `pending`, `sent`,
`retrying`, `replied`, `error`, `dlq`.

`kind=partition`: `partition_paused`, `partition_resumed`, `partition_halted`.

The message stages are exactly ADR-001 5.5 plus `replied` for synchronous reply mode. Adding a stage
is an additive change; renaming one is breaking.

### 2.2 Ordering and duplication

Events are keyed so that a single message's events land on one partition and stay ordered. Emission
is fire-and-forget (invariant 9), so events may be **lost** when the producer queue is full and may be
**duplicated** after a worker restart. Consumers must therefore be idempotent on
`(message_id, stage, destination, attempt)` and must not treat a missing event as a lost message.
The message store is Kafka; `_tb.events` is observability.

---

## 3. PHI rules

`_tb.events` flows into ClickHouse, Grafana and the UI. It is the single highest-risk path for
invariant 6.

1. **No payload bytes, ever.** Not a snippet, not a first line, not a "helpful" excerpt.
2. **`error_message` is static text plus non-PHI values.** `"FHIR server returned 422"` is fine.
   `str(exception)` is not, because parser and validator libraries routinely include the offending
   field value. Producers must map exceptions to a code plus a fixed message.
3. **`attrs` entries that derive from a patient identifier are keyed hashes**, and the schema now
   enforces it three ways rather than trusting the convention:
   - a key ending `_hmac` MUST have a value of **exactly 64 hex characters**, so a clear-text value
     cannot hide behind the suffix (`{"mrn_hmac": "0001234567"}` is rejected);
   - a key from a **deny-list of names that denote a direct identifier** (`mrn`, `patient_id`, `dob`,
     `name`, `accession`, `ssn`, `phone`, … — the full list is in the schema) is rejected in clear
     form. It is admissible only hashed, as `<name>_hmac`;
   - any other value must be a **short code**: `^[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}$`, 64 characters
     max, **no spaces**. Free text is where PHI arrives, so the column is shaped to refuse it.

   Correct: `{"mrn_hmac": "9f2a…64 hex", "trigger": "A01", "step": "to_fhir_bundle"}`. The browser
   hashes the search term with the same key (ADR-001 6.6). Key management is ADR-004.

   **This is defence in depth, not a proof.** Nothing stops a determined producer putting a surname in
   `attrs.reason`, because a surname is a valid short code. The deny-list catches the common mistake
   and makes the intent unambiguous; code review and `security-reviewer` remain the real control.
4. **No record key in `attrs`**, hashed or otherwise, under any name. The partition key is often the
   patient identifier ([envelope contract](../../envelope/v0/README.md) section 4); `key`,
   `message_key` and `partition_key` are on the deny-list for that reason.
5. **`claim_check` follows the envelope's URI rules**: opaque locator, no query string, no fragment,
   no userinfo. Events are persisted to ClickHouse and rendered in the browser, so a presigned URL
   here is a stored credential that stays replayable in backups long after the URL's own expiry.

Reviewers: an `error_message` built with an f-string from an exception is a Critical finding.

---

## 4. Mapping to the archive

`message_events` in ADR-001 6.6 maps field for field, with these additions to the DDL that
`archive-engineer` owns:

| Event field | ClickHouse |
|---|---|
| `v` | `UInt8` |
| `kind` | `LowCardinality(String)` |
| `ts` | `DateTime64(3, 'UTC')` |
| `message_id` | `Nullable(UUID)` — nullable because partition events have none |
| `duration_ms` | `Nullable(Float32)` |
| `claim_check`, `topic`, `offset`, `partition` | `String`, `String`, `Nullable(Int64)`, `Nullable(Int32)` |
| everything else | as ADR-001 6.6 |

---

## 5. Conformance

Tests load [`examples/manifest.json`](examples/manifest.json) and validate every file. All values are
synthetic.

## 6. Open items for v1

- No aggregate or rollup events (per-channel per-minute counters). Metrics cover that today; the
  browser may want it once the archive is measured.
- No control-plane events here: those go to `_tb.<env>.audit` with its own contract, unwritten.
- `duration_ms` versus deriving latency by joining stages: v0 allows both, which risks the two
  disagreeing. Pick one in v1 once the archiver exists.
