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
| `error_code` | `^[A-Z][A-Z0-9_]{0,63}$` | no | Same vocabulary as `tb-error-code`. Required when `status` is `error` or `timeout`. On `dropped` only for a registered engine code for an engine-initiated skip, today only `REPLY_EXPIRED` (set 1.4, envelope README 2.3.2); a `Drop` reason stays in `error_message`. |
| `error_message` | string ≤ 512 | no | **Redacted, static text only.** Section 3. |
| `duration_ms` | number ≥ 0 | no | Time spent in the stage the event closes. Lets the archiver compute stage latency without joining two events. |
| `attrs` | object, string→string | no | Searchable metadata. Section 3. At most 16 entries and 1024 serialized bytes. `tb-usr-*` headers are **not** projected here ([envelope contract](../../envelope/v0/README.md) 2.2). |
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
   enforces it four ways rather than trusting the convention:
   - a key ending `_hmac` MUST have a value of **exactly 64 lowercase hex characters**, so a
     clear-text value cannot hide behind the suffix (`{"mrn_hmac": "0001234567"}` is rejected).
     The shape is enforced; **the hashing mechanism is not specified in v0** — section 3.1;
   - a key from a **deny-list of names that denote a direct identifier** (`mrn`, `patient_id`, `dob`,
     `name`, `accession`, `ssn`, `phone`, … — the full list is in the schema) is rejected in clear
     form. It is admissible only hashed, as `<name>_hmac`;
   - any other value must be a **short code**: `^[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}$`, 64 characters
     max, **no spaces**. Free text is where PHI arrives, so the column is shaped to refuse it. The
     allow-list also excludes the HL7 delimiters `| ^ ~ \ &` and CR/LF, so a segment or a repeating
     field cannot be pasted in whole;
   - the object is capped at **16 entries**, and producers MUST additionally keep it under **1024
     serialized bytes** — a cap JSON Schema cannot express, so it is a producer-side check. Sixteen
     short codes is a metadata column; a hundred is a payload in disguise.

   Correct: `{"mrn_hmac": "9f2a…64 hex", "trigger": "A01", "step": "to_fhir_bundle"}`. The browser
   hashes the search term with the same key (ADR-001 6.6). Key management is ADR-004.

   **This is defence in depth, not a proof.** Nothing stops a determined producer putting a surname in
   `attrs.reason`, because a surname is a valid short code. The deny-list catches the common mistake
   and makes the intent unambiguous; code review and `security-reviewer` remain the real control.

   What the budget does buy, and the reason the numbers moved: the first draft allowed 32 entries of
   256 characters, about 8 KiB of channel-controlled text per event, against a typical ADT message of
   1-2 KiB. A buggy or hostile step could reassemble a whole message inside the archive while passing
   validation, so the claim that `attrs` "cannot become a payload smuggling channel" was simply
   false. At 16 entries, 64-character values and 1024 serialized bytes, `attrs` no longer has room
   for an ADT message, and the character allow-list means a segment cannot be carried verbatim in any
   case: `|`, `^`, `~`, `\`, `&`, CR and LF are all rejected. Chunking a payload across many events is
   still possible in principle. That is a code-review problem, and the contract says so rather than
   claiming otherwise.
4. **No record key in `attrs`**, hashed or otherwise, under any name. The partition key is often the
   patient identifier ([envelope contract](../../envelope/v0/README.md) section 4); `key`,
   `message_key` and `partition_key` are on the deny-list for that reason.
5. **`claim_check` follows the envelope's URI rules**: opaque locator, no query string, no fragment,
   no userinfo. Events are persisted to ClickHouse and rendered in the browser, so a presigned URL
   here is a stored credential that stays replayable in backups long after the URL's own expiry.
6. **`tb-usr-*` headers are not projected into events.** There is no field that can hold them and
   none is being added: see [envelope contract](../../envelope/v0/README.md) 2.2. A step that wants
   channel metadata searchable emits an `attrs` entry deliberately, under the rules above.
7. **`correlation_id` is an engine-minted UUID**, never a sender-supplied token
   ([envelope contract](../../envelope/v0/README.md) 3.2). It reaches ClickHouse and the browser in
   clear text, so the constraint is what keeps a payload-derived value out of an archive column that
   nothing treats as sensitive.

Reviewers: an `error_message` built with an f-string from an exception is a Critical finding.

### 3.1 The `_hmac` mechanism is UNRESOLVED

The schema enforces the **shape** of a hashed attribute — exactly 64 lowercase hex characters. It
cannot enforce the **mechanism**, and v0 deliberately does not specify one.

Why that matters: an unkeyed `sha256(mrn)` is also 64 lowercase hex characters, and an unkeyed digest
over a bounded identifier space is reversible by exhaustive search. A producer that follows this
contract to the letter with `sha256` publishes something that is, for practical purposes, the
identifier itself — stored in ClickHouse, rendered in the browser, and retained in backups. The
schema raises the cost of the careless mistake; it does not prevent this one.

Open, and **escalated to ADR-004 plus a human decision** — CLAUDE.md forbids an agent designing
cryptography or key management, so nothing below is settled here:

- Which construction: HMAC-SHA-256, keyed BLAKE2, or a per-channel pepper. The 64-hex shape fits all
  three, which is why the shape was chosen now and the mechanism left open.
- Key scope: one key per deployment, per environment, or per channel.
- Rotation: a rotated key makes old and new events for the same patient hash differently, so search
  across a rotation boundary either breaks or requires retaining superseded keys. Retention
  (`_tb.<env>.events` at 3 days, the archive far longer) decides how bad that is.
- Where the key lives and who may read it. The browser must hash the search term with the same key
  (ADR-001 6.6), so either the key reaches the API process or hashing becomes an API-side operation
  behind an authorization check.
- Whether a hashed identifier is de-identified **at all**. Under HIPAA Safe Harbor a keyed hash of an
  MRN is a re-identifiable code, not de-identified data. Until a human rules otherwise, treat
  `_hmac` values in ClickHouse as PHI, with the same access controls as any other PHI column.

**Until ADR-004 lands, producers SHOULD NOT emit identifier-derived `attrs` at all.** `attrs` is for
low-cardinality operational codes (`trigger`, `step`, `dest`); a message is already findable by
`message_id`, and per-patient search in the browser is a feature that depends on this unresolved
decision, not on the event contract. UNVERIFIED: no implementation exists, so no claim here has been
tested against one.

---

## 4. Mapping to the archive

`message_events` in ADR-001 6.6 maps field for field, with these additions to the DDL that
`archive-engineer` owns:

| Event field | ClickHouse |
|---|---|
| `v` | `UInt8` |
| `kind` | `LowCardinality(String)` |
| `ts` | `DateTime64(3, 'UTC')` |
| `message_id` | `UUID`, **not nullable** (set 1.8). The nil UUID `00000000-0000-0000-0000-000000000000` means "no message identity" (partition events without a triggering message). It cannot collide: the engine mints UUIDv7, which is never nil. *Was `Nullable(UUID)`; ClickHouse refuses a Nullable column in a sort key* |
| `duration_ms` | `Nullable(Float32)` |
| `claim_check`, `topic`, `offset`, `partition` | `String`, `String`, `Nullable(Int64)`, `Nullable(Int32)` |
| everything else | as ADR-001 6.6 |

### 4.1 Archive rules (set 1.8, ruled 2026-10-05 on `archive-engineer`'s W3-2a report)

1. **Dedup key.** Section 2.2's `(message_id, stage, destination, attempt)` becomes, in the archive,
   `(channel, scope, message_id, stage, destination, attempt)`. `scope` is `''` for `kind=message`;
   `<topic>:<partition>:<offset>` for `kind=record` (all three are required there); and
   `<topic>:<partition>` plus `:<offset>` when present for `kind=partition`. A missing `attempt` is
   stored as `0`. `channel` adds nothing for message events (UUIDv7 is unique) and separates the rest.
   **Give up:** a partition event re-emitted after a restart, without an `offset`, is kept twice when
   its `ts` differs. Partition events are a low-volume operator timeline, and alerting reads
   `tb_partitions_halted`, not the archive. Producers SHOULD set `offset` on partition events (the
   blocked record's offset); the schema keeps it optional, because requiring it would be breaking.
2. **Month partitions are accepted.** With `PARTITION BY toYYYYMM(ts)`, `ReplacingMergeTree`'s
   **background merges** never cross a partition, so a redelivery that crosses a month boundary is
   **stored** twice, and a plain `count()` counts both. **`SELECT ... FINAL` does collapse across
   partitions at read time** while the ClickHouse setting `do_not_merge_across_partitions_select_final`
   is `0`, its default (corrected in set 1.8 part 2, 2026-10-06: the earlier text said `FINAL` does not
   collapse across months. `archive-engineer` measured 6 rows plain and 5 under `FINAL` on a
   straddling duplicate in W3-3). An operator profile that sets it to `1`, or any server-side automatic
   partition-local `FINAL`, brings the duplicate back under `FINAL`. The replay window is
   `commit_every` records (seconds of traffic), so this affects a tail at month end only. **Rule:** the
   W3-3 query library deduplicates on the key at read time (`LIMIT 1 BY` the key), which is correct
   whatever that setting is, so the browser never shows the duplicate. Do not rely on `FINAL` for
   dedup in product code. A `FINAL` count test that straddles a month is valid only if it pins the
   setting to `0` in the query (`SETTINGS do_not_merge_across_partitions_select_final = 0`).
   **Rejected:** partitioning on the UUIDv7 time of `message_id`, which puts every nil-id partition
   event in one epoch partition and breaks partition-level TTL for them. **Give up:** storage for a
   rare duplicate, and unqualified `count()` over a month boundary can overcount by that tail.
3. **Attributes the archive refuses (lead decision, 2026-10-05).** An incoming `attrs` entry whose key
   ends `_hmac`, or whose key is on the identifier deny-list, is **dropped and counted**
   (`tb_archiver_attrs_dropped_total{reason}`), and the rest of the event is stored. `_hmac` is
   dropped even though its shape is schema-valid, because the mechanism is unresolved (3.1) and the
   human has not answered 7.3. This follows "lenient consumers": one bad entry does not lose the event.
4. **Retention of archived events.** Default **90 days** (`TB_ARCHIVE_TTL_DAYS`), an engineering
   placeholder matching ADR-001 6.6's illustrative content TTL. **TODO(human)**, decided together with
   audit retention (`topics/v0` 2.1, question 3, and `phases/wave-3.md` 7.2): whether a retention duty
   reaches event metadata is the same legal reading. The archiver keeps the value configurable. It is
   not a blocker for the build.

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
