# Contract: message envelope — v0 (draft)

**Contract id:** `tributary.envelope/v0`
**Status:** Draft. Frozen at the Phase 1 wave 1 gate; breaking changes after that create `../v1/`.
**Owner:** `architect`. Propose changes by message with the exact diff and impact.
**Source:** ADR-001 section 5.4, refined by ADR-002.
**Schema:** [`envelope.schema.json`](envelope.schema.json) · **Examples:** [`examples/manifest.json`](examples/manifest.json)

Realises invariant 8 (metadata in Kafka headers, payload opaque bytes, claim-check over 1 MB) and
supports invariants 1, 4, 5, 6.

---

## 1. Model

A Tributary message on any data-plane topic is one Kafka record:

| Part | Contract |
|---|---|
| **Headers** | The `tb-*` registry in section 3, plus `traceparent`. All values are UTF-8 text. |
| **Key** | UTF-8 bytes of the channel's `partitionKey` result, or null. Section 4. |
| **Value** | Opaque payload bytes, or null when `tb-claim-check` is set. Section 5. |

Nothing in the engine parses the value to make routing, retry or ordering decisions. Every decision
is made from headers, so a payload format we have never seen still flows end to end.

### 1.1 What the schema validates

JSON Schema cannot see a Kafka record, so the schema validates a **record projection**: a JSON
object a test builds from a real (or fixture) record.

```json
{
  "class": "raw",
  "topic": "prod.adt-to-fhir.raw",
  "key_present": true,
  "key_size": 26,
  "headers": { "tb-msg-id": "018f3a5c-...", "...": "..." },
  "payload_present": true,
  "payload_size": 1180
}
```

**The projection carries no key value and no payload bytes, by construction.** A validator embeds the
failing instance in its error message, and the record key is usually a patient identifier (section 4),
so a projection containing the key would put PHI into exceptions, log lines and pytest output — the
exact surfaces invariant 6 names. `key_present` and `key_size` express everything the schema was ever
checking about the key. Two consequences:

- A producer or test that builds a projection MUST derive it, never `json.dumps` the record.
- A `ValidationError` from this schema MUST NOT be logged or printed verbatim. Log the error's
  `json_path` and `validator` keyword, plus `tb-msg-id`. This holds even now that the key is gone:
  headers still carry `tb-source` (a peer address) and `tb-usr-*` values.

The key-identity rules per class (`dlq` keyed by `tb-msg-id`, `reply` by `tb-correlation-id`) are
therefore checked in code, not by this schema.

`class` is one of `raw`, `out`, `retry`, `dlq`, `reply` and drives the conditional rules. It is
derivable from `topic` by the [topics contract](../../topics/v0/README.md); it is carried
explicitly so the two schemas stay independent.

`class`, `key_present`, `key_size`, `headers`, `payload_present` and `payload_size` are all required,
even when a size is `0`. An omitted field would pass the claim-check and size rules by accident, which
is the opposite of what a conformance test is for. `topic` is optional.

**Conformance.** 28 examples: 8 valid, 20 invalid. Every invalid example is asserted to fail at the
`expected_json_path` and `expected_validator` its manifest pins, not merely to fail. Latest run:
`pytest tests/conformance` **105 passed** and `contracts/validate.py` **100 / 0** at `f5bc92d`
(2026-09-22), covering the two examples added for section 2.3 (`attempt-not-an-integer`,
`channel-version-not-an-integer`) and the quarantine projection for section 5.2
(`dlq-quarantine-undecodable`).

### 1.2 Strict producers, lenient consumers

- **Producers and CI validate strictly.** `envelope.schema.json` sets `additionalProperties: false`
  over the registry, so a typo (`tb-msgid`) or an unregistered `tb-` header fails conformance.
- **Consumers at runtime MUST NOT reject a record for an unknown header.** A v0 worker reading a
  record written by a future producer ignores headers it does not know. Rejecting them would turn
  an additive contract change into an outage.

---

## 2. Value encoding

| Rule | Value |
|---|---|
| Header names | Lowercase ASCII. `tb-` prefix reserved for this contract, plus W3C `traceparent` / `tracestate`. |
| Header values | UTF-8 text, never binary, never JSON. A header that needs structure gets its own header. |
| Integers | Decimal ASCII, no sign, no padding (`"0"`, `"12"`). |
| Timestamps | RFC 3339, UTC, 3 to 9 fractional digits, offset `Z` or `+00:00`. Example `2026-09-20T11:22:33.123Z`. |
| UUIDs | Canonical lowercase hyphenated form. |
| Absent | Omit the header. Never send an empty value to mean "absent". |
| Limits | At most 32 headers; name ≤ 64 bytes; registered header values ≤ 1024 bytes, `tb-claim-check` included; `tb-usr-*` and `tb-x-*` values ≤ 256 bytes; all headers together ≤ 16 KiB. |

Rationale for text values: `kcat`, the Kafka UI, ClickHouse and a human debugging a production
incident can all read them. The cost is a few bytes per record and a parse step for integers, which
does not register next to payload size.

That readability has a price, and the contract states it rather than leaving it implied: **any tool
that can read a data-plane topic is a PHI viewer.** `kcat`, a Kafka UI, a ClickHouse client and the
message browser all show the record key and every header. Deployments MUST put topic read access
behind the same authentication, authorization and audit as the message browser
([topics contract](../../topics/v0/README.md) 3.4), and MUST NOT run a shared, unauthenticated Kafka
UI against a production cluster. Recommending `kcat` for debugging without that sentence was how the
first draft turned an operational convenience into an unaudited PHI viewer.

The 16 KiB total is a producer-side check — JSON Schema cannot sum value lengths. The schema instead
bounds the worst case: the 16 registered header values sum to about 2.5 KiB, and the 16 remaining
slots can only be `tb-usr-*` or `tb-x-*` at 256 bytes each, so a schema-valid header set is roughly
7 KiB including names, comfortably inside the budget. The earlier 1024-byte allowance for those two
prefixes admitted about 32 KiB, twice the stated budget.

### 2.1 Reserved prefixes

| Prefix | Meaning | Runtime behaviour |
|---|---|---|
| `tb-` | Normative. Only names in section 3. | Load-bearing. |
| `tb-x-` | Experimental or spike-local (for example `tb-x-seq` in S5). | Never load-bearing. Must not survive into a released package. |
| `tb-usr-` | Channel-defined metadata set by a step through `ctx`. | Propagated, never interpreted by the engine. Not projected into `_tb.events`. |

`tb-usr-*` and `tb-x-*` values fall under invariant 6: no PHI. A Kafka header is readable by anything
with topic read access — `kcat`, a Kafka UI, and the record detail view in the message browser — so
"it is only a header" is not a containment argument.

### 2.2 `tb-usr-*` does not reach `_tb.events` in v0

An earlier draft said these headers flow into `_tb.events`, ClickHouse and the browser. The events
contract has no field that can hold them: `attrs` keys are `^[a-z][a-z0-9_]{0,31}$`, which cannot
represent a hyphenated 48-character header name, and `additionalProperties: false` blocks a new
top-level field. The contract described a path that does not exist, and an implementer who needed it
would have invented a mapping — the ad-hoc route by which unvetted tenant metadata reaches the
archive.

**Decision: `tb-usr-*` is propagated between hops in headers and is NOT copied into `_tb.events`.**

**Why this rather than defining the mapping:** an automatic header-to-archive projection makes every
channel-defined header a silent new archive column, populated by channel code, in the one place
invariant 6 is hardest to police. Making it explicit costs a step author one line and gives the
reviewer something to review.

**Give up:** a step that wants its metadata searchable must emit it deliberately, as an `attrs` entry
through `ctx`, under the events contract's short-code rules ([events contract](../../events/v0/README.md)
section 3). Two places to set metadata instead of one, and the browser cannot show a `tb-usr-*` value
in a list view — only in the record detail, which reads the record itself.

Revisit in v1 if a real channel needs it, with an explicit per-channel allow-list of which
`tb-usr-*` headers project into `attrs` and under which names. That is policy, which section 8
already assigns to ADR-004.

### 2.3 Every header is parsed defensively: reject, never crash

**Normative, and it applies to every component that reads a record.** A Kafka header is bytes
written by another process. Any of them may be missing, truncated, non-UTF-8, or syntactically
wrong, including on a record this engine wrote itself — a partial produce, a downgraded producer, or
a deliberately malformed record from a compromised writer all look the same at the consumer.

| Class of header | Rule |
|---|---|
| Integer (`tb-attempt`, `tb-channel-version`) | Parse with an explicit conversion that cannot raise past the call site. A value that is not decimal ASCII, is signed, is padded, or overflows the documented range is a **reject**. |
| Timestamp (`tb-received-at`, `tb-retry-not-before`) | Same, against the RFC 3339 form in section 2. A parse failure is a reject, not a silent `now()`. |
| UUID (`tb-msg-id`, `tb-correlation-id`, `tb-parent-id`) | Same, against the canonical form. |
| Enumerated (`tb-stage`, `tb-content-type`, `tb-error-code`) | An unknown value is not a parse failure — see "lenient consumers" in 1.2. Do not reject a record for a `tb-stage` value from a newer producer. |

**Reject** means: do not process the record, route it to `dlq` with `tb-stage` set to the stage that
read it and `tb-error-code: HEADER_PARSE_ERROR`, emit an `error` event, and commit the offset. It
does **not** mean an unhandled exception, and it does not mean skipping the record silently.

Three consequences that are easy to get wrong:

1. **The error must not quote the value.** `int(v)` raises `ValueError: invalid literal for int()
   with base 10: '<value>'`, and that string reaches logs and the exception surface. A header value
   can be attacker-supplied or PHI-adjacent, so catch the error and raise a typed one carrying the
   header *name* and `tb-msg-id` only (invariant 6, and section 4).
2. **A missing required header is the same class of failure as a malformed one.** `headers["tb-attempt"]`
   raising `KeyError` inside a consumer loop is a crash loop on one poison record, which stalls a
   partition — the same operational outcome as the bug this rule exists to prevent.
3. **This is total for the whole registry, not for the one header a spike happened to hit.**
   Phase 0's finding (SR-10) was against a single experimental header. Generalising it is Phase 1
   requirement 13, owned jointly by `platform-engineer` and `connector-engineer`.

Reserved error codes the contracts themselves rely on: `HEADER_PARSE_ERROR` (this section),
`TRANSFORM_ERROR` (ADR-001 6.4). Others are chosen by the component that raises them, within
`^[A-Z][A-Z0-9_]{0,63}$`; there is deliberately no central registry to keep in sync.

### 2.4 Known envelope drift in Phase 0 code — do not reproduce

Spike code diverged from this contract in three ways. All three are recorded here because the spike
directories stay in the tree as reference material, and the fastest way to write a Phase 1 emitter
is to copy one.

| Drift | Correct form |
|---|---|
| `tb-x-stage` used as a load-bearing header | `tb-stage`. `tb-x-*` is experimental, never load-bearing, and must not survive into a released package (section 2.1). |
| Non-canonical ids: uppercase hex, unhyphenated, or a UUIDv4 in `tb-msg-id` | Canonical lowercase hyphenated form, and `tb-msg-id` is **UUIDv7** specifically — the schema's `uuid7` pattern pins the version nibble. |
| Timestamps as epoch floats | RFC 3339, UTC, 3 to 9 fractional digits (section 2). The events contract has an `invalid/ts-epoch-float.json` example for the same mistake. |

Phase 1 requirement 10 is the check that catches a recurrence: every emitter validates its output
against this contract's schema in its own tests, using the SDK's projection helper rather than a
hand-built dict.

---

## 3. Header registry

`R` required · `O` optional · `C` conditional (condition in the notes) · `–` must not be set.

| Header | raw | out | retry | dlq | reply | Notes |
|---|---|---|---|---|---|---|
| `tb-msg-id` | R | R | R | R | R | UUIDv7, minted once at receive. Stable for the life of the message across every hop. The idempotency key every loader uses (invariant 4). |
| `tb-channel` | R | R | R | R | R | Channel id. Topic-safe: `^[a-z][a-z0-9-]{0,63}$`. |
| `tb-channel-version` | R | R | R | R | R | Spec version that produced the record. Lets the browser explain a behaviour change and lets a shadow run be attributed. |
| `tb-source` | R | R | R | R | O | Receiver instance and remote peer. Recommended `<component>:<instance>@<peer>`, for example `mllp-receiver:recv-7f3a@10.0.2.15:41522`. Syntax is deliberately loose in v0. |
| `tb-received-at` | R | R | R | R | R | Receive timestamp, set once at receive, never rewritten. End-to-end latency is `now - tb-received-at`. |
| `tb-content-type` | R | R | R | R | R | Media type of the current payload. Changes when a step changes the format. Section 3.1. |
| `tb-attempt` | R | R | R | R | O | Delivery attempt for the current stage. `0` at receive. |
| `traceparent` | R | R | R | R | R | W3C trace context. Same trace id across hops, new span id per hop. |
| `tb-correlation-id` | C | C | C | C | R | **A UUID minted by the engine, never a sender-supplied token.** Required on `raw` when the channel uses synchronous reply mode (ADR-001 6.5), and propagated wherever it was set. Also links split and aggregate flows. Section 3.2. |
| `tb-parent-id` | C | C | C | C | O | Required when this message was produced by a split, or re-injected by reprocess or replay. Any UUID version: it may be an id from an external system. |
| `tb-claim-check` | C | C | C | C | C | Required when the payload exceeded the inline limit. Section 5. |
| `tb-destination` | – | R | R | C | O | Destination name from the channel spec. **New in v0** (not in ADR-001 5.4): `retry` and `dlq` are per channel, not per destination, so without it a retry consumer cannot tell which loader owns the record. On `dlq` it is required only when the failure happened at a destination. |
| `tb-stage` | – | O | R | R | O | Stage that last wrote or failed the record, from the event stage enum. **New in v0**: ADR-001 6.4 requires the DLQ to carry the failing stage. |
| `tb-error-code` | – | – | R | R | – | Stable machine code, `^[A-Z][A-Z0-9_]{0,63}$`, for example `HTTP_503`, `TRANSFORM_ERROR`. **New in v0**. Never a human message: exception text can quote payload, and invariant 6 forbids PHI here. The redacted human message belongs in `_tb.events.error_message`. |
| `tb-retry-not-before` | – | – | R | – | – | Earliest time a retry consumer may deliver this record. **New in v0**: delay tiers on a shared `retry` topic need a visible-after timestamp, because Kafka has no per-record delay. |

### 3.1 Content types

| Value | Format |
|---|---|
| `application/hl7-v2+er7` | HL7 v2 pipe-delimited |
| `application/hl7-v2+xml` | HL7 v2 XML |
| `application/fhir+json`, `application/fhir+xml` | FHIR R4/R5 |
| `application/json`, `application/xml`, `text/csv`, `text/plain` | Generic |
| `application/edi-x12` | X12 |
| `application/dicom` | DICOM part 10 (normally claim-checked) |
| `application/octet-stream` | Unknown or binary |

Parameters are allowed and additive: `application/hl7-v2+er7; charset=UTF-8`,
`application/fhir+json; fhirVersion=4.0.1`. A consumer matching on type MUST compare the type and
subtype only, ignoring unknown parameters. Default charset when the parameter is absent is UTF-8,
except `application/hl7-v2+er7` where the channel spec decides (HL7 v2 in the field is frequently
ISO-8859-1 or a Japanese encoding, and guessing corrupts names).

### 3.2 Correlation ids are engine-minted UUIDs

`tb-correlation-id` is a UUID (any version), not free text. The first draft allowed
`^[A-Za-z0-9._:-]{1,128}$`, which invited the obvious implementation: reuse the sender's token —
MSH-10, a query tag, an HTTP request id — as the correlation id.

**Why that is a PHI leak and not a style point.** The correlation id is the `reply` topic's record
key, it is copied into `_tb.events`, ClickHouse and the browser in clear text, and unlike the message
key (section 4) nothing marked it as sensitive. A sender-controlled token is sender-controlled
content: systems in the field put visit numbers, accession numbers and worse into MSH-10. Constrained
to a UUID it cannot carry a value derived from the payload, and no "do not log the correlation id"
rule is needed, because there is nothing in it to leak.

Consequences, which ADR-001 6.5 already implies — the receiver generates the correlation id there:

1. The receiver MUST mint a fresh UUID per request and keep the sender's token only in its in-memory
   pending map, next to the open connection. That map is the mapping; nothing durable holds both.
2. A correlation id MUST NOT be derived from a patient identifier or any payload field — not even by
   hashing one into a UUIDv5 namespace, which would make it a stable pseudonym for that patient
   across every reply record and every archive row.
3. A split or aggregate flow reuses the existing correlation id rather than deriving a new one from
   content.

**Give up:** a sender cannot correlate by its own token in the message browser. Search by
`tb-msg-id` instead, or have the channel put the token in `attrs` as a short code if it is genuinely
not an identifier — a judgement a reviewer makes per channel, which is the intent.

---

## 4. Message key

| Rule | Value |
|---|---|
| Encoding | UTF-8 bytes of the channel's `partitionKey` expression result. |
| Length | ≤ 256 bytes. |
| Null | Allowed only when the channel declares `ordering: none`. A null key round-robins and destroys per-key order (invariant 5). |
| Per class | `raw`, `out`, `retry`: ordering key. `dlq`: `tb-msg-id`. `reply`: `tb-correlation-id`. |

**The key is frequently a direct patient identifier** — ADR-001's worked example is `hl7:PID.3.1`.
Therefore, under invariant 6, the key MUST NOT appear in clear text in any of:

| Surface | Note |
|---|---|
| Log lines, metric labels, trace attributes, exception messages, test output | Invariant 6 as written. Log `tb-msg-id`. |
| **URLs** — UI routes, query strings, API paths, and any HTTP loader or lookup URL the engine builds | A URL is logged by every proxy, gateway and access log between here and the destination, and lands in browser history. Put the identifier in the request body or a header. Where a destination API only accepts it in the path or query (FHIR `?identifier=`), that is a deliberate, documented channel behaviour, and Tributary MUST NOT log the URL it sent. |
| **Browser storage** — `localStorage`, `sessionStorage`, cookies, and the UI's client-side caches | Survives the session and the logout, on a machine Tributary does not control. |
| **Error responses returned to a sender**, including NACK text and HTTP error bodies | The sender's own logs are outside our trust boundary, and a NACK that echoes the key sends PHI to a system that may not expect to receive it. Return a code plus `tb-msg-id`. |
| **Other headers** — `tb-usr-*`, `tb-x-*`, `tb-source`, `tb-correlation-id` | Copying the key into a header launders it past every rule above, because headers are propagated and archived. Section 3.2 constrains `tb-correlation-id` structurally; the rest is a rule a reviewer enforces. |
| **`_tb.events.attrs`**, hashed or otherwise, under any name | [Events contract](../../events/v0/README.md) section 3, rule 4. |

Where a key must be searchable (the message browser), store a keyed hash, per ADR-001 6.6 and the
[events contract](../../events/v0/README.md) — noting that the hashing mechanism itself is
unresolved (events contract section 3.1).

Anything that can display a record key or a raw record — the message browser, `kcat`, a Kafka UI, a
ClickHouse client — is a PHI viewer, and MUST be authenticated, authorized and audited like one
(section 2, and [topics contract](../../topics/v0/README.md) 3.4). "It is only a debugging tool" is
not an exemption; it is how the access gets left unlogged.

This is the one place where an engine-internal value is PHI, and it is easy to leak by accident.
Reviewers: treat any `log`/`metric`/`span` call, URL construction, or sender-facing error message
that touches a record key as a Critical finding.

The same reasoning removed the key from the validation projection (section 1.1) and keeps it out of
`_tb.events.attrs` in clear text ([events contract](../../events/v0/README.md) section 3).

---

## 5. Payload and claim-check

| Rule | Value |
|---|---|
| Max inline payload | 1,000,000 bytes. |
| Broker `message.max.bytes` | 1,048,576 (1 MiB), leaving ~48 KB of headroom for headers and framing. |
| Over the limit | Store the payload in the claim-check object store, set `tb-claim-check` to its URI, and produce the record with a **null** value. |
| With `tb-claim-check` set | The value MUST be null, not empty bytes. An empty payload is otherwise indistinguishable from a bug. |
| URI schemes | `s3://`, `gs://`, `az://`, `https://`. |
| URI form | Opaque locator only: `<scheme>://<host-or-bucket>[:<port>]/<path segments>`. **No query string, no fragment, no userinfo.** Max 1024 bytes. |
| Object key | Derived from non-PHI parts only: `<env>/<channel>/<yyyy>/<mm>/<dd>/<tb-msg-id>`. Never a patient identifier, accession number, file name from the sender, or anything taken from the payload. |
| Signed URLs | Minted at read time by the API, with a short expiry. **Never** stored in an envelope, an event or the archive. |

### 5.1 Why the URI is constrained

A claim-check URI is persisted in Kafka, copied into `_tb.events`, written to ClickHouse and rendered
in the message browser. Two things therefore must not appear in it:

- **Credentials.** A presigned URL carries `X-Amz-Signature` (or an SAS token) in the query string, and
  an embedded `user:password@host` carries them in userinfo. Either one turns a long-lived event record
  into a stored credential that is replayable by anyone with archive or UI read access, and it will
  outlive the URL's own expiry in backups. Hence: no query, no fragment, no `@`.
- **Identifiers.** Object names are the classic accidental PHI channel — `.../SMITH_JOHN_20260920.dcm`
  is a PHI leak into a database column that is not treated as PHI. Hence a key derived from
  `tb-msg-id`, which is meaningless outside the engine.

Claim-check objects follow the retention of the topic class that referenced them, and their
lifecycle is owned by the archive design (ADR-004, not yet written). Deleting a claim-check object
inside the `raw` retention window breaks replay: treat the object store TTL as `max(raw retention,
dlq retention)` until ADR-004 says otherwise. **Assumption, unverified:** no spike measured
claim-check behaviour in Phase 0.

### 5.2 Quarantine records: when the headers cannot be read at all

Section 2.3 says a malformed header is a reject to `dlq`. That is impossible to do literally when
the headers cannot be decoded at all, because a `dlq` record requires the whole core header set and
those are exactly the bytes that could not be read. **A quarantined record is therefore a new record
the engine constructs, not the original record moved.**

| Part | Value |
|---|---|
| `tb-msg-id` | A **fresh** UUIDv7 minted by the quarantining component. Never a value recovered from the broken record: that is untrusted input, and a forged id would collide with a real message in the browser and in every loader's idempotency check. |
| `tb-channel`, `tb-channel-version` | Known from the subscription and the applied spec, not from the record. |
| `tb-source` | The quarantining component and instance, not the original peer, which is unknown. |
| `tb-received-at` | The quarantine time. **This is a deliberate deviation** from "set once at receive, never rewritten" (section 6 rule 1): the original receive time is unknowable. End-to-end latency for this record is meaningless and must not be computed. |
| `tb-content-type` | `application/octet-stream`. Unknown by definition. |
| `tb-stage`, `tb-error-code` | The stage that read it, and `HEADER_PARSE_ERROR`. |
| Key | `tb-msg-id`, as for every `dlq` record. |
| **Value** | The original record's bytes — headers and payload — as an opaque blob. |

The original bytes go in the **payload**, which is the one part of the envelope that is opaque and
already treated as PHI-bearing, on a topic whose ACLs and 30-day retention already assume that. They
do not go into headers, `attrs`, or an event.

**The logging rule is the part most likely to go wrong, and it is a Critical finding if it does.**
A developer diagnosing "why can this not be decoded" will reach for `repr(record)` or log the raw
header bytes. Those bytes are unvalidated input of unknown provenance: a mis-framed message puts
payload content — an HL7 segment, a patient name — in a header slot. So, from a quarantine path:

| May be logged | Must never be logged |
|---|---|
| `topic`, `partition`, `offset` | Any byte of the original record, in any encoding or escaping |
| The minted quarantine `tb-msg-id` | Header **names** as well as values — a name is also attacker-supplied, and under mis-framing it is payload |
| `tb-error-code`, byte counts | `repr()`/`str()` of the record, the header list, or any slice or prefix of either |

The matching event is `kind: "record"`, `stage: "quarantined"`, carrying the Kafka coordinates and
the minted id ([events contract](../../events/v0/README.md)). It deliberately has no `attrs`.

---

## 6. Propagation rules

1. `tb-msg-id`, `tb-channel`, `tb-received-at` and the trace id inside `traceparent` are **copied
   unchanged** at every hop. A message is one identity from receive to delivery.
2. `traceparent` gets a **new span id** per hop, keeping the trace id and flags.
3. `tb-content-type` is rewritten by the step that changes the format.
4. `tb-attempt` resets to `0` when a record enters a new stage, and increments per delivery attempt
   within that stage.
5. Reprocess and replay mint a **new** `tb-msg-id` and set `tb-parent-id` to the original. This
   keeps the browser's parent/child tree honest and keeps loader idempotency keys distinct, so a
   deliberate reprocess is not silently deduplicated away by the destination.
6. A step MUST NOT delete a registered header it does not understand.

---

## 7. Conformance

Tests load [`examples/manifest.json`](examples/manifest.json), validate each `valid/*.json` against
the schema expecting success, and each `invalid/*.json` expecting failure with the documented
`violates` rule. All example payload values and keys are synthetic (`TRIBUTARY-SYNTH`).

```python
manifest = json.loads(Path("contracts/envelope/v0/examples/manifest.json").read_text())
# manifest["schema"], manifest["valid"][*]["file"], manifest["invalid"][*]["file"]
```

`qa-engineer` owns the conformance suite under `tests/conformance/`.

---

## 8. Open items for v1

- `tb-source` syntax is loose. Tighten once receivers exist and the useful form is known.
- No header carries the bundle image digest. The browser may want it; `tb-channel-version` plus the
  control plane can resolve it today. Defer.
- Per-channel header allow-lists (a tenant leaking `tb-usr-*` PHI) are policy, not schema. ADR-004.
- Signed envelopes (per-record HMAC to detect tampering in transit) are not in v0. No requirement
  yet asks for it.
