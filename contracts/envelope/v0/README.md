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

**Conformance.** 34 examples: 10 valid, 24 invalid (set 1.2 added 2 and 4 for the reply headers;
their four pairs are **architect predictions, UNMEASURED**). Every invalid example is asserted to fail at the
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
bounds the worst case: the 19 registered header values (set 1.2 added three short ones) sum to about
2.6 KiB, and every remaining slot up to 32 can only be `tb-usr-*` or `tb-x-*` at 256 bytes each, so a
schema-valid header set is roughly 7 KiB including names, comfortably inside the budget. The earlier 1024-byte allowance for those two
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
| Integer (`tb-attempt`, `tb-channel-version`, `tb-reply-partition`) | Parse with an explicit conversion that cannot raise past the call site. A value that is not decimal ASCII, is signed, is padded, or overflows the documented range is a **reject** — except the reply-routing headers, 2.3.3. |
| Timestamp (`tb-received-at`, `tb-retry-not-before`, `tb-reply-deadline`) | Same, against the RFC 3339 form in section 2. A parse failure is a reject, not a silent `now()`. |
| UUID (`tb-msg-id`, `tb-correlation-id`, `tb-parent-id`) | Same, against the canonical form. |
| Enumerated (`tb-stage`, `tb-content-type`, `tb-error-code`, `tb-reply-status`) | An unknown value is not a parse failure — see "lenient consumers" in 1.2. Do not reject a record for a `tb-stage` value from a newer producer. An unknown `tb-reply-status` is never read as `accepted` (2.3.3). |

**Reject** means: do not process the record, route it to `dlq`, emit an `error` event, and commit the
offset. It does **not** mean an unhandled exception, and it does not mean skipping the record
silently. There are two shapes of reject, and 2.3.1 says which applies.

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

Reserved error codes the contracts themselves rely on:

| Code | Meaning |
|---|---|
| `HEADER_PARSE_ERROR` | One header was malformed on a record that could still be **identified**. The reject keeps the original `tb-msg-id` and the original identity, and `tb-stage` is the stage that read it. Usually a broken producer field. |
| `RECORD_UNDECODABLE` | The record could not be identified at all, so no original identity is trustworthy. Produces a quarantine wrapper (section 5.2). Framing damage, corruption or an attack. |
| `TRANSFORM_ERROR` | A pipeline or destination step raised on every attempt (`stepRetries` exhausted; ADR-001 6.4). |

The first two are deliberately separate codes for one operational reason: they need different alerts.
A rise in `HEADER_PARSE_ERROR` points at one producer's field; a rise in `RECORD_UNDECODABLE` points
at framing, corruption or someone writing to the topic. Collapsing them hides the second behind the
first. Every code matches `^[A-Z][A-Z0-9_]{0,63}$`. The codes the engine emits are listed in the
registry in 2.3.2. A channel's own `Reject` codes are chosen by the channel and are not registered.

A component MAY keep a finer internal reason code (`HEADER_NOT_TEXT`, `ENVELOPE_INVALID`) for its own
log line, below the contract code. Two limits: it MUST NOT appear in an envelope header or an event,
and **nothing may alert or dashboard on it.** A log-only taxonomy that operations comes to depend on
has become a contract nobody reviewed. If one of those codes turns out to be operationally
load-bearing, bring it to the architect and it becomes a real `tb-error-code`.

### 2.3.1 The two reject shapes, and what a reject does with the bad value

**The discriminator is identity, and identity is `tb-msg-id`.**

| Condition | Shape |
|---|---|
| `tb-msg-id` is present, decodable and a valid UUIDv7 | **Ordinary reject.** The record keeps its own identity. `tb-error-code: HEADER_PARSE_ERROR`, `tb-stage` = the stage that read it. |
| `tb-msg-id` is absent, undecodable or not a valid UUIDv7 | **Quarantine** (section 5.2). Nothing about the record is trustworthy. `tb-error-code: RECORD_UNDECODABLE`, `tb-stage: quarantined`. |
| `tb-claim-check` is undecodable or malformed, whatever `tb-msg-id` says | **Quarantine.** The payload lives elsewhere and the pointer to it is the broken part, so normalising would produce a conformant record whose bytes cannot be found. The blob preserves the malformed URI for a human to repair. |

**An ordinary reject normalises the malformed header. It does not preserve the bad value anywhere in
the envelope.**

| Malformed header | Value on the reject record |
|---|---|
| `tb-channel` | The channel the consumer is subscribed to. Known from the subscription, not from the record. |
| `tb-channel-version` | The applied spec version, known from `_tb.<env>.channels`. |
| `tb-source` | The **rejecting** component and instance. Not the original peer, which is no longer trustworthy. |
| `tb-received-at` | The reject time. Same caveat as a quarantine wrapper: end-to-end latency for this record is meaningless and must not be computed. |
| `tb-content-type` | `application/octet-stream`. |
| `tb-attempt` | `"0"`. |
| `traceparent` | A **new** trace. An unparseable trace context cannot be continued, and inventing a parent id would corrupt an unrelated trace. The event links the record by `message_id`. |
| Any optional header (`tb-correlation-id`, `tb-parent-id`, `tb-destination`, `tb-retry-not-before`) | **Dropped.** |

**Why normalise rather than preserve.** The original record is still on the topic it was read from, at
a known partition and offset, for that topic's retention. A reject does not need to copy the bytes to
keep them; it needs the record to stay findable, and the event carries enough to find it. Copying them
would mean either an invalid header value on the DLQ, which fails conformance, or a second blob format
for a case that does not need one.

**That is the asymmetry with quarantine, which does copy**, and the two reasons are why the shapes
differ at all: an unidentifiable record cannot be looked up by message id in the browser, so the bytes
are the only handle on it; and `dlq` retention (30 d) outlives `raw` (7 d), so a pointer expires before
a late investigation gets to it.

**Normalising is silent unless the event says otherwise, so the event must say.** A reject record with
`tb-attempt: "0"` is indistinguishable from a first attempt. The `error` event therefore carries an
`attrs` entry `bad_header` naming an offending header, and `error_message` may name all of them.
Header **names** are safe to record where values are not: a consumer must ignore headers it does not
recognise (section 1.2), so the only headers that can trigger a parse reject are registered ones from
the closed set in section 3. The names are ours; the values are the sender's.

**None of this is schema-enforceable**, because a normalised reject record is structurally identical to
an ordinary `dlq` record — which is exactly what normalising is for. It is a code rule needing a unit
test per placeholder, in the same category as the rules in 5.2.1 and in the channel-spec contract's
section 6.

### 2.3.2 Engine error-code registry (set 1.1, 2026-09-25; `REPLY_` added in set 1.2; loader fixed names in set 1.3; two `REPLY_BODY_` codes in set 1.7; five receiver `log_only` codes in set 1.8)

**Machine-readable:** [`error-codes.json`](error-codes.json). This section is its rationale; the file
is the list.

**Why a registry now, when 2.3 used to say "deliberately no central registry".** That sentence was
written when nothing consumed the list. Since W2-5, `Reject(code)` (channel-spec README 3.3.2)
consumes it: a channel step may not raise an engine code, because an alert on `HEADER_PARSE_ERROR`
must mean a broken producer, not a channel's validation rule. A deny-list needs a list, and the SDK
had grown one (`tributary.sdk.codes`) with no contract behind it. The cost the old sentence avoided,
keeping two lists in sync, is paid by a test instead of by review (below).

**Direction: the contract is the source, and the code is checked against it.** The registry is not
generated from `codes.py`. A contract generated from code is a contract nobody reviewed. That is the
failure 2.3 names for a log-only taxonomy that operations comes to depend on.

| Part | Meaning |
|---|---|
| `codes` | Every exact code the engine emits in `tb-error-code` or an event's `error_code`, with its emitter, its meaning and a status: `emitted`, or `reserved` (never emitted now, but kept so that a record from an older build stays attributable). Whether a code is retryable belongs to the emitter, not the registry. |
| `reserved_prefixes` | Families whose members are minted at run time (`HTTP_503`, `PG_23505`, `KEY_FIELD_ABSENT`). A new member is reserved without editing the file. |
| `log_only` | Finer reason codes a component uses in its own log line (2.3 last paragraph). They are reserved so a step cannot raise them, and they **must never appear** in a header or event. |

**Rules**

1. `Reject(code)` refuses every code in `codes` and `log_only`, and every code under a
   `reserved_prefixes` entry. `Reject.engine(code)`, which only the built-ins use, refuses any code
   that is **not** registered, so the engine cannot mint an unregistered code either.
2. A component that emits a new exact code adds it here first, through the architect, as a contract
   change. That includes a **fixed** name under a reserved prefix, such as `TB_PIPELINE_TIME_BUDGET`,
   because alert rules key on fixed names and this file is where an operator looks them up. Only
   family members minted at run time (`HTTP_503`, `PG_23505`, `KEY_FIELD_ABSENT`) need no entry.
3. **Checker:** a test in `tributary-sdk` loads `error-codes.json` and asserts that
   `tributary.sdk.codes.ENGINE_CODES` equals the union of `codes` and `log_only`, and that
   `RESERVED_PREFIXES` equals `reserved_prefixes`. Both directions are checked, so neither side can
   gain an entry alone. `contracts/validate.py` cannot do this, because it runs on a clean clone with
   no SDK installed (`contracts/README.md`, the two-checker split).
4. `SPLIT_TOO_LARGE`, `SPLIT_INPUT_INVALID`, `FILTER_UNRESOLVED` and the three `VALIDATE_*` codes are
   **engine codes raised by built-in steps**. They are registered, so a channel's own step cannot
   borrow them. A channel can still write `VALIDATE_ORDER_MISSING`, because `VALIDATE_` is not a
   reserved prefix. Only the exact codes are taken.

**Drift found while writing it, and how it is resolved.** This section named `TRANSFORM_ERROR` for a
step that exhausts its retries, and the envelope and events examples use it. The runtime executor
has emitted `STEP_ERROR` since wave 1 (`tributary.runtime.executor.STEP_ERROR_CODE`). The contract came
first and the examples depend on it, so **the executor changes to `TRANSFORM_ERROR`**
(`platform-engineer`). `STEP_ERROR` stays in the registry as `reserved`, so no step can take it and a
record from a pre-fix build stays attributable. No deployment exists, so nothing downstream keys on
either code yet. The fix is cheaper now than after the first alert rule is written.

**Not registered:** `Drop` reasons (`FILTERED`, `FILTERED_UNRESOLVED`, `EMPTY_SPLIT`). They are not
error codes: they reach `_tb.events` as the `error_message` of a `status: dropped` event, and never
reach `tb-error-code`.

**Set 1.2 (W2-8, 2026-09-28): the `REPLY_` prefix and six exact codes.** Four are receiver codes that
reach the **sender** (in `MSA-3`, and in the receiver's metrics), not a header or an event:
`REPLY_TIMEOUT` and `REPLY_ROUTE_LOST` answer `AE`; `REPLY_BUSY` and `REPLY_UNAVAILABLE` answer `AR`.
They are registered anyway, because integrators and alert rules key on them and a channel's `Reject`
must not be able to borrow them. Two are worker codes: `REPLY_BODY_MISSING` on a `failed` reply record
and its event, and `REPLY_ROUTE_INVALID` on a `replied` event only (2.3.3). A failed reply produce
reuses `PRODUCE_FAILED` with stage `replied`. Channel-spec README 3.2.6 has the full answer table.

**Set 1.3 (2026-09-29): seventeen fixed names registered, no prefix added.** Rule 2 had not been
applied to the loaders: `HTTP_REDIRECT_REFUSED`, `FHIR_BUNDLE_INVALID`, the two `SSH_` codes and the
four `MLLP_` codes were named in channel-spec README 3.4 from set 1.1 and never listed here. Added with
them are the wave 2 loaders' new fixed names (`FHIR_ENTRY_FAILED`, `FHIR_RESPONSE_INVALID`,
`CH_UNKNOWN`, `SFTP_NAME_CONFLICT`, and the four fixed `S3_` names) and the receivers'
`TLS_PEER_NOT_ALLOWED` (log and metrics only, never a header or event; channel-spec README 3.2.7). One
new rule: **a code minted at run time that equals a registered fixed name is not emitted as that name.**
Only `S3_` can collide today, because it upper-snake-cases a code the store chooses: a store answering
`KeyConflict` would otherwise read as the loader's own idempotency conflict. It becomes `S3_UNKNOWN`.
The numeric families (`HTTP_<status>`, `SFTP_<status>`, `CH_<code>`) and `PG_<class>` cannot collide.
Behaviour of `Reject` does not change: every new name was already under a reserved prefix. Only the
cross-check test moves, so `ENGINE_CODES` gains all seventeen in the same commit.

**Set 1.4 (2026-09-29): `REPLY_EXPIRED`.** The worker skips a reply that is past `tb-reply-deadline`
plus the 5 s skew allowance (channel-spec README 3.2.6). Its `replied` event keeps `status: dropped`,
because the skip is deliberate and must not count toward error-rate alerts. It now also carries
`error_code: REPLY_EXPIRED`; until set 1.4 it carried only a sentence in `error_message`. **Why a code
and not a `Drop`-style reason:** `Drop` reasons belong to a channel's step (`FILTERED` is the channel
author's decision), while an expiry is an engine condition, typically a backlog after an outage (S3
saw 1,026 late replies in one restart). Operators need to query it in the archive by a stable key.
**The rule this sets:** a `dropped` event may carry `error_code` only when the code is a registered
engine code for an engine-initiated skip. `Drop` reasons stay in `error_message`, unregistered. The
events schema already allows this, so there is no schema change. **What we give up:** the "`dropped`
never has a code" symmetry, and `ENGINE_CODES` must gain the code in the same commit, or the
registry cross-check goes red.

**Set 1.7 (2026-10-05): `REPLY_BODY_INVALID` and `REPLY_BODY_OVER_BUDGET`.** Receiver codes, like the
set 1.2 four: they reach the sender in `MSA-3` and the receiver's metrics, never a header or an event.
Both answer `AE`, because the message was committed and routed and only the `body: step` answer could
not be written: a framing byte in the body, or no room in `queue.maxBytes` to hold it (channel-spec
README 3.2.6). The first build reused `REPLY_BUSY` and `REPLY_BODY_MISSING`. That is **rejected**:
`REPLY_BUSY` is an `AR` code (not published), so reusing it under `AE` gives one code two opposite
resend meanings; `REPLY_BODY_MISSING` is the worker's code for a step that never called `ctx.reply`.
`ENGINE_CODES` gains both in the same commit, or the cross-check goes red.

### 2.3.3 Reply-routing headers: a parse failure disables the reply, not the message (set 1.2)

`tb-reply-partition` and `tb-reply-deadline` on a `raw` record say where and until when to answer.
They say nothing about the message. So a worker that finds either malformed, or
`tb-reply-partition` out of range for the real `reply` topic, **processes the message normally,
produces no reply, and emits a `replied` event with `status: error` and `REPLY_ROUTE_INVALID`.** It
does not dead-letter the message. That is the one exception to the 2.3 reject rule, and it is narrow:
dead-lettering a committed, valid message because its answer route is broken would turn an answer
problem into a delivery problem, while the sender still gets the truthful `AE` / `REPLY_TIMEOUT`.

Two consumer rules for the `reply` topic itself:

1. **The receiver's reply consumer drops a reply record it cannot parse**, with a metric and a log line
   carrying topic, partition and offset only. It cannot dead-letter (receivers have no `dlq` WRITE,
   `topics/v0` 3.4) and a reply is not a message. The pending request then times out.
2. **Only `tb-reply-status: accepted` can become `AA`.** Absent (a set 1.0-shaped reply) or an unknown
   value from a newer producer answers `AE`. This is how "lenient consumers" and invariant 1's
   no-false-ACK coexist: the unknown value is tolerated, and never trusted.

### 2.4 Known envelope drift in Phase 0 code — do not reproduce

Spike code diverged from this contract in three ways, and S3 used one experimental header that set
1.2 replaced. All four are recorded here because the spike
directories stay in the tree as reference material, and the fastest way to write a Phase 1 emitter
is to copy one.

| Drift | Correct form |
|---|---|
| `tb-x-stage` used as a load-bearing header | `tb-stage`. `tb-x-*` is experimental, never load-bearing, and must not survive into a released package (section 2.1). |
| Non-canonical ids: uppercase hex, unhyphenated, or a UUIDv4 in `tb-msg-id` | Canonical lowercase hyphenated form, and `tb-msg-id` is **UUIDv7** specifically — the schema's `uuid7` pattern pins the version nibble. |
| Timestamps as epoch floats | RFC 3339, UTC, 3 to 9 fractional digits (section 2). The events contract has an `invalid/ts-epoch-float.json` example for the same mistake. |
| `tb-x-reply-partition` (S3 targeted mode) | `tb-reply-partition`, set 1.2, with `tb-reply-deadline` beside it. The `tb-x-` form is retired. |

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
| `tb-error-code` | – | – | R | R | C | Stable machine code, `^[A-Z][A-Z0-9_]{0,63}$`, for example `HTTP_503`, `TRANSFORM_ERROR`. **New in v0**. Never a human message: exception text can quote payload, and invariant 6 forbids PHI here. The redacted human message belongs in `_tb.events.error_message`. **On `reply` (set 1.2):** required when `tb-reply-status` is `rejected` or `failed`, forbidden otherwise. |
| `tb-retry-not-before` | – | – | R | – | – | Earliest time a retry consumer may deliver this record. **New in v0**: delay tiers on a shared `retry` topic need a visible-after timestamp, because Kafka has no per-record delay. |
| `tb-reply-partition` | C | – | – | – | – | **Set 1.2.** Set iff the request is synchronous: the `reply` partition the receiving process holds, decimal `0`-`63`. Requires `tb-correlation-id` and `tb-reply-deadline`. The worker replies iff it is present, and strips it from everything it produces. Not PHI. |
| `tb-reply-deadline` | C | – | – | – | – | **Set 1.2.** Present iff `tb-reply-partition` is. An upper bound on when the receiver stops waiting, so it **must not be earlier** than the real deadline: set it to produce time + `ack.timeoutMs` + `reply.timeoutMs`, since the reply clock starts only at the commit callback. The worker skips a reply past it plus 5 s of skew. |
| `tb-reply-status` | – | – | – | – | C | **Set 1.2.** `accepted`, `rejected` or `failed`. Optional in the schema, so a set 1.0-shaped reply stays valid; **every W2-8 worker sets it**. Only `accepted` can become `AA` (2.3.3). |

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
| `tb-stage` | **`quarantined`**, always — not the stage that read it. The record never reached `queued` or `pending`, so naming either is false, and every component that can quarantine (a worker reading `raw`, a loader reading `out.<dest>` or `retry`) must stamp the same value or the browser's timeline disagrees with itself. *Where* it was read from is recorded precisely by `tb-source` and by the event's `topic`/`partition`/`offset`. |
| `tb-error-code` | **`RECORD_UNDECODABLE`**, which is distinct from `HEADER_PARSE_ERROR` (section 2.3). They are different failures and deserve different alerts: `HEADER_PARSE_ERROR` means one header was malformed on an otherwise identifiable record, which is usually a broken producer field and keeps its original identity; `RECORD_UNDECODABLE` means the record could not be identified at all, which is framing damage, corruption or an attack. The schema keys the quarantine shape off this code. |
| Key | `tb-msg-id` — the minted one — as for every `dlq` record. |
| **Value** | The original record's **key, headers and payload**, framed per 5.2.1. |

The original bytes go in the **payload**, which is the one part of the envelope that is opaque and
already treated as PHI-bearing, on a topic whose ACLs and 30-day retention already assume that. They
do not go into headers, `attrs`, or an event.

**The original key goes in the blob too.** Without it the quarantined artefact is not the record —
key, headers and payload are what arrived, and the key is what a reprocess would need to reproduce it
on a per-key channel. It adds no new exposure: it is going into a DLQ payload whose ACLs and
retention already assume PHI, and it already existed on the topic it came from. Two rules follow,
because this is the field the contract otherwise restricts hardest (section 4): the original key MUST
NOT become the wrapper's key, which is the minted `tb-msg-id`, and it MUST NOT be lifted out into a
header, an `attrs` entry or a log line. It lives in the blob and nowhere else.

### 5.2.1 Blob framing

**Pinned, not implementation-defined.** Two components can already write a quarantine wrapper — a
pipeline worker reading `raw` and a loader reading `out.<dest>` — so an implementation-defined
framing means the archive and the message browser cannot render either one reliably.

```
"TBQ1"                                   4 bytes, magic and format version
u32 key_len            key               key_len bytes
u32 header_count
  ( u32 name_len       name              per header, header_count times
    u32 value_len      value )
payload                                  to the end of the record value
```

- All lengths are **unsigned 32-bit big-endian**. Big-endian so a hex viewer reads them left to
  right in the order a human expects; the contract has to say which, or two implementations differ
  by byte order and both believe they conform.
- `0xFFFFFFFF` means null, distinguishing a null key or value from a zero-length one. Safe as a
  sentinel because the inline payload limit is 1,000,000 bytes.
- The magic carries the version. A future framing is `TBQ2` and is distinguishable on sight, which is
  what makes this safe to pin now.
- Names and values are **raw bytes, not text.** They are what could not be decoded; re-encoding them
  would destroy the evidence the blob exists to preserve.

**Fidelity limit on header names, and it is a client limitation rather than a choice.** `confluent-kafka`
hands header *names* to Python already decoded as `str`, with no API for the bytes that were on the
wire. A name that was not valid UTF-8 therefore cannot be written to the blob byte for byte; the
practical approach is to re-encode with `surrogateescape`, which round-trips whatever the client
decoded — **not necessarily what arrived.** Header *values* are untouched bytes and are exact. So for
values 5.2.1 is literally true, and for names it is true only as far as the client allows.

Consequences to state rather than discover: a blob header name is evidence of what the client saw, not
proof of what was sent, so it must not be treated as authoritative in a forensic review; and the common
undecodable cases — framing damage and value corruption — are unaffected, because they do not turn on
name bytes. **Assumption, unverified:** nobody has tested a non-UTF-8 header name end to end. Raised by
`platform-engineer` while implementing, and documented rather than worked around, which was the right
call — a client limitation hidden behind a conformant-looking blob is worse than one written down.

Two consumer rules:

1. **Nothing in the message flow parses this blob.** Receivers, workers and loaders write it and
   never read it. Only the archive, the message browser and the CLI parse it, and only on an explicit
   operator action. A component that parsed it automatically — to retry a quarantined record, say —
   would be feeding attacker-controlled bytes back into the pipeline on a loop.
2. **When the browser renders it, it is payload, not metadata.** The header names and values inside
   are normally metadata surfaces, but here they are unvalidated bytes from an unidentifiable record,
   and under mis-framing a "header name" is payload content. So the whole blob, names included, is
   masked by default with an audited reveal, exactly like a payload (W4-3), and never rendered in a
   list view or a tooltip.

Design credit: `platform-engineer` specified this framing while implementing, and it is adopted as
written apart from pinning the byte order and the two consumer rules.

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
   **`tb-received-at` is copied unchanged from the original** (set 1.8 part 2, ruled 2026-10-06 on
   `archive-engineer`'s W3-3 question). Rule 1 holds: a reprocess or replay is a re-injection, not a
   receive, and the original receive time is known, unlike a quarantine (5.2). Steps that read
   `received_at` therefore produce what the original should have produced. The reprocess time is
   recorded in the lifecycle event's `ts` and in the audit event (W3-1b), not in the header.
   **Consequence:** `now - tb-received-at` is not a latency for a reprocessed or replayed record and
   must not be computed as one. Until W3-3's origin discriminator exists (channel-spec section 9), a
   latency consumer cannot tell such a record from a split child, so it excludes every record that
   carries `tb-parent-id`. The new `tb-msg-id` may be derived deterministically from the original id
   and the operator's request id, so a retried action with an unknown outcome is absorbed by loader
   idempotency (`tributary-archiver` `republish.py`). The layout is pinned with W3-1b.
6. A step MUST NOT delete a registered header it does not understand.
7. **Set 1.2:** `tb-reply-partition` and `tb-reply-deadline` are **not** propagated. The worker strips
   them from every `out`, `retry` and `dlq` record it produces, and reprocess never copies them, so a
   reprocessed or replayed record never answers on a partition nobody is watching.
   `tb-correlation-id` still propagates as before.

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
