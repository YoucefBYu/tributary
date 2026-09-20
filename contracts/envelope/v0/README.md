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
  "key": "TRIBUTARY-SYNTH-MRN-000123",
  "headers": { "tb-msg-id": "018f3a5c-...", "...": "..." },
  "payload_present": true,
  "payload_size": 1180
}
```

`class` is one of `raw`, `out`, `retry`, `dlq`, `reply` and drives the conditional rules. It is
derivable from `topic` by the [topics contract](../../topics/v0/README.md); it is carried
explicitly so the two schemas stay independent.

`class`, `key`, `headers`, `payload_present` and `payload_size` are all required, even when the key
is `null` or the size is `0`. An omitted field would pass the claim-check and size rules by
accident, which is the opposite of what a conformance test is for. `topic` is optional.

**Verified:** all 21 examples in this contract pass `tests/conformance/test_contracts_v0.py`
(2026-09-20), and each invalid example fails for the reason its manifest states. See
[`../../README.md`](../../README.md) for the run command.

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
| Limits | At most 32 headers; name ≤ 64 bytes; value ≤ 1024 bytes (`tb-claim-check` ≤ 2048); all headers together ≤ 16 KiB. |

Rationale for text values: `kcat`, the Kafka UI, ClickHouse and a human debugging a production
incident can all read them. The cost is a few bytes per record and a parse step for integers, which
does not register next to payload size.

### 2.1 Reserved prefixes

| Prefix | Meaning | Runtime behaviour |
|---|---|---|
| `tb-` | Normative. Only names in section 3. | Load-bearing. |
| `tb-x-` | Experimental or spike-local (for example `tb-x-seq` in S5). | Never load-bearing. Must not survive into a released package. |
| `tb-usr-` | Channel-defined metadata set by a step through `ctx`. | Propagated, never interpreted by the engine. |

`tb-usr-*` and `tb-x-*` values fall under invariant 6: no PHI. They reach `_tb.events`, ClickHouse
and the browser.

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
| `tb-correlation-id` | C | C | C | C | R | Required on `raw` when the channel uses synchronous reply mode (ADR-001 6.5), and propagated wherever it was set. Also links split and aggregate flows. |
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

---

## 4. Message key

| Rule | Value |
|---|---|
| Encoding | UTF-8 bytes of the channel's `partitionKey` expression result. |
| Length | ≤ 256 bytes. |
| Null | Allowed only when the channel declares `ordering: none`. A null key round-robins and destroys per-key order (invariant 5). |
| Per class | `raw`, `out`, `retry`: ordering key. `dlq`: `tb-msg-id`. `reply`: `tb-correlation-id`. |

**The key is frequently a direct patient identifier** — ADR-001's worked example is `hl7:PID.3.1`.
Therefore, under invariant 6, the key MUST NOT appear in a log line, a metric label, a trace
attribute, an exception message or test output in clear text. Log `tb-msg-id`. Where a key must be
searchable (the message browser), store a keyed hash, per ADR-001 6.6 and the
[events contract](../../events/v0/README.md).

This is the one place where an engine-internal value is PHI, and it is easy to leak by accident.
Reviewers: treat any `log`/`metric`/`span` call that touches a record key as a Critical finding.

---

## 5. Payload and claim-check

| Rule | Value |
|---|---|
| Max inline payload | 1,000,000 bytes. |
| Broker `message.max.bytes` | 1,048,576 (1 MiB), leaving ~48 KB of headroom for headers and framing. |
| Over the limit | Store the payload in the claim-check object store, set `tb-claim-check` to its URI, and produce the record with a **null** value. |
| With `tb-claim-check` set | The value MUST be null, not empty bytes. An empty payload is otherwise indistinguishable from a bug. |
| URI schemes | `s3://`, `gs://`, `az://`, `https://`. |

Claim-check objects follow the retention of the topic class that referenced them, and their
lifecycle is owned by the archive design (ADR-004, not yet written). Deleting a claim-check object
inside the `raw` retention window breaks replay: treat the object store TTL as `max(raw retention,
dlq retention)` until ADR-004 says otherwise. **Assumption, unverified:** no spike measured
claim-check behaviour in Phase 0.

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
