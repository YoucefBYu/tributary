# Contracts changelog

Newest first. Each entry names the contract, the version, whether the change is additive or
breaking, and the reason. Breaking changes reference an ADR.

## 2026-09-20

### All v0 contracts — conformance-verified (no contract change)

`tests/conformance/test_contracts_v0.py` by `qa-engineer`: 62 passed, 0 failed, 0 skipped. Envelope
7 valid / 14 invalid, events 8 / 10, topic names 10 / 12. Every invalid example was additionally
checked to fail for the reason its manifest states rather than incidentally, and the two PHI guards
(`payload-in-event`, `attrs-clear-text-key`) are asserted by name. No schema bugs found, so no
contract text changed.

### `tributary.envelope/v0` — new, Draft

First publication. ADR-001 section 5.4 made concrete: value encoding, a per-topic-class required
header matrix, key rules, claim-check rules, propagation rules, JSON Schema and 7 valid plus 14
invalid examples. Additions beyond ADR-001 5.4, all additive and needed by ADR-003:

| Header | Why |
|---|---|
| `tb-destination` | `retry` and `dlq` are per channel, so a retry consumer otherwise cannot tell which destination failed. |
| `tb-stage` | ADR-001 6.4 requires the DLQ record to carry the failing stage. Ratifies the S5 spike's `tb-x-stage` as normative. |
| `tb-error-code` | Machine-readable failure cause. A code, never exception text, because exception text quotes payload (invariant 6). |
| `tb-retry-not-before` | Kafka has no per-record delay, so delay tiers on a shared `retry` topic need a visible-after timestamp. |

Also fixed: `tb-received-at` is RFC 3339 UTC with 3 to 9 fractional digits; `tb-msg-id` is UUIDv7
lowercase canonical; inline payload limit is 1,000,000 bytes with `message.max.bytes` 1,048,576;
`tb-x-*` is reserved for experiments and `tb-usr-*` for channel-defined metadata.

### `tributary.topics/v0` — new, Draft

ADR-001 section 5.3 made concrete: grammar, restricted segment charset, defaults per class, the
broker and client settings invariant 1 depends on, and consumer-group naming.

Deviation from ADR-001 5.3, recorded for human review in ADR-002: platform topics are
environment-scoped (`_tb.<env>.channels`, `_tb.<env>.events`, `_tb.<env>.audit`). The data-plane
names already carry `<env>`, which implies one cluster may host several environments; unqualified
platform topics would let a dev control plane publish specs that production workers consume. The
topic *class* `_tb.events` named in invariant 2 is unchanged.

Partition defaults (12 per channel topic, 24 for events) are ADR-001 planning values and carry TODO
markers until S4 reports.

### `tributary.events/v0` — new, Draft

ADR-001 sections 5.5, 6.6 and 6.10 made concrete: JSON on `_tb.<env>.events`, a `v` field, explicit
PHI rules, and the ClickHouse column mapping.

Differences from the S5 spike emitter, which is the current de facto shape:

| Change | Why |
|---|---|
| `kind` discriminator (`message` \| `partition`) | S5 gives partition pause and resume events a `message_id` that is not really about one message. |
| `ts` is RFC 3339 UTC text, not an epoch float | One time format across contracts, readable in `kcat` and ClickHouse. |
| `v` is required | An event stream with no version field cannot be evolved safely. |
| `error_message` is capped and must be static redacted text | `str(exception)` from a parser or validator routinely contains field values, which is a PHI leak (invariant 6). |
| `instance` replaces `worker` | Receivers and loaders emit events too. |
