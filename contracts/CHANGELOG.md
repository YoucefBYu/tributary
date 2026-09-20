# Contracts changelog

Newest first. Each entry names the contract, the version, whether the change is additive or
breaking, and the reason. Breaking changes reference an ADR.

## 2026-09-20 (later) — security review fixes

`security-reviewer` raised 5 HIGH findings against v0. All five accepted, none disputed. v0 is still
Draft and its only consumer is the conformance suite, so these land in place without a version bump —
had v0 been frozen, findings 1, 2 and 3 would be **breaking** and would have needed `v1` plus an ADR.

| # | Finding | Fix |
|---|---|---|
| 1 | The validation projection carried `key`, which is usually a patient identifier. A validator embeds the failing instance in its error, so strict validation would put PHI into exceptions, logs and pytest output. | `key` replaced by `key_present` (bool) and `key_size` (int ≤ 256) — the only key properties the schema ever checked. Went further than the finding asked (redact or hash) by removing the value entirely, so no caller can reintroduce it. Added the rule that a `ValidationError` must never be logged verbatim: log `json_path`, `validator` and `tb-msg-id`. Per-class key identity (`dlq` keyed by `tb-msg-id`, `reply` by `tb-correlation-id`) moves to a code check. |
| 2 | The `attrs` "clear-text" guard guarded nothing: the example failed only on key casing, and `{"mrn": "0001234567"}` was schema-valid. | Three enforceable rules: a `_hmac`-suffixed key requires exactly 64 hex characters; a deny-list of 35 names that denote a direct identifier (`mrn`, `patient_id`, `dob`, `name`, `accession`, `phone`, `key`, `partition_key`, …) is rejected in clear form; every other value must be a short code, ≤ 64 characters, **no spaces**. Added `attrs-clear-text-mrn`, `attrs-hmac-not-fixed-length-hex`, `attrs-free-text-value`. Documented as defence in depth, not a proof — a surname is still a valid short code. |
| 3 | `tb-claim-check` and `events.claim_check` allowed any URI, so presigned URLs (credentials in the query string), `user:pass@host` userinfo, and identifier-bearing object names could be persisted to Kafka, ClickHouse and the browser. | Pattern forbids query, fragment and userinfo; max length 1024. Object key must derive from non-PHI parts only: `<env>/<channel>/<yyyy>/<mm>/<dd>/<tb-msg-id>`. Signed URLs are minted at read time by the API and never stored. Added `claim-check-presigned-url` (both contracts) and `claim-check-with-userinfo`. |
| 4 | The contract that defines broker and client configuration specified no transport security or authorization, so an implementer following it literally got a plaintext, wide-open cluster carrying PHI. | New required security profile (topics contract 3.4 and `topic-defaults.json.security`): SASL_SSL or mTLS, `SCRAM-SHA-512`/`OAUTHBEARER`, hostname verification on, `StandardAuthorizer` with `allow.everyone.if.no.acl.found=false`, `super.users` limited to the control plane and the topic-creation job, and least-privilege prefix ACLs per component on `<env>.<channel>.`. Deployment-specific values stay TODO. Two load-bearing rules: no data-plane component gets cluster-wide WRITE/DELETE/ALTER, and no worker or loader gets WRITE on `_tb.<env>.channels` (invariant 3 makes workers trust it completely, so write access there is data-plane-wide RCE). The full role model, rotation and key management are escalated to ADR-004 plus a human decision. |
| 5 | **Invariant 1 bug.** `min.insync.replicas` appeared only under `profiles` and `unclean.leader.election.enable` only under `broker`, but both are *topic-level* configs. A topic created from `classes[*].configs` inherited the broker default of 1, so `acks=all` was satisfied by a single replica: the receiver ACKs the sender and the next broker loss drops an acknowledged message, with every setting apparently correct. | Both pinned in all 8 per-class configs at the production floor (`min.insync.replicas: 2`, `unclean.leader.election.enable: "false"`), plus an explicit `apply_order` and a `durability_floor` block explaining why they are repeated. Only the explicit `lite` profile lowers the floor, via `class_config_overrides`. A tool that applies class configs and forgets the profile now fails safe. |

**Conformance suite must be re-run** — the earlier green result is superseded. New counts: envelope 7
valid / 16 invalid, events 8 / 14, topic names 10 / 12.

## 2026-09-20

### All v0 contracts — conformance-verified (no contract change, superseded by the security fixes above)

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
