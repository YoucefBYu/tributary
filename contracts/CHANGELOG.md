# Contracts changelog

Newest first. Each entry names the contract, the version, whether the change is additive or
breaking, and the reason. Breaking changes reference an ADR.

## 2026-09-22 (W1-1) — contract set 1.0: new `tributary.channel-spec/v0`, plus envelope and topics additions

Phase 1 wave 1. Everything here is **additive** to Draft contracts, so no version bump; had v0 been
frozen, none of it would be breaking either — the envelope and topics changes add rules and examples,
they do not narrow what an existing conforming producer may send.

**Decision: "contracts v1" is a contract-set milestone, not a directory rename.** Set 1.0 is
`envelope/v0` + `topics/v0` + `events/v0` + `channel-spec/v0`, frozen together at the wave-1 gate.
Rejected alternative: copy the three existing contracts into `v1/` directories. That moves about
sixty files, breaks every path in `tests/conformance`, ADR-002, ADR-003 and `STATUS.md`, and changes
no schema. This README's own rule is that a new major directory is created by a breaking change
*after* a freeze, not by the freeze. **Give up:** the deliverable name and the directory names no
longer match, so the mapping table in `README.md` has to stay accurate. Reversible by the lead or
the human, and much cheaper to reverse now than after wave-1 code references land.

### New: `tributary.channel-spec/v0` (Draft)

`channel-spec/v0/channel.schema.json` plus 23 examples (4 valid, 19 invalid). Consumers: runtime,
control plane, CLI, UI, migration. Decisions recorded in
[ADR-007](../docs/adr/ADR-007-channel-spec-shape-and-refusals.md), with the options and what each
gives up (ADR-004 to ADR-006 are reserved for the topics already named in `docs/adr/README.md`). The schema-enforced rules and the rule each serves are tabulated in
`channel-spec/v0/README.md` section 2; the load-bearing ones:

| Rule | Basis |
|---|---|
| `ordering` required with no default; `partitionKey` required iff `per-key` | Invariant 5 |
| `source.ack.mode` is a one-member enum (`after-commit`) | Invariant 1 — no representable way to express ACK-before-commit |
| `destinations[].idempotency` required; `none` requires a justification note | Invariant 4 |
| `retry.policy: retry-topic` is in the enum and **rejected by the validator** | ADR-003 D1/D2 — defined vocabulary, never implemented or measured |
| No field anywhere carries inline code | Invariant 7 |
| `pipeline.bundle` must be digest-pinned | Supply chain; browser attribution |
| TLS required on a non-loopback `bind`; plaintext HTTP only to loopback; `tls.clientAuth` must be stated | Phase 1 requirement 8 (SR-8, CR-10) |
| `queue.maxBytes` required when `queue` is present | Phase 1 requirement 5 (SR-3) |
| Destination URL carries no query string or fragment | Envelope section 4 — PHI in URLs |
| `with` rejects credential-shaped property names | No secrets in a spec |

Also fixed by this contract: the `_tb.<env>.channels` record shape (key, value, tombstone, the
`metadata.version` rule) — `channel-spec/v0/README.md` section 6. Invariant 3 makes that topic the
worker's only source of truth, so it needed a contract and had none.

### `tributary.envelope/v0`

| Change | Reason |
|---|---|
| New section 2.3, **every header is parsed defensively: reject, never crash** | Phase 1 requirement 13 (SR-10), generalised from the one experimental header a spike hit to every integer, timestamp and UUID header. Defines `reject` (dlq + `HEADER_PARSE_ERROR` + event + commit), forbids quoting the value in the error, and names the missing-header case as the same class of failure |
| New section 2.4, **known envelope drift in Phase 0 code** | `tb-x-stage` as a load-bearing header, non-canonical ids, epoch-float timestamps. The spike directories stay in the tree and are the fastest thing to copy |
| Two invalid examples: `attempt-not-an-integer`, `channel-version-not-an-integer` | Give section 2.3 a conformance guard |
| Conformance paragraph corrected | It still claimed 74 expected and "that re-run has not happened yet"; it was 76 at `83a1fd2` |

### `tributary.topics/v0`

| Change | Reason |
|---|---|
| New section 3.7 and a `must_not_hardcode` block in `topic-defaults.json` | The runtime, control plane, topic creation, Compose and Helm read these values, never restate them. `min.insync.replicas` is topic-level: a topic created without it inherits the broker default of 1, `acks=all` is satisfied by one replica, and a broker loss drops an acknowledged message with every setting apparently correct. That was a HIGH finding, and a copied literal in one of five places is how it returns |
| New section 3.8 and `profiles.lite` fields `production_use`, `startup_warning_required`, `warning_acceptance` | The lite RF=1 warning is mandatory and testable, not advisory, and `lite` is barred from production. Human-approved profile exception to invariant 1, ADR-002 D14, 2026-09-21 |

### `tributary.events/v0`

**No change, deliberately.** Wave 1 emits lifecycle events through the existing schema; nothing in
W1-3 to W1-8 needs a new field, and the `_hmac` question is human-reserved. Phase 1 requirement 10
(validate the real emitters against this schema) is a test obligation, not a contract change.

### Not verified

`contracts/validate.py` is new and **has never been run** — this session has no shell. The 19
channel-spec expected-failure pairs and the 2 new envelope pairs are the architect's **predictions**,
not measurements, and are labelled as such in the manifests. `qa-engineer` runs
`--emit-expectations` and the pairs are corrected from the measured output before the wave-1 gate.
Every other contract's pairs were measured at `83a1fd2` and are unaffected.

## 2026-09-20 (P0-3) — `tributary.topics/v0`: partition defaults and consumer timeouts, from measurements

Still Draft, so these land in place without a version bump. Had v0 been frozen this would be
**breaking for any deployment that already created topics**: partition counts cannot be changed on an
existing topic without recreating it, and on an ordered channel `out.<dest>` must keep matching
`raw`.

| Change | From | To | Basis |
|---|---|---|---|
| Partitions per channel topic (`raw`, `out`, `retry`, `reply`) | 12 | **3** | **S4 measured.** At 12, the cluster was lost at about 425-450 channels — the plan's own 500-channel threshold failed. At 3, 1,000 channels (12,009 partitions) were healthy |
| Partition budget | absent | `partition_budget` block: warn at 8,000 projected partitions, share at 12,000, believed fatal about 20,000 | S4. **12,000 measured; 8,000 judgement; 20,000 is one failure in one run at a computed count, with nothing tested between 12,000 and 21,000** |
| `session.timeout.ms` | unset (Kafka default 45 s) | **15000**, band 10000-15000, `heartbeat.interval.ms` 5000 | **S5 and S4 independently.** It is the recovery-time floor for an ordered channel: 10.01 s and 11.32 s takeover at a 10 s setting (S5); 43.8 s static-member removal at the 45 s default (S4) |
| `commit_every` | absent | tunable per destination, default well above 1 | **S5 measured:** 4.9x throughput (151 → 733 msg/s) for a replay window of 50 instead of 1 |
| `_tb.<env>.events` partitions | 24, "TODO confirm against S1/S2" | 24, **"still unmeasured"** | No Phase 0 spike exercised the archive path under load. Honest label, not a new number |

Every figure above is a **lower bound from one laptop-hosted cluster** (3 brokers, 1 GB heap, 7.4 GiB
Docker VM on Windows, idle topics, mostly single runs) and must be re-measured on real broker memory
before being used for sizing. Do not scale them with heap: the 2 GB runs disagreed with each other by
more than 3x. See `docs/spikes/S4-topic-scale.md`, `docs/spikes/S5-ordering-retry.md` and
`docs/spikes/PHASE-0-DECISION.md`.

No schema changed, so the conformance suite is unaffected.

## 2026-09-20 (round 2) — security review, findings 6-14

Second round against the same v0 Draft, from `docs/security/contracts-v0-review.md`. Nine findings:
one HIGH left deliberately unresolved and escalated, five MEDIUMs fixed, one MEDIUM referred to the
human, two LOWs fixed. **None disputed.** Still Draft, so these land in place without a version bump;
had v0 been frozen, findings 9, 10 and 13 would be **breaking** (they narrow what a producer may
send) and would have needed `v1` plus an ADR.

| # | Sev | Finding | Status |
|---|---|---|---|
| 6 | HIGH | The `_hmac` convention names a keyed hash but nothing specifies or verifies one. An unkeyed `sha256(mrn)` is also 64 hex characters and is reversible by exhaustive search over a bounded identifier space, so a producer can follow the contract exactly and still publish the identifier into ClickHouse and the browser. | **UNRESOLVED, escalated.** Accepted as real; deliberately not fixed here. A schema can constrain shape, not mechanism, so the 64-lowercase-hex shape stays (it fits HMAC-SHA-256, keyed BLAKE2 and a peppered digest alike, which is what keeps the choice open). Construction, key scope, rotation, key store, and whether a keyed hash is de-identified at all, are cryptography and key management — CLAUDE.md escalates both to a human. Questions enumerated in `events/v0/README.md` 3.1, owned by ADR-004 (ADR-002 follow-up 6). Interim contract position: producers SHOULD NOT emit identifier-derived `attrs`, and `_hmac` values are treated as PHI in ClickHouse. This blocks per-patient browser search (ADR-001 6.6), not the Phase 0 gate. |
| 7 | MEDIUM | `attrs` was claimed not to be a payload smuggling channel, but 32 entries × 256 characters is ~8 KiB of channel-controlled text per event against a 1-2 KiB ADT message. | **Fixed.** The false claim is gone. `attrs` is now 16 entries, values ≤ 64 characters from an allow-list that excludes the HL7 delimiters `\| ^ ~ \ &` and CR/LF, and a producer-side cap of 1024 serialized bytes (JSON Schema cannot sum bytes). `attrs` no longer has room for an ADT message and cannot carry a segment verbatim. Chunking across many events remains possible and the contract says so instead of claiming otherwise. Two new invalid examples: `attrs-too-many-entries`, `attrs-hl7-delimiters`. |
| 8 | MEDIUM | The record-key prohibition covered logs, metrics, traces, exceptions and test output, but not URLs, browser storage, sender-facing error responses or other headers — and section 2 promoted `kcat`/Kafka UI with no access-control requirement, i.e. an unaudited PHI viewer. | **Fixed.** Envelope section 4 is now a table of surfaces: URLs (including HTTP loader and lookup URLs — a URL is logged by every proxy in the path), `localStorage`/`sessionStorage`/cookies, NACK text and HTTP error bodies returned to senders, and `tb-usr-*`/`tb-x-*`/`tb-source`/`tb-correlation-id`. Added the rule that anything able to display a record key or raw record — browser, `kcat`, Kafka UI, ClickHouse client — is a PHI viewer and must be authenticated, authorized and audited like one. |
| 9 | MEDIUM | `tb-correlation-id` was free text (`^[A-Za-z0-9._:-]{1,128}$`) with unconstrained provenance, and it is the `reply` record key, copied to ClickHouse and the browser in clear text. Nothing forbade setting it from payload content. | **Fixed structurally.** It is now a UUID (`$ref uuidAny`) in both the envelope and the event schema, so a payload-derived value cannot be represented at all — better than a rule saying "do not log it". Envelope section 3.2 states the consequences: the receiver mints a fresh UUID per request (ADR-001 6.5 already says it generates one) and keeps the sender's token only in its in-memory pending map; no derivation from an identifier, including via a UUIDv5 namespace, which would be a stable per-patient pseudonym. **Give up:** a sender cannot correlate by its own token in the browser. New invalid example `correlation-id-sender-token`. |
| 10 | MEDIUM | `tb-usr-*` was documented as reaching `_tb.events`, ClickHouse and the browser, but the event schema has no field that can hold it (`attrs` keys are `^[a-z][a-z0-9_]{0,31}$`; `additionalProperties: false`), so implementers would invent a mapping. | **Fixed by deciding the other way: `tb-usr-*` does NOT reach `_tb.events` in v0** (envelope 2.2, events README rule 6). An automatic header-to-archive projection would make every channel-defined header a silent new archive column populated by channel code, in the one place invariant 6 is hardest to police. **Give up:** a step that wants metadata searchable emits an `attrs` entry deliberately — two places instead of one, and no `tb-usr-*` in a browser list view. Revisit in v1 with a per-channel allow-list if a real channel needs it. |
| 11 | MEDIUM | Shared topics were listed as having "no per-channel ACL" as a *cost*, which in a multi-tenant deployment means one channel's credentials read another's PHI. | **Fixed: it is now a restriction.** A share group MUST NOT span a tenant or trust boundary, the control plane must refuse such a deployment, and the group inherits the union of its members' sensitivity. `tb-channel` filtering is consumer-side convention, not access control. "Opt-in only" describes who turns it on, not who can read the data afterwards. |
| 12 | MEDIUM | `_tb.<env>.audit` retains 30 days against a likely six-year retention duty. | **Deferred to the human, not decided.** 30 d was picked to match the DLQ; it is an engineering default masquerading as a policy. Written up for decision in `topics/v0/README.md` 2.1 and carried in `docs/agent-team/STATUS.md`; the value is unchanged and marked `TODO(human)` in `topic-defaults.json`. The architect will not pick a retention: if the duty applies, the fix is architectural (ClickHouse or object storage as the system of record, Kafka as a buffer), and it also puts `_tb.<env>.events` (3 d) and `raw` (7 d) in scope. No legal review has been done and no agent should do one. |

| 13 | LOW | The 16 KiB total-header budget was unenforced: `tb-usr-*` and `tb-x-*` allowed 1024 bytes each, so a schema-valid header set could reach ~32 KiB, twice the stated budget. | **Fixed.** Both prefixes are capped at 256 bytes. JSON Schema cannot sum value lengths, so the total stays a producer-side check, but the worst case is now bounded: 16 registered headers under 2.5 KiB plus 16 open slots at 256 bytes is under 7 KiB. New invalid example `usr-header-over-length`. |
| 14 | LOW | No encryption-at-rest requirement for topics or the claim-check bucket, and unlike the ACL item it was not even listed as deferred. | **Fixed.** New topics contract section 3.5 and `topic-defaults.json.security.encryption_at_rest`: encrypted broker data directories, bucket-default encryption on the claim-check store (whole payloads — the largest single PHI store), encrypted archive volumes, encrypted backups. Stated with its limits: this covers a lost or decommissioned disk and a shared snapshot, **not** a compromised broker, an over-broad ACL or an unaudited Kafka UI. Application-level payload encryption is explicitly not required in v0 and not designed here; key custody (cloud-managed versus customer-managed KMS) is ADR-004 plus a human. |

Also in this round: `events/v0/README.md` gained section 3.1 (finding 6) and was resynced with the
schema, which it contradicted on the `attrs` entry cap. Consistency fix in `envelope/v0/README.md`:
section 2 capped `tb-claim-check` at 2048 bytes while section 5 and the schema say 1024; 1024 wins.

Four invalid examples added, so the conformance suite goes **70 → 74 tests**: envelope 7 valid / 18
invalid, events 8 / 16, topic names 10 / 12, plus the 3 guard tests. No valid example changed and no
schema rule was loosened, so nothing that passed before should fail now. **Not run by the architect**
(no shell in this session).

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

**Conformance re-run: 70 passed, 0 failed at commit `ae4d3bd`** — envelope 7 valid / 16 invalid,
events 8 / 14, topic names 10 / 12. This supersedes the 62-test green result below.

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
markers until S4 reports. **Superseded by the P0-3 entry at the top of this file: S4 disproved the 12,
which is now 3. The 24 for events is still unmeasured.**

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
