# ADR-004: Archive Design, PHI at Rest, Retention Mechanics and the Kafka Role Model

- **Status:** **Proposed, draft** (2026-10-05, W3-10). **No invariant changes.** Sections 3-6 are the
  architect's proposal. Every key-custody, `_hmac` and retention item is a **`TODO(human)`** with
  options (section 7) and is **not designed here**: CLAUDE.md escalates key management and
  cryptography. The role model (section 5) is also a human decision under the 2026-09-21 ADR-004 scope
  approval, so this ADR stays Proposed until a human approves section 5 and answers section 7.
- **Date:** 2026-10-05
- **Deciders:** `architect` (draft). **Owed:** `security-reviewer` must read it (W3-10 verify);
  the human answers section 7 (`phases/wave-3.md` 7.2-7.4).
- **Contracts:** `events/v0` 4 and 4.1; `topics/v0` 2.1, 3.4, 3.4.1, 3.5, 3.6; `envelope/v0` (claim
  check); `observability/v0` (archiver metrics). Contract changes named here land with the task that
  needs them (section 8).

## 1. Context

The approved scope (decisions log, 2026-09-21) is the Kafka role and ACL model, credential rotation,
key custody for encryption at rest and for the `_hmac` key, and claim-check object lifecycle. W3-10 adds
the archive design and content retention mechanics.

Evidence and constraints:

- **W3-2a built the events archiver** (`tributary-archiver`, `clickhouse-connect`): `_tb.<env>.events` to
  ClickHouse, idempotent under `FINAL`, no identifier-derived attributes. It was ruled on 2026-10-05:
  non-nullable `message_id` with the nil UUID, dedup key `(channel, scope, message_id, stage,
  destination, attempt)`, month partitions, `*_hmac` dropped and counted (`events/v0` 4.1).
- **W3-2b needs content storage**, and exit criterion 1 needs per-stage content in the message
  browser. Events carry no payload (invariant 6, `events/v0` 3 rule 1), so content must come from the
  Kafka topics that hold it: `raw`, `out.<dest>`, `dlq`.
- **The ACL table was written from intent and was wrong on every first run** (set 1.4: a missing spec
  READ, a missing group, a missing transactional-id grant, unused `.retry` grants, an undelimited
  prefix). The archiver's own set 1.4 grant was PREFIXED and undelimited until set 1.8.
- **Encryption at rest is required, and key custody is not decided** (`topics/v0` 3.5). The `_hmac`
  mechanism has been deferred "until before the archiver is built" (2026-09-21). The archiver is now
  built.
- **Audit retention is open** (`topics/v0` 2.1), and on 2026-10-05 it gained archived events as a
  third place the same answer applies.

## 2. Options for content storage (the one structural choice in this ADR)

| Option | For | Against | Complexity / cost / scalability / familiarity |
|---|---|---|---|
| **A. A content archiver consumes `raw`, `out.<dest>` and `dlq` into ClickHouse `message_content` (chosen)** | Off the critical path (invariant 9): a consumer only. Exactly the bytes each stage committed. Backfill from `raw` is the same code. One query engine for the browser | One principal reads every channel's PHI in the environment (section 5, accepted exposure). Doubles read load on those topics | Medium; ClickHouse is already in the stack; scales with partitions |
| B. Workers and loaders write content themselves | No extra consumer | Puts the archive in the message path: a slow ClickHouse slows delivery, which breaks invariant 9 | Rejected on the invariant |
| C. Content in object storage only (S3/MinIO), ClickHouse holds locators | Cheap at scale, lifecycle rules, object lock | A second PHI store with its own keys before custody is decided; the browser needs a fetch path; MinIO is AGPL and never shipped (ADR-001 2.3), so lite has no store | Higher; operator-supplied store |
| D. Rely on Kafka retention for content (no archive copy) | No new PHI store | The browser loses content after 7 days (`raw`) and 3 days (`events`); reprocess after retention is impossible | Low, but fails exit criterion 1 for older messages |

## 3. Archive design (proposal)

| # | Decision | Gives up |
|---|---|---|
| A1 | **Two writers, two principals.** The events archiver (built, W3-2a) and a **content archiver** (W3-2b) are separate processes with separate principals, so the PHI-reading grant (section 5) sits on one process that does nothing else | One more deployment |
| A2 | **`message_content`** holds one row per `(message_id, stage, destination)`: `received` from `raw`, `routed` from `out.<dest>` (destination set), `dlq` from `dlq`. Payload bytes are stored as received, plus `content_type`, `size_bytes` and the envelope headers needed to show them, but **not** `tb-usr-*`: they are not projected anywhere outside Kafka (`envelope/v0` 2.2). Idempotent on the key under `ReplacingMergeTree`. The same month-partition caveat as `events/v0` 4.1 rule 2 applies | Stages inside a pipeline run in process (invariant 2), so there is no content for `transformed` separately from `routed`. The browser shows input, per-destination output and dead letters |
| A3 | **Claim-checked payloads are stored as locators only** (`claim_ref`), never fetched into ClickHouse. The browser fetches through the API, which audits the view (W3-6) | Two places to look, one of them the operator's bucket |
| A4 | **Content storage is a deployment switch, `TB_ARCHIVE_CONTENT`**: on by default in the `lite` profile, **off by default in `production` until the human answers 7.1 and 7.2**. A production operator can turn it on, and the startup log says which custody and retention placeholders apply | Production sites get events without content until they opt in. That is the point: the system does not create a long-lived PHI store under placeholder decisions |
| A5 | **ClickHouse access:** the archiver users hold INSERT on the archive database only; the API user holds SELECT; no other user exists in shipped configuration. Content is read only through the API, which emits an audit event for every content view (W3-6, exit criterion 4). Row-level policies are not in v0 | Operators with ClickHouse admin bypass the audit, which is an operational control, not a technical one |
| A6 | **Backfill** (W3-2b) re-reads `raw` from a given offset or time and writes through the same idempotent path. A second run changes nothing (W3-2b verify) | Only within `raw` retention (7 d default) |

## 4. Retention mechanics (proposal; the values are `TODO(human)`)

| # | Mechanism | Value |
|---|---|---|
| R1 | ClickHouse table TTL on `ts` with month partitions; whole parts are dropped where possible (`ttl_only_drop_parts`), so expiry does not rewrite data | Events: **90 d placeholder** (ruled 2026-10-05). Content: **30 d placeholder**, shorter than events because it is the payload. **Both `TODO(human)` (H4)** |
| R2 | Values are per environment in v0 (`TB_ARCHIVE_TTL_DAYS`, `TB_ARCHIVE_CONTENT_TTL_DAYS`). Per-channel retention is a later, additive channel-spec field | — |
| R3 | Kafka retention stays per `topics/v0` 2 (`raw` 7 d, `dlq` 30 d, events 3 d, audit 30 d `TODO(human)`) | `TODO(human)` for audit, H4 |
| R4 | **Erasure** of one message is mechanically available as a ClickHouse lightweight delete by `message_id` across both tables. Whether and when to use it, and whether crypto-shredding is wanted instead, is **`TODO(human)` (H6)** | — |
| R5 | **Claim-check objects** expire by bucket lifecycle rule no earlier than the longest retention of anything that can reference them: `max(raw, dlq, archive content)` plus a replay margin. An object is never deleted while a record or row still points at it | Storage for the margin |

## 5. Kafka role model (proposal; human approval required)

Builds on `topics/v0` 3.4.1 (set 1.4, reconciled with code) and ADR-008 D6-D9.

| Role | Principal | Grants (summary; exact strings in `topic-defaults.json`) |
|---|---|---|
| receiver | one per `(env, channel)` | WRITE `raw`; READ `_tb.<env>.channels` + spec-reader group; sync only READ `reply` + LITERAL `replier` group; IDEMPOTENT_WRITE |
| pipeline worker | one per `(env, channel)` | READ `raw`; WRITE `out.*`, `dlq`, `_tb.<env>.events`; sync only WRITE `reply`; groups as 3.4.1; transactional id prefix when transactional |
| loader | one per `(env, channel, destination)` | READ `out.<dest>`; WRITE `dlq`, `_tb.<env>.events`; LITERAL `loader.<dest>` group |
| events archiver | one per `env` | READ `_tb.<env>.events`; LITERAL group `tb.<env>.archiver` (set 1.8) |
| **content archiver (new)** | one per `env` | READ `<env>.*.raw`, `<env>.*.out.*`, `<env>.*.dlq` (**PREFIXED on `<env>.`**, the only cross-channel data grant); LITERAL group `tb.<env>.archiver.content` |
| control-plane publisher | one per `env` | WRITE `_tb.<env>.channels`, `_tb.<env>.audit`; DESCRIBE cluster |
| topic bootstrap | one per cluster | super user (creates topics and ACLs) |
| users-init | one per cluster | ALTER on CLUSTER **only during provisioning** (W3-16, rule K3) |
| operator admin | humans | super user, break-glass, audited outside Tributary |

Rules:

- **K1. Principal names** `tb-<env>-<role>[-<channel>[-<destination>]]`, truncated to the SCRAM or
  certificate limits by a stable hash suffix. Per-channel principals are what make 3.4's claim true:
  "compromising one channel's credentials does not grant read access to another channel's traffic".
  **Give up:** principal count grows with channels × destinations, which is provisioning work.
- **K2. Accepted exposure, needing human sign-off:** the content archiver reads every channel's PHI in
  its environment. That is inherent to an archive. Mitigations: its own principal and process (A1),
  no WRITE anywhere, and a separate secret. **Rejected:** per-channel content archivers (one more
  consumer per channel against the S4 budget, for no gain in what the ClickHouse store then holds).
- **K3. Provisioning grants are temporary.** `users-init` receives ALTER on CLUSTER at provisioning
  start (from the broker identity, as today) and **revokes its own grant at the end**. The next run
  re-grants it, so the job stays re-run safe. This is W3-16's specification. ALTER on CLUSTER also
  authorises ACL writes (ADR-008 D8), which is why it must not persist.
- **K4. Structured grants.** `security.acls` moves from parsed English to a structured list
  (resource type, pattern type, name template, operations) with this ADR's acceptance: ADR-008
  follow-up 8. This is breaking for `bootstrap_topics.py` only, so it gets a versioned field
  (`security.grants`) beside the old one for one set.
- **K5. Credential rotation mechanics (proposal):** SCRAM keeps one credential per mechanism per user
  (**assumption, not re-checked against the Kafka 4.x docs**), so an overlap window needs a second
  principal (`-b`) with identical grants. Roll clients to it, then delete the old one. mTLS: overlapping
  certificate validity, reload on restart. **Rotation without restart** (ADR-008 follow-up 5) needs
  the client to re-read its secret, which librdkafka supports only for some mechanisms. **Unverified.**
  **Cadence, secret store and who holds the secrets are `TODO(human)` (H5).**
- **K6. `OAUTHBEARER`** for Kafka is out of v0; SCRAM-SHA-512 and mTLS stay (`topics/v0` 3.4).

## 6. PHI at rest: what is decided and what is not

| Surface | Decided now (contract) | `TODO(human)` |
|---|---|---|
| Broker volumes, ClickHouse, PostgreSQL, backups | Encryption on, operator-provided (`topics/v0` 3.5) | **Key custody, H1** |
| Claim-check bucket | Bucket-default encryption on; lifecycle per R5 | **Key custody, H1** |
| Archived content column | Stored on an encrypted volume; content off by default in production (A4) | **Application-level encryption, H2** |
| `attrs` `*_hmac` | Not emitted by producers, dropped by the archiver (`events/v0` 4.1 rule 3) | **Construction, key and de-identification status, H3** |
| Audit trail | Event shape in W3-1b | **Retention and system of record, H4** |

## 7. Questions for the human (`TODO(human)`, none designed here)

**H1. Key custody for encryption at rest** (`wave-3.md` 7.4). Covers broker volumes, ClickHouse,
PostgreSQL, the claim-check bucket and backups.
- `TODO(human)`: (a) provider-managed keys (cloud disk default encryption, SSE-S3); (b)
  customer-managed KMS keys (SSE-KMS or CMEK, the customer controls rotation and revocation); (c)
  customer-held keys outside the cloud (HSM, Vault) for on-premises sites. Who holds them, the
  rotation cadence, and whether the answer differs for lite and production.
- Default until answered: operator-provided volume encryption, no Tributary-managed keys. **Gates
  production use of the archive, not the build.**

**H2. Application-level encryption of archived content.**
- `TODO(human)`: (a) none beyond volume encryption; (b) envelope encryption of the payload column with
  a KMS or Vault data key per channel or per period, which enables crypto-shredding but rules out
  server-side content search; (c) encryption of claim-check objects only. Choosing (b) or (c) starts a
  key-management design, which a human must own.

**H3. `_hmac` mechanism** (`wave-3.md` 7.3; `events/v0` 3.1). Due now: the archiver is built.
- `TODO(human)`: decide now or extend the deferral. If now:
  - **Construction:** HMAC-SHA-256, keyed BLAKE2b-256, or a per-channel pepper. All fit the 64-hex
    shape.
  - **Key scope:** deployment, environment or channel.
  - **Rotation:** dual-key search window, re-hash on rotation, or accept the break.
  - **Key location:** in the API process via KMS, or hashing as an authorised API operation.
  - **Is a keyed hash of an MRN de-identified for your purposes?** Under Safe Harbor it is not.
- Default: no identifier-derived attributes; per-patient search refused; archiver drops `*_hmac`.

**H4. Retention values and the audit system of record** (`wave-3.md` 7.2; `topics/v0` 2.1).
- `TODO(human)`: the retention duty for the access and control-plane audit trail (30 d versus a
  possible six-year duty; no legal reading has been done by agents); whether it reaches archived
  events (placeholder 90 d), archived content (placeholder 30 d), `raw` and `_tb.<env>.events`; and
  whether the system of record is ClickHouse, object storage with object lock, or Kafka with tiered
  storage. **Blocks exit criterion 4 (audit).**

**H5. Credential rotation policy.**
- `TODO(human)`: cadence for Kafka SCRAM and mTLS credentials and for ClickHouse and PostgreSQL users;
  the secret store (Kubernetes Secrets with external-secrets, Vault, a cloud secrets manager); and who
  may read the content archiver's credential (K2).

**H6. Erasure.**
- `TODO(human)`: whether erasure requests are honoured by delete (R4), by crypto-shredding (requires
  H2 (b)), or not at all where medical-record retention overrides erasure (ADR-001 8: per-jurisdiction
  review).

**H7. Role model sign-off.**
- `TODO(human)`: approve section 5, including the K2 accepted exposure and per-channel principals
  (K1).

## 8. Consequences, contracts and follow-ups

- **Invariants:** 9 holds (A1, archive consumers only); 6 holds (content is a PHI store by design,
  never a log, label or event; `tb-usr-*` not projected); 3 holds (no data-plane read of the archive).
- **Contract changes when the tasks start** (architect, set 1.8 part 2 or later):
  - `topics/v0`: content-archiver grants and the LITERAL group `tb.<env>.archiver.content` (the
    `consumerGroup` platform alternative gains `(\.content)?`);
  - `observability/v0`: content-archiver metrics;
  - `security.grants` structured list (K4).
- **Follow-ups:**
  1. `security-reviewer`: read this draft (W3-10 verify).
  2. `archive-engineer` (W3-2b): A2-A6 and R1-R2 against the placeholders, with no key handling.
  3. `devops-engineer` (W3-16): K3.
  4. The architect sends H1-H7 to the human through the lead and mirrors them in STATUS.
