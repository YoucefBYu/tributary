# ADR-004: Archive Design, PHI at Rest, Retention Mechanics and the Kafka Role Model

- **Status:** **Proposed** (draft 2026-10-05, W3-10; **rev 2 2026-10-06** after the human's answers).
  **No invariant changes.** Section 7 state per question:
  - **H1 key custody, H2 application-level encryption, H5 rotation:** **DECIDED 2026-10-06,
    concurred with conditions C1-C8** (section 7.1) by the lead, the architect and
    `security-reviewer`, under the human's delegation (*"the lead, architect and security
    engineer"*). Only documented KMS, cloud-volume and library features are used. **No cryptography
    or key hierarchy is designed here** (CLAUDE.md escalation stays in force).
  - **H3 `_hmac`:** **DEFERRED** by the lead and architect under the human's delegation, until H1 has
    concurrence and before any per-patient search task starts.
  - **H4 retention:** **DECIDED.** Operator-configurable with documented defaults (section 4).
  - **H6 erasure and H7 role-model sign-off:** still **`TODO(human)`**. The human did not answer
    them on 2026-10-06.

  This ADR stays Proposed until a human approves section 5 (H7) and answers H6.
- **Date:** 2026-10-05 (rev 2: 2026-10-06; rev 3, conditions C1-C8: 2026-10-06)
- **Deciders:** `architect` (draft). H3 and H4 rev 2: lead and architect, under the human's 2026-10-06
  delegation. H1/H2/H5: lead, architect and `security-reviewer` (**CONCUR WITH CONDITIONS**,
  lead-relayed 2026-10-06; the architect has not read the review itself). Owed from the human: H6, H7.
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
| A4 | **Content storage is a deployment switch, `TB_ARCHIVE_CONTENT`**: on by default in the `lite` profile, **off by default in `production`**. A production operator turns it on after meeting H1's custody requirement (rev 2: a customer-managed key on the ClickHouse volume). The startup log states the effective content retention (section 4) and that the custody requirement is the operator's to meet. *(Rev 1 text tied the default to the human answering 7.1 and 7.2. Those were the wrong references: 7.1 is the PostgreSQL driver. H1 and H4 were meant.)* | Production sites get events without content until they opt in. That is the point: the system does not create a long-lived PHI store before the operator has confirmed custody |
| A5 | **ClickHouse access:** the archiver users hold INSERT on the archive database only; the API user holds SELECT; no other runtime user exists in shipped configuration. **Schema migration is a separate step** (an init job or `tributary-archiver migrate`, run before the archivers start) **with its own migration credential** holding CREATE/ALTER on the archive database. The archiver processes never run DDL and never receive that credential; at startup they check that the expected schema version is present and exit non-zero if it is not. (Today the archiver runs CREATE/ALTER at startup; `archive-engineer` is moving it out.) Content is read only through the API, which emits an audit event for every content view (W3-6, exit criterion 4). Who may view content: ADR-009 D14. Row-level policies are not in v0 | Operators with ClickHouse admin bypass the audit, which is an operational control, not a technical one. One more job to order before the archivers in Compose and Helm |
| A6 | **Backfill** (W3-2b) re-reads `raw` from a given offset or time and writes through the same idempotent path. A second run changes nothing (W3-2b verify) | Only within `raw` retention (7 d default) |

## 4. Retention (rev 2, 2026-10-06: DECIDED, operator-configurable with documented defaults)

**Human answer to H4, 2026-10-06 (relayed by the lead), verbatim:** *"Give the user a possibility to
change it"*. So retention is an operator setting, not a number this project fixes. Defaults are
documented, and the operator owns the duty that applies to their site. **No legal reading has been
done by an agent, and none is implied by the defaults below.**

**Options considered.** (a) One fixed value per store (rev 1 placeholders). Rejected: the human ruled
it out. (b) Per-environment operator settings applied by the migrate step (**chosen**). (c) A
retention write endpoint in the API. Rejected: the API process would need the ClickHouse DDL
credential (A5 gives it SELECT only). An admin token, or a compromised one, could then shorten audit
retention and erase the record of its own actions. (d) Per-channel retention. Deferred: a later,
additive channel-spec field. Criteria: invariant 9 (nothing in message flow waits on retention), A5
least privilege, irreversibility of deletion, and operator effort. **Give up:** changing retention
needs a redeploy (`helm upgrade` or Compose `up`), not a UI click. Retention is per environment, not
per channel. A UI can show the value but not change it.

| # | Rule |
|---|---|
| R1 | **Mechanism:** ClickHouse table TTL on `ts`, month partitions, `ttl_only_drop_parts = 1`, so expiry drops whole parts without rewriting data. **Stated cost:** a part is dropped only when all of its rows have expired, so a row can outlive its TTL by up to one month partition. An operator who needs prompt deletion of one message uses R4, not a shorter TTL |
| R2 | **Settings and defaults** (per environment, integer days, **1-36500**; anything else refuses to start, as `TB_ARCHIVE_TTL_DAYS` does today): **archived events** `TB_ARCHIVE_TTL_DAYS`, default **90** (name unchanged from W3-2a). **Archived content** `TB_ARCHIVE_CONTENT_TTL_DAYS`, default **30**, read only when `TB_ARCHIVE_CONTENT` is on (W3-2b). It is shorter because it holds payloads. **Audit records** `TB_ARCHIVE_AUDIT_TTL_DAYS`, default **2190** (six years). Why 2190: deleting an audit trail is irreversible, while over-keeping it costs little. Audit rows carry ids, actor and action, never payload (invariant 6). A six-year duty *may* apply (`topics/v0` 2.1). This is a conservative engineering default, **not a compliance determination**; the operator lowers or raises it. **Decrease guard:** `TB_ARCHIVE_ALLOW_TTL_DECREASE`, default `false`. It is read by the migrate step only (R6) |
| R3 | **Surfaces.** *Helm:* `archiver.retention.eventsDays: 90`, `contentDays: 30`, `auditDays: 2190`, `allowDecrease: false`, rendered into the env of both the migrate Job and the archivers. The current `archiver.ttlDays: 365` is **drift** from the documented default and is removed. The chart `fail`s at render if it is still set, so it is never ignored silently. *Compose:* the same four env vars with the same defaults on the migrate service. *API (read-only):* `GET` of the effective retention per environment (W3-1b), readable by every role (metadata, no PHI). It reads the `archive_settings` table that the migrate step writes. *No write surface in the API* (option c, rejected). *Docs:* a chart README section, "Changing retention", with the R6 procedure and R1's cost |
| R4 | **Erasure** of one message is mechanically available as a ClickHouse lightweight delete by `message_id` across the archive tables. Whether and when to use it, and whether crypto-shredding is wanted instead, is still **`TODO(human)` (H6)** |
| R5 | **Claim-check objects** expire by bucket lifecycle rule no earlier than the longest retention of anything that can reference them: `max(raw, dlq, TB_ARCHIVE_CONTENT_TTL_DAYS)` plus a replay margin. An object is never deleted while a record or row still points at it. The rule is in the operator docs, because the bucket is operator-supplied |
| R6 | **Applying a change to existing tables.** Only the migrate step (`tributary.archiver --migrate-only`, migration credential, A5) applies retention. It runs before the archivers on every `helm upgrade` and Compose `up`. Per table, it reads the current TTL from `system.tables` and, only when the value differs: **(1)** if the new value is *longer*, it runs `ALTER TABLE <t> MODIFY TTL toDateTime(ts) + INTERVAL <n> DAY` with `materialize_ttl_after_modify` left at its default `1`. Existing parts then get the new TTL, so a lengthened retention protects rows already stored (ClickHouse docs; **verified only by the R6 test**, not yet run). **(2)** If the new value is *shorter*, it **refuses**: exit non-zero, `startup_refused`, code `RETENTION_DECREASE_REFUSED` (L6a, not registered), table unchanged. It applies the change only with `TB_ARCHIVE_ALLOW_TTL_DECREASE=true`, because rows past the new TTL are deleted irreversibly at the next TTL merge. **(3)** It writes an `archive_settings` row (table, days, previous days, applied-at, release) and logs `retention_applied` with the same fields. Lengthening never restores rows already dropped. The archivers do not read or apply TTL values (INSERT only) |
| R7 | **Audit system of record: ClickHouse `audit_log`** (architect decision under the same delegation; it was the open half of H4). The API produces each audit record to `_tb.<env>.audit` (`acks=all`). The `requested` phase fails closed, as `archiver/audit.py` already requires. The events archiver consumes that topic into `audit_log`, with the same idempotent `ReplacingMergeTree` pattern and month partitions, and TTL from R2. **Kafka is a buffer:** `_tb.<env>.audit` keeps 30 d (`topics/v0` 2), which bounds how long the archiver may be down before audit records are lost. Rejected: Kafka as the store (six years at RF=3 on brokers); object storage with object lock (the stronger tamper evidence, but a second store, and lite has none, since MinIO is never shipped). **Give up:** a ClickHouse admin can alter or delete `audit_log` rows. Tamper evidence is an operational control in v0 (risk row) |
| R8 | **Kafka topic retention is not a records-retention control.** `raw` 7 d, `dlq` 30 d, `_tb.<env>.events` 3 d and `_tb.<env>.audit` 30 d stay as in `topics/v0` 2 and are not operator settings in v0 (`topic-defaults.json` is read, never restated). Per-topic overrides are a later, additive item if operators ask |

## 5. Kafka role model (proposal; human approval required)

Builds on `topics/v0` 3.4.1 (set 1.4, reconciled with code) and ADR-008 D6-D9.

| Role | Principal | Grants (summary; exact strings in `topic-defaults.json`) |
|---|---|---|
| receiver | one per `(env, channel)` | WRITE `raw`; READ `_tb.<env>.channels` + spec-reader group; sync only READ `reply` + LITERAL `replier` group; IDEMPOTENT_WRITE |
| pipeline worker | one per `(env, channel)` | READ `raw`; WRITE `out.*`, `dlq`, `_tb.<env>.events`; sync only WRITE `reply`; groups as 3.4.1; transactional id prefix when transactional |
| loader | one per `(env, channel, destination)` | READ `out.<dest>`; WRITE `dlq`, `_tb.<env>.events`; LITERAL `loader.<dest>` group |
| events archiver | one per `env` | READ `_tb.<env>.events`; **rev 2: READ LITERAL `_tb.<env>.audit`** (R7; set 1.8 part 2); LITERAL group `tb.<env>.archiver` (set 1.8) |
| **content archiver (new)** | one per `env` | READ `<env>.*.raw`, `<env>.*.out.*`, `<env>.*.dlq` (**PREFIXED on `<env>.`**, the only cross-channel data grant); LITERAL group `tb.<env>.archiver.content` |
| reprocessor (W3-29, 2026-10-06) | one per `(env, channel)` | READ, WRITE and DESCRIBE on LITERAL `<env>.<ch>.raw`; IDEMPOTENT_WRITE; **DESCRIBE only** on LITERAL group `tb.<env>.reprocessor` (assign needs a `group.id`; never joined or committed; measured by `archive-engineer`). Replay targets its source channel only (`wave-3.md` 2.6 H8). Set 1.8 `topic-defaults.json` |
| lag_reader (W3-29) | one per `env` | DESCRIBE on PREFIXED `<env>.` and LITERAL `_tb.<env>.events`; DESCRIBE on PREFIXED group `tb.<env>.`. No READ or WRITE |
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
  **Scope of the PREFIXED `<env>.` READ grant (`security-reviewer`, 2026-10-05):** a Kafka PREFIXED
  ACL matches every topic whose name starts with `<env>.`, not only the three patterns in the table.
  It therefore also covers each channel's `reply` and `retry` topics and **any topic added under
  `<env>.` later**, whatever it holds. Kafka ACLs have no wildcard in the middle of a name, so
  `<env>.*.raw` cannot be granted as such. Alternatives: LITERAL grants per channel topic, added and
  removed by provisioning on every deploy (exact, but provisioning becomes part of deploy); or a
  topic naming change that puts the stage first (breaking, `topics/v0`). The proposal keeps PREFIXED
  and records the wider scope under H7.
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
| Broker volumes, ClickHouse, PostgreSQL, backups | Encryption on, operator-provided (`topics/v0` 3.5) | Key custody: **H1 draft decision**, pending `security-reviewer` |
| Claim-check bucket | Bucket-default encryption on; lifecycle per R5 | Key custody: **H1 draft decision**, pending `security-reviewer` |
| Archived content column | Stored on an encrypted volume; content off by default in production (A4) | **H2 draft decision: none beyond volume encryption in v0**, pending `security-reviewer` |
| `attrs` `*_hmac` | Not emitted by producers, dropped by the archiver (`events/v0` 4.1 rule 3) | **H3 deferred** (rev 2) |
| Audit trail | Event shape in W3-1b; system of record ClickHouse `audit_log` (R7) | Retention **decided**: operator setting, default 2190 d (R2) |

## 7. Questions H1-H7 (rev 2, 2026-10-06)

Human answers, 2026-10-06, relayed by the lead, verbatim: on key custody *"Let the lead, architect
and security engineer decide"*; on `_hmac` *"let the lead and architect decide"*; on retention
*"Give the user a possibility to change it"*. H6 and H7 were not answered. "Security engineer" is read
as the `security-reviewer` role.

**Boundary kept:** the drafts below choose among **documented** features of KMS services, cloud
volume encryption, object-store server-side encryption and existing libraries. They design no
cipher, key hierarchy, key-wrapping scheme or key derivation. Anything that would need that goes to
the human (CLAUDE.md). Provider behaviour is cited from public documentation and **was not re-checked
in this session** (marked *docs*).

### H1. Key custody for encryption at rest: DECIDED, concurred with conditions (7.1)

Covers broker volumes, ClickHouse, PostgreSQL, the claim-check bucket and backups. **C5** adds the Kafka
tiered-storage remote tier, ClickHouse S3 disks and ClickHouse `BACKUP` targets.

| Option | For | Against |
|---|---|---|
| (a) Provider-managed keys (cloud disk default encryption, SSE-S3) | Zero setup; always on | The customer cannot revoke or audit key use separately from the provider. Protects a lost disk only |
| **(b) Customer-managed KMS keys (AWS KMS customer managed key for EBS and SSE-KMS; GCP CMEK; Azure customer-managed keys), through an encrypted StorageClass and bucket default encryption (recommended for `production`)** | The customer controls rotation, revocation and key-use logs (*docs*: KMS audit logging such as CloudTrail). Disabling the key blocks new attach and decrypt. Volumes already attached may stay readable until detached (provider docs) (**C6**, replacing the rev 2 "emergency stop" wording). Tributary writes no crypto code | KMS setup per site; small per-key and per-request cost to the customer |
| (c) Customer-held keys outside the cloud (HSM, Vault; LUKS/dm-crypt on premises) | Required by some on-premises sites; the same properties as (b) | The site operates the key service; unlock-at-boot is the site's own mechanism |

Criteria: who can revoke access to PHI at rest; key-use auditability; no Tributary-written
cryptography; works on premises and in lite; operator effort.

**Recommendation:**
1. **Tributary manages no encryption keys** and implements no at-rest encryption. Encryption at rest
   is the platform's documented feature. The requirement that it be on (`topics/v0` 3.5) is unchanged.
2. **`production` profile: customer-managed keys, (b) or (c), on every PHI-at-rest surface**: broker
   volumes, ClickHouse, PostgreSQL, the claim-check bucket and backups, plus the Kafka tiered-storage
   remote tier, ClickHouse S3 disks and `BACKUP` targets (C5). Provider-managed keys (a) are not
   sufficient for `production`.
3. **`lite` profile:** (a), (b) or (c). Lite already carries the RF=1 warning (ADR-002 D14). The
   docs state that (a) protects a lost disk only. **C7:** lite without a customer-managed key, or on a
   host without full-disk encryption, holds **synthetic data only**. Real PHI requires the production
   custody rules.
4. **Claim-check bucket:** bucket default encryption SSE-KMS with the customer-managed key (S3 Bucket
   Keys allowed, *docs*). Tributary writers send no per-object key parameters, so the bucket default
   governs. The identities that write or read claim-checked objects need the KMS permissions the
   provider documents for SSE-KMS (generate data key on write, decrypt on read), scoped to that key.
   That grant is customer IAM, listed in the chart README. **Unbuilt:** no claim-check writer exists
   yet.
5. **Rotation:** the KMS's own automatic key rotation, enabled. Earlier key versions stay usable for
   decryption, so stored data is not re-encrypted (*docs*). Cadence is the customer's policy;
   recommended setting: automatic rotation on at the provider's default period.
6. **Who holds keys:** the customer's KMS administrators. No Tributary process holds key material.
   Volume encryption is transparent to them. Only claim-check access needs KMS permissions (item 4).
7. **What Tributary can check:** not volume encryption, reliably, from inside a pod. The controls
   are a chart README checklist and the production default of `TB_ARCHIVE_CONTENT=off` (A4). An
   operator turns content on after meeting item 2.
8. **The `_hmac` key:** not covered; follows H3.

**Give up:** production needs a KMS or HSM before content is turned on. Tributary cannot prove from
inside that encryption is on. The docs carry per-provider variance.

### H2. Application-level encryption of archived content: DECIDED (a), concurred with conditions (7.1)

- **(a) None beyond volume encryption with customer-managed keys (H1) in v0: chosen.** (b) Envelope
  encryption of the payload column with per-channel or per-period data keys needs a key hierarchy,
  data-key caching, rotation and re-wrapping. That is key-management design, so it is a human
  escalation. It also rules out server-side content search. (c) Extra encryption of claim-check
  objects is unnecessary: SSE-KMS with the customer's key (H1 item 4) is already the store's
  documented envelope encryption.
- **What (a) does not cover:** a compromised ClickHouse credential or admin reads content in clear.
  Mitigations: A5 (API holds SELECT only, archivers INSERT only), content off by default in production
  (A4), ADR-009 D14 (only `operator`/`admin` see content), audit of every view (R7).
- **Revisit trigger:** H6 choosing crypto-shredding, or a customer requirement for column encryption.
  Either one goes to the human as a design question.

### H3. `_hmac` mechanism: DEFERRED (lead and architect, 2026-10-06, under the human's delegation)

- **Decision: defer.** Reasons: (1) no wave 3 exit criterion needs per-patient search. Criterion 1
  reads by message id. (2) The key's custody follows H1, which has no concurrence yet. (3) A keyed hash
  over a bounded identifier space (MRNs) is only as strong as the key's secrecy, so building it before
  custody is settled creates a reversible PHI index. (4) Deferral loses no data: producers SHOULD NOT
  emit `*_hmac`, and the archiver drops and counts it (`events/v0` 4.1 rule 3).
- **Position that removes the de-identification question from the design:** any `*_hmac` value is
  treated as **PHI** whatever its construction (stored, access-controlled and audited like content).
  The system never relies on a keyed hash being de-identified, so no agent needs a compliance reading.
- **Revisit trigger:** H1 concurrence, **and** before W4-3 or any per-patient search task is
  scheduled. When revisited: a documented construction from an existing library only. RFC 2104
  HMAC-SHA-256 through Python's stdlib `hmac` is the candidate that fits the pinned 64-hex shape. The
  key is held per H1/H5. Key scope and rotation go to `security-reviewer`, and anything beyond
  documented features goes to the human.
- **Give up:** no per-patient search in wave 3. Operators find a patient's messages through their own
  systems, then by message id.

### H4. Retention values and the audit system of record: DECIDED (section 4)

Operator-configurable, defaults events 90 d, content 30 d, audit 2190 d (R2). Applied by the migrate
step with a decrease guard (R6). Audit system of record is ClickHouse `audit_log` with Kafka as a
buffer (R7). **Exit criterion 4 (audit) is no longer blocked on a human answer.**

### H5. Credential rotation: DECIDED, concurred with conditions (7.1)

- **Secret store:** Tributary reads secrets only from files (`secretRef` under `TB_SECRETS_DIR`,
  `engine-kafka/v0`), unchanged. **Exception (C2):** the API reads its database credential from the
  env var `TB_DATABASE_URL`, until W3-34 moves it to a `secretRef` file. The operator's store fills those files: Kubernetes Secrets,
  optionally synced from Vault or a cloud secret manager by an external-secrets operator. Tributary
  calls no secret-manager API in v0. **Give up:** no dynamic, short-lived database credentials in v0.
- **Cadence:** the customer's policy. Recommended default (judgement, not measured): 90 days for
  Kafka SCRAM, ClickHouse and PostgreSQL passwords; mTLS client certificates with at most 90-day
  validity; immediate rotation on suspected compromise or staff departure. The mechanics are K5, whose
  SCRAM single-credential assumption is **still unverified**.
- **Content archiver credential (K2):** its own Secret, mounted only into the content-archiver
  StatefulSet, never shared with another role. Namespace Secret-read is the residual exposure, an
  operational control, listed in the chart README.

### 7.1 Conditions of concurrence on H1, H2 and H5 (`security-reviewer`, 2026-10-06, lead-relayed)

Verdict: **CONCUR WITH CONDITIONS**. H1, H2 and H5 are decided by the lead, the architect and
`security-reviewer` under the human's 2026-10-06 delegation. Conditions as relayed by the lead:

| # | Condition | Task |
|---|---|---|
| C1 | **TLS in transit.** Production requires TLS with certificate and hostname verification on every PHI hop: Kafka (`topics/v0` 3.4); archivers and the API to ClickHouse; the API to PostgreSQL (`sslmode=verify-full`); HTTPS-only access to the claim-check bucket, with a bucket policy that denies non-TLS requests. ClickHouse and PostgreSQL plaintext ports must not be reachable from anywhere but loopback | W3-34 (API to PostgreSQL), W3-35 (chart: ports, bucket policy in the checklist) |
| C2 | The API reads its DB credential from the env var `TB_DATABASE_URL`. **Recorded as an exception to H5**, closed by moving it to a `secretRef` file | W3-34 |
| C3 | **Revocation.** Brokers set `connections.max.reauth.ms` (KIP-368; suggested at most 3600000). Revocation means removing the principal's ACLs or `KafkaUser` first, then rotating | W3-35 |
| C4 | Secret files are read once at startup, so rotation and revocation take effect only after a pod restart. Do not use `subPath` mounts for Secrets | W3-35 (chart check); documented here |
| C5 | Add the Kafka tiered-storage remote tier, ClickHouse S3 disks and `BACKUP` targets to the surfaces that need a customer-managed key | Applied in H1 items 2 and the scope line |
| C6 | Replaces the H1 option (b) wording: "Disabling the key blocks new attach and decrypt. Volumes already attached may stay readable until detached (provider docs)." | Applied in the H1 table |
| C7 | Lite without a customer-managed key, or on a host without full-disk encryption, holds **synthetic data only**. Real PHI requires the production custody rules | Applied in H1 item 3 and `topics/v0` 3.8 rule 4; W3-36 (Compose and chart READMEs) |
| C8 | Turning on content storage in production also requires the ClickHouse `default` user to be disabled and admin queries to be logged outside ClickHouse. Both go into the chart README checklist | W3-35 |

**H6. Erasure.**
- `TODO(human)`: whether erasure requests are honoured by delete (R4), by crypto-shredding (requires
  H2 (b)), or not at all where medical-record retention overrides erasure (ADR-001 8: per-jurisdiction
  review).

**H7. Role model sign-off.**
- `TODO(human)`: approve section 5, including the K2 accepted exposure and per-channel principals
  (K1).
- **Know before approving K2:** the content archiver's PREFIXED `<env>.` READ also covers every
  channel's `reply` and `retry` topics and any topic created under `<env>.` in future (K2, last
  paragraph). Approve that wider scope, or choose per-topic LITERAL grants maintained by provisioning
  on deploy.

## 8. Consequences, contracts and follow-ups

- **Invariants:** 9 holds (A1, archive consumers only); 6 holds (content is a PHI store by design,
  never a log, label or event; `tb-usr-*` not projected); 3 holds (no data-plane read of the archive).
- **Contract changes when the tasks start** (architect, set 1.8 part 2 or later):
  - `topics/v0`: content-archiver grants and the LITERAL group `tb.<env>.archiver.content` (the
    `consumerGroup` platform alternative gains `(\.content)?`);
  - `observability/v0`: content-archiver metrics;
  - `security.grants` structured list (K4).
  - **Rev 2 (set 1.8 part 2, with W3-1b):** events-archiver READ LITERAL `_tb.<env>.audit` (R7);
    `_tb.<env>.audit` record shape with the retention settings of R2 named; read-only retention `GET`
    in `openapi/v0`; `topics/v0` 2.1 and `topic-defaults.json` `classes.audit` restated as decided
    (done 2026-10-06, README and description only).
- **Follow-ups:**
  1. `security-reviewer`: read this draft (W3-10 verify). **Rev 2: concurred with conditions on H1,
     H2, H5 (2026-10-06, section 7.1).**
  6. **Rev 3 tasks** (`phases/wave-3.md` 2.8): W3-34 `control-plane-engineer` (C1, C2), W3-35
     `devops-engineer` (C1 ports, C3, C4, C8), W3-36 `devops-engineer` (C7 docs).
  2. `archive-engineer` (W3-2b): A2-A6 against R1-R2, with no key handling.
  3. `devops-engineer` (W3-16): K3.
  4. ~~The architect sends H1-H7 to the human through the lead and mirrors them in STATUS.~~ Done
     2026-10-05. Answers recorded 2026-10-06. H6 and H7 remain open with the human.
  5. **Rev 2 tasks** (`phases/wave-3.md` 2.7): W3-30 `archive-engineer` (R2, R6, R7 in the archiver
     and migrate step), W3-31 `devops-engineer` (R3 Helm, Compose, chart README; creates
     `_tb.<env>.audit` in bootstrap), W3-32 `control-plane-engineer` (audit producer and retention
     `GET`).
