# Contracts changelog

Newest first. Each entry names the contract, the version, whether the change is additive or
breaking, and the reason. Breaking changes reference an ADR.

## 2026-09-23 (W1-8) — the walking-skeleton example is made runnable; `ref` is documented as unimplemented

`qa-engineer-2` found that `valid/walking-skeleton-loopback.json` pointed `steps[0].ref` at
`adt_flow.steps.passthrough` inside an OCI bundle, and **nothing in the repo publishes, vendors or
installs `adt_flow`, nor is there any bundle-fetch implementation.** Confirmed empirically against the
Compose stack (`ModuleNotFoundError`). The example was schema-valid and unrunnable, and its file name
claims to be the wave-1 runnable shape, so anyone using it as a reference hit a wall. My defect.

Worse than a broken example: the failure is **late**. Step resolution is lazy, so `channel_spec_applied`
succeeds at deploy and the error arrives with the first real message.

**Decision: the example uses `uses: tributary.core.passthrough`** — an entry point in the
`tributary.steps` group (ADR-001 6.2), resolved from the running image with no bundle-fetch involved.

Rejected alternative: bake a demo `adt_flow` package into the worker image for the lite stack. It would
exercise the `ref` path *fake*, via `PYTHONPATH` rather than bundle-fetch, so it proves nothing about
the real path while looking as though it does — the "a green test whose fault never fired" pattern this
project has already been bitten by twice. It also has to be maintained forever or the example breaks
again. **Give up:** no example demonstrates `ref` end to end until bundle-fetch exists, which is honest
about what wave 1 has.

New README section 3.3.1 states the rest:

- `uses` resolves by entry point and works; `ref` needs bundle-fetch and **does not exist in wave 1**.
  A spec that must run today uses `uses` only.
- **Why `ref` is documented-as-unimplemented rather than schema-rejected, unlike `retry-topic`.** The
  failure modes are not comparable: `retry-topic` would *silently* reorder messages, a patient-safety
  failure that looks like success, while an unresolvable `ref` raises loudly and delivers nothing. And
  `ref` is the primary authoring mechanism for real channels, so rejecting it would block wave 2
  authoring on bundle-fetch. Loud-and-unimplemented gets a doc; silent-and-unmeasured gets a validator
  rule.
- `bundle` stays required for a `uses`-only pipeline: the digest then names the image whose entry points
  provide the steps, which keeps "what code ran" answerable from `tb-channel-version` alone.
- **Every other valid example is illustrative, not runnable**, and now says so. They reference
  `tributary.hl7v2.parse` (W2-1) and deliberately fictional bundle modules. Only the walking-skeleton
  example claims to run, and it is the one that has to resolve.

**Depends on `platform-engineer` registering `tributary.core.passthrough`** in `tributary-runtime`'s
`tributary.steps` entry points. **Done, `754845d`**, verified by loading and *running* the
walking-skeleton example's steps through the shipping loader rather than asserting the name exists.

### Follow-up the same day: `runnable` becomes a manifest field

`platform-engineer` then found the other three valid examples are in the same class — they name
`tributary.hl7v2.parse` / `to_json`, which no package provides — and correctly did **not** treat it as a
defect: illustrating a channel wave 2 will make authorable is a legitimate job for an example. They made
the distinction testable with a namespace allow-list in their corpus test, and asked whether the corpus
should instead contain only runnable examples.

**Decision: keep illustrative examples, and move the classification out of their test into this
manifest.** A corpus restricted to what wave 1 happens to have built would be a picture of current
progress rather than of the document, and ADR-001 6.1's own example is illustrative for the same reason.
But the classification was then living in two places — my README prose and their hard-coded allow-list —
and a duplicated allow-list drifts from the contract that owns the examples.

So every `valid` entry now declares `runnable`, and a `false` entry lists `requires`: what is missing and
the task that delivers it. `contracts/validate.py` enforces that the field is present, and that a
`false` entry names its `requires`, so a new example cannot skip the classification. It deliberately does
**not** check that the names resolve — that needs the installed distribution, and this script runs on a
clean clone with only `jsonschema`. The resolving test stays in `tributary-runtime` and now reads these
fields instead of its own list.

Their mutation control is adopted as a contract rule rather than a test detail, because it is the part
that keeps the exemption honest: a `runnable: true` example whose steps stop resolving fails, **and** a
`runnable: false` example whose `requires` have all become resolvable also fails, forcing promotion. A
stale exemption that outlives the gap it describes is indistinguishable from coverage.

## 2026-09-23 (W1-4 follow-up) — the ordinary reject shape, so records stop being over-quarantined

`platform-engineer` reported that section 2.3's ordinary reject path is unimplemented: every decode
failure becomes a full `RECORD_UNDECODABLE` quarantine, even when identity is perfectly readable and
one non-identity header is malformed. They were right not to guess — a `dlq` record must carry a
schema-valid core header set and the offending header is part of it, so producing it verbatim fails
conformance. The contract owed them a rule for the bad value. Additive; no schema change.

**New envelope section 2.3.1.** The discriminator is identity, and identity is `tb-msg-id`: present,
decodable and a valid UUIDv7 means an ordinary reject that keeps the record's own identity; absent,
undecodable or not a UUIDv7 means quarantine. A third trigger was found while writing it — **a
malformed `tb-claim-check` quarantines regardless of `tb-msg-id`**, because the payload lives elsewhere
and the pointer to it is the broken part, so normalising would produce a perfectly conformant record
whose bytes cannot be found.

**An ordinary reject normalises and does not preserve the bad value**, with a placeholder pinned per
header (`tb-attempt` → `"0"`, `tb-content-type` → `application/octet-stream`, `tb-channel` and
`tb-channel-version` from the subscription and applied spec, `tb-source` the rejecting component,
`tb-received-at` the reject time, `traceparent` a new trace, optional headers dropped).

Why normalise rather than copy, which is also **the asymmetry with quarantine** and the reason the two
shapes exist: the original is still on the source topic at a known offset, so a reject needs the record
to stay findable, not duplicated. Quarantine copies because an unidentifiable record cannot be looked up
by message id at all, and because `dlq` retention (30 d) outlives `raw` (7 d), so a pointer expires
before a late investigation reaches it.

**Normalising is silent, so the event must not be.** A reject record with `tb-attempt: "0"` is
indistinguishable from a first attempt, so the `error` event carries `attrs.bad_header`. Header *names*
are safe to record where values are not: a consumer must ignore unrecognised headers, so only
registered names can trigger a parse reject — the names are ours, the values are the sender's. New
example `events valid/reject-header-parse-error.json`. **Not schema-enforceable** — a normalised reject
is structurally identical to an ordinary `dlq` record, which is what normalising is for — so it is a
code rule needing a unit test per placeholder.

Two smaller items from the same report:

| Item | Decision |
|---|---|
| Header-name fidelity in the quarantine blob | **Documented as a client limitation, in 5.2.1.** `confluent-kafka` hands header names to Python already decoded as `str` with no API for the wire bytes, so a non-UTF-8 name cannot be preserved byte for byte; `surrogateescape` round-trips what the client decoded, **not necessarily what arrived**. Values are exact. So a blob header name is evidence of what the client saw, not proof of what was sent, and must not be treated as authoritative in a forensic review. Marked **assumption, unverified** — nobody has tested a non-UTF-8 name end to end |
| A finer log-only `reason_code` below the contract code | **Allowed, with two limits** (2.3): never in an envelope header or an event, and **nothing may alert or dashboard on it.** A log-only taxonomy that operations depends on has become a contract nobody reviewed. If one becomes load-bearing it comes to the architect and becomes a real `tb-error-code` |

Byte order confirmed: `platform-engineer`'s implementation is big-endian (`struct.pack(">I", ...)`),
matching what 5.2.1 pins. Contract and code agree; no churn.

### Verified, on the second run

`contracts/validate.py` **102 / 0** and `pytest tests/conformance` **106 passed** (lead). Recorded
because the **first** run failed, and that is the useful part: the new valid example carried a
`traceparent` with a 34-character trace id where the pattern requires 32 — an architect typo, not a
schema bug, caught by the run and fixed in one character. The other three examples written the same
day were checked for the same error class and were correct.

Worth keeping straight for the gate: the 23-of-23 record on predicted failure pairs is about reasoning
over schema semantics, and it does not transfer to hand-typed example data. Counting hex characters is
clerical, and only running the validator catches it. A hand-authored valid example is never "verified"
until something has executed it.

## 2026-09-22 (W1-4 implementation) — quarantine blob framing, the original key, and a stage that stops being a guess

Three items `platform-engineer` raised while implementing quarantine (`a96edb1`). All **additive**;
the first two ratify what they built, the third changes it. Two were gaps in section 5.2 as I wrote
it, and the third was another cross-contract inconsistency of mine: `events/v0` already had a
`quarantined` stage and `envelope/v0`'s `tb-stage` enum did not.

| Item | Decision |
|---|---|
| **Blob framing** | **Pinned, not implementation-defined**, adopted as `platform-engineer` specified it: `"TBQ1" \| key \| header_count \| (name, value)* \| payload` with length prefixes and `0xFFFFFFFF` for null, in new section 5.2.1. Implementation-defined was the wrong answer because two components can already write a wrapper — a worker reading `raw` and a loader reading `out.<dest>` — so the archive and browser could not render either reliably. Added on top of their design: **lengths are unsigned 32-bit big-endian** (a contract that omits byte order lets two implementations differ and both believe they conform), nothing in the message flow parses the blob (a component that parsed it to auto-retry would feed attacker-controlled bytes back into the pipeline), and the browser masks the whole blob **including header names** as payload rather than metadata |
| **The original key belongs in the blob** | **Kept**, and now named in 5.2. Their reasoning is better than my table: without the key the artefact is not the record, and a per-key reprocess would need it. It adds no exposure — it is a DLQ payload whose ACLs and retention already assume PHI, and the key already existed on the source topic. Two rules pinned because this is the field section 4 restricts hardest: it MUST NOT become the wrapper's key, and MUST NOT be lifted into a header, `attrs` or a log line |
| **`tb-stage` on a wrapper** | **Always `quarantined`**, not the stage that read it. `envelope/v0`'s stage enum gains `quarantined` (it was already in `events/v0` — my inconsistency). Stamping `queued` or `pending` claims a stage the record never reached, and two components guessing differently makes the browser timeline disagree with itself, which is what `platform-engineer` predicted. Where it was read from is carried precisely by `tb-source` and the event's `topic`/`partition`/`offset` — strictly more information than a stage name |

**New error code `RECORD_UNDECODABLE`, separate from `HEADER_PARSE_ERROR`.** Needed as the schema's
discriminator for the quarantine shape, but justified independently: they are different failures
needing different alerts. `HEADER_PARSE_ERROR` is one malformed header on a record that could still
be identified, which keeps its original identity and its reading stage; `RECORD_UNDECODABLE` means
nothing about the record is trustworthy. A rise in the first points at one producer's field, a rise
in the second at framing, corruption or someone writing to the topic. Reserved-code table in 2.3.

Enforced, not just documented: a conditional keyed on `tb-error-code: RECORD_UNDECODABLE` requires
`tb-stage: quarantined` and `tb-content-type: application/octet-stream`, with the new invalid example
`quarantine-stage-not-quarantined.json`. **`platform-engineer` must change `HEADER_PARSE_ERROR` to
`RECORD_UNDECODABLE` on the quarantine path** — a one-string change they offered to make either way.

## 2026-09-22 (W1-4 questions) — quarantine shape for unidentifiable records; `commit_every` becomes a number

Two questions from `platform-engineer` during runtime v0. Both **additive** to Draft contracts. Both
were holes in my own W1-1 increment, not in the questions.

### `tributary.events/v0` + `tributary.envelope/v0`: a record whose headers cannot be decoded

**The gap was a contradiction between two of my own contracts.** `envelope/v0` 2.3 requires a
malformed header to be rejected to `dlq` with an event emitted. But a `dlq` record requires the whole
core header set — the exact bytes that could not be read — and an event with `kind: message` requires
`message_id`, which cannot be trusted from a broken record. So the mandated behaviour was
unimplementable, and the honest workaround (log it and move on) put unvalidated bytes on the one
surface invariant 6 most restricts. Deferring to Phase 2 was offered and **rejected**: a deferral
would still have required editing 2.3 to stop mandating an event, and the PHI risk is live in code
being written this wave.

| Change | Detail |
|---|---|
| `events/v0`: new `kind: "record"` | Identifies a record by `topic`/`partition`/`offset` — the only identity an unidentifiable record has. Requires all three; pins `stage: quarantined`, `status: error`, `content_type: application/octet-stream`; **forbids `attrs`**, since every value would derive from the broken record's own bytes and `attrs` are searchable columns in ClickHouse and the browser |
| `events/v0`: new stage `quarantined` | |
| `envelope/v0`: new section 5.2 | A quarantined record is a **new record the engine constructs**, not the original moved: freshly minted `tb-msg-id` (never one recovered from untrusted input, which would collide with a real message in every loader's idempotency check), channel and version from the subscription, `tb-received-at` = quarantine time (a deliberate, named deviation from "set once at receive" — the original is unknowable, so end-to-end latency for it is meaningless), `application/octet-stream`, and the **original bytes as the payload** |
| `envelope/v0` 5.2: the logging table | May log topic, partition, offset, the minted id, the error code and byte counts. **Never any byte of the original record, header names included** — a name is attacker-supplied too, and under mis-framing it is payload. No `repr()` of the record or any slice of it |
| Examples | `events valid/record-quarantined`, `invalid/record-without-offset`, `invalid/record-with-attrs` (**a PHI guard** — to be listed in `EVENT_PHI_GUARDS`); `envelope valid/dlq-quarantine-undecodable` |

### `tributary.topics/v0`: `commit_every` is now `50`, not prose

It was the string `"tunable per destination, default well above 1"`. W1-1 then added `commit_every`
to the `must_not_hardcode` list — **so the contract required reading a value that was not
machine-readable, which made conformance impossible.** That is my defect from the same increment.

Now `50`, the S5-measured point, with two things stated that the prose lost: it **is** the replay
window (up to 50 redeliveries after an unclean death; S5 saw 12 at 20), so a destination declaring
`idempotency.technique: none` has no absorber and should lower it or justify it; and 50 is a measured
*point*, not a measured optimum — the surrounding curve was never run. Scope stays deployment-level
and deliberately out of the channel spec: it is an operational knob, not channel semantics, and
there is no evidence yet of per-destination variation.

### `tributary.channel-spec/v0`: the `queue.maxBytes` default is filled in

The W1-1 `TODO(connector-engineer)` is closed. When `source.queue` is absent the receiver default is
**64 MiB (67,108,864)**, now carried as a JSON Schema `default` annotation on the `queue` object and
in README 3.2.1. **Labelled a judgement, not a measurement**, and it stays labelled: no Phase 0 spike
pinned steady-state buffered bytes at a receiver — S2 measured ACK latency and loss, not memory — so
there is no number to cite. The Linux S2 re-run (its follow-up 1) is where to measure it.

Also documented rather than assumed: **`maxMessageBytes` must not exceed `queue.maxBytes`**, enforced
by a receiver startup check because JSON Schema cannot compare two properties. It cannot fire for a
schema-valid spec today — `maxMessageBytes` tops out at 1,000,000 and `queue.maxBytes` starts at
1,048,576 — but that overlap is a coincidence of two independently chosen bounds, not a guarantee,
and it disappears silently if claim-check work raises the inline limit. Raised by
`connector-engineer` as defence-in-depth; written down because the reason it is unreachable is
fragile.

### Verified

`contracts/validate.py` **100 checks, 0 failed** (lead) and `pytest tests/conformance` **105 passed**
(`qa-engineer`, `f5bc92d`), both first-run green. The two new `(json_path, validator)` pairs were
written as architect predictions and **both matched the measured run**, including
`record-with-attrs.json` at `$` / `not`, which I had flagged as the one with no precedent in this
repo — a branch-level `not` inside a `then` rather than a `not` on a property. No manifest needed
correcting. `record-with-attrs.json` is now in `EVENT_PHI_GUARDS` (8 guards).

The `queue.maxBytes` `default` annotation and the 3.2.1 rules change no validation outcome, so no
example was added for them and they do not move the counts.

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

### Verified

Three independent first-run-green checks on 2026-09-22, all after the artifacts were written and
none by the architect, who has no shell:

| Check | By | Result |
|---|---|---|
| `contracts/validate.py` | lead | 94 / 0, then **96 / 0** with the two envelope examples |
| `contracts/validate.py --emit-expectations` | `qa-engineer` | **All 21 predicted `(json_path, validator)` pairs matched the measured ones.** No manifest correction needed |
| `pytest tests/conformance` | `qa-engineer` | **99 passed** at `5eb3949` (76 + 23), then **101** with the envelope pair for requirement 13 |
| CI wiring | `devops-engineer` | `f649665` — a standalone `contracts` job running `validate.py` before `uv sync`, separate from the pytest conformance job |

The 21 pairs were written as predictions and labelled UNMEASURED everywhere they appeared, because
the other three contracts' pairs were measured and these would otherwise have borrowed that
authority. They are now measured; the labels are gone.

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
