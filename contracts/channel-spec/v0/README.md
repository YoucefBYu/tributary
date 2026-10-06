# Contract: channel spec — v0 (draft)

**Contract id:** `tributary.channel-spec/v0`
**Status:** Frozen at the Phase 1 wave 1 gate as part of contract set 1.0. **Set 1.1 (2026-09-24)
adds the wave 2 connector types, per-destination steps and the key-expression semantics — all
additive**: every document valid under 1.0 is still valid. **Set 1.2 (2026-09-28) adds the optional
`source.reply` block for synchronous reply mode (3.2.6), also additive.** **Set 1.3 (2026-09-29)
adds the optional `tls.allowedClientSans` (3.2.7) and writes down which values are schema-valid but
refused at component startup (3.2.8, 3.4.1), also additive.** **Set 1.5 (2026-09-29) adds the
optional `pipeline.context` (3.3.4), which declares the context extensions a channel loads, also
additive.** **Set 1.6 (2026-09-30) pins each `uses` step to one distribution: a registry for the
engine's `tributary.*` namespaces ([`step-namespaces.json`](step-namespaces.json)) and the optional
`pipeline.stepNamespaces` for every other namespace (3.3.5), also additive for documents.**
**Set 1.7 (2026-10-05) writes down receiver-side sync-reply rules found while building the MLLP
receiver (3.2.6: step-body edge cases with two new codes, the per-connection fairness bound, the unique
`--instance` rule) and states how N4 treats a shadowed installation (3.3.5). No schema or example
change.** **Set 1.7.1 (2026-10-05, patch, README only) brings the 3.0 status table up to date: the
`http`, `sftp` and `kafka` receivers and the `clickhouse`, `mllp` and `kafka` loaders are built.**
Breaking changes create `../v1/`.
**Owner:** `architect`. Propose changes by message with the exact diff and impact.
**Source:** ADR-001 sections 6.1 and 6.4, ADR-003 D1/D3/D4/D6, ADR-007 (D1-D9, and D10-D17 for set
1.1), Phase 0 gate section 7.1.
**Schema:** [`channel.schema.json`](channel.schema.json) · **Examples:** [`examples/manifest.json`](examples/manifest.json)

Realises invariant 7 (channels are declarative, steps are unit-testable functions) and carries the
declarative half of invariants 1, 4 and 5.

**Set 1.0 verified 2026-09-22; set 1.1 verified 2026-09-24** (`qa-engineer`): `validate.py` 122 / 0,
all 86 expected-failure pairs matched with 0 mismatches (the 16 set 1.1 predictions included), and
`pytest tests/conformance` 127 passed. See section 8.

---

## 1. Model

One channel is one document, authored as YAML in a bundle and published as JSON on the compacted
`_tb.<env>.channels` topic. JSON Schema validates the parsed document, so the examples are JSON and
the authored file is YAML; nothing else differs.

```yaml
apiVersion: tributary.dev/v1
kind: Channel
metadata:
  name: adt-to-fhir
  labels: {domain: adt, owner: integration-team}
spec:
  ordering: per-key
  partitionKey: "hl7:PID.3.1"
  source:
    type: mllp
    bind: "0.0.0.0"
    port: 6661
    tls: {secretRef: mllp-tls, clientAuth: required, minVersion: "1.3"}
    ack: {mode: after-commit, nackOn: [parse-error]}
    queue: {maxBytes: 67108864}
  pipeline:
    bundle: ghcr.io/example/adt-integration@sha256:<64 hex>
    steps:
      - {name: parse,   uses: tributary.hl7v2.parse, with: {version: "2.5.1"}}
      - {name: filter,  ref: adt_flow.filters.only_admit_transfer_discharge}
      - {name: to-fhir, ref: adt_flow.mapping.to_fhir_bundle}
  destinations:
    - name: fhir-server
      type: http
      with: {url: "https://fhir.example.org/fhir", method: POST, timeoutMs: 8000,
             auth: {type: bearer, secretRef: fhir-token}}
      idempotency: {technique: idempotency-key-header, header: Idempotency-Key}
      retry: {policy: blocking, backoff: exponential, maxAttempts: 10, onExhaust: halt-partition}
```

`apiVersion` is the document's API group and version, Kubernetes style, fixed by ADR-001 6.1. It is
**not** the contract directory version: this is `tributary.channel-spec/v0`. A future incompatible
document shape bumps both together.

**Field names differ from the ADR-001 6.1 illustration** where that illustration was informal:
`retry.maxAttempts` not `max`, `retry.onExhaust` not `on-exhaust`, and `idempotency` is a new
required block. ADR-001 6.1 is labelled illustrative; this contract is normative.

### 1.1 Strict producers, lenient consumers

The CLI and the control plane validate strictly and refuse to deploy a spec that fails. The
runtime, reading `_tb.<env>.channels`, MUST ignore fields it does not recognise rather than refusing
to start: a worker that rejects a spec written by a newer control plane takes the data plane down
over an additive change (invariant 3).

---

## 2. What the schema enforces, and which rule each serves

Everything in this table is a validator rule, not a review convention. Phase 0's two High findings
were both controls that were specified and then quietly not applied; anything expressible in the
schema is expressed there.

| Rule | Serves |
|---|---|
| `spec.ordering` required, no default; `partitionKey` required iff `per-key` and forbidden otherwise | Invariant 5 — ordering is explicit |
| `source.ack.mode` is a one-member enum (`after-commit`) | Invariant 1 — ACK timing is not a tuning knob |
| `destinations[].idempotency` required, with a justification note when `none` | Invariant 4 — every loader declares its technique |
| `destinations[].retry.policy` may not be `retry-topic` | ADR-003 D1/D2 — defined, never measured |
| `retry.onExhaust` required, no default | ADR-003 D3 — a clinical-safety choice per destination |
| No field anywhere carries inline code | Invariant 7 — steps must be unit-testable |
| `pipeline.bundle` must be digest-pinned | Supply chain; browser attribution |
| TLS required on a non-loopback `source.bind`; plaintext HTTP only to loopback | Phase 1 requirement 8 (SR-8, CR-10) |
| `tls.clientAuth` required to be stated | Phase 1 requirement 8 — mTLS is a decision, not a default |
| `queue.maxBytes` required when `queue` is present | Phase 1 requirement 5 (SR-3) — byte budgets |
| Destination URL carries no query string or fragment | Envelope section 4 — PHI in URLs |
| `with` rejects credential-shaped property names | No secrets in a spec |
| `metadata.labels` are short low-cardinality codes | Invariant 6 — labels reach metric labels and the UI |
| `additionalProperties: false` and a bound on every object and array | A spec is tenant-supplied and then trusted completely (invariant 3) |
| **Set 1.1:** a source carries only its own type's fields | A field from another type is an error, never silently ignored |
| **Set 1.1:** an `http` source states `auth`; `none` only on loopback; `mtls` requires `clientAuth: required` | An unauthenticated PHI ingest endpoint is an injection point |
| **Set 1.1:** every client connection is TLS (or SSH) unless the peer is loopback, and no field can disable certificate verification | Phase 1 requirement 8, extended from listeners to clients |
| **Set 1.1:** SFTP host keys pinned, required, no trust-on-first-use | SSH's equivalent of certificate verification |
| **Set 1.1:** no connector field can hold a credential value; every credential is a `secretRef` | No secrets in a spec |
| **Set 1.1:** Kafka connectors cannot name a `_tb.*` topic or a channel-shaped topic | The per-channel ACL model (topics/v0 3.4) |
| **Set 1.1:** each destination type may declare only the idempotency techniques it can honour | Invariant 4 — a declared technique must be true |
| **Set 1.1:** `s3` states `serverSideEncryption` | Encryption at rest for every PHI store (topics/v0 3.5) |
| **Set 1.1:** `fhir` is `interaction: transaction` only | Identifiers stay in the Bundle body, out of the request line |
| **Set 1.2:** `source.reply` only on `mllp` and `http`; every field bounded; `after` accepts `routed` only | Invariant 1 — sync mode changes what the sender waits for, never when the commit happens (3.2.6) |
| **Set 1.2:** `source.reply` with `pipeline.transactional: true` is refused | A reply outside the transaction could answer `AA` for aborted work (3.2.6) |
| **Set 1.3:** `tls.allowedClientSans` only with `clientAuth: required`; entries are typed `dns:`/`uri:` names with no wildcard | One CA's certificates must not open every mTLS channel (3.2.7) |
| **Set 1.5:** `pipeline.context` lists registered capability names only, at least one, no duplicates | Only reviewed, declared plugin code runs beside the steps (3.3.4; `context-extension/v0`) |
| **Set 1.6:** `pipeline.stepNamespaces` cannot name a `tributary.*` namespace; values are PEP 503 normalised distribution names; at least one entry | A spec cannot rebind an engine step to a look-alike package (3.3.5). The rest of 3.3.5 (N1-N4) is a runtime refusal, because a cross-field prefix match is not expressible in JSON Schema |

---

## 3. Fields

### 3.0 Connector types, and which exist

Set 1.1 publishes every wave 2 connector shape **before** the connector is written (ADR-007 D6 as
amended, D10). A spec may therefore select a type whose receiver or loader has not shipped yet. That
is refused **loudly at component startup** ("no loader for type `sftp`"), never accepted and ignored —
the same arrangement as `ref` steps (3.3.1). The owner updates this table, through the architect,
when a connector lands with its tests.

| Kind | Type | Task | Status |
|---|---|---|---|
| source | `mllp` | W1-5 | **Implemented.** **Sync reply (`source.reply`, 3.2.6): Built** (set 1.7, 2026-10-05), `body: ack` and `body: step`. Receiver side `8bfe205`, `33b2207`, `f420634`, `4134ab3`, plus `c051002` (step-body charge released after a cancelled wait; real-Kafka test that a fenced replier exits 1); worker side earlier (reply producer, `ReplyContext`, `REPLY_` codes). The reply consumer reads its own `clients.reply_consumer` profile, no worker settings and no literals (`c5fc09a`, `e563d42`; set 1.7 ruling 5). Not yet "Implemented": the round-trip latency tail is unexplained (STATUS) and receivers have no readiness probe (both wave 3) |
| source | `http` (HTTP and FHIR ingest) | W2-7 | **Built** (set 1.7.1). Spec read from `_tb.<env>.channels`, `--channel-spec` refused at startup. **Sync reply: not built** (wave 3); a spec with `source.reply` is refused at receiver startup (`receivers/http/config.py`, 3.2.6), so it emits no `REPLY_` code |
| source | `sftp` (poller) | W2-7 | **Built** (set 1.7.1). Spec read from `_tb.<env>.channels`. `paramiko`, human-approved (section 10.2) |
| source | `kafka` | W2-7 | **Built** (set 1.7.1). Spec read from `_tb.<env>.channels` |
| destination | `http` | W1-6 | **Implemented** |
| destination | `fhir` | W2-6 | **Built** (2026-09-29, `tributary.loaders.fhir`): HAPI FHIR integration test; `auth` `bearer` and `basic` only (3.4.1). A 5.1 redelivery test exists (`tests/integration/test_fhir_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once `qa-engineer` confirms it green in CI |
| destination | `postgresql` | W2-6 | **Built** (`eca445a`, `asyncpg`): integration test against a real PostgreSQL, startup uniqueness check. A 5.1 redelivery test exists (`tests/integration/test_postgresql_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once confirmed green in CI |
| destination | `clickhouse` | W2-6 | **Built** (`tributary.loaders.clickhouse`, stdlib HTTP interface, human decision 2026-09-25). A 5.1 redelivery test asserting a `FINAL` count exists (`tests/integration/test_clickhouse_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once confirmed green in CI |
| destination | `sftp` | W2-6 | **Built** (2026-09-29, `tributary.loaders.sftp`): `paramiko`, human-approved (section 10.2). A 5.1 redelivery test exists (`tests/integration/test_sftp_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once confirmed green in CI |
| destination | `s3` | W2-6 | **Built** (2026-09-29, `tributary.loaders.s3`). **Licence questions resolved by the human 2026-09-29** (ADR-001 2.3): `botocore` is approved in the runtime for this loader only, including its bundled, unmodified `cacert.pem` (MPL-2.0 data), and the loader still prefers the OS CA file; MinIO (AGPL-3.0) is allowed **only** as the test container for its integration and redelivery tests, never in Compose, Helm or a shipped artefact. A 5.1 redelivery test exists (`tests/integration/test_s3_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once confirmed green in CI |
| destination | `mllp` (client) | W2-6 | **Built** (`tributary.loaders.mllp`). A 5.1 redelivery test exists, in the package rather than `tests/integration/` (`packages/tributary-loaders/tests/mllp/test_integration_mllp_loader.py`, marked `integration`); **Implemented** once confirmed green in CI |
| destination | `kafka` | W2-6 | **Built** (`tributary.loaders.kafka`). A 5.1 redelivery test exists (`tests/integration/test_kafka_loader_duplicate_delivery_uncommitted_redelivery.py`); **Implemented** once confirmed green in CI |

**Receivers, set 1.7.1 (2026-10-05).** `http`, `sftp` and `kafka` were built and moved onto
`_tb.<env>.channels` in `1cd9fe4`, `359f5fd` and `737fcd8`, with a security review, as relayed by the
lead; the architect read the code (`receivers/cli.py` uses `open_spec_source`, `receivers/startup.py`
refuses `--channel-spec`) and did not run it. The lead reports 105-112 integration tests passing in
CI. They become "Implemented" when their integration tests are confirmed by name in that log. Each
receiver also applies a republished spec live (`*/reload.py`); `nackOn: spec-invalid` stays refused
until that live reload defines it (3.2.8, ADR-007 follow-up 11).

A connector is "Implemented" when it has its integration test and, for a destination, its
duplicate-delivery test (invariant 4, wave 2 exit criterion 3) — not when its config parses. The
duplicate test must meet 5.1: a genuine redelivery from an uncommitted offset. **"Built"** means the
connector runs and its manifest `connector:` entry is removed, while that redelivery test is still
pending. Two calls to `deliver()` in one process show that the sink absorbs a repeat, which is
necessary but not the same thing.

### 3.1 `metadata`

| Field | Required | Notes |
|---|---|---|
| `name` | R | `^[a-z][a-z0-9-]{0,63}$`. Becomes the `tb-channel` header and the channel segment of every topic name. |
| `version` | C | Integer, set by the **control plane** at publish time, absent in an authored bundle file. Becomes `tb-channel-version`. See section 6. |
| `labels` | O | ≤ 16 short codes. No PHI: labels reach metric labels and the UI. |
| `description` | O | ≤ 512 characters. |

### 3.2 `spec.source`

Every type requires `type` and `ack`, and may carry `maxMessageBytes`. A source may carry **only the
fields of its own type** — the schema enforces it per type, so an `sftp` source with a `bind` fails
instead of being silently ignored. The `mllp` table below is unchanged from set 1.0; 3.2.2 onwards
covers the set 1.1 types.

| Field | Required | Notes |
|---|---|---|
| `type` | R | `mllp`, `http`, `sftp` or `kafka`. See 3.0 for which are built. |
| `bind` | R | IPv4 literal, `localhost`, `::1` or `::`. **No default:** a receiver must say what it listens on. A hostname other than `localhost` is not accepted; resolving a name at bind time is a way to bind something you did not mean to. |
| `port` | R | 1-65535. |
| `tls` | C | **Required whenever `bind` is not loopback**, wildcard included. `secretRef` and `clientAuth` are both required inside it; `clientAuth: required` is mTLS. **Set 1.3:** optional `allowedClientSans` limits which client certificates are accepted (3.2.7). |
| `ack` | R | `mode` is `after-commit` and nothing else. `nackOn` lists the conditions that NACK instead; which values each type accepts is 3.2.8 (`spec-invalid` is refused by every receiver). A NACK carries a code and `tb-msg-id` only — never the record key, never exception text. |
| `maxMessageBytes` | O | ≤ 1,000,000, the envelope's inline limit. Claim-check for larger payloads is **untested** (gate 7.2 item 18); do not design a channel that depends on it yet. |
| `queue` | O | When present, `maxBytes` is required; `maxMessages` alone does not conform. When absent the receiver default applies: **64 MiB (67,108,864)**. See 3.2.1. |
| `reply` | O | **Set 1.2.** Synchronous reply mode, `mllp` and `http` only. Its presence switches sync mode on. See 3.2.6. |

`ack.timeoutMs` bounds the wait for the `raw` commit (queue admission plus publish); its expiry is a
NACK before any commit is confirmed. `reply.timeoutMs` (3.2.6) is a different clock that starts only
once the commit is confirmed.

#### 3.2.1 The 64 MiB queue default is picked, not sized

64 MiB is roughly 64 messages at the 1 MB inline ceiling, or many thousands of typical
few-hundred-byte HL7 messages. **It is a judgement and not a measurement.** No Phase 0 spike pinned
steady-state buffered bytes at a receiver — S2 measured ACK latency and loss, not memory — so there
is no number to cite, and the label stays until there is. The Linux S2 re-run (its follow-up 1) is
the natural place to measure it. Set by `connector-engineer`, whose module docstring says the same.

**`maxMessageBytes` must not exceed the queue's `maxBytes`.** A single maximum-size message that can
never fit its own queue budget is a configuration error, not a runtime condition, and the receiver
refuses to start on it. JSON Schema cannot express a comparison between two properties, so this is a
code rule with a startup check, like the other rules in section 6.

Today it cannot fire for a schema-valid spec, because `maxMessageBytes` tops out at 1,000,000 and
`queue.maxBytes` starts at 1,048,576 — **but that is a coincidence of two independently chosen
bounds, not a guarantee.** Raise the inline limit for claim-check work, or lower the queue minimum,
and the overlap disappears silently. That is exactly why the check is worth keeping and worth
writing down rather than treating as unreachable code.

#### 3.2.2 What ACK and NACK mean for each source type

`ack.mode: after-commit` is the only value for every type. What it *does* differs by protocol, and
this table is normative — invariant 1 is the column on the left.

| Type | ACK, only after the `raw` record is committed (`acks=all`) | NACK | Kafka unavailable | Input over `maxMessageBytes` |
|---|---|---|---|---|
| `mllp` | HL7 `AA` | `AR` with a code and `tb-msg-id` only, before any commit is confirmed. `AE` only in sync mode, and only after the commit (3.2.6) | `AR` or no ACK | `AR` |
| `http` | `202 Accepted`, body carries `tb-msg-id` only | `400` parse error, `415` content type, `401`/`403` auth — code only, never payload or exception text | `503` with `Retry-After` | `413` before the body is buffered past the limit |
| `sftp` | `afterCommit`: delete, or move to `archiveDirectory` | move to `rejectDirectory`; **never delete** | leave the file; retry next poll | move to `rejectDirectory` |
| `kafka` | commit the **source** offset | none exists — see below | do not commit; pause the source partition | halt that source partition (below) |

**`202` means committed, not processed.** An HTTP sender that needs the pipeline's outcome needs
synchronous reply mode (W2-8, ADR-003 D9 and D10), added in set 1.2 (3.2.6).

**One rule for MLLP senders, in both modes: `AR` means the engine did not confirm a commit, so a
resend cannot lose data. `AE` means committed, so a resend creates a duplicate.** The async receiver
already sends only `AA` (after commit) and `AR` (before it); `AE` appears only in sync mode.

**A Kafka source cannot NACK, and it cannot write to `dlq` either**: the receiver role's ACL is
WRITE on `<env>.<channel>.raw` only (`topics/v0` `security.acls.receiver`), and granting it `dlq` is
an ACL-model change that belongs to ADR-004. So a record the receiver must refuse — over the size
limit, or a per-key channel whose key does not resolve (4.3) — **halts that source partition**: no
offset commit, the partition paused, a metric and a log line carrying topic, partition and offset only.
Nothing is dropped and order is kept; the cost is a stalled partition until an operator acts, the same
trade as `halt-partition`. Recorded as ADR-007 D16.

**Duplicates at ingest.** A crash after commit and before the ACK re-ingests the input with a **new**
`tb-msg-id` — the MLLP sender re-sends, the SFTP file is still there, the source offset was not
committed. Loader idempotency keyed on `tb-msg-id` does not absorb these, because they are different
messages as far as the engine knows. This is inherent in at-least-once ingest from a sender that
retries, and is bounded by what was in flight at the crash. The duplicate-delivery tests for wave 2
exit criterion 3 must not claim to cover it.

#### 3.2.3 `http` — HTTP and FHIR ingest

A listener that accepts `POST` on one fixed path. **It is an ingest endpoint, not a FHIR server**: no
read, search, history or capability statement.

| Field | Required | Notes |
|---|---|---|
| `bind`, `port`, `tls`, `queue`, `maxMessageBytes` | as `mllp` | Same rules: TLS required on a non-loopback bind, byte-bounded queue |
| `path` | R | Fixed path, no templating, no path parameters — a request line is logged by every proxy in front of the listener |
| `accept` | R | Content types admitted; anything else is `415`. `tb-content-type` is the matched type, plus `charset` if the sender gave one; any other media-type parameter from the sender is dropped rather than copied into a header |
| `auth` | R | `bearer` (token named by `secretRef`, compared in constant time), `mtls` (requires `tls.clientAuth: required`), or `none` — **loopback only** |

No query string on the request is read, and none is logged: the receiver's access log records method,
status, byte count and `tb-msg-id`, never the request line.

#### 3.2.4 `sftp` — poller

A client of the sender's SFTP server. One file is one message.

| Field | Required | Notes |
|---|---|---|
| `contentType` | R | SFTP does not say what the bytes are, so the spec must |
| `sftp.host`, `sftp.port` | R, O | DNS names accepted; the host key is verified |
| `sftp.hostKeys` | R | 1-4 OpenSSH `SHA256:` fingerprints. **No trust-on-first-use and no "accept any" option** |
| `sftp.auth` | R | `private-key` or `password`, via `secretRef` (which names the username too) |
| `sftp.directory`, `sftp.filePattern` | R | Absolute path without `.`/`..` segments; a glob, not a path; no recursion |
| `sftp.pollIntervalSeconds`, `sftp.maxFilesPerPoll` | R, O | |
| `sftp.minFileAgeSeconds` | O | Skip files younger than this, so a half-uploaded file is not ingested |
| `sftp.afterCommit` | R | `delete` or `move` (then `archiveDirectory` is required). No default |
| `sftp.rejectDirectory` | R | Where a refused file goes. Required: the alternatives are deleting it (dropping a message) or re-reading it forever |

**Remote file names are PHI.** Senders name files after patients and accession numbers. A file name
never appears in a log line, metric label, trace attribute, header, event or NACK. Log `tb-msg-id`,
the channel, byte counts and outcome codes. Moving a file to `rejectDirectory` keeps its name —
that is on the sender's server, not ours.

#### 3.2.5 `kafka` — external topic

Consumes one topic on a cluster that is **always configured explicitly with its own credentials**,
even when it is the same physical cluster the engine runs on (ADR-007 D11).

| Field | Required | Notes |
|---|---|---|
| `contentType` | R | Becomes `tb-content-type` |
| `kafka.connection.bootstrapServers` | R | 1-16 `host:port` |
| `kafka.connection.security.protocol` | R | `SSL`, `SASL_SSL` (with `saslMechanism: SCRAM-SHA-512` and `secretRef`), or `PLAINTEXT` — **only when every bootstrap server is loopback** |
| `kafka.connection.security.secretRef` | C | The client's identity: `username` and `password` for `SASL_SSL`; `tls.crt` and `tls.key` (mTLS) for `SSL`. **Corrected in set 1.3**: earlier text put the client certificate under `tls.secretRef`, which neither implementation read. Same layout as `engine-kafka/v0` section 3 |
| `kafka.connection.security.tls` | O | Trust bundle only (`ca.crt` under `tls.secretRef`; absent means the platform trust store); verification cannot be disabled |
| `kafka.topic` | R | Not `_tb.*`, and not shaped like a channel topic (`<env>.<channel>.raw` and so on) |
| `kafka.startFrom` | R | `earliest` or `latest` for a group with no committed offset. No default |

The consumer group is `tb.<env>.<channel>.receiver` (`topics/v0` 3.6, role added in set 1.1).
**No header on the inbound record is copied into the envelope** — in particular an inbound `tb-*`
header is never trusted; the receiver mints the envelope fresh. The inbound key is used only when
`partitionKey` is `record:key`. Offsets are committed after the `raw` produce is acknowledged, in
batches of `commit_every` (`topics/v0` 3.3), which is also this source's replay window.

#### 3.2.6 `reply` — synchronous reply mode (set 1.2, W2-8)

Approved 2026-09-28 from `platform-engineer`'s proposal (`docs/agent-team/proposals/W2-8-sync-reply.md`),
with the amendments marked below. Decisions and what they give up: ADR-003 D9 (as amended) and D10.

| Field | Default | Bound | Meaning |
|---|---|---|---|
| `timeoutMs` | 10000 | 1000-60000 | Wait for the reply, counted from the receiver's successful `raw` delivery callback. Expiry answers `AE` / `REPLY_TIMEOUT` |
| `after` | `routed` | `routed` only | Every `out.<dest>` record acknowledged by Kafka, or deliberately dropped. `delivered` is reserved: it needs loader WRITE on `reply` and a rule for which destination answers |
| `body` | `ack` | `ack`, `step` | `ack`: the receiver builds the answer from the reply status. `step`: a pipeline step supplies the response payload through `ctx.reply`. `step` may ship after `ack`; until then it is refused at startup |
| `maxPending` | 10000 | 1-50000 | In-flight requests per receiver process (S3: about 4.6 KB each). Over it: `AR` / `REPLY_BUSY`, nothing published. **Set 1.7:** one connection may hold at most `max(1, maxPending // 4)` of them (receiver rules, fairness bound) |
| `partitions` | 3 | 1-64 | Partition count of `<env>.<ch>.reply`, and the only thing that sizes it (`spec.partitions` does not apply). **At least the number of receiver processes** |

The block's presence switches sync mode on; `reply: {}` means every default. `ack.mode` stays
`after-commit`. Sources: `mllp` now, `http` once W2-7 implements it (refused at receiver startup until
then, as in 3.0). `sftp` and `kafka` reject the field in the schema.

**Refusals.**

1. `pipeline.transactional: true` — **in the schema** (amended from a code-only rule; ADR-007 D7).
2. **Any splitter** in the channel, pipeline or destination steps — refused **at load**, because a
   splitter named by `ref` is known only once the bundle loads. One request would have N outcomes.
3. `partitions` below the deployed receiver process count — a deploy **warning**. At run time a
   process with no reply partition is not ready (below).

**Flow.** The receiver mints `tb-correlation-id`, sets `tb-reply-partition` to a reply partition it
holds and `tb-reply-deadline`, records `{correlation id → tb-msg-id, sender control id, connection
slot, deadline}` in its pending map, and produces to `raw` as today. The sender's MSH-10 lives **only**
in that map. The worker runs the pipeline, waits for every `out` (and any `dlq`) delivery callback,
then produces one record to `reply` on the named partition, keyed by `tb-correlation-id`. **The worker
replies if and only if the `raw` record carries `tb-reply-partition`** — the record decides, not the
spec, so a hot reload between sync and async strands nothing. Exception: a worker whose applied spec
is transactional or contains a splitter produces no reply for such a record and counts
`replies_skipped`; the sender gets `AE` / `REPLY_TIMEOUT`, which is truthful. The receiver resolves an
entry only when the correlation id is in its map **and** `tb-msg-id` matches.

**What the sender gets (MLLP, normative).** `AR` = commit not confirmed; `AE` = committed; `AA` =
committed **and** routed. `MSA-2` is the sender's MSH-10 from the map; `MSA-3` is `<CODE> <tb-msg-id>`,
never payload or exception text.

| Case | Committed | MLLP |
|---|---|---|
| Reply `accepted` (routed; includes a whole-message `Drop`) | yes | `AA`, or the step body |
| Reply `rejected` (a `Reject` code) | yes | `AE` + code |
| Reply `failed` (e.g. `TRANSFORM_ERROR`, `REPLY_BODY_MISSING`) | yes | `AE` + code |
| Reply `accepted` under `body: step`, but the body contains `0x0B` or `0x1C` (set 1.7) | yes | `AE` `REPLY_BODY_INVALID`. The body is never written |
| Reply `accepted` under `body: step`, but `queue.maxBytes` cannot hold the body (set 1.7) | yes | `AE` `REPLY_BODY_OVER_BUDGET` |
| Reply with `tb-reply-status` absent or unrecognised (fail-safe, amendment) | yes | `AE`, with the reply's `tb-error-code` if it has one. Never `AA` |
| No reply by the deadline | yes | `AE` `REPLY_TIMEOUT` |
| Reply partition revoked with the request in flight | yes | `AE` `REPLY_ROUTE_LOST`, at once — **but only after the `raw` callback has succeeded** (amendment) |
| Kafka down, publish failed or timed out (existing path) | not confirmed | `AR` |
| Pending map full, or this connection at its fairness bound / process holds no reply partition | no, not published | `AR` `REPLY_BUSY` / `REPLY_UNAVAILABLE` |

Rules behind the table:

- **`AA` needs both** the receiver's own successful `raw` callback and an `accepted` reply; a reply can
  in principle arrive first. **`AE` also never precedes the successful callback** (amendment): an entry
  whose partition is revoked, or whose sender-side state changes, before the callback waits for it,
  then answers `AE` on success or `AR` on failure. Answering `AE` ("committed") for a produce that then
  fails would be false.
- **A pipeline `Reject` answers `AE`, not HL7's `AR`**, because it was committed and a resend
  duplicates it. HL7 original mode also reads `AE` as a content error, so this is the conventional code
  as well. Only original-mode ACKs are in scope; enhanced mode is the async path.
- **A whole-message `Drop` answers `AA`** (ruling on the proposal's open point). The message was
  committed and filtered on purpose; `AR` would claim no commit, and `AE` would report an error that did
  not happen. Some channels may want a filtered message refused; that is a candidate `onDrop` option,
  not in set 1.2 (section 9). **Unverified:** whether migrating Mirth channels expect a different code
  for filtered messages.
- **Status and code for a partial failure:** `accepted` only if every destination was produced or
  deliberately dropped. Otherwise the status and code are those of the **first failure in spec order**
  — pipeline steps, then destinations in list order — not the first in time, so the answer is
  deterministic (amendment).
- **The integrator rule, stated with the answers:** `AR` — resend, nothing is lost. `AE` — do not
  resend blindly: the engine has the message, and **a resend is a new message with a new `tb-msg-id`
  that loader idempotency does not absorb** (3.2.2 "Duplicates at ingest"; ADR-003 D9 as amended). One
  exception to `AR`: after a publish **timeout** librdkafka may still complete the write, so a resend
  after that `AR` can duplicate, though it cannot lose data. **`AA` means routed, not delivered.**
- The control plane warns on every sync channel that resends after `AE` are duplicates unless a
  destination deduplicates on a business identifier. It cannot tell that from the spec, so the warning
  is unconditional (amended from "when every destination relies only on `tb-msg-id`": the key is
  always `tb-msg-id`, 5).

**HTTP mapping: not fixed in set 1.2** (amendment). It is decided when W2-7 implements sync mode, under
one binding constraint: **not-committed outcomes are exactly `503` with `Retry-After`, and no committed
outcome uses a status that standard clients or proxies retry automatically** (`5xx`, `408`, `429`). The
proposal's `500` for a pipeline failure breaks that constraint: a generic HTTP client resends on `500`,
and here that resend is a duplicate.

**Worker rules** (ADR-003 D9, fail-fast plus DLQ, batched commit):

- The reply is produced per record; offsets still commit every `commit_every` records. After a crash
  up to `commit_every` records replay: their `out` duplicates keep their `tb-msg-id`, and their late
  replies are dropped by the receiver as unknown ids.
- Producing a reply is never retried and never pauses a partition. A failed reply produce emits a
  `replied` event with `status: error` and `PRODUCE_FAILED`, and the offset still commits.
- A `Reject` or an exhausted step goes to `dlq` first; the reply follows the `dlq` callback.
- **Expired replies are skipped:** past `tb-reply-deadline` plus 5 s of clock-skew allowance, no reply
  is produced (`replies_expired`). **The message is still processed.** Sync mode never cancels work.
  Set 1.4: the `replied` event has `status: dropped` **and `error_code: REPLY_EXPIRED`** (envelope
  README 2.3.2). The sender's answer is unchanged: `AE` / `REPLY_TIMEOUT` from its receiver.
- **A malformed or out-of-range reply-routing header** (`tb-reply-partition`, `tb-reply-deadline`)
  disables the reply, not the message: the message is processed normally, no reply is produced, and a
  `replied` event carries `REPLY_ROUTE_INVALID` (amendment: renamed from `REPLY_PARTITION_INVALID` to
  cover both headers; envelope 2.3.3).
- `REPLY_BODY_MISSING` (`body: step`, no `ctx.reply` call, message not dropped) fails the **reply**
  only. The message was routed, so there is no `dlq` record.
- The reply producer is the worker's existing idempotent `acks=all` producer. A weaker `acks` needs a
  measurement and a ruling first.

**Receiver rules** (`topics/v0` 3.3.1 has the consumer settings):

- Every receiver process of the channel joins `tb.<env>.<ch>.replier` with its **own**
  `group.instance.id`. A duplicate id fences a live process out of the group (amendment: the id is per
  process, not per pod or per receiver instance).
- A process **advertises partition p only after its reply consumer holds p and has an established
  fetch position on it** (amendment). With `auto.offset.reset=latest` and no committed offsets, a reply
  written between assignment and the offset reset would otherwise be skipped silently. Safe (it ends
  as `AE`), but a race the tests must close.
- A process with no assigned partition, or whose reply consumer is not connected, answers every sync
  request `AR` / `REPLY_UNAVAILABLE` **before publishing** and fails its readiness probe.
- On revoke of p, entries advertised on p answer `AE` / `REPLY_ROUTE_LOST` at once, subject to the
  callback rule above.
- The reply consumer runs on its own thread, only resolves futures, and never writes to a socket.
  MLLP answers each connection in request order, so a slow reply delays later answers on **that
  connection only**, bounded by `timeoutMs`.
- A reply record it cannot parse (any header malformed) is **dropped** with a metric and a log line
  carrying topic, partition and offset only. The receiver has no `dlq` WRITE, and a reply is not a
  message. The pending entry then times out.
- **Memory.** After the `raw` commit the request payload is released and only the fields needed to
  answer stay in the map, so the W1-5 byte-budget reservation ends at commit for a sync request. That
  is safe only because `maxPending` bounds the entry count and entries do not hold payload. **Condition
  on `body: step`:** a resolved step body waiting behind a slower request on the same connection holds
  up to 1 MB, so its implementation must charge those bodies to `queue.maxBytes`, with a test. Without
  that, `maxPending` × 1 MB is the bound.

**Receiver rules added in set 1.7** (ruled 2026-10-05 on the built MLLP receiver; ADR-003 D10
amendment):

- **A step body the receiver cannot write is a failed reply, answered `AE` with its own code.** The
  message was committed and routed, so `AR` would be false and `AA` would hide that the sender did not
  get the application answer it waits for (a query response, for example). The reply is replaced, in
  the receiver, by a `failed` one before it reaches the answer rules, so every rule above still holds:
  - the body contains an MLLP framing byte (`0x0B` start or `0x1C` end): **`REPLY_BODY_INVALID`**.
    Writing it would desynchronise the sender's framing for every later answer on the connection. MLLP
    only; the check is the receiver's, because only the receiver knows the framing;
  - `ByteBudget` cannot charge the body to `queue.maxBytes` now: **`REPLY_BODY_OVER_BUDGET`**. The
    receiver does not wait for room: the body is already in memory, and waiting would hold it outside
    the budget, which is the unbounded case the `body: step` condition above exists to prevent.
  **Rejected:** reusing `REPLY_BUSY` and `REPLY_BODY_MISSING`, as first built. `REPLY_BUSY` means
  "not published, resend is safe" (`AR`); under `AE` the same code would tell an integrator or an alert
  rule the opposite. `REPLY_BODY_MISSING` is the worker's "no step called `ctx.reply`", a channel-logic
  fault; a framing byte is a content fault in a step that did answer. One code, one meaning. Both new
  codes are sender-facing (`MSA-3`) and metrics only, never a header or an event, like `REPLY_TIMEOUT`.
  The worker's `replied` event still says `accepted`: what the worker produced, not what the sender got
  (see "What the browser shows").
- **Per-connection fairness bound.** One connection may hold at most `max(1, maxPending // 4)` pending
  entries. Over it, the request is answered `AR` / `REPLY_BUSY` before anything is published, exactly
  as at `maxPending`. It is a bound **inside** the per-process cap, not a new setting: it stops one
  pipelining peer from taking the whole pool and pushing every other connection into `REPLY_BUSY`. The
  receiver counts the two refusals separately in its metrics, so an operator can tell a full pool from
  one greedy peer. **Give up:** a channel with a single sending connection can use only a quarter of
  `maxPending`. Size `maxPending` at four times the deepest pipelining one sender uses. An original-mode
  MLLP sender waits for each answer, so its depth is 1 and the bound does not bind. **Not a spec
  field** yet: no sender has needed another ratio, and adding an optional field later is additive.
- **One `group.instance.id` per process, from an explicit `--instance`.** The reply consumer's
  `group.instance.id` is `<instance>-p<pid>`. With `source.reply` present the receiver **refuses to
  start without an explicit `--instance`**: a built-in default is identical in every replica (and the
  pid is 1 in every container), so replicas fence one another. **Deployment rule:** every receiver
  replica of a channel in one environment gets a unique `--instance`. Helm templates it from the pod
  name (downward API `metadata.name`, or a StatefulSet ordinal); Compose sets it per service. A process
  that is fenced anyway (librdkafka `_FATAL`) is not ready and exits non-zero, so a supervisor restarts
  it (`c051002` tests the exit on a real broker). `topics/v0` 3.3.1.

**`ctx.reply` (SDK, approved with amendments).** `ctx.reply(body: bytes, *, content_type: str) -> None`.

**Where it lives: on `ReplyContext(Context)`, not on `Context`** (ruled 2026-09-29, set 1.3.1, on
`platform-engineer`'s implementation in `tributary.sdk.context`). A step that answers annotates
`ctx: ReplyContext`. The runtime's context and `FakeContext` implement it.

- **Why.** `Context` is a structural `Protocol` and a public SDK interface. Adding a member to it
  breaks every implementer written against set 1.1: `tributary-fhir`'s `_Attached` failed `mypy
  --strict`, and a third-party wrapper would too. Under the contract rules that is a breaking change,
  needing an ADR, to gain one method. A sub-protocol is additive, and it is the pattern `FhirContext`
  already uses. **Rule from here on: `Context` is frozen for set 1.x; a new `ctx` capability arrives
  as an additive sub-protocol.**
- **Rejected:** `reply` on `Context` with a one-line forward in `_Attached`. That fixes the one
  in-tree wrapper and leaves every out-of-tree one broken.
- **Give up:** a step typed `ctx: Context` cannot call `reply`, and a wrapper does not gain it by
  itself. That is the real cost. Once the runtime wraps each message's context with
  `tributary.fhir.attach` (ADR-001 6.2; the `attach` docstring says this is not wired yet, and the
  architect has not checked), a FHIR pipeline on a `body: step` channel receives an `_Attached` with no
  `reply` member and cannot answer: the step raises `AttributeError`.
- **Condition, `healthcare-standards-engineer`, before `body: step` is lifted:** a context produced by
  `attach()` from a `ReplyContext` must also be a `ReplyContext` under `mypy --strict` and forward
  `reply` to its base. The owner chooses the shape (for example a `FhirReplyContext(FhirContext,
  ReplyContext)` protocol with an `attach` overload). Include a test showing that a body set through
  the attached context is recorded by `FakeContext`. `body: ack` is unaffected, and `body: step` is
  refused at startup until it is built, so nothing is broken today.

- At most once per message; a second call raises. `FakeContext` records the call (invariant 7).
- **Pipeline steps only.** A destination step gets its own copy per destination, so N destinations
  could each answer; in a destination step the call raises (amendment).
- `content_type` must be a registered envelope type (`envelope/v0` 3.1); for an `mllp` source it must
  be `application/hl7-v2+er7`. `body` is at most 1,000,000 bytes (no claim-check on `reply`). A
  violation raises a typed error naming neither the body nor any part of it.
- **A body set during a step attempt that then raised is discarded** before the retry (amendment), so
  "at most once" holds per successful pipeline run.
- Calling it is always legal. The body is used only when the record carries `tb-reply-partition` and the
  applied spec says `body: step`; otherwise it is discarded and counted (`reply_body_discarded`). A step
  written for sync mode then keeps working across a hot reload to async.
- A whole-message `Drop` under `body: step` answers as `accepted` with the receiver-built `AA`, not
  `REPLY_BODY_MISSING`.
- The receiver does not parse a step body. `MSA-2` inside it is the step's responsibility, and the
  step sees the request, so it is unit-testable.
- The body is payload: PHI-bearing, never logged, and never printed by `FakeContext` in test output.

**PHI (invariant 6).** `tb-reply-partition`, `tb-reply-deadline` and `tb-reply-status` carry an
integer, a timestamp and an enum. The reply key is the engine-minted correlation id. MSH-10 and the
record key appear in no header, log line, metric label or span attribute; receiver logs carry
`tb-msg-id` and the correlation id only, and metric labels carry the channel and an outcome code only.
Targeted routing means a process reads only its own partitions by design. That is **not** an access
control: every receiver process of a channel shares one principal with READ on the whole topic.

**What the browser shows.** The worker's `replied` event records what the worker produced, not what
the sender received. A sender-side `AE` after a timeout is a receiver metric and a log line keyed by
`tb-msg-id`, because the receiver has no `_tb.events` ACL (section 9).

#### 3.2.7 `tls.allowedClientSans` — which client certificates an mTLS listener accepts (set 1.3)

Approved 2026-09-29 from `connector-engineer`'s W2-7 proposal (`tls.allowedSubjects`), **with
amendments**: SAN entries only, typed, no wildcard, and on `mllp` as well as `http` (ADR-007 D18).

**The gap.** `clientAuth: required` proves that the client's certificate chains to the trust bundle in
`tls.secretRef`. It does not say *which* client. Without a list, every certificate that CA ever
issued can post to every channel trusting it, which with an enterprise CA means every host in the
enterprise.

| Rule | |
|---|---|
| Where | `source.tls.allowedClientSans`, `mllp` and `http`. Only with `clientAuth: required` (schema) |
| Entries | 1-32, each `dns:<lower-case DNS name>` or `uri:<URI>` (for example a SPIFFE ID). No `*`. No email or IP entries in set 1.3 |
| Match | The **leaf** certificate's `subjectAltName` must contain at least one listed entry of the same type. `dns:` compares case-insensitively (the spec value is lower-case by pattern); `uri:` compares byte for byte. A wildcard SAN in the certificate matches nothing, because no entry can contain `*` |
| When | After the TLS handshake, **before any application byte is read** and before anything is queued or published |
| Refusal, `http` | A bare `403` with `Connection: close` and no body, sent without reading the request, then close. If the server cannot answer without reading, it closes without answering. Never `401` (that invites a credential retry) and never a `5xx` |
| Refusal, `mllp` | Close the connection without reading a frame and without an ACK: there is no message to ACK |
| Observability | Code `TLS_PEER_NOT_ALLOWED` (registered, set 1.3) in the log line and a counter labelled by channel only. The log names the SHA-256 fingerprint of the presented leaf certificate, **never** its subject or SANs: they are attacker-chosen strings, and a subject can name a person |
| Absent | Unchanged from set 1.2: any certificate that chains is accepted. The control plane (wave 3) warns on a non-loopback `clientAuth: required` listener without a list |

**Rejected:** subject DN matching. A DN has no single canonical string form (RDN order, multi-valued
RDNs, escaping differ between OpenSSL's one-line form, RFC 4514 and Python's `getpeercert()` tuples),
so an exact-string DN list is a control whose success depends on formatting: it fails closed for one
sender and open for another. CN matching was deprecated for identity by RFC 6125. **Rejected:** making
the list required with `clientAuth: required`: it would invalidate set 1.0-1.2 valid documents, which
is breaking.

**Until a receiver enforces it, that receiver refuses a spec carrying it at startup**, the same rule as
every other schema-valid feature a connector has not built (3.0). `http` is first (W2-7); `mllp`
follows (`connector-engineer`).

#### 3.2.8 `ack.nackOn`: what each source type accepts (set 1.3)

The schema admits three values on every source type. A receiver refuses at startup any value it
cannot produce, because a declared NACK that can never fire is a false promise to the sender's
integrator. This is normative:

| Type | `parse-error` | `queue-full` | `spec-invalid` |
|---|---|---|---|
| `mllp` | accepted | accepted | **refused at startup** |
| `http` | accepted | accepted (`503` fires whether declared or not) | **refused at startup** |
| `sftp` | accepted (move to `rejectDirectory`) | refused: a poller admits nothing until the previous file commits | **refused at startup** |
| `kafka` | accepted (halts the source partition, 3.2.2) | refused: no NACK exists | **refused at startup** |

**`spec-invalid` is reserved** (ruled 2026-09-29, ADR-007 D19). It would mean "NACK because this
receiver's channel spec became invalid after it started". No receiver watches its spec after startup:
receivers do not consume `_tb.<env>.channels` and have no live reload, so nothing can detect the
condition. It stays in the enum, because removing it would make set 1.0-1.2 documents invalid, and it
stays refused until receiver live reload is designed. That design must also say what the value means
(the lenient-consumer rule in 1.1 suggests "keep serving the last good spec", which would not NACK at
all). Unscheduled. The valid example `two-destinations-fanout.json` lists it, and its manifest entry
says so.

### 3.3 `spec.pipeline`

| Field | Required | Notes |
|---|---|---|
| `bundle` | R | OCI reference ending `@sha256:<64 hex>`. A tag fails validation. |
| `steps` | R | 1-32. Each step has `name` plus **exactly one** of `uses` (a registered step) or `ref` (a dotted path in the bundle), plus optional `with`. |
| `stepRetries` | O | In-place retries before the DLQ (ADR-003 D4). Default 3 — a **judgement, unmeasured**: S5 exercised the DLQ path, not the retry count. |
| `transactional` | O | Kafka transactions on the `raw` → `out.<dest>` hop (ADR-003 D6). Opt-in, default false. |
| `context` | O | **Set 1.5.** Registered context capabilities the channel's steps need, e.g. `[fhir]`. Only these extensions load (3.3.4). |
| `stepNamespaces` | O | **Set 1.6.** `{namespace: distribution}` for every non-engine namespace a `uses` name in this channel needs, e.g. `{"acme.lab": "acme-lab-steps"}`. Engine (`tributary.*`) namespaces come from `step-namespaces.json` and cannot appear here (3.3.5). |

`with` takes scalars and flat arrays of scalars. No nested objects: a nested structure is where
configuration turns into a program the runtime has to interpret. Property names that denote a
credential (`password`, `token`, `apiKey`, …) are rejected outright.

**How `with` reaches a step (set 1.1, approved 2026-09-24).** Each key binds, by exact name, to a
**keyword-only parameter** of the step function, e.g. `def parse(message, ctx, *, version: str = "2.5.1")`.
The runtime binds once, when it loads the step. The following are refused with a `ChannelSpecError`
at load, loudly, and the previous spec keeps running:
- an unknown key;
- a key that is not a Python identifier (the schema admits `-`; the binder does not);
- a required keyword-only parameter with no `with` value;
- a value whose type does not match the parameter's annotation, where the binder understands that
  annotation (`str`, `int`, `float`, `bool`, `Literal`, unions, and sequences of those). Other
  annotations bind unchecked.

**Python keywords (W2-5, 2026-09-25).** A `with` key that is a Python keyword binds to the parameter
with one trailing underscore: `in` binds to `in_`, because `def f(*, in: ...)` is not valid Python.
The spec always writes the keyword itself. Writing `in_` in a spec is refused, so each option has
exactly one spelling. Soft keywords such as `type` and `match` are valid parameter names and bind
unchanged.

**A step may not declare `**kwargs` or `*args`.** The SDK refuses the definition when it decorates the
function. A `**kwargs` parameter would accept any `with` key, which silently removes the unknown-key
refusal above.

`run_step` takes the same `with` values, so a step is unit-tested with its config. Rejected
alternative: `ctx.params` as an untyped mapping, where a mistyped key is silently ignored, which is the
failure this contract exists to prevent.

#### 3.3.1 `ref` needs bundle-fetch, which does not exist in wave 1

The two step forms resolve by different mechanisms, and only one of them is implemented:

| Form | Resolves by | Wave 1 |
|---|---|---|
| `uses` | A Python entry point in the `tributary.steps` group, already present in the running image (ADR-001 6.2), **supplied by the one distribution its namespace is bound to (set 1.6, 3.3.5)** | **Works.** |
| `ref` | A dotted path inside the OCI bundle named by `pipeline.bundle`, which something has to fetch and put on the worker's import path | **Not implemented.** No bundle-fetch exists; the runtime calls `importlib.import_module` and assumes the code is already importable. |

So **a spec that must run today uses `uses` only.** A `ref` step raises at first execution — not at
deploy, because step resolution is lazy, so `channel_spec_applied` succeeds and the failure arrives
with the first real message. Found by `qa-engineer-2` against the Compose stack, where
`valid/walking-skeleton-loopback.json` referenced `adt_flow.steps.passthrough` and nothing in the repo
publishes, vendors or installs an `adt_flow` package. That example now uses
`uses: tributary.core.passthrough`.

**Why `ref` is documented-as-unimplemented rather than rejected by the schema, unlike `retry-topic`
(section 4.1).** The two failure modes are not comparable. Selecting `retry-topic` would *silently*
reorder messages, which is a patient-safety failure that looks like success; an unresolvable `ref`
raises loudly and delivers nothing. And `ref` is the primary authoring mechanism for real channels —
ADR-001 6.1's own example uses it for two of three steps — so rejecting it would block wave 2 channel
authoring on bundle-fetch landing first. Loud-and-unimplemented gets a doc; silent-and-unmeasured gets
a validator rule.

**When bundle-fetch exists, `ref` resolves only inside the pinned bundle (approved 2026-09-25,
`platform-engineer`).** The first dotted segment of a `ref` must be a top-level package that the
bundle named by `pipeline.bundle` itself provides. A `ref` into the standard library, into the
engine (`tributary.*`, which `uses` covers), or into any other package on the worker's import path is
refused at load. Without this, `ref` is "import any callable the worker can reach", including engine
internals and another channel's bundle, and the digest pin no longer answers "what code ran". The
rule costs nothing today because `ref` is unimplemented. It is written now so that bundle-fetch is
built to it, not retrofitted. No example changes: they reference `adt_flow`, `lab_flow` and
`fhir_flow`, which are the bundles' own packages by construction.

**`bundle` stays required even for a `uses`-only pipeline.** The digest then names the image whose
entry points provide those steps, which is the worker or runtime image itself. That keeps the property
digest-pinning exists for: "what code ran" is answerable from `tb-channel-version` alone, regardless of
which step form a channel used.

**Which examples are runnable is recorded as data, not prose.** Every `valid` entry in
[`examples/manifest.json`](examples/manifest.json) declares `runnable`, and a `false` entry lists in
`requires` what does not exist yet and which task delivers it. Today only
`walking-skeleton-loopback.json` is `runnable: true`; the other three illustrate channels wave 2 will
make authorable and reference `tributary.hl7v2.*` (W2-1) plus deliberately fictional bundle modules
such as `adt_flow` and `lab_flow`.

Illustrative examples are kept deliberately. A corpus restricted to what wave 1 happens to have built
would be a picture of current progress rather than of the document, and ADR-001 6.1's own example is
illustrative for the same reason.

The field is data because two places need the classification: this contract and the schema-corpus test
in `tributary-runtime`. An allow-list of exempt namespaces hard-coded in that test drifts from the
contract that owns the examples — so the test reads these fields instead. `contracts/validate.py`
enforces that every valid entry declares `runnable`, and that a `false` one names its `requires`, so a
new example cannot skip the classification. Two rules for the resolving test, stated in the manifest:
a `runnable: true` example whose steps stop resolving fails, and a `runnable: false` example whose
`requires` have **all** become resolvable also fails — it must then be promoted, so a stale exemption
cannot quietly mask a real gap.

#### 3.3.2 Built-in steps (W2-5, approved 2026-09-24; shapes pinned 2026-09-25)

Their `with` shapes are public surface, so they live here. Locators use the `partitionKey` grammar
and the shared evaluator (4.3), so there is one locator language. **A locator always carries its
scheme prefix** (`hl7:MSH.3`, `json:/resourceType`); a bare `MSH.3` is refused. Built-ins accept
`hl7:` and `json:` locators only. `header:` and `record:key` are refused, because they read metadata
rather than the message.

**Maps are written as `name=value` arrays.** `with` admits no nested objects (3.3), so a map such as
enrich's `attrs` is a flat array: `attrs: ["feed=adt-main", "site=north"]`. Each entry splits on its
**first** `=`, so a value may contain `=`. Both sides must be non-empty, and a name may appear once per
key. All of this is refused at load.

| Step | `with` | Rules |
|---|---|---|
| `tributary.core.filter` | `field` (locator) plus exactly one of `in`, `notIn` (non-empty arrays of non-empty strings) or `present` (boolean); **`onUnresolved: drop \| keep \| reject`, required, no default** | A filtered message is `Drop(reason="FILTERED")`. `in`/`notIn` compare the located bytes exactly: case-sensitive, with no trimming. **`field` must be exactly one of the filter allow-list below.** The deny-list is checked first, as a second line of defence: **`field` may not point into a patient-identifying segment** (`PID`, `PD1`, `NK1`, `GT1`, `IN1`, `IN2`, `MRG`), and **no token of a JSON `field` may be** `identifier`, `name`, `birthDate`, `telecom` or `address`. The reason: the `in` values are written in the spec, which is published on `_tb.<env>.channels` and shown in the UI, and a spec must not carry PHI. See "unresolved", `present` and repeated segments below |
| `tributary.core.enrich` | `attrs` (static pairs), `usr` (static pairs), `attrFrom` (`name=locator` pairs); at least one | `attrFrom` locators must be **exactly** one of: `hl7:MSH.3`, `hl7:MSH.4`, `hl7:MSH.5`, `hl7:MSH.6`, `hl7:MSH.9.1`, `hl7:MSH.9.2`, `hl7:MSH.9.3`, `hl7:MSH.11`, `hl7:MSH.12`, `hl7:EVN.1`, `json:/resourceType`. `hl7:MSH.9` as a whole field is not on the list. Static values must pass the `attrs` or `tb-usr-*` guard at load. A derived value that fails the guard at run time is skipped and logged by attr name and reason, never by value. Enrichment never fails a message. An attr name may not appear in both `attrs` and `attrFrom`. At most 16 attrs (`events/v0` 3). `usr` is static only, because copying payload into a header launders PHI |
| `tributary.core.validate` | `requireSegments` (segment ids), `version` (one HL7 version id or an array of them, written as strings); at least one | Raises `Reject` with `VALIDATE_NOT_HL7` (wrong content type, a claim-checked payload, or no leading `MSH`), `VALIDATE_SEGMENT_MISSING` or `VALIDATE_VERSION_MISMATCH` (`MSH-12.1` absent or not listed). JSON-schema validation comes later |
| `tributary.core.split` | `by: hl7-batch \| json-array`, required; `pointer` (RFC 6901, `json-array` only, default empty = the document itself) | See the split rules below |

**Filter allow-list (ratified 2026-09-25, with one amendment).** `field` must be exactly one of these
locator texts:

| Scheme | Fields | What they hold |
|---|---|---|
| `hl7:` | `MSH.3`, `MSH.4`, `MSH.5`, `MSH.6`, `MSH.9.1`, `MSH.9.2`, `MSH.9.3`, `MSH.11`, `MSH.12`, `EVN.1` | Feed and message typing, the same set as enrich's `attrFrom` |
| `hl7:` | `PV1.2` (patient class), `PV1.4` (admission type), `ORC.1` (order control), `ORC.5` (order status), `OBR.25` (result status), `OBX.2` (value type), `OBX.11` (observation result status) | Status-style codes with small, published HL7 vocabularies |
| `json:` | `/resourceType`, `/status`, `/intent`, `/type` | Top-level FHIR typing and status codes |

**Why an allow-list and not a longer deny-list.** The decision of 2026-09-24 said the filter "may not
filter on identifier fields", and the segment deny-list could not deliver that. The W2-5 security
review found MEDIUM 3: `PV1.19` (visit number), `ORC.3`/`OBR.3` (order numbers), `OBX.5` (a result
value), any Z-segment, `json:/subject/reference`, `/subject/display` and `/id` all passed. A deny-list
fails open on every field nobody thought of. An allow-list fails closed. **Give up:** the built-in
filter cannot route on anything else, and wider routing needs a channel step. A field is added to
the list through the architect, and only if it holds a small coded vocabulary that cannot hold an
identifier.

**Amendment: a repeated segment is unresolved for the filter.** The shared evaluator reads the
**first** occurrence of a segment (4.3). `ORC`, `OBR` and `OBX` repeat inside one message, so
`field: hl7:OBX.11, in: [F]` would judge a whole multi-result message on its first observation. It
would then drop a message whose later results are final, silently. For the filter only, an `hl7:`
`field` whose segment occurs more than once in the payload is **unresolved**, and `onUnresolved`
decides what happens, which the author wrote down. This is the same rule 4.3 applies to a repeated
header: never "the first one". The key evaluator is unchanged. (Change request to `platform-engineer`.)

**No example spec is affected.** No valid or invalid example, and nothing in `deploy/` or `docs/`,
uses `tributary.core.filter`. The ADT examples filter through a `ref` step.

**Filter: "unresolved" and `present`.** For `in` and `notIn`, a locator result that is not a value is
unresolved. That covers an absent segment, field, repetition or pointer, a payload not in the
locator's format, an empty value, and a value over the evaluator's 256-byte bound.

`present: true` keeps a message whose field has a value, and drops the rest as `FILTERED`.
`present: false` does the opposite. A value over 256 bytes, or a non-scalar JSON value, counts as
present. Absent or empty counts as not present. For `present`, an absent field is therefore an answer,
not an unresolved result. Only a payload the locator cannot read at all is unresolved: wrong format or
content type, or an ambiguous duplicate JSON member.

`onUnresolved` then decides:

- `keep` passes the message on;
- `drop` is `Drop(reason="FILTERED_UNRESOLVED")`, a separate reason from `FILTERED` so the browser
  shows why a message went nowhere;
- `reject` raises the engine code `FILTER_UNRESOLVED`.

**Why every JSON token, not "ends in" (changed 2026-09-25).** The first text said a JSON `field` may
not *end in* a denied name. That misses the most common shapes: `/identifier/0/value` ends in `value`,
and `/name/0/family` ends in `family`. `platform-engineer` implemented the stricter rule, and this
contract now states it. The change is not breaking: the rule arrived in set 1.1, which is not frozen
yet, and schema validity is unchanged. Since the allow-list, the deny-list is the second line of
defence, and it matters again only if the allow-list grows.

**`Reject(code)`**, a new SDK exception: the step fails **non-retryably**. The executor dead-letters
the message immediately with `tb-error-code` = the code (`^[A-Z][A-Z0-9_]{2,63}$`), skipping
`stepRetries`. A step may not raise any code in the engine's registry: `envelope/v0` 2.3.2 and
[`error-codes.json`](../../envelope/v0/error-codes.json), including every code under a reserved
prefix. `Reject` refuses such a code where it is raised, so an alert on an engine code stays
meaningful. Any other exception is still retryable, and dead-letters as `TRANSFORM_ERROR` once
`stepRetries` is exhausted.

**Split rules.** Split is a separate node type, a `@splitter` decorator, not a widened return type
for every step. Fan-out changes identity, commit and ordering, so it should be visible, not something
any step can do by returning a list.
1. **Child ids are deterministic.** A child's `tb-msg-id` is a UUIDv7 whose timestamp is the
   parent's and whose remaining bits come from `SHA-256(parent tb-msg-id ‖ child index)`. Redelivery of
   the parent therefore reproduces the same children, and loader idempotency absorbs them. A random id
   per run would make every replay a new set of messages, breaking invariant 4. This is deterministic
   id derivation from a random parent id, not a keyed construction; nothing secret is involved.
   The exact encoding is pinned in 3.3.3, because a second implementation (the CLI, another language)
   has to agree with it bit for bit.
2. Children carry `tb-parent-id` = the parent's id, **and the parent's key**, and run through the
   remaining steps and destinations in list order. The `raw` offset commits once every child has been
   produced or dead-lettered.
3. **Split is refused at load on a `per-key` channel in set 1.1.** A multi-patient HL7 batch is keyed
   at the receiver on one patient, so its children would inherit the wrong key, and a later message
   for another patient in the batch could overtake them. That is invariant 5 broken silently. Batches
   destined for per-key channels must be split **at ingest**, where each message is keyed on its own.
   A receiver-side batch option is a candidate for a later set.
4. At most 10,000 children, otherwise `Reject("SPLIT_TOO_LARGE")`. An empty result is
   `Drop(reason="EMPTY_SPLIT")`.
5. **What a dead-lettered child carries (2026-09-25).** A `dlq` record's payload is the input at the
   point where the message would re-enter. For a child that fails in a pipeline step after the
   splitter, that is **the child as the splitter produced it**, not the parent batch. For a child that
   fails in a destination step, 3.5 applies: the destination step's input. Either record carries the
   child's own `tb-msg-id` and `tb-parent-id`.
6. **Reprocess refuses any `dlq` record carrying `tb-parent-id`** until W3-3 builds a re-entry point
   for it. The general rule (ADR-003 D5) re-injects into `raw` under a new `tb-msg-id`. For a child,
   that runs the splitter again on one child: a JSON element has no `/entry` array, so it is refused
   as `SPLIT_INPUT_INVALID`, and a single HL7 message splits to itself under a new identity that no
   loader has seen. This extends ADR-003 follow-up 10, which covers `tb-destination` in the same way.
   **It over-refuses on purpose.** `tb-parent-id` is also set by reprocess and replay (`envelope/v0`
   section 3), so a reprocessed message that fails again is refused as well. The header alone cannot
   tell a split child from a reprocessed record, and W3-3 must settle how to tell them apart (see
   section 9). Failing closed is the safe interim choice: nothing can reprocess before W3-3 anyway.

**The built-in splitter, `tributary.core.split`.**

- `by: hl7-batch` produces one child per `MSH` segment group. The batch envelope (`FHS`, `BHS`, `BTS`,
  `FTS`) belongs to no child. Segments are re-joined with CR, the HL7 segment terminator. So a batch
  delimited with LF or CRLF yields children delimited with CR, and empty lines are dropped. Field
  bytes are unchanged. **A child's final segment gets a CR only if the parent terminated that
  segment.** A split therefore never adds a byte its parent did not have. Otherwise a
  1,000,000-byte message with no final CR would become a 1,000,001-byte child that no topic accepts
  (W2-5 security review). A segment before the first `MSH`, or a line that is not a segment, is
  `Reject("SPLIT_INPUT_INVALID")`.
- `by: json-array` produces one child per element of the array at `pointer`. **Each child is the
  element's exact source text**, not a re-serialisation: re-dumping would turn `1.50` into `1.5`, and
  significant figures are clinical data. In set 1.1, `pointer` tokens are limited to
  `[A-Za-z0-9._:-]`, with no `~0`/`~1` escapes. A payload that is not strict JSON (`NaN` and
  `Infinity` are refused), a missing pointer, a duplicated member name on the path, or a pointer that
  does not name an array is `Reject("SPLIT_INPUT_INVALID")`.
- Children keep the parent's `tb-content-type`.

#### 3.3.3 Derived child id: the exact encoding (pinned 2026-09-25)

Proposed by `platform-engineer` with W2-5, and pinned here because the code comment is the only other
place it is written. All bit positions count from the most significant bit.

```
input   = parent.bytes (16 bytes, RFC 9562 network order) ‖ uint32_be(index)     # index is 0-based
D       = the first 10 bytes of SHA-256(input), read as an 80-bit big-endian integer
child   = unix_ts_ms(parent)   48 bits   # the parent's timestamp, unchanged
        ‖ 0b0111                4 bits   # version 7
        ‖ D[0:12]              12 bits   # rand_a = D >> 68
        ‖ 0b10                  2 bits   # RFC 9562 variant
        ‖ D[18:80]             62 bits   # rand_b = D mod 2^62
```

**Bits 12-17 of `D` are discarded.** This matters because the other reading of "set the version
and variant bits UUIDv7-style" is to overwrite them in place, i.e. to place `D` across bits 48-127 and
then force the version and variant. That gives a different `rand_a`, and so different ids. Only the
layout above is conformant.

The parent must be a UUIDv7. A child is one too, so a second splitter derives grandchildren from the
child's id. The index is 0 to 9,999 under rule 4, although the encoding admits up to 2^32 - 1.

Known-answer vectors, for parent `01923a7b-4c5d-7e6f-8a9b-0c1d2e3f4a5b`:

| index | child `tb-msg-id` |
|---|---|
| 0 | `01923a7b-4c5d-7a07-9a78-dd094f20ee20` |
| 1 | `01923a7b-4c5d-7075-b061-620e49e95a83` |
| 9999 | `01923a7b-4c5d-780f-9067-11c550358874` |

**Provenance of the vectors: produced by the SDK implementation**
(`packages/tributary-sdk/tests/test_splitter.py`, `derived_uuid7`). The architect has no shell and did
not recompute them. The test's own cross-check reuses the implementation's formula, so an **independent**
recomputation (a few lines of `hashlib`) is part of verifying this section. If they disagree, the
layout above wins, and both the table and the implementation are defects.

#### 3.3.4 `pipeline.context`: which context extensions a channel loads (set 1.5)

```yaml
pipeline:
  bundle: ghcr.io/example/adt-integration@sha256:<64 hex>
  context: [fhir]            # ctx.fhir and ctx.terminology, from tributary-fhir only
  steps: [...]
```

- **Optional.** Absent means the channel gets **no** context extensions. `[]` is refused (one
  spelling for "none"), and so are duplicates. Order carries no meaning.
- **Items are registered capability names** (`contracts/context-extension/v0/capabilities.json`);
  set 1.5 registers `fhir`. The schema enum and the registry are kept identical by `validate.py`.
- **Scope: every step the worker runs for this channel**, including `destinations[].steps`. The
  exit-criterion-1 example uses `ctx.fhir` in a destination step and declares it here.
- **The worker loads only these** `tributary.context` entry points, each from its registered
  distribution, and refuses the spec otherwise (context-extension README section 2, L1-L2). A
  refused spec keeps the previous version running, as for an unresolvable `uses` step.
- **Why in the spec** (ADR-007 D21): it is how `uses` already works (`steps.py` loads only names the
  spec references; since set 1.6, each from one pinned distribution, 3.3.5). A reviewer sees in the spec diff what code runs beside the steps, and "what code
  ran" stays answerable from `tb-channel-version` alone.
- **Failure mode to know:** a step that uses `ctx.fhir` on a channel that did not declare `fhir`
  raises `AttributeError` on every message and dead-letters it (`STEP_ERROR`), loudly but late. Until
  steps declare their own capability needs (ADR-007 follow-up 14), declaring `context` correctly is
  the spec author's job. The runtime cannot infer it.

#### 3.3.5 Which distribution may supply a `uses` step (set 1.6)

**Source:** `security-reviewer` Medium (re-review, 2026-09-30), pre-existing: `steps.py` loaded a
`uses` name from the **first** installed `tributary.steps` entry point carrying it, with no duplicate
check and no distribution pin. A typosquat, or a transitive dependency that registers
`tributary.core.passthrough`, could win. This is the class set 1.5 closed for context extensions.
Proposed by `platform-engineer` as rule S1, amended here (ADR-007 D23). The `bundle` digest does not
close it: the digest pins the whole image, not which distribution in it answers a name.

```yaml
pipeline:
  bundle: ghcr.io/example/lab-integration@sha256:<64 hex>
  stepNamespaces: {acme.lab: acme-lab-steps}   # only for non-engine namespaces
  steps:
    - {name: parse,     uses: tributary.hl7v2.parse}       # bound by step-namespaces.json
    - {name: normalise, uses: acme.lab.normalise_units}    # bound by stepNamespaces above
```

Normative for `tributary-runtime`. Rules N1-N6.

1. **N1. Namespace.** A `uses` name's namespace is its first two dot-separated segments. A name needs
   at least three segments (`<org>.<package>.<step>`). A two-segment name has no namespace and is
   refused. The schema still admits two segments, because tightening the pattern would be breaking; the
   CLI and control plane refuse it as well.
2. **N2. Engine namespaces are reserved.** Every namespace whose first segment is `tributary` is bound
   only by [`step-namespaces.json`](step-namespaces.json). A `tributary.*` name whose namespace is not
   in that file is refused. A spec cannot declare a `tributary.*` namespace (schema).
3. **N3. Other namespaces are bound by the spec.** `spec.pipeline.stepNamespaces` maps each non-engine
   namespace the channel uses to one distribution, in PEP 503 normalised form. A `uses` name whose
   namespace is in neither place is refused. An entry that no `uses` references loads nothing (the CLI
   should warn).
4. **N4. Resolution.** A `uses` name resolves only if **exactly one** installed `tributary.steps`
   entry point carries the name, and that entry point's distribution name, PEP 503 normalised, equals
   the namespace's bound distribution. An entry point with no distribution metadata never matches.
   Otherwise the spec is refused with a `ChannelSpecError`. **`EntryPoint.load()` is called only on
   that one verified entry point**, and discovery before that reads metadata only, so no other
   registrant is imported. At startup the worker refuses to start. On a hot reload the previous version
   keeps running, as for every other step refusal. "Exactly one" is counted after `importlib.metadata`'s
   own de-duplication by normalised distribution name (CPython 3.12 `entry_points()`; **unverified
   by the architect**, and `platform-engineer` pins it with a test), so one distribution visible twice
   on `sys.path` is not a duplicate. Two different distributions carrying the name always are.
   **Shadowed installations (set 1.7, ADR-007 D24).** When two installations share a distribution
   name, `importlib.metadata` keeps the one earlier on `sys.path`. N4 resolves against that one, which
   is also the one `import` would load, and it must still pass every check above (name, install
   folder, `RECORD` target). Each hidden installation is **not loaded and not refused**: it is logged
   as `step_entry_point_skipped`, `reason: duplicate` (N5) whenever a spec names one of its entry
   points or it is in a reserved `tributary.*` namespace. **Rejected:** refusing the spec whenever two
   installations share a name. It does not stop the threat, because whoever can write to an earlier
   `sys.path` entry can shadow the target module with no metadata at all, or drop a `.pth` file, and
   neither leaves a second distribution to count. It would turn a stale `.dist-info` or a dev
   workspace's editable-plus-wheel install into a refused channel. A duplicate distribution name is
   refused where it is cheap to fix: the image build (ADR-007 follow-up 18).
5. **N5. Log lines** (structured, no payload, no header values, invariant 6). Names are package
   metadata:
   - `step_entry_point_loaded` (info): `step` (the `uses` name), `distribution`, `version` (from the
     distribution's metadata), `channel`, `channel_version`. One per distinct `uses` name per applied
     version.
   - `step_entry_point_skipped` (warning): the same metadata fields plus `reason`. Emitted for each
     installed entry point that carries a name the spec references and is not loaded (`duplicate`,
     `distribution-mismatch`, `unregistered-namespace`), and for each installed entry point in a
     reserved `tributary.*` namespace that fails `step-namespaces.json` (`unregistered-namespace` or
     `distribution-mismatch`) even if no spec names it. That last case is the image-wide signal for a
     look-alike package, and it does not stop the worker, because nothing was imported. There is no
     `not-referenced` line: an unused step in the image is not news.
6. **N6. Test seam.** Entry-point discovery and the namespace registry may be replaced by explicit
   arguments for unit tests only. No CLI flag or environment variable reaches them.

**Why the spec, and not an operator file, binds third-party namespaces.** Same reasons as
`pipeline.context` (3.3.4, ADR-007 D21): the reviewer sees the binding in the spec diff, and "what code
ran" stays answerable from `tb-channel-version` plus the `bundle` digest. A per-site file would vary
per deployment. A core-registry-only rule would force every hospital's in-house package through an
upstream contract change and publish its package names, which ADR-001 2.2 ("Extensibility:
third-party connectors and steps as Python packages via entry points") and 6.2 rule out.

**What this does and does not stop.**

| Threat | Outcome |
|---|---|
| A look-alike registers `tributary.core.passthrough` beside `tributary-runtime` | Duplicate, so a spec naming it is refused (N4); `step_entry_point_skipped`, warning |
| A look-alike registers a `tributary.*` name and the real package is absent | Distribution mismatch, refused (N4) |
| A spec binds `tributary.core` to another distribution | Refused by the schema (N2) |
| A look-alike registers `acme.lab.normalise_units` beside `acme-lab-steps` | Duplicate, refused (N4) |
| A package that **replaces** a bound distribution under the same name | **Not detected at runtime.** The controls are the locked, hash-pinned dependency set, the digest-pinned image, `licence_check.py` and the SBOM, as for context extensions |
| A second installation with the bound distribution's name, **earlier on `sys.path`** (set 1.7) | **Loaded if it passes N4's checks, not refused**; the hidden installation is logged `duplicate` (warning). It needs write access to the image's import path, which also allows shadowing modules with no metadata at all, so a runtime refusal would add no protection. Control: the image-build check refuses any duplicate distribution name (ADR-007 follow-up 18) |
| A spec author binds a namespace to a distribution that is in the image for another reason | Loads only entry points that distribution itself registers under that exact name. The spec is trusted input (`_tb.<env>.channels` is written only by the control plane, `topics/v0` 3.4) |

**Give up.** Every `uses` name now carries a three-segment shape, and a channel using a customer step
restates its binding in every spec that uses it. A second distribution cannot contribute steps to an
engine namespace: a contributed step for `tributary.hl7v2` must ship in `tributary-hl7v2` or take its own
namespace. **Adding an engine namespace** is additive: the owner proposes the namespace and
distribution, the architect approves, and `security-reviewer` reviews the package. `contracts/validate.py`
checks that the `tributary.steps` entry points in `packages/*/pyproject.toml` and the registry agree
both ways, so an unregistered engine namespace fails the contract run.

### 3.4 `spec.destinations`

| Field | Required | Notes |
|---|---|---|
| `name` | R | Becomes the `out.<destination>` topic segment and the `tb-destination` header. Uniqueness within a channel is checked by the control plane — JSON Schema cannot express it over an array of objects. |
| `type` | R | `http`, `fhir`, `postgresql`, `clickhouse`, `sftp`, `s3`, `mllp` or `kafka`. See 3.0 for which are built. |
| `with` | R | The params object for the type, 3.4.1-3.4.8. |
| `steps` | O | Set 1.1. Steps for this destination only — section 3.5. |
| `idempotency` | R | Section 5; the techniques each type may declare are in 5.1. |
| `retry` | R | Section 4. |

`timeoutMs` is required for every type and interacts with the runtime: `max.poll.interval.ms` must be
at least the slowest destination timeout plus backoff (`contracts/topics/v0` 3.3), or a slow sink
looks like a dead consumer and causes a rebalance loop.

**Four rules for every loader**, stated once because the wave 1 High was the first of them:

1. **Classify every outcome explicitly; nothing is success by default.** The wave 1 HTTP loader
   followed a redirect with the library's default handler, read the redirect target's `200` as
   success and committed a message that was never delivered. No loader follows redirects, and an
   outcome the loader does not recognise is a retryable failure, not a success.
2. **Error codes, never error text.** `tb-error-code` and logs carry a code from the family below
   (`^[A-Z][A-Z0-9_]{0,63}$`); a driver's exception message, a server's response body, a FHIR
   `OperationOutcome`, an HL7 `MSA-3`/`ERR` segment and a database error detail all routinely quote
   the payload, and none of them is logged, put in a header or put in an event.
3. **The payload is never modified by the loader** beyond what its type states below. Shaping the
   payload for a sink is a step's job (3.5), where it is testable.
4. **Credentials are resolved from `secretRef` at startup** and never logged, never in an exception.

| Type | Error code family |
|---|---|
| `http`, `fhir` | `HTTP_<status>`; `HTTP_REDIRECT_REFUSED`; `FHIR_BUNDLE_INVALID`, `FHIR_ENTRY_FAILED`, `FHIR_RESPONSE_INVALID` (3.4.2) |
| `postgresql` | `PG_<SQLSTATE>` (a class code, never the detail text) |
| `clickhouse` | `CH_<numeric code>`; `CH_UNKNOWN` (an error response with no ClickHouse code, e.g. a proxy's page; not retryable) |
| `sftp` | `SFTP_<status>` (the numeric SFTP status), `SFTP_NAME_CONFLICT` (the name exists with another size: not ours, not retryable), `SSH_HOST_KEY_MISMATCH`, `SSH_AUTH_FAILED` |
| `s3` | `S3_<ERROR_CODE>` upper-snake-cased; fixed names `S3_KEY_CONFLICT` (the key exists with another size: not retryable), `S3_KEY_VANISHED` (deleted between the `412` and the `HEAD`: retryable), `S3_NO_CREDENTIALS`, `S3_UNKNOWN` (no usable store code). **A store code that would map onto a registered fixed name is emitted as `S3_UNKNOWN`** (set 1.3): a store answering `KeyConflict` must not read as the loader's idempotency conflict |
| `mllp` | `MLLP_AE`, `MLLP_AR`, `MLLP_TIMEOUT`, `MLLP_NO_ACK` |
| `kafka` | `KAFKA_<librdkafka error name>` |
| all | `TIMEOUT`, `CONNECT_FAILED`, `TLS_VERIFY_FAILED`, `PAYLOAD_SHAPE_INVALID` |

#### 3.4.1 `http`

Unchanged from set 1.0: `url`, `method`, `timeoutMs` required; optional `headers`, `auth`,
`circuitBreaker`.

**`auth.type` `oauth2-client-credentials` and `mtls` are reserved: schema-valid, refused at startup
by the `http` and `fhir` loaders** (ruled 2026-09-29, ADR-007 D20). `basic` and `bearer` are the
implemented types. The shape `{type, secretRef}` cannot say where the token endpoint, client id and
scope come from, and defining them is a design of its own, not a field list:

- a second outbound connection per destination (the token endpoint), with the same TLS, redirect,
  timeout and error-code rules as the destination;
- token caching, expiry with clock skew, refresh under concurrency, and a `401` that must not turn
  into a re-send loop carrying PHI;
- the client authentication method. SMART Backend Services, which most EHR FHIR APIs require, uses
  `private_key_jwt`: a JWT signed with the client's private key. That is key management, which
  ADR-004 and a human own. A shared-secret-only design would likely be redone for it.

It is **not needed for wave 2 exit criterion 1**, whose end-to-end test targets a HAPI FHIR server,
so the exit-criterion example now uses `bearer`. **Give up:** until it is designed, EHR FHIR APIs that
require OAuth cannot be `fhir` destinations, and a `bearer` token is long-lived and rotated only by
replacing the secret and restarting the loader (credentials are resolved at startup, rule 4 above).
Unscheduled; a candidate for wave 3, together with ADR-004. `mtls` needs client-certificate plumbing
in the shared HTTP client and is additive when built.

#### 3.4.2 `fhir`

| Field | Required | Notes |
|---|---|---|
| `url` | R | The FHIR base URL. HTTPS unless loopback, no query, no fragment |
| `fhirVersion` | R | `4.0.1`, `4.3.0` or `5.0.0` |
| `interaction` | R | `transaction` only |
| `timeoutMs`, `headers`, `auth`, `circuitBreaker` | | As `http`, including the reserved `auth` types (3.4.1) |

The payload is a FHIR Bundle with `type: transaction`, built by a step. The loader POSTs it to `url`
exactly and checks `Bundle.type` first; any other payload, or an entry that is not idempotent by
construction, is `FHIR_BUNDLE_INVALID`, non-retryable. `3xx` is `HTTP_REDIRECT_REFUSED`,
`408`/`429`/`5xx` are retryable, `409` is retryable (the retry re-runs the conditions and converges),
`412` and any other `4xx` are non-retryable.

**A `2xx` alone is not success** (set 1.3, rule 1 of 3.4). Success is a `2xx` whose body is a
`transaction-response` Bundle with the request's entry count and every entry status `2xx`.

| `2xx`, but | Code | Retryable |
|---|---|---|
| an entry status is `>= 400` | `FHIR_ENTRY_FAILED` | only if a failed entry is `408`, `409`, `429` or `5xx` |
| no parseable `transaction-response` of the right size (a gateway's `200` page, a truncated body, a response over the size cap) | `FHIR_RESPONSE_INVALID` | yes: every entry is idempotent, so a resend converges; bounded by `maxAttempts` |

The server's `OperationOutcome` is never copied into a code, header or log line; it routinely quotes
the payload.

**Why transaction only (ADR-007 D12).** Conditional create and update put a business identifier in the
request — `?identifier=system|MRN` on the URL, or an `If-None-Exist` header — and the request line is
what proxies log. Inside a transaction, the same conditions live in the Bundle body
(`entry.request.ifNoneExist`, a conditional `entry.request.url`), which is payload, not metadata. That
closes the open item set 1.0 left for wave 2 without a no-log exception. **Give up:** a server without
transaction support cannot be a `fhir` destination in set 1.1 (it can still be an `http` destination
with a step-built body), and plain create/update/batch are unavailable.

**Idempotency.** `conditional-update` means **every entry in the Bundle is idempotent on its own**,
so a redelivered Bundle converges instead of creating twice. Two entry forms qualify:

- `PUT` conditional on an identifier that **uniquely identifies the resource being written**, in one
  named system (`Patient?identifier=<mrn-system>|<value>` inside the body). A replace is idempotent.
  This is the usual form for state resources (Patient, Encounter).
- `POST` with `ifNoneExist` on such an identifier. This is for event resources that must not be
  overwritten (Observation, Provenance). Where no business identifier is unique to the resource, it
  uses one derived from `tb-msg-id` plus the entry's position, in a Tributary system.

**Not allowed:** a `POST` with no condition, a condition on an identifier that can match more than one
resource (the server then returns 412, or worse, updates the wrong one), and a condition that lists
several identifier systems. The identifier values sit in the request body, never the request line.
Which system each resource type uses is W2-3's to define (`healthcare-standards-engineer`).
`idempotency-key-header` is allowed for servers that honour one; `none` needs its note.

**A limitation of the state form to know about:** a conditional replace re-applied out of order
overwrites newer data with older data. Replay within a partition is in order, so the newer message is
re-applied after it. A DLQ reprocess of an old message, though, lands after newer ones, and nothing
in the loader can tell. That is an operator concern for reprocess (ADR-003 D5), stated here so nobody
mistakes idempotent for order-safe.

#### 3.4.3 `postgresql` and `clickhouse`

Both use the same params object:

| Field | Required | Notes |
|---|---|---|
| `host`, `port` | R, O | |
| `database` | R | Lowercase identifier |
| `table` | R | `name` or `schema.name`, lowercase; the loader still quotes it |
| `idColumn` | O | Default `tb_msg_id`. The loader fills it with `tb-msg-id` |
| `auth` | R | `password` or `client-certificate`. **`auth.secretRef` holds everything about how this client authenticates**: the user always, plus the password or the client certificate and key |
| `tls.mode`, `tls.secretRef` | R, O | `verify-full` (certificate and hostname) or `disable` — **loopback only**. There is deliberately no "encrypt without verifying" mode. `tls.secretRef` holds the CA trust bundle only |
| `timeoutMs`, `circuitBreaker` | | |

**The payload is a row**: a JSON object (`application/json`) whose keys are column names matching
`^[a-z_][a-z0-9_]{0,62}$` and whose values are scalars or null. A step builds it. There is no
column-mapping language in the spec, because that is where configuration becomes a program the loader
has to interpret (ADR-007 D13). A payload that is not such an object, or that sets `idColumn` itself,
is `PAYLOAD_SHAPE_INVALID`, non-retryable. Values are always bound parameters, never interpolated.

**Idempotency is `sink-native` only, and the loader proves it at startup rather than trusting the
declaration:**

- `postgresql`: `INSERT … ON CONFLICT (<idColumn>) DO NOTHING`. At startup the loader checks that a
  unique index or primary key covers exactly `idColumn`, and **refuses to start** otherwise — without
  it, `ON CONFLICT` has nothing to conflict on and duplicates are silent. **Check the catalog, not
  `information_schema`**: a bare `CREATE UNIQUE INDEX` satisfies `ON CONFLICT` but has no row in
  `information_schema.table_constraints` (found by `connector-engineer` against a real server). Only a
  **non-partial, non-expression** unique index on exactly `idColumn` qualifies: without the predicate,
  `ON CONFLICT (<idColumn>)` cannot infer a partial index, and an expression index is not on the column.
- `clickhouse`: ClickHouse has no unique constraint. The table must be a `ReplacingMergeTree`-family
  engine whose sorting key starts with `idColumn`; the loader checks this at startup and refuses to
  start otherwise. **Deduplication is eventual** (at merge time): readers that need exact counts query
  with `FINAL`, and the duplicate-delivery test must assert on a `FINAL` count, not a plain count,
  or it proves nothing. `insert_deduplication_token` set to `tb-msg-id` is recommended as well, but it
  only covers a recent window and is not the technique relied on.

**Driver licence for PostgreSQL is open — section 10.2.**

#### 3.4.4 `sftp`

| Field | Required | Notes |
|---|---|---|
| `host`, `port`, `hostKeys`, `auth`, `directory` | R (port O) | As the `sftp` source |
| `fileNamePrefix`, `fileExtension` | O | |
| `timeoutMs`, `circuitBreaker` | | |

The remote file name is `<fileNamePrefix><tb-msg-id><fileExtension>` — **nothing from the payload**,
because a directory listing is a surface too. The loader writes `<name>.part`, then renames it to
`<name>`. If `<name>` already exists the delivery is a duplicate and counts as success
(`deterministic-name`). A leftover `.part` from a crash is overwritten on retry.

#### 3.4.5 `s3`

| Field | Required | Notes |
|---|---|---|
| `bucket`, `region` | R | |
| `prefix` | O | Lowercase segments, no `.`/`..` |
| `endpoint` | O | For S3-compatible stores such as MinIO. Scheme and host only; `http://` to loopback only |
| `serverSideEncryption` | R | `AES256` or `aws:kms`; `kmsKeyId` only with `aws:kms`. **Which key, and who holds it, is ADR-004 plus a human** — this field only makes "on" mandatory |
| `auth` | R | `access-key` (via `secretRef`) or `ambient` (workload identity or instance profile) |
| `expectedBucketOwner` | O | **Set 1.4.** 12-digit account id (`^[0-9]{12}$`). Sent as `ExpectedBucketOwner` on every call, so a bucket re-created under another account refuses the write instead of receiving PHI. Recommended on AWS. Optional because not every S3-compatible store implements the check |
| `fileExtension`, `timeoutMs`, `circuitBreaker` | O | |

The object key is `<prefix>/<yyyy>/<mm>/<dd>/<tb-msg-id><fileExtension>`, the date from
`tb-received-at` in UTC so a retry computes the same key — the claim-check key rule from
`envelope/v0` section 5. A PUT to the same key is the same object (`deterministic-name`).

#### 3.4.6 `mllp` (client)

| Field | Required | Notes |
|---|---|---|
| `host`, `port` | R | |
| `tls` | C | **Required unless `host` is loopback.** Client TLS; verification cannot be disabled |
| `timeoutMs` | R | Connect plus ACK wait. No ACK in time is `MLLP_TIMEOUT`, retryable, never success |
| `onNegativeAck` | R | `retry` or `fail`, for `AE`/`AR`/`CE`/`CR`. No default (below) |
| `circuitBreaker` | O | |

`AA`/`CA` is success. The payload is sent as-is: the loader does not rewrite `MSH`. `onNegativeAck`
has no default for the same reason `onExhaust` has none: retrying a message the receiver rejected as
malformed blocks that patient's queue, and failing one rejected for a transient reason abandons it.
`fail` hands the message straight to `retry.onExhaust`.

**Idempotency** is `sink-native` (the receiving system deduplicates on a control id a step set from
`tb-msg-id` — note that `MSH-10` is 20 characters in HL7 v2.5 and a UUID is 36, so the mapping must
say how it fits) or `none` with its note. HL7 v2 over MLLP has no general idempotency mechanism.

#### 3.4.7 `kafka`

| Field | Required | Notes |
|---|---|---|
| `connection` | R | As the `kafka` source: explicit, own credentials, TLS unless loopback |
| `topic` | R | Not `_tb.*`, not channel-shaped |
| `key` | R | `preserve` or `none`. No default |
| `timeoutMs` | R | |

**Cross-cluster, so at-least-once, not exactly-once.** The loader reads `out.<dest>` from the engine's
cluster and produces to another; a Kafka transaction cannot span two clusters, so invariant 4's
exactly-once carve-out does not apply. Every produced record carries `tb-msg-id` as a Kafka header
(named by `idempotency.header` when the technique is `idempotency-key-header`) so a downstream consumer
can deduplicate. **No other `tb-*` header is propagated** in set 1.1.

`key: preserve` hands the record key — usually a patient identifier — to whoever reads that topic.
That is a disclosure decision and is stated, not defaulted. **A per-key channel with `key: none` gives
up order at this destination; the control plane refuses that combination** (a cross-field rule JSON
Schema cannot express here, like destination-name uniqueness).

### 3.5 Destination steps

Set 1.1 adds optional `destinations[].steps`, because one payload cannot be both a FHIR Bundle and a
table row, and wave 2's exit criterion 1 needs both from one channel.

- They run **in the pipeline worker**, after `pipeline.steps`, before the record is produced to
  `out.<dest>`. No new Kafka hop, so invariant 2 is untouched; the loader stays a dumb sink.
- Each destination's steps get their own copy of the pipeline output, with `ctx.destination` set to
  the destination's name — the SDK already defines `ctx.destination` for this.
- A `Drop` in a destination step filters the message for that destination only; the others proceed.
  This is also the per-destination filter set 1.0 listed as an open item.
- A failure is retried in place `pipeline.stepRetries` times, then dead-lettered with `tb-destination`
  set. It does not stop the other destinations. **The `dlq` record's payload is the destination
  step's input** (the pipeline output), not the raw payload. That is what failed, and it is what a
  destination-scoped reprocess needs.
- **Reprocess must be destination-scoped** for any `dlq` record carrying `tb-destination`. This
  covers destination-step failures and loader exhaustion alike, and the latter already exists in
  wave 1. Re-injecting such a record into `raw` would re-deliver it to *every* destination under a
  new `tb-msg-id`, which no loader's idempotency can absorb. Until destination-scoped re-entry is
  built (deferred from W3-3 to wave 4 on 2026-10-06, ADR-003 follow-up 10), the reprocess tooling must
  refuse these records. Recorded in ADR-003 D5.
- The `raw` offset is committed once every destination has either been produced to or dead-lettered;
  a redelivery re-runs all of them, which invariant 4's loader idempotency absorbs. With
  `pipeline.transactional: true`, all the `out` records for one message commit atomically.

Implementation belongs to `platform-engineer` (W2-5). **Give up:** the executor becomes a small tree
instead of a list, and per-destination timing needs its own metric label (the destination name, which
is a spec-defined, non-PHI value).

---

## 4. Ordering, the key, and retry

`ordering: per-key` means the Kafka record key is the result of `partitionKey`, and per-key order is
preserved end to end. `ordering: none` means a null key, round-robin partitioning, and no ordering
promise at all. There is no default, because invariant 5 says ordering is explicit.

**The key is usually a direct patient identifier.** `hl7:PID.3.1` is ADR-001's worked example.
Everything in `contracts/envelope/v0` section 4 applies to it: never in logs, metric labels, trace
attributes, exception messages, URLs, browser storage, sender-facing errors, or other headers.
Declaring `partitionKey` is therefore a decision with a PHI consequence, not a performance knob.

`partitions` overrides the measured default of 3 (`contracts/topics/v0` 2.2). On a per-key channel
the runtime MUST refuse to start when an `out.<dest>` topic's partition count differs from `raw`: a
different count changes the key-to-partition mapping, so two messages for one patient can be
delivered by different workers out of order — invariant 5 violated silently.

### 4.1 `retry-topic` is defined and not selectable

`retry.policy` accepts `blocking` only. `retry-topic` is in the enum so that specs, docs and code
share one name for it, and a sibling `not` makes selecting it a validation failure.

**Why the schema and not a sentence:** ADR-003 D1 already says it may not be selected until it is
spiked, and ADR-003 D2 leaves its delay mechanism undecided. A rule that lives only in an ADR is a
rule nothing applies — the shape of both Phase 0 High findings. Removing the `not` after the spike
is a relaxation: one line, non-breaking, recorded in the CHANGELOG.

**Give up:** a channel with an order-insensitive sink and a slow destination has to accept blocking
retry's visible lag until the spike lands. That is the cost of not shipping an unmeasured path.

#### 4.1.1 What lifts the refusal

The `not` is removed — in the schema **and** in the runtime's refusal in `tributary.runtime.spec` —
when all of the following are true, and not before. The spike is `platform-engineer`'s, inside W2-4
(ADR-003 D2 and follow-up 5; ADR-007 D5 and D15).

1. **A written spike report exists** in `docs/spikes/` and ADR-003 D2 records the chosen mechanism:
   (a) one `<env>.<channel>.retry` topic plus `tb-retry-not-before`, or (b) one topic per tier. The
   topic is the one `topics/v0` section 1 already defines — **shared across a channel's destinations**
   and keyed by the ordering key, with `tb-destination` telling the retry consumer which destination
   failed. A per-destination retry topic would be a `topics/v0` change and needs its own proposal.
2. **Head-of-line cost measured** — the number D2 was deferred for. For (a): delivery lateness when a
   long-delay record sits ahead of short-delay ones on the same partition. For (b): the topic and
   partition count per channel, stated against the `topics/v0` 2.2 budget.
3. **Delay accuracy reported** per tier as the distribution of actual minus scheduled redelivery time,
   with p50 and p99, and **resume is not driven by the poll timeout** (Phase 0 memo, change 7).
4. **Zero loss** across: retry-consumer kill in the middle of a delay window, a broker leader kill, and
   a rebalance of the retry consumer group. Duplicate rate reported, not thresholded.
5. **Commit discipline shown**: the `out.<dest>` offset is committed only after the `retry` record is
   acknowledged, and the `retry` offset only after the redelivery outcome is durable — a sampled
   commit-after-delivery check like S5's, not an argument.
6. **Exhaustion**: after `maxAttempts` the record reaches `dlq` with `tb-error-code`, `tb-attempt` and
   `tb-destination`, and the partition keeps moving. Every `retry` and `dlq` record passes
   `validate_for_produce` against `envelope/v0`.
7. **The reordering is characterised, not denied.** `retry-topic` does not preserve order for retried
   messages — that is ADR-003 D1 and invariant 5's own wording. The spike must show that **messages
   that never failed keep their order**, and must measure how far a retried message lands out of
   order; it must not claim retried messages stay ordered.
8. **Every fault is proven to have fired**, with a deliberately broken implementation per headline
   check that turns the check red — the Phase 0 rule (a green test whose fault never fired is
   indistinguishable from a passing system).
9. **`code-reviewer` has rated the evidence** the way `phase-0-evidence-review.md` did.

**What changes on lift, decided now so it is not relitigated then:** the `not` goes; a `retry-topic`
destination on a **per-key channel additionally requires an explicit `acceptReordering: true`** in its
`retry` block (additive), because invariant 5 forbids *silent* reordering and a per-key channel has
declared that order matters; the CLI scaffolds `onExhaust: dlq-and-continue` for it (ADR-003 D3).
**Set 1.4 adds:** the loader gains READ and WRITE on `<env>.<channel>.retry` (`topic-defaults.json`
`security.acls.loader_retry_topic`; no role holds either today). Each destination's loader consumes
`retry` in its own group `tb.<env>.<channel>.loader.<dest>` and skips other destinations'
`tb-destination`. A channel-wide retry group is forbidden, because one destination's commits would
skip another's retries (`topics/v0` 3.4.1). The spike's item 4 gains a two-destination case, with a
shared-group control that must go red.

**If the spike shows (a)'s head-of-line cost is intolerable and (b) breaks the partition budget,** the
answer is that `retry-topic` stays refused and ADR-003 says why. That is a legitimate outcome, and the
wave 2 exit criteria do not depend on it.

### 4.2 `onExhaust`

`halt-partition` never delivers out of order and never abandons a message, at the cost of growing
lag and an operator. `dlq-and-continue` keeps the pipeline moving and leaves a gap in that key's
order — later messages for a patient are delivered while an earlier one sits in the DLQ.

Required, not defaulted, even though ADR-003 D3 names `blocking → halt-partition` as the default.
The difference between the two is clinical, per destination, and should be written in the spec
where a reviewer sees it. The CLI scaffolds the ADR-003 default into a new channel.

### 4.3 `partitionKey`: what each scheme means (set 1.1)

Set 1.0 fixed the shape only and deferred the semantics to W2-4. They are fixed here so that the
receivers (W2-7), which **evaluate the key when they produce to `raw`**, and the runtime (W2-4) agree.
The key is set once, at ingest; `out.<dest>` keeps it unchanged.

**One evaluator, shared.** `platform-engineer` builds it (W2-4) as a pure function — expression plus
payload plus inbound transport headers in, key bytes or "unresolved" out — with no I/O, and every
receiver calls it. Two implementations of this function would put two copies of one patient in
different partitions, which is invariant 5 broken silently. Where it lives and its signature are SDK
public surface and come to the architect for approval. The CLI and the control plane call the same
function's parser at deploy, so a malformed locator is refused before it reaches a receiver.

| Scheme | Locator grammar | Evaluated against | Valid for |
|---|---|---|---|
| `hl7:` | `SEG.F`, `SEG.F.C` or `SEG.F.C.S` — segment id, then 1-based field, component, subcomponent, HL7 numbering (`MSH.1` is the field separator, `MSH.3` sending application). Optional **repetition selector** on the field: `SEG.F[c:VALUE]` picks the first repetition whose component `c` equals `VALUE` exactly, e.g. `hl7:PID.3[5:MR].1` (the MR-typed identifier). A numeric `[n]` index stays **reserved** | First occurrence of the segment; the first repetition of the field, or the first matching the selector | `application/hl7-v2+er7` payloads |
| `json:` | An RFC 6901 JSON Pointer, e.g. `json:/subject/reference`. `~0`/`~1` escapes are not expressible in set 1.1 | The parsed payload | `application/json`, `application/fhir+json` |
| `header:` | A header name on the **inbound transport message** | HTTP request header (`http` source); inbound record header (`kafka` source) | `http`, `kafka` sources |
| `record:key` | Literal | The inbound Kafka record key, byte for byte | `kafka` source |
| `xpath:` | — | **Reserved. No evaluator exists; a spec using it is refused at apply** | — |

**Rules for every scheme:**

- **The key is the value's bytes exactly as they appear**: UTF-8 for text formats, no HL7 unescaping,
  no trimming, no case folding. Normalising is a choice with clinical consequences (is `12345` the same
  patient as `012345`?), and set 1.1 offers **no place to make it**. The receiver evaluates the key
  before any step runs, so a step cannot normalise it. (The first version of this rule said "normalise
  in a step". That was impossible; `platform-engineer` caught it.) A channel whose senders encode one
  patient's identifier differently cannot key reliably on it in set 1.1. Fix it at the sender, or
  choose a field that is stable. A declarative normalisation is a candidate for a later set.
- **Key on a field that identifies one thing.** PID-3 repeats, and senders order its repetitions
  differently, so a bare `hl7:PID.3.1` can take an MR number from one feed and an account number from
  another. The same patient then lands on two partitions and is reordered silently: an invariant 5
  failure in exactly the case the key exists for. **Use the selector (`hl7:PID.3[5:MR].1`)** whenever
  the field repeats. The control plane warns on an unselected key into a field that repeats in the
  HL7 standard. It warns rather than refuses because set 1.0 examples use the bare form.
- **Header names** match case-insensitively for the `http` source (HTTP semantics) and exactly for the
  `kafka` source. A header present more than once is unresolved (`KEY_HEADER_AMBIGUOUS`), never "the
  first one".
- **Unresolved** means: the segment, field, pointer target or header is absent, the value is empty,
  the value is not a scalar, or it exceeds 256 bytes (`envelope/v0` key limit). On a per-key channel an
  unresolved key is a **`parse-error` at the receiver** — the NACK column of 3.2.2 — and never a null
  key, a default key or round-robin, any of which would silently drop that patient out of order.
- **A scheme that cannot apply to the source type** (`header:` on `mllp` or `sftp`, `record:key` on
  anything but `kafka`, `hl7:` on a JSON-only `http` source) is refused at deploy.
- **The key is PHI.** The evaluator's result, and the value it failed to parse, never reach a log,
  metric, trace attribute or exception message (`envelope/v0` section 4). An "unresolved" outcome is
  reported by reason code only.

---

## 5. Idempotency is declared per destination

Invariant 4 requires every loader to document **and test** its idempotency technique keyed by
`tb-msg-id`. Declaring it in the spec is what makes that reviewable per channel instead of a
sentence in a loader's docstring.

| `technique` | Meaning |
|---|---|
| `idempotency-key-header` | The destination honours an idempotency header; `header` names it. |
| `conditional-update` | Conditional create/update, e.g. FHIR `If-None-Exist` or a PUT on a business identifier. |
| `deterministic-name` | The object or file name is derived from the message id. |
| `sink-native` | The sink deduplicates, e.g. `INSERT … ON CONFLICT` or a ReplacingMergeTree. |
| `none` | Duplicates are visible at this destination. Requires `note`. |

**The key is always `tb-msg-id` and is not configurable.** A second idempotency key is a second way
to be wrong, and reprocess deliberately mints a new `tb-msg-id` with `tb-parent-id` set (ADR-003
D5) so that a deliberate reprocess is not deduplicated away by a correctly idempotent destination.

### 5.1 Which techniques each destination type may declare (set 1.1)

A declared technique is a claim that invariant 4 holds at that sink, so the schema refuses a claim
the type cannot honour (ADR-007 D14). `http` is unrestricted, as in set 1.0, because a generic HTTP
endpoint may implement any of them.

| Type | Permitted | Mechanism |
|---|---|---|
| `http` | any | As declared |
| `fhir` | `conditional-update`, `idempotency-key-header`, `none` | 3.4.2 |
| `postgresql` | `sink-native` | `ON CONFLICT` on a unique `idColumn`, checked at startup |
| `clickhouse` | `sink-native` | `ReplacingMergeTree` on `idColumn`, checked at startup; eventual |
| `sftp` | `deterministic-name` | File named from `tb-msg-id`, write-then-rename |
| `s3` | `deterministic-name` | Object key from `tb-msg-id` |
| `mllp` | `sink-native`, `none` | The receiving system deduplicates, or duplicates are visible |
| `kafka` | `idempotency-key-header`, `none` | `tb-msg-id` as a record header for the consumer |

**Wave 2 exit criterion 3 ("every loader has a passing duplicate-delivery test") means, per type:**
deliver the same `out` record twice — the second as a genuine redelivery from an uncommitted offset,
not a second call in the same process — and assert what the sink holds. For `clickhouse` that is a
`FINAL` count. For `mllp` and `kafka`, where the sink is someone else's, the test asserts that both
deliveries carry the same `tb-msg-id`, which is all the engine can promise.

---

## 6. The `_tb.<env>.channels` record

Invariant 3 makes this topic the worker's only source of truth for a channel, so its shape is part
of this contract.

| Part | Value |
|---|---|
| Key | UTF-8 bytes of `metadata.name`. Nothing else — this is the compaction key. |
| Value | The whole spec document as JSON, **including `metadata.version`**. |
| Tombstone | A null value means undeployed. The runtime MUST stop the channel and release its partitions. |
| Headers | None required. This is not a data-plane message; do not invent `tb-*` headers for it. |

Two rules the schema cannot express:

1. **The runtime MUST reject a published record whose `metadata.version` is absent**, log the
   channel name and the offset, and keep the previously applied spec running. `version` is optional
   in the schema because one schema validates both the authored and the published form; this is a
   code rule with a named test, the same arrangement as the envelope's per-class key identity rules
   (`contracts/envelope/v0` 1.1).
2. **A spec must be self-contained.** No field may require a control-plane call to resolve.
   `secretRef` resolves against the deployment's secret store at component startup, never via the
   API — otherwise the data plane cannot start while the API is down, and invariant 3 is false.

---

## 7. What this contract refuses, and why

**Inline code.** There is no `script`, `code` or `transformer` field, and there never will be in
this major version. Mirth's inline JavaScript is the affordance migrating users will ask for first.
Inline source in a spec cannot be imported by pytest, reviewed in a diff, type-checked by `mypy`, or
pinned by a digest — so invariant 7's "every step must be unit-testable without Kafka" would become
untrue for any channel that used it. **Give up:** a one-line filter needs a bundle, a module and a
test. `packages/tributary-mirth-import` owns the conversion path, which is where that cost belongs.

**Connector types nobody is building.** Set 1.0 listed only what wave 1 implemented. Set 1.1 adds
the wave 2 connectors at the **start** of the wave that builds them, because contract-first is the
point of this directory and two engineers are coding against these shapes in parallel (ADR-007 D6 as
amended, D10). What is still refused is a type no wave has scheduled (DICOM, CDC, database polling):
publishing its config would be invented detail with nothing to check it. **Give up:** until a set
1.1 connector ships, a spec can select it and fail at startup rather than at validation — loud, and
tracked in 3.0.

**Per-channel durability settings.** `retention` is the only per-channel topic override
(`contracts/topics/v0` `apply_order`). `min.insync.replicas`, `acks` and
`unclean.leader.election.enable` are not negotiable per channel, because invariant 1 is not.

---

## 8. Conformance

```
uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py
```

49 examples: 9 valid, 40 invalid (set 1.0: 4 and 19; set 1.1 added 4 and 16; set 1.2 added 1 and 5).
**The five set 1.2 pairs are architect predictions, UNMEASURED** until `--emit-expectations` runs.
Tests load
[`examples/manifest.json`](examples/manifest.json) and assert that each invalid example fails at the
`expected_json_path` and `expected_validator` it pins, not merely that it fails.

**Set 1.1 status: passing, first run, 2026-09-24** (`qa-engineer`, local Windows checkout):
`validate.py` **122 / 0**; `--emit-expectations` **86 pairs, all matched, 0 mismatches**, including the
one flagged as least certain (`source-field-from-another-type.json`, `$.spec.source` / `enum`);
`pytest tests/conformance` **127 passed**. All 16 set 1.1 pairs were architect predictions, and none
needed correcting.

**Set 1.0 status: passing.** Three independent runs on 2026-09-22, all first-run green:

| Run | By | Result |
|---|---|---|
| `contracts/validate.py` | lead | **94 / 0** across all four contracts at publication, **100 / 0** at `f5bc92d` once the requirement-13 and quarantine examples landed |
| `contracts/validate.py --emit-expectations` | `qa-engineer` | All **19 predicted pairs matched the measured ones exactly.** No correction needed |
| `pytest tests/conformance` | `qa-engineer` | **99 passed** at `5eb3949` (76 + 23), **105** at `f5bc92d` |

The pinned pairs were written as architect predictions, because this session has no shell, and were
labelled as such until they were measured. The three flagged as least certain — the `propertyNames`
deny-list path, the shared `oneOf` pair, and the if/then/else `else` branch — all matched.

`qa-engineer` added `channel-spec` to the `FILE_CONTRACTS` constant in
`tests/conformance/test_contracts_v0.py` (`5eb3949`); the manifest shape is identical to the
envelope and events manifests, so nothing else in the suite changed.

---

## 9. Open items

- ~~**Synchronous reply mode** has no fields here.~~ **Set 1.2**, section 3.2.6. What it leaves open:
  - `after: delivered` (a loader's outcome), which needs loader WRITE on `reply` and a rule for which
    destination answers;
  - an `onDrop` option for channels that want a filtered message refused rather than `AA`'d;
  - the HTTP status mapping, decided with W2-7 under the constraint in 3.2.6;
  - whether the browser needs "the sender got `AE`", which needs a receiver ACL on `_tb.<env>.events`
    (an ACL-model change, ADR-004);
  - edge dedupe (ADR-003 D8), the only mitigation for resends after `AE`, still deferred.
- ~~`partitionKey` expression semantics.~~ **Fixed in set 1.1**, section 4.3. The evaluator is W2-4.
- **Claim-check** has no channel-level knob, because it is untested (gate 7.2 item 18) and the
  behaviour is automatic above the inline limit. It is **not scheduled in wave 2** (the phase plan has
  no task for it) and the Phase 0 memo requires a spike before any implementation. Until then every
  source refuses input over 1,000,000 bytes (3.2.2) — which the SFTP and HTTP sources will hit more
  often than MLLP did.
- ~~Destination-level filters.~~ **Set 1.1**: a `Drop` in a destination step (3.5).
  **Routing conditions** as declarative data (Mirth's per-destination filter expression) are still
  not in the spec; a destination step does the same job as tested code.
- **Alert rules and SLOs per channel** belong to the control plane, not the spec, until there is a
  reason to move them.
- ~~A destination URL that must carry an identifier in a query string.~~ **Set 1.1** avoids it for
  FHIR with transaction-only (3.4.2). If a real destination can only take an identifier in a query,
  it needs its own proposal with a no-log rule.
- **Channel chaining** (one channel's `out` feeding another's source) is not in set 1.1: a `kafka`
  source cannot name a channel topic (3.2.5). It needs its own design, because it crosses the
  per-channel ACL boundary.
- **`retry-topic`**: section 4.1.1.
- **Telling a split child from a reprocessed record (W3-3).** Both carry `tb-parent-id`, so the
  interim rule in 3.3.2 (split rule 6) refuses both. Two candidates, not decided here:
  (a) no contract change: a record is a split child if its `tb-msg-id` equals the 3.3.3 derivation
  from its `tb-parent-id` for some index below 10,000. That is cheap, but it stops working once a
  child is itself reprocessed;
  (b) an additive header naming the origin.
  W3-3 brings a proposal.
- ~~JSON filter deny-list candidates (`subject`, `patient`).~~ **Superseded the same day** by the
  filter allow-list (3.3.2), under which neither is reachable.
- **Declared `Reject` codes: deferred to bundle-fetch, not wave 2** (`platform-engineer`'s proposal,
  2026-09-25). The proposal: each bundle declares its codes in its manifest, and `Reject` refuses an
  undeclared one at load, so `f"MRN_{pid3}"` fails. The PHI path is real: the code grammar admits
  digits, and `tb-error-code` reaches headers, events and ClickHouse. **Why later:** the proposed home
  is a bundle manifest, and no bundle format exists until bundle-fetch. In wave 2 every `uses` step
  is engine or first-party code under `code-reviewer`. **Interim rule:** a `Reject` code is a literal
  or a module constant, never built from message data. `code-reviewer` treats a violation as Critical
  (invariant 6). Designed with bundle-fetch, together with the `ref` restriction in 3.3.1.

---

## 10. Connector security posture, and one licence question

### 10.1 Per connector (ADR-007 D7, extended)

| Connector | Transport | Peer verification | Credentials | Plaintext allowed | PHI surfaces to keep clean |
|---|---|---|---|---|---|
| `mllp` source | TLS | optional mTLS (`clientAuth`) | trust bundle via `secretRef` | loopback bind | NACK text |
| `http` source | TLS | mTLS or bearer, stated | `secretRef` | loopback bind, and only then `auth: none` | request line, query, response body, access log |
| `sftp` source | SSH | pinned host keys | key or password via `secretRef` | never | **file names** |
| `kafka` source | TLS | server cert always; client cert or SCRAM | `secretRef` | loopback brokers | record key, inbound headers |
| `http`, `fhir` destination | TLS | server cert always | `secretRef` | loopback URL | URL, response body, `OperationOutcome` |
| `postgresql`, `clickhouse` | TLS `verify-full` | server cert and hostname | `secretRef` | loopback host | driver error detail |
| `sftp` destination | SSH | pinned host keys | `secretRef` | never | file names (built from `tb-msg-id` only) |
| `s3` | TLS | server cert always | `secretRef` or ambient | loopback endpoint | object keys (built from `tb-msg-id` only) |
| `mllp` destination | TLS | server cert always | trust bundle via `secretRef` | loopback host | `MSA-3`, `ERR` |
| `kafka` destination | TLS | server cert always | `secretRef` | loopback brokers | record key when `preserve` |

Every row: no connector object has a field that can hold a secret value; `secretRef` resolves at
startup from the deployment's secret store, never via the API (section 6, rule 2).

**Not in set 1.1, and deliberately so:** certificate revocation checking (a wave 1 Low, still open),
OIDC/JWT validation on the `http` source, and which KMS key an `s3` destination uses. The first two
are designs of their own; the third is key management, which is ADR-004 plus a human.

### 10.2 Licence question for SFTP and PostgreSQL — for the human, not decided here

To the architect's knowledge — **unverified, and to be confirmed from the resolved package metadata by
`licence_check.py`, not from this paragraph** — the established Python SSH/SFTP libraries are not
under a licence invariant 10 permits (`paramiko` is LGPL-2.1; `asyncssh` is EPL-2.0 or GPL-2.0-or-later),
and `psycopg` 3, which CLAUDE.md names for the control plane, is LGPL-3.0. Invariant 10 allows
Apache-2.0, MIT, BSD, ISC and PSF, MPL by approval, and names no LGPL or EPL path.

- **PostgreSQL loader: settled.** It uses `asyncpg` (Apache-2.0, as reported; `licence_check.py`
  confirms it when the dependency lands). The control plane's `psycopg` (LGPL-3.0, denied by
  `licence_check.py`) is a separate human decision for wave 3, because CLAUDE.md names it.
- **SFTP source and destination: decided 2026-09-24.** The human approved `paramiko` (LGPL-2.1) as
  an unmodified dependency of `tributary-loaders` and `tributary-receivers`, for SFTP only (ADR-001
  2.3). The shapes above are unchanged. `hostKeys` stay SHA-256 fingerprints, which the loader compares
  against the fingerprint of the key the server presents. That comparison must happen before
  authentication, and there must be no fallback to paramiko's auto-add policy, whose permissive
  default is the library default to watch here.
