# Contract: channel spec — v0 (draft)

**Contract id:** `tributary.channel-spec/v0`
**Status:** Frozen at the Phase 1 wave 1 gate as part of contract set 1.0. **Set 1.1 (2026-09-24)
adds the wave 2 connector types, per-destination steps and the key-expression semantics — all
additive**: every document valid under 1.0 is still valid. Breaking changes create `../v1/`.
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
             auth: {type: oauth2-client-credentials, secretRef: fhir-oauth}}
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
| source | `mllp` | W1-5 | **Implemented** |
| source | `http` (HTTP and FHIR ingest) | W2-7 | Contracted, not built |
| source | `sftp` (poller) | W2-7 | Contracted, not built. **Licence question open, section 10.2** |
| source | `kafka` | W2-7 | Contracted, not built |
| destination | `http` | W1-6 | **Implemented** |
| destination | `fhir` | W2-6 | Contracted, not built |
| destination | `postgresql` | W2-6 | Contracted, not built. Driver: `asyncpg` (section 10.2) |
| destination | `clickhouse` | W2-6 | Contracted, not built |
| destination | `sftp` | W2-6 | Contracted, not built. **Licence question open, section 10.2** |
| destination | `s3` | W2-6 | Contracted, not built |
| destination | `mllp` (client) | W2-6 | Contracted, not built |
| destination | `kafka` | W2-6 | Contracted, not built |

A connector is "Implemented" when it has its integration test and, for a destination, its
duplicate-delivery test (invariant 4, wave 2 exit criterion 3) — not when its config parses.

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
| `tls` | C | **Required whenever `bind` is not loopback**, wildcard included. `secretRef` and `clientAuth` are both required inside it; `clientAuth: required` is mTLS. |
| `ack` | R | `mode` is `after-commit` and nothing else. `nackOn` lists the conditions that NACK instead. A NACK carries a code and `tb-msg-id` only — never the record key, never exception text. |
| `maxMessageBytes` | O | ≤ 1,000,000, the envelope's inline limit. Claim-check for larger payloads is **untested** (gate 7.2 item 18); do not design a channel that depends on it yet. |
| `queue` | O | When present, `maxBytes` is required; `maxMessages` alone does not conform. When absent the receiver default applies: **64 MiB (67,108,864)**. See 3.2.1. |

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
| `mllp` | HL7 `AA` | `AE` / `AR` with a code and `tb-msg-id` only | NACK or no ACK | NACK |
| `http` | `202 Accepted`, body carries `tb-msg-id` only | `400` parse error, `415` content type, `401`/`403` auth — code only, never payload or exception text | `503` with `Retry-After` | `413` before the body is buffered past the limit |
| `sftp` | `afterCommit`: delete, or move to `archiveDirectory` | move to `rejectDirectory`; **never delete** | leave the file; retry next poll | move to `rejectDirectory` |
| `kafka` | commit the **source** offset | none exists — see below | do not commit; pause the source partition | halt that source partition (below) |

**`202` means committed, not processed.** An HTTP sender that needs the pipeline's outcome needs
synchronous reply mode (W2-8, ADR-003 D9), which is not in set 1.1.

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
| `kafka.connection.security.tls` | O | Trust bundle and client certificate; verification cannot be disabled |
| `kafka.topic` | R | Not `_tb.*`, and not shaped like a channel topic (`<env>.<channel>.raw` and so on) |
| `kafka.startFrom` | R | `earliest` or `latest` for a group with no committed offset. No default |

The consumer group is `tb.<env>.<channel>.receiver` (`topics/v0` 3.6, role added in set 1.1).
**No header on the inbound record is copied into the envelope** — in particular an inbound `tb-*`
header is never trusted; the receiver mints the envelope fresh. The inbound key is used only when
`partitionKey` is `record:key`. Offsets are committed after the `raw` produce is acknowledged, in
batches of `commit_every` (`topics/v0` 3.3), which is also this source's replay window.

### 3.3 `spec.pipeline`

| Field | Required | Notes |
|---|---|---|
| `bundle` | R | OCI reference ending `@sha256:<64 hex>`. A tag fails validation. |
| `steps` | R | 1-32. Each step has `name` plus **exactly one** of `uses` (a registered step) or `ref` (a dotted path in the bundle), plus optional `with`. |
| `stepRetries` | O | In-place retries before the DLQ (ADR-003 D4). Default 3 — a **judgement, unmeasured**: S5 exercised the DLQ path, not the retry count. |
| `transactional` | O | Kafka transactions on the `raw` → `out.<dest>` hop (ADR-003 D6). Opt-in, default false. |

`with` takes scalars and flat arrays of scalars. No nested objects: a nested structure is where
configuration turns into a program the runtime has to interpret. Property names that denote a
credential (`password`, `token`, `apiKey`, …) are rejected outright.

**How `with` reaches a step (set 1.1, approved 2026-09-24).** Each key binds, by exact name, to a
**keyword-only parameter** of the step function, e.g. `def parse(message, ctx, *, version: str = "2.5.1")`.
The runtime binds once, when it loads the step. The following are refused with a `ChannelSpecError`
at load, loudly, and the previous spec keeps running:
- an unknown key;
- a key that is not a Python identifier (the schema admits `-`; the binder does not);
- a required keyword-only parameter with no `with` value.

`run_step` takes the same `with` values, so a step is unit-tested with its config. Rejected
alternative: `ctx.params` as an untyped mapping, where a mistyped key is silently ignored, which is the
failure this contract exists to prevent.

#### 3.3.1 `ref` needs bundle-fetch, which does not exist in wave 1

The two step forms resolve by different mechanisms, and only one of them is implemented:

| Form | Resolves by | Wave 1 |
|---|---|---|
| `uses` | A Python entry point in the `tributary.steps` group, already present in the running image (ADR-001 6.2) | **Works.** |
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
| `http`, `fhir` | `HTTP_<status>`; `HTTP_REDIRECT_REFUSED`; `FHIR_BUNDLE_INVALID` |
| `postgresql` | `PG_<SQLSTATE>` (a class code, never the detail text) |
| `clickhouse` | `CH_<numeric code>` |
| `sftp` | `SFTP_<status>`, `SSH_HOST_KEY_MISMATCH`, `SSH_AUTH_FAILED` |
| `s3` | `S3_<ERROR_CODE>` upper-snake-cased |
| `mllp` | `MLLP_AE`, `MLLP_AR`, `MLLP_TIMEOUT`, `MLLP_NO_ACK` |
| `kafka` | `KAFKA_<librdkafka error name>` |
| all | `TIMEOUT`, `CONNECT_FAILED`, `TLS_VERIFY_FAILED`, `PAYLOAD_SHAPE_INVALID` |

#### 3.4.1 `http`

Unchanged from set 1.0: `url`, `method`, `timeoutMs` required; optional `headers`, `auth`,
`circuitBreaker`. `auth.type` values `oauth2-client-credentials` and `mtls` are in the schema but the
wave 1 loader refuses them at startup; see `tributary.loaders.http.config`.

#### 3.4.2 `fhir`

| Field | Required | Notes |
|---|---|---|
| `url` | R | The FHIR base URL. HTTPS unless loopback, no query, no fragment |
| `fhirVersion` | R | `4.0.1`, `4.3.0` or `5.0.0` |
| `interaction` | R | `transaction` only |
| `timeoutMs`, `headers`, `auth`, `circuitBreaker` | | As `http` |

The payload is a FHIR Bundle with `type: transaction`, built by a step. The loader POSTs it to `url`
exactly and checks `Bundle.type` first; any other payload is `FHIR_BUNDLE_INVALID`, non-retryable.
A `2xx` transaction-response is success. `3xx` is refused, `408`/`429`/`5xx` are retryable, and any
other `4xx` is non-retryable.

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
| `auth` | R | `password` or `client-certificate`, via `secretRef` |
| `tls.mode` | R | `verify-full` (certificate and hostname) or `disable` — **loopback only**. There is deliberately no "encrypt without verifying" mode |
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
  it, `ON CONFLICT` has nothing to conflict on and duplicates are silent.
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
  new `tb-msg-id`, which no loader's idempotency can absorb. Until W3-3 implements destination-scoped
  re-entry, the reprocess tooling must refuse these records. Recorded in ADR-003 D5.
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

43 examples: 8 valid, 35 invalid (set 1.0: 4 and 19; set 1.1 added 4 and 16). Tests load
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

- **Synchronous reply mode** (ADR-001 6.5, ADR-003 D9) has no fields here. W2-8 implements it; the
  `reply` block is additive and arrives with it, as a proposal from `connector-engineer` and
  `platform-engineer`.
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
- **SFTP source and destination:** if no permissive SFTP client exists, the options are an unmodified
  LGPL/EPL dependency by human approval, shelling out to an OpenSSH `sftp` binary, or descoping SFTP
  from the MVP. **That is a licensing decision, so it goes to the human.** `connector-engineer` should
  not add an SFTP dependency until it is made. The contract shapes above stand whichever way it goes.
