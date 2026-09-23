# Contract: channel spec — v0 (draft)

**Contract id:** `tributary.channel-spec/v0`
**Status:** Draft. Frozen at the Phase 1 wave 1 gate; breaking changes after that create `../v1/`.
**Owner:** `architect`. Propose changes by message with the exact diff and impact.
**Source:** ADR-001 sections 6.1 and 6.4, ADR-003 D1/D3/D4/D6, Phase 0 gate section 7.1.
**Schema:** [`channel.schema.json`](channel.schema.json) · **Examples:** [`examples/manifest.json`](examples/manifest.json)

Realises invariant 7 (channels are declarative, steps are unit-testable functions) and carries the
declarative half of invariants 1, 4 and 5.

**Verified 2026-09-22.** `check_schema` passes, all 23 examples behave as the manifest says, and
every invalid example fails at the pinned location and keyword — see section 8.

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

---

## 3. Fields

### 3.1 `metadata`

| Field | Required | Notes |
|---|---|---|
| `name` | R | `^[a-z][a-z0-9-]{0,63}$`. Becomes the `tb-channel` header and the channel segment of every topic name. |
| `version` | C | Integer, set by the **control plane** at publish time, absent in an authored bundle file. Becomes `tb-channel-version`. See section 6. |
| `labels` | O | ≤ 16 short codes. No PHI: labels reach metric labels and the UI. |
| `description` | O | ≤ 512 characters. |

### 3.2 `spec.source` (v0 defines `mllp` only)

| Field | Required | Notes |
|---|---|---|
| `type` | R | `mllp`. Adding `http`, `sftp` or `kafka` is an enum relaxation — additive, and it arrives with the connector that implements it. |
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

### 3.4 `spec.destinations` (v0 defines `http` only)

| Field | Required | Notes |
|---|---|---|
| `name` | R | Becomes the `out.<destination>` topic segment and the `tb-destination` header. Uniqueness within a channel is checked by the control plane — JSON Schema cannot express it over an array of objects. |
| `type` | R | `http`. The wave 2 set arrives one enum value plus one config object at a time, each with its loader and its duplicate-delivery test. |
| `with` | R | `url`, `method`, `timeoutMs` required; optional `headers`, `auth`, `circuitBreaker`. |
| `idempotency` | R | Section 5. |
| `retry` | R | Section 4. |

`timeoutMs` interacts with the runtime: `max.poll.interval.ms` must be at least the slowest
destination timeout plus backoff (`contracts/topics/v0` 3.3), or a slow sink looks like a dead
consumer and causes a rebalance loop.

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

### 4.2 `onExhaust`

`halt-partition` never delivers out of order and never abandons a message, at the cost of growing
lag and an operator. `dlq-and-continue` keeps the pipeline moving and leaves a gap in that key's
order — later messages for a patient are delivered while an earlier one sits in the DLQ.

Required, not defaulted, even though ADR-003 D3 names `blocking → halt-partition` as the default.
The difference between the two is clinical, per destination, and should be written in the spec
where a reviewer sees it. The CLI scaffolds the ADR-003 default into a new channel.

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

**Connector types nobody has built.** `source.type` and `destination.type` list only what wave 1
implements. Publishing config shapes for unwritten receivers and loaders would be invented detail
with nothing to check it, and an enum relaxation is additive — so each connector brings its own
config object when it lands. **Give up:** every new connector costs one contract change and one
approval round. That is the intent: the architect sees each connector's configuration surface.

**Per-channel durability settings.** `retention` is the only per-channel topic override
(`contracts/topics/v0` `apply_order`). `min.insync.replicas`, `acks` and
`unclean.leader.election.enable` are not negotiable per channel, because invariant 1 is not.

---

## 8. Conformance

```
uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py
```

23 examples: 4 valid, 19 invalid. Tests load [`examples/manifest.json`](examples/manifest.json) and
assert that each invalid example fails at the `expected_json_path` and `expected_validator` it pins,
not merely that it fails.

**Status: passing.** Three independent runs on 2026-09-22, all first-run green:

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

## 9. Open items for v1

- **Synchronous reply mode** (ADR-001 6.5, ADR-003 D9) has no fields here. W2-8 implements it; the
  `reply` block is additive and arrives with it.
- **`partitionKey` expression semantics.** v0 fixes the shape (`scheme:locator`) only. The
  evaluator and the scheme set are W2-4.
- **Claim-check** has no channel-level knob, because it is untested (gate 7.2 item 18) and the
  behaviour is automatic above the inline limit. Revisit after the spike.
- **Destination-level filters and routing conditions** (Mirth's per-destination filter) are not in
  v0. A step returning `Drop` filters the whole message, not one destination.
- **Alert rules and SLOs per channel** belong to the control plane, not the spec, until there is a
  reason to move them.
- **A destination URL that must carry an identifier in a query string** (FHIR `?identifier=`) needs
  a deliberate, documented channel behaviour and a no-log rule. Wave 2, as an explicit contract
  change, not a default anyone can reach for.
