# Contracts changelog

Newest first. Each entry names the contract, the version, whether the change is additive or
breaking, and the reason. Breaking changes reference an ADR.

## 2026-10-05 (latest) — contract set 1.8, part 1: `observability/v0` (W3-12), `openapi/v0` part 1 (W3-1a), archive rulings, `--check-expectations` (W3-11)

**Additive. Two new contract directories. No existing schema, example or manifest changes validity,
and no existing pinned pair moves. No invariant changes.** Part 2 of set 1.8 (W3-1b: messages,
reprocess, replay, stats, events, audit shape) follows.

### `tributary.openapi/v0` (new, part 1)

- `openapi.json` (OpenAPI 3.1.0): health, environments, channels and immutable versions, validate,
  deploy, rollback and undeploy per environment, with `x-tributary-roles` per operation. Error bodies
  are restricted RFC 9457 problems with a closed `code` list and no `instance`.
- Decisions (README 3): the channel document is typed loosely, with `channel-spec/v0` as the
  authority; the server numbers versions; **rollback republishes the old document unchanged**, so
  `tb-channel-version` is not monotonic; `published` means committed to `_tb.<env>.channels`, not
  applied; optimistic concurrency on `expected_current_version`; sub-resource paths, not `:verb`
  (ADR-001 6.7 annotated).
- **ADR-009 (Proposed): OIDC bearer, resource server, PyJWT.** Awaiting `security-reviewer`.
- `validate.py` checks the document with `openapi-spec-validator` (licence closure checked from PyPI
  metadata, all permissive, no `certifi`; README 5). **A missing library fails the run.** It is not
  skipped.

### `tributary.observability/v0` (new)

- **Section 2, readiness and liveness (normative, published first for W3-17).** Ops listener separate
  from ingest, `127.0.0.1:9180` by default, non-loopback only when explicit, fail closed on bind, no I/O
  on the request path. `/livez` never depends on Kafka. Receiver `/readyz` gates on `spec`,
  `component`, `raw_producer` and, with sync reply, `reply_consumer`, each with a closed reason list.
  Rulings R1-R6: Kafka all-brokers-down makes a receiver not ready; a channels-topic outage after the
  first spec is `degraded` only (invariant 3); warm-up (one `raw` metadata round trip on the receiver's
  own producer) before the listener binds. Whether metadata alone removes the S2 first-ACK outlier is
  **unverified** and measured by W3-17.
- **Section 3, metric registry** `metrics.json` (35 metrics, 19 label names, closed) and
  `metrics.schema.json`. Seeded from the counters in code today and ADR-001 6.10.
- **Section 4, log line** `log-line.schema.json`: the `JsonLogger` shape, identifier deny-list (same as
  `events/v0` attrs), a new content deny-list, `message_id` as the canonical spelling with `tb_msg_id`
  and `msg_id` as deprecated aliases.

### `tributary.events/v0` and `tributary.topics/v0`: archive rulings (W3-2a report, 2026-10-05)

`archive-engineer` raised four gaps through the lead. README and `$defs`/description changes only. No
validated document changes validity, because `$defs.consumerGroup` is not referenced by the root topic
schema.

| # | Gap | Ruling |
|---|---|---|
| 1 | `message_id` is `Nullable(UUID)` in events 4, but the sort key cannot be Nullable; partition and record events have no message identity | **Ratified as built:** non-nullable `UUID`, nil UUID means no identity (UUIDv7 is never nil). Archive dedup key `(channel, scope, message_id, stage, destination, attempt)`, with `scope` defined per kind (events README 4.1 rule 1) |
| 2 | Group `tb.<env>.archiver` does not match `$defs.consumerGroup`; the ACL said PREFIXED `tb.<env>.archiver*` | **Both fixed:** `consumerGroup` gains the platform alternative `^tb\.<env>\.archiver$` and loses the per-channel `archiver` role. The ACL becomes LITERAL: the PREFIXED form broke 3.4.1 rule 2 and matched a channel named `archiver`. `topic-defaults.json` gains `consumer_groups.platform_groups`; README 3.4.1 and 3.6 |
| 3 | Archive event retention (`TB_ARCHIVE_TTL_DAYS` 90) | **Default 90 d stands as an engineering placeholder; the final value is TODO(human)**, decided with audit retention (wave-3 7.2), because it is the same legal reading. Not a build blocker |
| 4 | Month partitions do not collapse a redelivery that crosses a month | **Accepted and documented.** The W3-3 query library deduplicates on the key at read (`LIMIT 1 BY`); `FINAL` count tests must not straddle a month. Rejected: partitioning on the UUIDv7 time (nil-id events would all land in one partition) |
| — | Lead decision | A `*_hmac` or deny-listed `attrs` key is dropped and counted (`tb_archiver_attrs_dropped_total{reason}`), and the rest of the event is stored (events README 4.1 rule 3) |

Change requests: **`platform-engineer`**: `runtime/topics.py` `Role` still lists `archiver` for a
per-channel group. Remove it, or keep it unused, but no code may build `tb.<env>.<ch>.archiver`.
**`devops-engineer`**: when the archiver joins Compose, provision the LITERAL group grant.
**`archive-engineer`**: set the nil-UUID and `scope` rules in code comments, and use the counter name
above.

### `contracts/validate.py`

- **Per-entry `schema` in a manifest** (an entry's own key overrides the manifest's), and a schema
  reference may carry a JSON pointer (`file.json#/pointer`), used by `openapi/v0` next.
- `observability` added to the file contracts; new `check_metric_registry()` cross-check (adds no
  count).

### Change requests that follow

- **`connector-engineer` (W3-17):** build to section 2. Receivers' `print(json.dumps(...))` lines
  (`kafka_source/receiver.py`, `sftp/poller.py`, the `listening` events) move to `JsonLogger`;
  `tb_msg_id` and `msg_id` fields become `message_id`.
- **`qa-engineer`:** `tests/conformance` honours per-entry `schema` and adds `observability`;
  `OBSERVABILITY_PHI_GUARDS` (10, listed in the manifest `note`).
- **`platform-engineer` (on call):** the L4 content deny-list in `tributary.sdk.guards`; a runtime test
  validating `JsonLogger` output against `log-line.schema.json`; the exporter, once `security-reviewer`
  has cleared the listener.
- **`devops-engineer` (W3-8):** probes and the `partition_halted` alert from `tb_partitions_halted`;
  NetworkPolicy on the ops port; `--ops-bind` set to the pod IP.

### Verification owed

Not run by the architect, who has no shell.

- **First run (lead) crashed** with `FileNotFoundError`: `openapi/v0/examples/manifest.json` named the
  cross-contract readiness example as `../../observability/...`. Manifest `file` and `schema` paths
  resolve against the manifest's own directory, so it is `../../../observability/...`; fixed. Also
  hardened in `validate.py`: `check_metric_registry` no longer raises `TypeError` on a list-valued
  `buckets` or a non-list `labels`, and `openapi-spec-validator` before 0.7 (`validate_spec`) is accepted.
- `validate.py` (with `--with openapi-spec-validator`): **predicted 210 / 0**. That is 156 measured at
  set 1.7, plus observability 30 (11 valid, 19 invalid), plus openapi 24 (13 valid including the
  cross-contract readiness example, 10 invalid, and the document itself). Without the library:
  **209 passed, 1 failed** (the document check), exit 1, by design.
- Pinned pairs: **139** (110 + 19 + 10 new, all predictions).
- `--check-expectations`: predicted `139 invalid examples, 0 drifted`, exit 0.
- `pytest tests/conformance`: unchanged until `qa-engineer` adds both contracts.
- **CI:** the `contracts` job must add `--with openapi-spec-validator` in the same push, or it goes red
  (`devops-engineer`, W3-14).

## 2026-10-05 — contract set 1.7.1 (patch): status table and `REPLY_` emitters corrected

**Patch. Documentation only: no schema, example, manifest or code-list change, so every count is
unchanged from set 1.7 (`validate.py` 156 / 0 predicted). No invariant changes.** Both edits correct
statements that had become false; neither changes behaviour.

- **`tributary.channel-spec/v0` README 3.0.** Receivers `http`, `sftp` and `kafka`: **Built**, spec read
  from `_tb.<env>.channels` (`1cd9fe4`, `359f5fd`, `737fcd8`, security-reviewed, as relayed by the lead;
  lead reports 105-112 integration tests green in CI; the architect read `receivers/cli.py` and
  `receivers/startup.py`, did not run them). `http` keeps the caveat: sync reply not built, `source.reply`
  refused at startup. `mllp` row: the reply consumer no longer borrows the worker's settings
  (`c5fc09a`, `e563d42`). Loaders `clickhouse`, `mllp`, `kafka` were still "Contracted, not built";
  now **Built**, each with its 5.1 redelivery test named; `postgresql`'s redelivery test now exists.
  All of them become "Implemented" when the tests are confirmed green by name in the CI log.
- **`tributary.envelope/v0` `error-codes.json`.** `REPLY_BODY_OVER_BUDGET` listed the `http` receiver
  as an emitter, and so did `REPLY_TIMEOUT`, `REPLY_ROUTE_LOST`, `REPLY_BUSY` and `REPLY_UNAVAILABLE`
  since set 1.2. The `http` receiver refuses `source.reply`, so it emits none of them. Emitter strings
  now say `mllp` only. `set` is `1.7.1`. The `tributary-sdk` cross-check compares codes only, so it is
  unaffected.

**Verification owed:** none beyond set 1.7's. `uv run pytest packages/tributary-sdk/tests/test_codes_contract.py`
stays green (no code changed), and `error-codes.json` must still parse as JSON.

## 2026-10-05 — contract set 1.7: receiver-side sync-reply rules, two codes, N4 shadowing

**Additive. No schema, example or manifest change, so no document changes validity and no
expected-failure pair moves. No invariant changes.** A minor set rather than a 1.6.1 patch because
`envelope/v0` `error-codes.json` gains two codes, which is how set 1.4 treated `REPLY_EXPIRED`.
Sources: the lead's batch on the MLLP sync-reply receiver (`8bfe205`, `33b2207`, `f420634`,
`4134ab3`, `c051002`) and `platform-engineer`'s `a120747`. Checked by reading the code, not by a run.
Decisions and what they give up: **ADR-007 D24**, **ADR-003 D10 amendment (set 1.7)**.

| # | Question | Ruling and why |
|---|---|---|
| 1 | N4: refuse when two installations share a distribution name? | **No. Resolve against the installation `importlib.metadata` shows (first on `sys.path`, also what `import` loads), log each hidden one `duplicate`, refuse duplicates at image build.** A refusal does not stop the threat: write access to an earlier `sys.path` entry can shadow the module with no metadata, or drop a `.pth`. It does turn a stale `.dist-info` or an editable-plus-wheel dev install into a refused channel. Same rule for context extensions (L5 gains `duplicate`). ADR-007 D24, follow-ups 18 (now required, covers duplicate names) and 19 |
| 2 | `body: step` body with a framing byte, or over the `queue.maxBytes` budget | **Outcome ratified (`failed` reply, `AE`; the message was committed and routed). Codes not ratified:** `REPLY_BUSY` is an `AR` code and `REPLY_BODY_MISSING` is the worker's no-`ctx.reply` code. **New: `REPLY_BODY_INVALID`** (`0x0B`/`0x1C` in the body, MLLP) **and `REPLY_BODY_OVER_BUDGET`** (body cannot be charged when it arrives; the receiver does not wait). Sender-facing and metrics only, like `REPLY_TIMEOUT` |
| 3 | Per-connection `maxPending` fairness | **Ratified as built, documented as normative:** `max(1, maxPending // 4)` per connection, over it `AR` / `REPLY_BUSY`, nothing published; the two refusals counted separately. Not a spec field. Give up: a single-connection channel uses a quarter of the pool |
| 4 | `group.instance.id` for the replier | **Deployment rule recorded:** `<instance>-p<pid>`; `--instance` required with `source.reply`; unique per replica; Helm templates it from the pod name; a fenced process is not ready and exits non-zero (`c051002`) |
| 5 | Typed `reply_consumer` defaults | **Already typed in `topics/v0` since set 1.2** (15000 / 5000 / `latest` / `read_uncommitted`). The defect is the receiver borrowing `worker_consumer` and writing literals. Ruled: read `clients.reply_consumer`; `isolation.level` added to `must_not_hardcode`; new `group.instance.id_format` and `read_from_here` notes |
| 6 | README 3.0 | `mllp` sync reply **Built** (`body: ack` and `body: step`); `http` sync reply **not built**, refused at startup |

### `tributary.channel-spec/v0`

README only: status line; 3.0 `mllp` and `http` rows; 3.2.6 `maxPending` row, two answer-table rows,
the fairness wording on the `AR` row, **new "Receiver rules added in set 1.7"**; 3.3.5 N4 shadowing
paragraph and one threat-table row.

### `tributary.envelope/v0`

`error-codes.json` set 1.7: `REPLY_BODY_INVALID`, `REPLY_BODY_OVER_BUDGET`. README 2.3.2 heading and a
set 1.7 paragraph.

### `tributary.topics/v0`

`topic-defaults.json`: `must_not_hardcode` gains `clients.reply_consumer.isolation.level`;
`clients.reply_consumer` gains `group.instance.id_format` and `read_from_here` (descriptive strings; no
value changed). README: status line, 3.3.1.

### `tributary.context-extension/v0`

README: status line, L5 `duplicate` reason for a shadowed installation.

### Change requests that follow

- **`platform-engineer`, with this contract commit or immediately after:** `REPLY_BODY_INVALID` and
  `REPLY_BODY_OVER_BUDGET` in `ENGINE_CODES`, or the `tributary-sdk` registry cross-check goes red.
  Also: `TopicDefaults` parses `clients.reply_consumer` as its own typed profile, and `consumer_config`
  accepts it; shadow-aware discovery and the `duplicate` line in `extensions.py` (ADR-007 follow-up 19).
- **`connector-engineer`:** `pending.py` emits the two new codes in place of `REPLY_BODY_MISSING` and
  `REPLY_BUSY` on the body paths, with tests; `feed.py` reads `clients.reply_consumer` (no literals) with
  the 3.7 effective-configuration test; a separate metric for the per-connection refusal; confirm the
  `http` receiver refuses `source.reply` at startup (grep finds no `reply` in `receivers/http`).
- **`devops-engineer`:** Helm sets a unique `--instance` per receiver pod from the pod name; Compose
  per service; the image-build check (ADR-007 follow-up 18) is now required and also refuses duplicate
  distribution names.
- **`qa-engineer`:** the counts below.

### Verification owed

Not run by the architect, who has no shell.

- `validate.py`: expected **156 / 0, unchanged from set 1.6** (itself predicted, unverified). If set 1.6
  measured N, expect N. `validate.py` reads none of the edited JSON files' changed keys
  (`error-codes.json` and `topic-defaults.json` are outside it; `step-namespaces.json` is unchanged).
- `--emit-expectations`: not needed; no invalid example or schema changed.
- `pytest tests/conformance`: unchanged from the set 1.6 measurement.
- `uv run pytest packages/tributary-sdk`: **red** until `ENGINE_CODES` gains the two codes.

## 2026-09-30 — contract set 1.6: each `uses` step loads from one pinned distribution

**Additive for every document.** Every channel spec valid under set 1.5 stays valid:
`pipeline.stepNamespaces` is optional, and the `uses` pattern is unchanged. **No invariant changes.**
**Runtime behaviour changes** once `platform-engineer` lands N1-N6: a `uses` name is refused if its
namespace is unbound, if two installed entry points carry it, or if its entry point comes from another
distribution. Nothing in the repo relies on the old behaviour. Every `uses` in `contracts/`,
`packages/`, `tests/` and `deploy/` is in `tributary.core`, `tributary.hl7v2` or
`tributary.content_hl7_fhir`, except the runtime's own unit tests that inject fake entry points
(`t.broken`, `t.exiting`; checked by grep, not by a run).

Source: `security-reviewer` Medium on re-review, pre-existing. `steps.py:78-82` loaded the **first**
installed `tributary.steps` entry point carrying a name: no duplicate check, no distribution pin. It is
the typosquat class set 1.5 closed for context extensions. `platform-engineer` proposed rule S1
(engine registry, exactly-one, distribution match, a skip log line, a `validate.py` check). Decision
and what it gives up: **ADR-007 D23**.

| # | Question | Ruling and why |
|---|---|---|
| 1 | S1 as proposed | **Approved, amended.** Registry, exactly-one, PEP 503 distribution match, refusal with `ChannelSpecError`, never loading another entry point: all kept. Confirmed against the repo: three namespaces register `tributary.steps` entry points (`tributary.core` by `tributary-runtime`, five names; `tributary.hl7v2` by `tributary-hl7v2`, one; `tributary.content_hl7_fhir` by `tributary-content-hl7-fhir`, two). The prefix cannot yield the distribution (`tributary.core` is not `tributary-core`), so a registry is needed |
| 2 | Third-party and customer steps (the amendment) | **The engine registry covers only the reserved `tributary.*` namespaces. Every other namespace is bound per channel by the new optional `pipeline.stepNamespaces` (`{namespace: distribution}`).** S1 as written refused every non-registered namespace, so each hospital's in-house step package would need an upstream contract change, contrary to ADR-001 2.2 ("third-party steps as Python packages via entry points"). **Rejected:** an operator-supplied namespace file or env var (the reviewer cannot see it, and "what code ran" would depend on the site rather than on `tb-channel-version`; the same reasons as D21); exactly-one without a distribution pin for non-engine names (still loads a look-alike when the real package is absent) |
| 3 | Namespace shape | **The first two segments; a `uses` name needs at least three.** Enforced at runtime, not by tightening the schema pattern, which would be breaking. All 34 `uses` values in the set 1.5 examples already have three segments |
| 4 | Log lines | **`step_entry_point_loaded`** (info: `step`, `distribution`, `version`, `channel`, `channel_version`) and **`step_entry_point_skipped`** (warning, `reason`: `duplicate`, `distribution-mismatch`, `unregistered-namespace`), including for any installed `tributary.*` entry point that fails the registry even if no spec names it. No `not-referenced` line |

### `tributary.channel-spec/v0`

- **New `step-namespaces.json`**: the engine registry, three namespaces.
- **Schema:** `$defs/pipeline.properties.stepNamespaces` (object, 1-8 entries; keys are two segments,
  64 characters at most, and never `tributary.*`; values are PEP 503 normalised names). Top-level
  description and the `step` `$comment` updated. The `uses` pattern is unchanged.
- **README:** status line, section 2 row, 3.3 table row, the 3.3.1 `uses` row, **new 3.3.5** (rules
  N1-N6, threat table, give-ups).
- **Examples** (predicted, unmeasured):

| Example | Kind | Pinned pair |
|---|---|---|
| `valid/third-party-step-namespace.json` | valid, `runnable: false` (fictional `acme-lab-steps`) | none |
| `invalid/step-namespace-reserved-prefix.json` | invalid | `$.spec.pipeline.stepNamespaces` / `not` (same mechanism as the measured `step-with-inline-secret`) |
| `invalid/step-namespaces-empty.json` | invalid | `$.spec.pipeline.stepNamespaces` / `minProperties` (first use; least certain) |

### `contracts/validate.py`

New `check_step_namespaces()`, run before the suites like `check_context_registry()`, adds no count.
It exits non-zero if a registry namespace is not a two-segment `tributary.*` name with a normalised
distribution, if any channel-spec example (valid or invalid) has a `uses` with fewer than three
segments or in a namespace neither registered nor declared in its own `stepNamespaces`, or if the
`tributary.steps` entry points in `packages/*/pyproject.toml` (read with `tomllib`, stdlib) and the
registry disagree in either direction.

### Change requests that follow

- **`platform-engineer`** (owner of `steps.py`, `spec.py`):
  - parse `pipeline.stepNamespaces` strictly (like `context`: a malformed value or a `tributary.*` key
    refuses the spec rather than being ignored, because dropping it would refuse every customer step
    later with a less useful error);
  - a runtime copy of the registry with a two-way test against `step-namespaces.json`; reuse
    `capabilities.normalise_distribution`;
  - the step resolver needs the spec's bindings (N3). Suggested, not required: an optional keyword on
    `load_step` defaulting to "engine namespaces only", so existing callers (`resolve_custom` in
    `test_envelope_limits_and_budget.py`, the corpus test) keep working, and `build_plan` passes the
    spec's map;
  - N4: select by name from metadata, refuse on zero, more than one or a mismatch, and call `load()`
    only on the verified entry point; N5 log lines; N6 seam (`points`, `registry`), as `ContextResolver`
    already has;
  - tests: a look-alike duplicate is refused and **never imported** (a `load()` that raises must not be
    reached); a distribution mismatch is refused; a missing-distribution entry point is refused; a
    two-segment name is refused; a customer namespace resolves only with its binding; a `tributary.*`
    binding in the spec is refused; `importlib.metadata` de-duplication of one distribution seen twice
    (the N4 claim the architect did not verify). Update `t.broken` and `t.exiting` in
    `test_tree_execution.py` to a registered three-segment name and a fake distribution through the
    seam, so they keep testing the import-failure path rather than the new namespace refusal.
- **`qa-engineer`:** measure the counts below.
- Optional, **`devops-engineer`:** fail the image build if any installed `tributary.steps` entry point
  in a `tributary.*` namespace is not from its registered distribution (ADR-007 follow-up 18).

### Verification owed

Not run by the architect, who has no shell.

- `validate.py`: expected **156 / 0** (set 1.5's predicted 153, itself unverified, + 1 valid + 2
  invalid channel-spec). If set 1.5 measured N, expect N + 3.
- `validate.py --emit-expectations`: the two pairs above.
- `pytest tests/conformance`: +3 cases over the set 1.5 measurement.
- `tributary-runtime` corpus and parse tests: +1 parametrised case each (`test_spec.py` globs
  `valid/*.json`; `test_core_steps.py` reads the manifest). The new example parses under today's
  lenient runtime, which ignores `stepNamespaces`, and stays `runnable: false`.

## 2026-09-29 — contract set 1.5: context extensions load only when the spec declares them

**Additive for every document.** Every channel spec valid under set 1.4 stays valid, because
`pipeline.context` is optional. One valid example gains the field and stays valid. **No invariant
changes.** **Runtime behaviour does change**: once `platform-engineer` lands L1, a channel that omits
`context` no longer receives installed extensions. Nothing deployed relies on the old behaviour. It
arrived in `fb60f3a` on this wave branch, and the one channel that uses `ctx.fhir` (the
exit-criterion-1 example) now declares it.

Source: `security-reviewer` High on `fb60f3a`. Any installed distribution that registered
`tributary.context`, including a transitive or typosquatted one, ran in every worker with the full
payload and `ctx.secrets`, while `tributary.steps` loads only names the reviewed spec references
(`steps.py:78`). The start-up probe checks delegation. It cannot enforce invariants 6 or 7. Decisions
and what they give up: **ADR-007 D21-D22**.

| # | Question | Ruling and why |
|---|---|---|
| 1 | Where the allow-list lives | **In the channel spec (`spec.pipeline.context`), with each name pinned to one distribution by a contract registry.** The worker imports only declared entry points and refuses a spec whose name is unregistered, duplicated or supplied by another distribution. Deployment config (env var or file) is **rejected** as the allow-list: invisible to the spec reviewer, it varies per deployment so "what code ran" is not answerable from `tb-channel-version`, and each worker serves one channel, so it would restate the spec. Invariant 3 holds either way (the spec comes from `_tb.<env>.channels`). Per-step declaration is **rejected**: steps on one branch share one wrapped view (one `ctx.fhir` per message, W2-2) |
| 2 | Record the group as a contract | **New `context-extension/v0`**: entry-point group and name grammar, the factory `() -> ContextExtension`, `ContextExtension = Callable[[Context, Message], Context]` (a `ReplyContext` in, a `ReplyContext` out), loading rules L1-L7, plugin obligations P1-P7 (delegate with `log`/`secrets`/`vars`/`kv` identity; no own logger; no global mutable state; no side effects outside `ctx`, no I/O in set 1.x; deterministic per message; fake shipped; import side-effect free) |
| 3 | Loaded log line | **`context_extension_loaded` carries `extension`, `distribution`, `version`, `channel`, `channel_version`.** New `context_extension_skipped` with `reason` (`not-declared` info; `unregistered`, `distribution-mismatch` warning) makes a surprise registrant in the image visible without importing it |

### `tributary.context-extension/v0` (new)

`README.md` and `capabilities.json` (registry: `fhir` → `tributary-fhir`, `ctx.fhir` and
`ctx.terminology`). Not in `FILE_CONTRACTS`: it validates no instances. `validate.py` gains
`check_context_registry()`, which exits non-zero if the schema enum and the registry differ. It adds
no check to the count.

### `tributary.channel-spec/v0`

Schema: `$defs/pipeline.properties.context` (array, 1-8, `uniqueItems`, items `enum: ["fhir"]`).
The top-level description is updated. README: status line, section 2 row, 3.3 table row, **new 3.3.4**.
Example `valid/adt-mllp-to-fhir-clickhouse-mllp.json` gains `"context": ["fhir"]`, and `tests/e2e`
derives the exit-criterion-1 spec from it. Three invalid examples, **predicted, unmeasured**:

| Example | Pinned pair |
|---|---|
| `pipeline-context-unregistered` | `$.spec.pipeline.context[0]` / `enum` |
| `pipeline-context-duplicate` | `$.spec.pipeline.context` / `uniqueItems` (first use here; least certain) |
| `pipeline-context-empty` | `$.spec.pipeline.context` / `minItems` |

### Change requests that follow

- **`platform-engineer`** (owner of `extensions.py`, `worker.py`, `spec.py`):
  - parse `pipeline.context`, refusing a non-list or unknown name, and resolve it with the steps per
    channel version (L1, L2, L6); move loading after the spec is read;
  - never `load()` an undeclared name, and pin the distribution against a runtime copy of the
    registry, with a two-way test against `capabilities.json`;
  - the probe calls `reply` for real and checks `base.reply_body`, expects `reply` to raise on the
    destination context, and checks `view.log is base.log` and `view.secrets is base.secrets` (L3);
  - log lines per L5, with `version` from `point.dist.version`.
  - Tests: an undeclared registrant whose import would raise is never imported; a typosquat
    duplicate and a wrong distribution are each refused; a wrapper that copies `log` fails the probe.
- **`healthcare-standards-engineer`:** P1-P7 for `tributary-fhir`. The code already delegates by
  identity (`_Attached`, read, not run). Add tests for `log`/`secrets` identity and for `reply`
  forwarding over `FakeContext`. Update the `runtime.py` docstring ("loaded when a channel declares
  `fhir`"). Document on `tributary.content_hl7_fhir.adt` (and `oru`) that the step needs
  `context: [fhir]`.
- **`qa-engineer`:** the exit-criterion-1 spec declares `fhir`, inherited from the example. Add an
  assertion in `build_spec` that it does, and a control with `context` removed that must dead-letter
  the FHIR branch. Measure the counts below.
- Optional, **`devops-engineer`:** fail the image build if any installed `tributary.context`
  registrant is not in `capabilities.json` (ADR-007 follow-up 15).

### Verification owed

Not run by the architect, who has no shell.

- `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expected
  **153 / 0** (set 1.4's 150, unverified, + 3 channel-spec invalid).
- `validate.py --emit-expectations`: the three pairs above.
- `pytest tests/conformance`: +3 cases over the set 1.4 measurement.
- The runtime corpus test: unchanged. `adt-mllp-to-fhir-clickhouse-mllp.json` stays `runnable: false`
  for its existing `requires`, and today's runtime ignores `context` (lenient parse, `spec.py`).

## 2026-09-29 — contract set 1.4.1 (patch): `kafka-users-init` is not a super user

**Additive, documentation only. No schema, example or manifest changes. No invariant changes.** Expected
`validate.py` stays **150 / 0**, unverified: the architect has no shell, and `validate.py` does not
read `topic-defaults.json`. Source: `devops-engineer`, `5495498`. Checked against
`deploy/compose/kafka/tls-init.sh` (`super.users=User:CN=kafka;User:tb-lite-topic-bootstrap;User:tb-lite-admin`)
and `start-broker.sh` (`kafka-acls --add --allow-principal User:CN=tb-lite-users-init --operation Alter
--cluster`, run with `broker-client.properties` on `127.0.0.1:9096` before `/tmp/tb-ready`). The
checks were done by reading the files, not by a run.

| # | Change | Why |
|---|---|---|
| 1 | Super users: broker (`CN=kafka`), `tb-lite-topic-bootstrap`, `tb-lite-admin`. **`CN=tb-lite-users-init` is removed**: it holds only ALTER on CLUSTER, granted by `start-broker.sh` with the broker certificate | ALTER on CLUSTER is what `AlterUserScramCredentials` needs. Set 1.4 made the provisioning identity a super user, which is more than it needs |
| 2 | Recorded caveat: ALTER on CLUSTER also authorises `CreateAcls` and `DeleteAcls` (Kafka "Authorization and ACLs" operations table, API keys 30, 31, 51). The identity can still grant itself more, so its key stays super-user-grade | Calling the grant "least privilege" without this caveat would overstate it. The gain is real: every further right now needs an explicit, listable ACL write |

### `tributary.topics/v0`

README: status line, 3.4 `super.users` row. `topic-defaults.json`: the **description string**
`security.broker.super.users`. This is the one non-README edit. No code or test reads that key
(`bootstrap_topics.py` reads only `security.acls`, and `validate.py` does not load the file; checked
by reading, not by a run), and no `must_not_hardcode` value changed.

### `tributary.engine-kafka/v0`

README only: status line, section 6 rule 3 (table row, `super.users` value, ALTER-on-CLUSTER note).

ADR-008 D8 amended; follow-up 9 (optional: revoke the grant after provisioning; fix the
`add_scram_users.py` docstring).

## 2026-09-29 — contract set 1.4: ACL role table reconciled with the code, super users, `expectedBucketOwner`, `REPLY_EXPIRED`

**Additive for every document:** every envelope, event, channel spec and engine-kafka document valid
under set 1.3 stays valid. One valid example gains an optional field without changing its validity.
**No invariant changes.** The one deployment-facing change is `topics/v0` `security.acls`. It **adds**
grants that code paths needed, **removes** grants no code uses, and **narrows** two undelimited group
prefixes. No role loses a grant that any code in `packages/` exercises (checked by reading the
code, not by running it). Recorded in ADR-008 D6-D9 and the ADR-003 D10 amendment.

Source: `devops-engineer`'s first deny-by-default run of the Compose lite stack. The loader exited with
`ChannelSpecError: channels topic _tb.dev.channels is not available` (its `TOPIC_AUTHORIZATION_FAILED`),
and the worker logged `GROUP_AUTHORIZATION_FAILED` (error code 30) on its spec reader. With a temporary
READ grant, MLLP → worker → loader ran end to end. Four queued items from the lead are included.

| # | Question | Ruling and why |
|---|---|---|
| 1 | Data-plane roles cannot read `_tb.<env>.channels` | **Receiver, worker and loader each get READ on `_tb.<env>.channels`, plus READ on group PREFIXED `tb.<env>.<ch>.channels.`**, the spec reader's per-process group (`tb.<env>.<ch>.channels.<instance>`, built as an f-string in `worker.py` and in every `loaders/*/main.py`). The reader uses `assign()` and never commits, but librdkafka still looks up the coordinator for a configured `group.id`. WRITE stays control-plane only. **Receivers** read a spec file today (wave-1 stand-in, an invariant-3 gap), so their grant comes ahead of the migration. Named in `consumer_groups.spec_reader`. topics README 3.4.1 rule 1 |
| 2 | Reconcile the whole table against the code | Worker: WRITE on `.retry` **removed** (nothing writes it, and under the 4.1.1 design only the loader would). Pipeline group `pipeline*` → LITERAL `pipeline` + PREFIXED `pipeline.shadow-`. **New `pipeline_worker_transactional`**: WRITE on transactional id prefix `tb.<env>.<ch>.pipeline.`, which was missing, so a transactional worker could never start on a deny-by-default broker. Kept out of the parsed string, and it fails closed. Loader: READ/WRITE on `.retry` **removed** (`retry-topic` refused, no code touches it); group `loader.<dest>*` → LITERAL `loader.<dest>` (no loader builds a shadow group). Receiver sync-reply group `replier*` → LITERAL `replier`. IDEMPOTENT_WRITE not added to worker or loader: WRITE on a topic suffices since Kafka 3.0 (KIP-679), and the lite run confirms it for both producers. Archiver unchanged, **unverified** (no package yet) |
| 3 | `engine-kafka/v0` 6 rule 3: super users | **Amended to the implementation.** Super users: `CN=kafka` (broker, mTLS), `CN=tb-lite-users-init` (`kafka-users-init`, mTLS over the loopback `ADMIN` listener, one-shot), `tb-lite-topic-bootstrap`, `tb-lite-admin`. `tb-lite-channel-publish` is **not** one: its `control_plane` grants only. `CN=tb-lite-healthcheck` has **no ACL** (it sends only `ApiVersions`). No data-plane role and no `ANONYMOUS`. The security review judged this acceptable and tighter than the set 1.3 text. `topic-defaults.json` `security.broker.super.users` (was `TODO`) and topics README 3.4 restated to match |
| 4 | Loaders' shared `.retry` READ across destinations | **Ruled now, applied on lift.** Offsets are per `(group, topic, partition)`, so skipping happens only if destinations **share a group**. Each destination consumes `retry` in its own group `tb.<env>.<ch>.loader.<dest>` and filters on `tb-destination`; a channel-wide retry group is forbidden, and the LITERAL group ACL enforces that at start. Rejected: a per-destination retry topic (D−1 more topics per channel against the S4 budget; grammar change). **Give up:** read amplification × destinations, and loaders read sibling destinations' retry records within one channel. topics README 3.4.1; channel-spec README 4.1.1 |
| 5 | `expectedBucketOwner` on `s3` | **Added**, optional, `^[0-9]{12}$`. The `s3` loader already sends it when configured. Optional because some S3-compatible stores lack the check, and required would be breaking. channel-spec README 3.4.5 |
| 6 | A code for an expired reply | **`REPLY_EXPIRED` registered**, carried as `error_code` on the `replied` event, which **keeps `status: dropped`** (a deliberate skip, not an error-rate event). Rule: `dropped` carries a code only for a registered engine-initiated skip; `Drop` reasons stay in `error_message`. No schema change (the events schema already permits it). The sender still gets `AE` / `REPLY_TIMEOUT`. Rejected: no code (not queryable by a stable key), or `status: error` (would page on a backlog the engine handled correctly). envelope README 2.3.2 |

### `tributary.topics/v0`

`topic-defaults.json`: `security.broker.super.users`; `security.acls` `grammar`, `reconciled_with_code`,
`receiver`, `pipeline_worker`, `pipeline_worker_transactional` (new), `loader`, `loader_retry_topic`
(new, not granted), `archiver_note` (new); `consumer_groups.spec_reader` (new). No value that
`must_not_hardcode` lists changed. README: status line, section 1 `retry` row, 3.4 `super.users` row,
**new 3.4.1**, 3.6.

**ACL table diff** (lite parser output for `dev` / `adt-passthrough` / `http-stub`; + added, − removed):

| Role | Change |
|---|---|
| receiver | + TOPIC `_tb.dev.channels` LITERAL READ; + GROUP `tb.dev.adt-passthrough.channels.` PREFIXED READ |
| pipeline_worker | − TOPIC `dev.adt-passthrough.retry` LITERAL WRITE; − GROUP `tb.dev.adt-passthrough.pipeline` PREFIXED READ; + GROUP `tb.dev.adt-passthrough.pipeline` LITERAL READ; + GROUP `tb.dev.adt-passthrough.pipeline.shadow-` PREFIXED READ; + GROUP `tb.dev.adt-passthrough.channels.` PREFIXED READ |
| loader | − TOPIC `dev.adt-passthrough.retry` LITERAL READ and WRITE; − GROUP `tb.dev.adt-passthrough.loader.http-stub.` PREFIXED READ; + TOPIC `_tb.dev.channels` LITERAL READ; + GROUP `tb.dev.adt-passthrough.channels.` PREFIXED READ |
| control_plane | unchanged |

### `tributary.engine-kafka/v0`

README only: status line, section 1 scope table (worker produces no `retry`; the loader reads
`_tb.<env>.channels`; the receiver will), section 6 rule 3 (super users table).

### `tributary.channel-spec/v0`

Schema: `$defs/s3Params.expectedBucketOwner`. README 3.2.6 (expired-reply code), 3.4.5, 4.1.1 (ACL and
group on lift). Examples: `valid/sftp-drop-to-s3-and-sftp.json` gains `expectedBucketOwner:
"111122223333"` (AWS's documentation placeholder account id); new
`invalid/destination-s3-bucket-owner-not-account-id.json`, **predicted, unmeasured**:

| Example | Pinned pair |
|---|---|
| `destination-s3-bucket-owner-not-account-id` | `$.spec.destinations[0].with.expectedBucketOwner` / `pattern` |

### `tributary.envelope/v0`

`error-codes.json` set 1.4: `REPLY_EXPIRED`. README 2.3.2 set 1.4 paragraph.

### `tributary.events/v0`

README field table (`error_code` on `dropped`). New `valid/replied-expired.json`. No schema change.

### Change requests that follow

- **`platform-engineer`, same commit as this contract or immediately after:** `REPLY_EXPIRED` in
  `ENGINE_CODES`, or the registry cross-check goes red. `reply.py` emits it on the expired path.
  Add a `spec_reader_group(env, channel, instance)` helper to `tributary.runtime.topics` and use it in
  `worker.py`; `connector-engineer` switches the eight `loaders/*/main.py` copies of the f-string to it.
- **`devops-engineer`:** update the three `test_bootstrap_topics.py` tests the diff above breaks
  (receiver exact set; worker `.retry` and `pipeline` PREFIXED; loader `.retry`, `loader.http-stub.`
  PREFIXED, and the now-inverted "no `_tb.dev.channels`" assertion). Then remove the temporary READ
  grant, recreate the stack (`down`; ACLs are only ever added), and re-run MLLP → worker → loader.
- **`connector-engineer`:** the eight loader copies onto `spec_reader_group`; receivers onto
  `open_spec_source` (invariant 3), for which the grant is already in place.
- **`qa-engineer`:** measure the counts below, and add ACL negative cases to the Compose e2e: the
  loader cannot READ `.retry`, and a principal holding `loader.fhir` cannot join `loader.fhir-server`.

### Verification owed

Not run by the architect, who has no shell.

- `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expected
  **150 / 0** (set 1.3's 148, unverified, + 1 channel-spec invalid + 1 events valid).
- `validate.py --emit-expectations`: **one** new pair, above.
- `pytest tests/conformance`: +2 cases over the set 1.3 measurement.
- `pytest deploy/compose/topic-bootstrap`: **3 known failures** until `devops-engineer` updates them.
- `uv run pytest packages/tributary-sdk`: red until `ENGINE_CODES` gains `REPLY_EXPIRED`.
- `pytest packages/tributary-loaders/tests/s3`: unchanged, green; the loader already accepts the field.

## 2026-09-29 (later) — contract set 1.3.1 (patch): exact engine-Kafka key allow-list, `ctx.reply` shape, `s3` licences

**Patch. No schema, example or manifest changes**, so `validate.py` and conformance counts are
unchanged from set 1.3 (expected 148 / 0). No invariant changes. Documentation of rules that were
already implemented, plus two human licence decisions.

| # | Contract | Change | Why |
|---|---|---|---|
| 1 | `engine-kafka/v0` README 4 rule 4 | `ssl.*` / `sasl.*` prefixes → **exact ten-key allow-list** (`security.protocol`, `sasl.mechanism`, `sasl.username`, `sasl.password`, `enable.ssl.certificate.verification`, `ssl.endpoint.identification.algorithm`, `ssl.cipher.suites`, `ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location`) | `platform-engineer` implemented the exact list (`tributary.runtime.kafka_security.SECURITY_KEYS`) and flagged that a prefix admits `ssl.engine.location` and `ssl.providers` (shared-object loading) and inline PEM keys. The list is exactly what the schema can express, so no valid document is affected. Narrowing only what the loader's internal map accepts is why this is a patch, not a breaking change. The schema encodes no prefixes and is unchanged. ADR-008 D4 amended |
| 2 | `channel-spec/v0` README 3.2.6 | `ctx.reply` lives on the additive `ReplyContext(Context)` protocol, not on `Context`. `Context` is frozen for set 1.x, and new capabilities arrive as sub-protocols. `attach()` of a `ReplyContext` must become a `ReplyContext` before `body: step` is lifted | Adding a `Protocol` member breaks every structural implementer (`tributary-fhir`'s `_Attached` failed `mypy --strict`). ADR-003 D10 |
| 3 | `channel-spec/v0` README 3.0 | `s3` row: licence questions **resolved** (human, 2026-09-29): `botocore` approved in the runtime for the `s3` loader including its bundled `cacert.pem`; MinIO test-only, never shipped | ADR-001 2.3 |

**Follow-ups:** `connector-engineer` moves `RawPublisher` onto the runtime loader and deletes the
prefix check in `tributary.receivers.engine_kafka`. This is shadowed today by `KafkaSettings`'
exact-list check, read from the source and not run (ADR-008 follow-up 6). `healthcare-standards-engineer`
makes `attach()` of a `ReplyContext` a `ReplyContext`. `devops-engineer` records botocore's bundled-file
scope in `licence_check.py` (ADR-001 2.3).

## 2026-09-29 — contract set 1.3: engine Kafka credentials, mTLS client allow-list, W2-6 loader codes

**Additive throughout; no invariant changes.** Every document valid under set 1.2 stays valid. One valid
channel-spec example changes content without changing validity (`auth` → `bearer`), and two gain
manifest `requires` entries (below). Sources: the W2-7
security review HIGH (receivers produced to `raw` over PLAINTEXT, unauthenticated), fixed for
receivers in `e3ddee1` by `connector-engineer`, whose handoff asked for rulings 1-4; the lead's W2-6
items 5-7. Decisions and what they give up: **ADR-008** (D1-D5) and **ADR-007 D18-D20**.

| # | Question | Ruling and why |
|---|---|---|
| 1 | Where engine-side Kafka credentials come from, for every data-plane role | **New contract `engine-kafka/v0`**: one JSON document per process, named by `TB_ENGINE_KAFKA_FILE`, holding `bootstrapServers` and the channel-spec `kafkaSecurity` shape tightened (SSL must carry a client certificate), with secrets by `secretRef` from local files under `TB_SECRETS_DIR` (invariant 3). Same for receivers, workers, loaders and the archiver. `--bootstrap`, `--engine-security` and `TB_BOOTSTRAP_SERVERS` are retired. **Workers and loaders enforce the same rules as `e3ddee1`**, from one implementation in `tributary-runtime`: PLAINTEXT only to loopback brokers with no opt-out, verified TLS with the AEAD pin, credentials present, and no property passthrough. Rejected: per-role CLI flags (every role parses differently; this is how the gap arose), env vars carrying values (visible in `docker inspect` and `/proc`), fetching from the control plane (invariant 3). ADR-008 D1-D3 |
| 1a | Found while ruling: `KafkaSettings.security` is merged **after** the contract's producer defaults | **Defect, invariant 1.** `producer_config` builds `{..., **defaults.producer, **settings.security}`, so a security map carrying `acks` or `enable.idempotence` silently overrides them. `engine-kafka/v0` section 4 rule 4: only `security.protocol`, `sasl.*`, `ssl.*` and `enable.ssl.certificate.verification` are accepted, and contract values win. ADR-008 D4 |
| 2 | Compose lite stack: SASL_SSL, or a dev-only PLAINTEXT opt-out | **SASL_SSL, no opt-out.** A dev CA, broker certificate and per-component SCRAM users generated at `up`; deny-by-default ACLs applied from `topic-defaults.json`. `engine-kafka/v0` section 6. Rejected: an opt-out flag, because no component can tell a Compose network from a production network (both are non-loopback) and lite is also allowed for small sites that carry PHI (`topics/v0` 3.1); a shared network namespace (`network_mode: service:kafka`), which makes "loopback" literally true but leaves TLS, SCRAM and the ACL model unexercised until Helm. ADR-008 D5 |
| 3 | `tls.allowedSubjects` for the HTTP receiver | **Approved with amendments** as `tls.allowedClientSans`: typed `dns:`/`uri:` SAN entries only, exact match, no wildcard, 1-32 entries, only with `clientAuth: required`, on `mllp` as well as `http`. Checked after the handshake and before any application byte is read; `403` + close on `http`, close on `mllp`; code `TLS_PEER_NOT_ALLOWED`; the log carries the certificate fingerprint, never its names. Rejected: subject DN strings (no canonical form, so the control's result depends on formatting) and CN. Optional, because requiring it would be breaking. README 3.2.7, ADR-007 D18 |
| 4 | `nackOn: spec-invalid` refused on HTTP and MLLP | **Keep the refusal, on all four source types, documented as reserved.** Nothing detects the condition: receivers do not watch `_tb.<env>.channels`. Stays in the enum (removing it is breaking). Lifts with receiver live reload, which must also define what it means. README 3.2.8, ADR-007 D19 |
| 5 | `oauth2-client-credentials` in the exit-criterion-1 example | **Not defined in set 1.3.** The example's `fhir` destination now uses `bearer`; `oauth2-client-credentials` and `mtls` are documented as reserved and refused at startup by the `http` and `fhir` loaders. Defining OAuth means a token-endpoint client, caching and refresh rules, and for SMART Backend Services `private_key_jwt`, which is key management (ADR-004 and a human). Exit criterion 1 targets HAPI and does not need it. README 3.4.1, ADR-007 D20 |
| 6 | Error codes `FHIR_ENTRY_FAILED`, `FHIR_RESPONSE_INVALID`, `S3_KEY_CONFLICT`, `SFTP_NAME_CONFLICT` | **Registered**, all under prefixes already reserved (`FHIR_`, `S3_`, `SFTP_`). **Extended to every fixed name the loaders emit** (rule 2 had not been applied since set 1.1): `HTTP_REDIRECT_REFUSED`, `FHIR_BUNDLE_INVALID`, `CH_UNKNOWN`, `SSH_HOST_KEY_MISMATCH`, `SSH_AUTH_FAILED`, `S3_KEY_VANISHED`, `S3_NO_CREDENTIALS`, `S3_UNKNOWN`, `MLLP_AE`, `MLLP_AR`, `MLLP_TIMEOUT`, `MLLP_NO_ACK`, plus `TLS_PEER_NOT_ALLOWED`: seventeen. New rule: a minted `S3_<CODE>` equal to a fixed name becomes `S3_UNKNOWN`. `HTTP_<status>` confirmed for non-2xx. envelope README 2.3.2 |
| 7 | README 3.0 status: `fhir`, `sftp`, `s3` loaders | **Moved to Built.** Each has a 5.1 redelivery test in `tests/integration/`; "Implemented" waits for `qa-engineer` to confirm them green in CI. **`s3` carries two open licence questions the lead has taken to the human** (botocore's bundled `cacert.pem`, MPL-2.0 data; MinIO, AGPL-3.0, test-only). Not ruled here. Manifest `connector:destination/{fhir,sftp,s3}` entries removed |

### `tributary.engine-kafka/v0` (new)

`engine-kafka.schema.json`, README, and 11 examples (4 valid, 7 invalid). `contracts/validate.py`
gains `engine-kafka` in `FILE_CONTRACTS`. **Predicted, unmeasured** pairs:

| Example | Pinned pair |
|---|---|
| `plaintext-compose-service-name` | `$.bootstrapServers[0]` / `pattern` |
| `ssl-without-client-certificate` | `$.security` / `required` |
| `sasl-plain-mechanism` | `$.security.saslMechanism` / `enum` |
| `inline-password` | `$.security` / `additionalProperties` |
| `tls-verification-disabled` | `$.security.tls` / `additionalProperties` |
| `librdkafka-passthrough` | `$` / `additionalProperties` (first root-level pair; least certain) |
| `plaintext-with-credentials` | `$.security` / `not` |

### `tributary.channel-spec/v0`

Schema: `$defs/tls.allowedClientSans` with an `allOf` tying it to `clientAuth: required`; new
`$defs/clientSan`; `kafkaSecurity.secretRef` comment corrected (the client certificate is under
`security.secretRef`, not `tls.secretRef`, as both implementations already read it; no validation
change). README: status line, 1 (model snippet now `bearer`), 2, 3.0, 3.2, 3.2.5, new 3.2.7 and 3.2.8,
3.4 family table, 3.4.1, 3.4.2. Examples: new `valid/http-mtls-allowed-client-sans.json`
(`runnable: false`); `valid/adt-mllp-to-fhir-clickhouse-mllp.json` `auth` → `bearer`;
manifest `requires` added to `adt-to-fhir-per-key-mtls` (oauth2) and `two-destinations-fanout`
(`spec-invalid`). Two invalid examples, **predicted, unmeasured**:

| Example | Pinned pair |
|---|---|
| `source-tls-allowed-sans-without-client-auth` | `$.spec.source.tls.clientAuth` / `const` |
| `source-tls-allowed-san-wildcard` | `$.spec.source.tls.allowedClientSans[0]` / `pattern` |

### `tributary.envelope/v0`

`error-codes.json` set 1.3: seventeen codes, three prefix owner notes. README 2.3.2.

### `tributary.topics/v0`

README 3.4: where engine credentials come from, and what "loopback" means (a container network is
not). `topic-defaults.json` `security.clients.credentials` (was `TODO`) and a new
`security.clients.plaintext` note. No value that `must_not_hardcode` lists changed.

### Change requests that follow

- **Must land with this contract commit or immediately after** (`platform-engineer`, owner of
  `tributary-sdk`): the seventeen codes in `ENGINE_CODES`, or the registry cross-check goes red.
- The rest are in STATUS.md (2026-09-29) and ADR-008 section 6, per owner.

### Verification owed

Not run by the architect, who has no shell.

- `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expected
  **148 / 0** (134 + 14). If set 1.2's 134 was never confirmed, 148 depends on it.
- `validate.py --emit-expectations`: **needed** for the nine new pairs.
- `pytest tests/conformance`: `qa-engineer` adds an `engine-kafka` suite; channel-spec gains 3 cases.
- `uv run pytest packages/tributary-sdk` once `ENGINE_CODES` is updated.
- The runtime corpus test: `http-mtls-allowed-client-sans.json` must stay unresolved (it names `ref`
  steps); the three examples whose `connector:` entries were removed still have unresolved entries.

## 2026-09-28 — contract set 1.2: synchronous reply mode (W2-8)

**Additive throughout.** Every document valid under set 1.1 stays valid, including the set 1.0 reply
example, which has no `tb-reply-status` (that header is schema-optional for exactly this reason).
Source: `platform-engineer`'s proposal `docs/agent-team/proposals/W2-8-sync-reply.md` (`b0c91cb`).
Decisions and what they give up: ADR-003 D9 (amended) and D10.

| # | Proposal item | Ruling and why |
|---|---|---|
| 1 | `spec.source.reply` on `mllp` and `http`: `timeoutMs`, `after`, `body`, `maxPending`, `partitions` | **Approved with amendments.** Shape as proposed; `after` is `const: routed` (`delivered` reserved). `reply.partitions` is the only sizing input for `reply` (`spec.partitions` does not apply), since it tracks processes, not throughput. The transactional refusal moves **into the schema** (ADR-007 D7: a rule the schema can hold is held there). The split refusal stays a load-time rule, because a `ref` splitter is unknown until load. `ack.timeoutMs` is now documented as the commit-wait bound, distinct from `reply.timeoutMs` |
| 2 | Targeted routing through the `replier` group | **Approved with three amendments.** (a) `group.instance.id` is unique **per process**, otherwise one process fences another. (b) A process advertises p only once its consumer has an **established fetch position** on p, which closes the `latest`-reset race. (c) A reply record the receiver cannot parse is dropped with topic/partition/offset only, never crashing the consumer. Broadcast is rejected as the default |
| 3a | `AR` = commit not confirmed, `AE` = committed, `AA` = commit confirmed plus `accepted` reply | **Approved with an amendment: `AE` is also never sent before the successful `raw` callback.** As proposed, a revoke arriving before the callback answered `AE` at once, which claims a commit that may still fail. It now waits for the callback: `AE` on success, `AR` on failure. The same rule already held the async receiver to `AA` and `AR` only, so the rule holds in both modes |
| 3b | Pipeline `Reject` → `AE`, not `AR` | **Approved.** It was committed; `AR` would tell the sender a resend is safe. HL7 original mode reads `AE` as a content error anyway |
| 3c | Whole-message `Drop` → `AA` | **Approved** (the proposal's open point). Committed and filtered on purpose. An `onDrop` option is an open item, not in 1.2. Partial failure: status and code of the **first failure in spec order**, not in time (amendment, for determinism). An absent or unknown `tb-reply-status` answers `AE`, never `AA` (amendment, fail-safe) |
| 3d | HTTP column | **Not adopted in 1.2; decided with W2-7.** Binding constraint: not-committed is exactly `503` + `Retry-After`, and no committed outcome uses a status clients retry automatically (`5xx`, `408`, `429`). The proposed `500` for `failed` breaks it |
| 4 | Refusals: transactional, split | **Approved** (transactional now in the schema, split at load). A worker whose applied spec cannot reply skips the reply and counts it; the sender gets a truthful `AE` |
| 5a | Envelope: `tb-reply-partition`, `tb-reply-deadline`, `tb-reply-status`; `tb-error-code` on `reply` | **Approved with amendments.** `tb-reply-status` is schema-optional and code-required: making it `R` would invalidate the set 1.0 valid reply example, which is breaking. `tb-reply-deadline` is `C`, present iff `tb-reply-partition` (was `O`), enforced by `dependentRequired`, and it must not be earlier than the real deadline (produce time + `ack.timeoutMs` + `reply.timeoutMs`), because the reply clock starts at the callback. `tb-reply-partition` pinned to `0`-`63`. A malformed or out-of-range reply-routing header **disables the reply, not the message** (new envelope 2.3.3), the one narrow exception to the 2.3 reject rule. Propagation rule 7: the worker strips both routing headers |
| 5b | `REPLY_` prefix and six exact codes | **Approved with one rename:** `REPLY_PARTITION_INVALID` → `REPLY_ROUTE_INVALID`, because it also covers a malformed `tb-reply-deadline`. The four receiver codes reach the sender and metrics, never a header or event, and are registered anyway so a `Reject` cannot borrow them. `REPLY_BODY_MISSING` fails the reply only; the message was routed, so there is no `dlq` record |
| 5c | ACLs; per-channel reply partition count | **Approved.** Receiver: READ on `.reply` and group `tb.<env>.<ch>.replier*`, no WRITE. Worker: WRITE on `.reply`. `topic-defaults.json` gains `clients.reply_consumer` and three `must_not_hardcode` entries. **Found while writing it:** `consumer_groups.roles` had omitted `receiver` since set 1.1; added |
| 5d | SDK `ctx.reply(body, *, content_type)` | **Approved with amendments:** pipeline steps only (raises in a destination step, where N copies could each answer); a registered content type (`hl7-v2+er7` for `mllp`); body ≤ 1,000,000 bytes; a body set by an attempt that then raised is discarded before the retry; a call on a record that will not reply is discarded and counted, so a step survives a hot reload to async. **Condition on `body: step`:** resolved bodies waiting behind a slower request are charged to `queue.maxBytes` |
| 6 | Section 3: D9's mitigation does not cover a resend after `AE` | **Correct, and ADR-003 D9 is amended.** A resend is a new message with a new `tb-msg-id` (3.2.2). `tb-msg-id` idempotency absorbs worker replays only. The same error was in ADR-001 sections 7 and 13 and in the STATUS risk table; all corrected, and ADR-001 6.5 notes the targeted refinement. The control-plane warning is made unconditional, because every technique is keyed on `tb-msg-id` (5), so "warn when every destination relies only on `tb-msg-id`" would always fire |
| 7 | `events/v0` | **No change.** Whether the browser needs "the sender got `AE`" is open: it needs a receiver ACL on `_tb.<env>.events`, an ACL-model change for ADR-004 |

### `tributary.channel-spec/v0`

New `$defs/syncReply`; `reply` in `source.properties` and in the `mllp` and `http` `propertyNames`
lists; a spec-level `if source.reply then pipeline.transactional const false`. README 3.2.6 (new), 2,
3.2, 3.2.2, 8, 9. Examples: `valid/lab-query-mllp-sync-reply.json` (`runnable: false`) and five
invalid, with **predicted, unmeasured** pairs:

| Example | Pinned pair |
|---|---|
| `reply-on-sftp-source` | `$.spec.source` / `enum` |
| `reply-timeout-below-minimum` | `$.spec.source.reply.timeoutMs` / `minimum` |
| `reply-after-delivered` | `$.spec.source.reply.after` / `const` |
| `reply-unknown-field` | `$.spec.source.reply` / `additionalProperties` |
| `reply-with-transactional-pipeline` | `$.spec.pipeline.transactional` / `const` |

### `tributary.envelope/v0`

Three headers; `tb-error-code` on `reply` conditional; the three forbidden on every class they do not
belong to (they were already invalid everywhere under `additionalProperties: false`, so nothing
narrows). README 1.1, 2, 2.3, 2.3.2, new 2.3.3, 2.4, 3, 6. `error-codes.json` set 1.2. Examples:
`valid/raw-mllp-sync-request.json`, `valid/reply-rejected-with-code.json`, and four invalid, with
**predicted, unmeasured** pairs:

| Example | Pinned pair |
|---|---|
| `reply-rejected-without-error-code` | `$.headers` / `required` |
| `raw-reply-partition-without-deadline` | `$.headers` / `dependentRequired` (first use in these contracts) |
| `raw-reply-partition-out-of-range` | `$.headers['tb-reply-partition']` / `pattern` |
| `out-with-reply-partition` | `$.headers` / `not` |

### `tributary.topics/v0`

README 2, 2.2, new 3.3.1, 3.6, 3.7; `topic-defaults.json` `classes.reply`, `security.acls.receiver`
and `pipeline_worker`, `clients.reply_consumer`, `must_not_hardcode`, `partition_budget`,
`consumer_groups`.

### Change requests that follow

- `platform-engineer`: add the six `REPLY_` codes to `ENGINE_CODES` and `REPLY_` to
  `RESERVED_PREFIXES` **in the same commit as this contract change, or immediately after**: the
  two-way registry cross-check test in `tributary-sdk` goes red until both sides agree.
- `connector-engineer` and `devops-engineer`: the three new `must_not_hardcode` values each need the
  effective-configuration test (3.7) when the reply consumer lands.

### Verification owed

Not run by the architect, who has no shell.

- `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expected
  **134 / 0** (122 + 12 new examples).
- `validate.py --emit-expectations`: **needed**. The nine new pairs above are predictions. The least
  certain is `dependentRequired`, which has no precedent here.
- `pytest tests/conformance`: expected **139 passed** (127 + 12), if the suite still has one case per
  example.
- `uv run pytest packages/tributary-sdk` once `codes.py` is updated: the registry cross-check.
- The runtime corpus test: `lab-query-mllp-sync-reply.json` must stay unresolved (it names a `ref` step).

## 2026-09-26 — `TB_DLQ_UNWRITABLE` registered; `raw` `max.message.bytes` ruled not an envelope control

Both **additive**. No schema, example or invalid-example change, so no expected-failure pair moves.

| # | Contract | Change | Ruling and why |
|---|---|---|---|
| 1 | `envelope/v0` `error-codes.json` | `TB_DLQ_UNWRITABLE`, status `emitted`, emitter the runtime worker loop | **Registered by exact name** (2.3.2 rule 2). A dead letter that breaks the envelope even when reduced (a `raw` payload over 1,000,000 bytes) now halts the partition with a `partition_halted` event carrying this code, instead of crash-looping the worker (`361935e`). It is never on a record: nothing is committed or lost, and the record stays on `raw` |
| 2 | `topics/v0` `topic-defaults.json`, README 3.1 | `raw` gains `max.message.bytes_source`; the value **stays 1,048,576** | Kafka checks `max.message.bytes` against the record batch **after compression**, and `raw` is `compression.type: producer` with receivers producing `lz4` (`clients.receiver_producer`). No broker value can refuse a compressible payload over the 1,000,000-byte inline limit. Lowering it to the envelope-derived bound (1,019,105) would refuse only incompressible oversize records and read as a guarantee it does not give. **Rejected** for that reason. The controls that hold: WRITE on `raw` for receivers only (3.4), receivers refuse input over 1,000,000 bytes (channel-spec 3.2.2), and the worker halts the partition with `TB_DLQ_UNWRITABLE`. The real fix is claim-check. README 3.1's "matches the envelope's 1 MB inline limit" is corrected so the prose no longer implies enforcement |

**Re-run:** `validate.py` (expect 122 / 0) and `pytest tests/conformance` (expect 127). Neither file is
in their scope, so the counts should not move. The `tributary-sdk` registry cross-check test exercises
change 1: `ENGINE_CODES` already includes `TB_DLQ_UNWRITABLE` (`codes.py`).

## 2026-09-25 (later) — W2-5 security follow-ups: filter allow-list, split terminator, two engine codes

From `platform-engineer`'s commits `2a16e24`, `8af0d63` and `6daf4f4`. As before, no schema, example
or invalid-example change, so no expected-failure pair moves.

| # | Change | Ruling and why |
|---|---|---|
| 1 | channel-spec 3.3.2: filter `field` must be exactly one of a 21-entry allow-list; the deny-list is still checked first | **Ratified with one amendment.** The review's security MEDIUM 3 showed the deny-list failing open (`PV1.19`, `ORC.3`, `OBR.3`, `OBX.5`, Z-segments, `/subject/reference`, `/id`). Every listed field holds a small coded vocabulary. **Amendment:** `ORC`, `OBR` and `OBX` repeat within a message, and the evaluator reads the first occurrence. So for the filter, a segment occurring more than once is **unresolved**, and `onUnresolved` decides. Otherwise `OBX.11 in [F]` silently drops a message whose later results are final. **No example spec is refused**: nothing in `contracts/`, `docs/` or `deploy/` uses `tributary.core.filter` |
| 2 | channel-spec 3.3.2: an hl7-batch child's final CR only if the parent terminated that segment | **Ratified.** A split never adds a byte, so a child cannot exceed the inline limit its parent met |
| 3 | `error-codes.json`: `TB_OUTPUT_ENVELOPE_INVALID`, `TB_PIPELINE_TIME_BUDGET` | **Registered by exact name.** envelope 2.3.2 rule 2 now says that fixed names under a reserved prefix are listed, because alerts key on them. `ENGINE_CODES` must include both for the cross-check test to pass |
| 4 | Bundle-declared `Reject` codes | **Deferred to bundle-fetch** (channel-spec section 9). There is no bundle format to declare them in yet. Interim: codes are literals, and `code-reviewer` treats a data-built code as Critical |
| 5 | `ref` restricted to the pinned bundle's own top-level packages | **Approved now**, enforced when bundle-fetch lands (channel-spec 3.3.1). Nothing runnable is affected |

**Change requests to `platform-engineer`:** the filter treats a repeated segment as unresolved; add the
two `TB_` codes to `ENGINE_CODES`.

**Re-run:** `validate.py` (expect 122 / 0) and `pytest tests/conformance` (expect 127), because
`error-codes.json` changed. That file is outside their scope, so the counts should not move. The
`tributary-sdk` registry cross-check test, once it exists, is the one that exercises the change.

## 2026-09-25 (W2-5 follow-ups) — set 1.1: built-in shapes pinned, derived-id encoding, engine error-code registry

Six proposals from `platform-engineer` with the W2-5 delivery (`74c9853` to `288903a`). **No schema
change, no example file changed, no invalid example added**, so no `(json_path, validator)` pair moves.
One new machine-readable file, and manifest `requires` edits. Set 1.1 is not frozen until the wave 2
gate, and every item is additive or a correction to set 1.1 text. No set 1.0 document changes validity.

| # | Change | Ruling and why |
|---|---|---|
| 1 | channel-spec 3.3: a `with` key that is a Python keyword binds to `key_` (`in` → `in_`). Writing `in_` in a spec is refused. `**kwargs` and `*args` are refused when the step is defined. Wrong-typed values are refused at bind | **Approved.** The spec keeps one spelling per option, and `**kwargs` would silently remove the unknown-key refusal |
| 2 | channel-spec new 3.3.3: exact derived child id layout plus three known-answer vectors | **Approved with an amendment.** The proposal's wording ("version and variant bits set UUIDv7-style") also reads as overwrite-in-place, which yields different ids. The contract pins the layout the code implements: `D` = the first 80 bits of the digest, `rand_a = D[0:12]`, `rand_b = D[18:80]`, **bits 12-17 discarded**. The vectors come from the implementation and still need an independent recomputation. The `ids.py` docstring ("next 62 bits") is wrong and goes back to `platform-engineer` |
| 3 | envelope new 2.3.2 plus `envelope/v0/error-codes.json`: the engine error-code registry | **Approved with amendments.** (a) The contract is the source; `codes.py` is **checked against it** by a `tributary-sdk` test in both directions, and is not generated from it. (b) Reserved prefixes grow from `KEY_`, `HTTP_`, `TB_` to include every loader family in channel-spec 3.4: `FHIR_`, `PG_`, `CH_`, `SFTP_`, `SSH_`, `S3_`, `MLLP_`, `KAFKA_`. Without them a step could raise `PG_23505` and pass for a loader failure. (c) **Drift found:** 2.3 names `TRANSFORM_ERROR` for exhausted step retries, but the executor emits `STEP_ERROR`. The contract wins, and `STEP_ERROR` stays registered as `reserved`. (d) `ENVELOPE_INVALID` is registered as `log_only`. The 2.3 sentence "deliberately no central registry" is replaced, with the reason |
| 4 | channel-spec 3.3.2: built-in `with` shapes. `name=value` arrays; `hl7:` prefix required, and `MSH.9.x` spelled out as `hl7:MSH.9.1`-`.3`; `onUnresolved: drop` → `FILTERED_UNRESOLVED`; `present` defined; split's `by` and `pointer` | **Approved.** The filter's JSON deny-list now applies to **every pointer token**, not "ends in". The old wording missed `/identifier/0/value`, the most common shape. That is my defect, and the implementation was right |
| 5 | channel-spec 3.3.2 split rules 5-6, ADR-003 follow-up 10: a dead-lettered child carries the child as split; reprocess refuses records with `tb-parent-id` | **Approved, as "refuse", not "refuse or scope".** Scoping needs a post-split re-entry point, and that is W3-3. The rule over-refuses reprocessed records, because D5 also sets `tb-parent-id`. That is stated, and the discriminator is left to W3-3 (section 9) |
| 6 | Manifest: remove `per-destination steps (W2-5)` | **Approved; it was three examples, not two** (`adt-mllp-…`, `http-fhir-ingest-…`, `kafka-lab-events-…`). Also removed `record:key evaluator (W2-4)`, which `tributary.sdk.keys` now implements. **Added** the `tributary.hl7v2.parse` `with: {version}` gap to the two examples that pass `version` (`adt-to-fhir-per-key-mtls`, `adt-mllp-…`). Neither listed it, and the binder refuses the unknown key today. **Nothing flips to runnable**: every changed example still names a `ref` step (bundle-fetch is unscheduled), and three of the four still name unbuilt connectors |

**Change requests to `platform-engineer`** that follow from these rulings:

- the executor emits `TRANSFORM_ERROR`;
- add the eight prefixes to `RESERVED_PREFIXES`;
- add the registry cross-check test;
- correct the `ids.py` docstring.

None changes behaviour a test currently pins, except the `STEP_ERROR` literal.

### Verification owed

Not run by the architect, who has no shell.

- `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expected
  **122 / 0, unchanged**. No example was added, and the manifest's `requires` lists stay non-empty.
- `pytest tests/conformance`: expected **127 passed, unchanged**.
- `--emit-expectations` is **not needed**, because no invalid example or schema changed.
- The manifest edits are checked by the corpus test in `tributary-runtime`:
  `uv run pytest packages/tributary-runtime/tests/test_core_steps.py`. Every `runnable: false` entry
  must still fail to resolve.
- The 3.3.3 vectors need one independent `hashlib` recomputation.

## 2026-09-24 (PostgreSQL loader built) — documentation only

`connector-engineer` shipped the `postgresql` loader (`eca445a`) against `sqlSinkParams` exactly. The
schema changes only in a `$comment`, so there is no validation change.

| Change | Why |
|---|---|
| README 3.0: `postgresql` is **Built**, a new status meaning "runs, `connector:` entry removed, genuine-redelivery duplicate test still pending" | Its duplicate tests call `deliver()` twice in one process, against a real server in the integration test. That proves the sink absorbs a repeat, but it is not the 5.1 redelivery. "Implemented" waits for the W2-9 harness |
| `auth.secretRef` holds all client-authentication material (user, password, or client certificate and key); `tls.secretRef` is the CA bundle only | `connector-engineer`'s split, adopted over mine: with the certificate under `tls`, `auth.secretRef` was redundant for certificate auth |
| The uniqueness check reads the catalog, and only a non-partial, non-expression unique index on exactly `idColumn` qualifies | A bare unique index satisfies `ON CONFLICT` but is invisible in `information_schema.table_constraints` (found against a real server). Partial and expression indexes added by the architect |
| Manifest: `connector:destination/postgresql` removed from `http-fhir-ingest-to-fhir-and-postgresql.json` | The loader exists; the example is still not runnable (`ref` steps, `fhir`, `http` source) |

## 2026-09-24 (later) — set 1.1 follow-ups from implementers; no schema change

All in `channel-spec/v0/README.md`, all additive or corrections to prose. The schema is unchanged.
One valid example changed (its key expression), so the conformance run should be repeated.

| Change | Why |
|---|---|
| **PID-3 repetition selector** `hl7:SEG.F[c:VALUE]`, e.g. `hl7:PID.3[5:MR].1` (4.3). Fits the published pattern, so the schema is unchanged. `valid/adt-mllp-to-fhir-clickhouse-mllp.json` now uses it | `platform-engineer`: PID-3 repeats and senders order it differently, so a bare `PID.3.1` can give one patient two keys, which is invariant 5 broken silently in the reference ADT flow. Brought forward from "v1" because the wave 2 exit-criterion channel keys on exactly this field |
| **Normalisation text corrected** (4.3) | My rule said "normalise in a step". The receiver evaluates the key before any step runs, so that was impossible. Set 1.1 has no place to normalise a key, and the text now says so |
| Header-name matching and duplicate headers (4.3) | Case-insensitive for HTTP, exact for Kafka; a repeated header is unresolved, never "the first one" |
| **`with` binds to keyword-only step parameters by name** (3.3) | `platform-engineer`'s proposal, approved. An unknown or non-identifier key, or a missing required value, is refused at step load |
| **A destination-step `dlq` record carries the step's input**; reprocess of any `tb-destination` record must be destination-scoped (3.5, ADR-003 follow-up 10) | Re-injecting into `raw` would re-deliver to every destination under a new `tb-msg-id`. This is already true of wave 1 loader-exhaustion records |
| FHIR `conditional-update` (3.4.2) broadened | It covers a conditional PUT on a unique business identifier, as the W2-2 proposal needs and README 5 already listed. It still requires exactly one identifier system and no unconditional POST |
| PostgreSQL loader not blocked on licensing (3.0, 10.2) | It uses `asyncpg`. `psycopg` is a separate wave 3 question |
| Manifest `requires` trimmed after W2-1 (`9078982`) | `tributary.hl7v2.parse` exists; no example flips to runnable (each also names a `ref` step, or `to_json`) |
| Manifest labels | PREDICTED changed to MEASURED |
| **Built-in steps' `with` shapes** (3.3.2): `filter` (required `onUnresolved`; identifier segments denied as `field`), `enrich` (`attrFrom` allow-list), `validate` plus the SDK's non-retryable `Reject(code)` | W2-5 public surface, `platform-engineer`'s proposal with changes |
| **Split** (3.3.2): a `@splitter` node, **deterministic child ids** derived from the parent id plus the child index, refused on per-key channels in set 1.1, capped at 10,000 children | Random child ids would make every replay a new set of messages, breaking invariant 4; multi-patient batches keyed on one patient would break invariant 5. `tb-parent-id` for split children was already in `envelope/v0` |
| SFTP licence decided (3.0, 10.2) | Human approved `paramiko` 2026-09-24 (ADR-001 2.3). Shapes unchanged |

## 2026-09-24 (wave 2 start) — contract set 1.1: the wave 2 connectors, destination steps, key semantics

**Additive throughout.** Every document valid under set 1.0 is still valid; nothing a conforming
producer could send before is refused now. Set 1.0 was frozen at the wave 1 gate, so this is recorded
as **set 1.1** rather than as an in-place Draft edit. Decisions, options and what each gives up are in
[ADR-007](../docs/adr/ADR-007-channel-spec-shape-and-refusals.md) D10-D17.

**Why now and not "with each connector" as set 1.0 said.** ADR-007 D6 said each connector would bring
its own config object when it landed. Wave 2 builds seven loaders and three receivers in parallel,
against a runtime and an evaluator being built at the same time; letting each shape arrive with its
code would have the implementers inventing the contract. D6 is amended (D10), not abandoned: types
still enter the enum only for a wave that is building them.

### `tributary.channel-spec/v0`

| Change | Detail |
|---|---|
| `source.type` gains `http`, `sftp`, `kafka` | Per-type field sets enforced by `propertyNames`; listeners stay flat as `mllp` was, client types nest (`sftp: {}`, `kafka: {}`). README 3.2.2 defines what ACK and NACK mean per type — the invariant 1 table |
| `http` source | `path`, `accept`, `auth` required; `auth: none` only on a loopback bind; `auth: mtls` requires `tls.clientAuth: required`. An ingest endpoint, not a FHIR server |
| `sftp` source | Pinned host keys required (no TOFU), `afterCommit` required with no default, `rejectDirectory` required. **Remote file names are PHI** |
| `kafka` source | Explicit connection with its own credentials, `PLAINTEXT` only to loopback, `startFrom` required, cannot name `_tb.*` or channel-shaped topics. A refused record **halts the source partition** — the receiver's ACL has no `dlq` WRITE (D16) |
| `destination.type` gains `fhir`, `postgresql`, `clickhouse`, `sftp`, `s3`, `mllp`, `kafka` | One params object per type, selected by an `allOf` branch on `type` so a failure reports at the wrong field, not as an opaque `oneOf` |
| Per-type idempotency | Each type may declare only the techniques it can honour (README 5.1, D14). `http` unrestricted as before |
| `fhir` | `interaction: transaction` only — identifiers stay in the Bundle body (D12) |
| `postgresql` / `clickhouse` | Row-shaped JSON payload built by a step, no mapping language (D13); `sink-native` idempotency **verified by the loader at startup** (unique index; `ReplacingMergeTree` sorting key) |
| `s3` | `serverSideEncryption` required; key from `tb-msg-id` and `tb-received-at` only |
| `sftp` / `mllp` destinations | Host keys pinned; MLLP TLS unless loopback; `onNegativeAck` required with no default |
| `kafka` destination | `key: preserve \| none` required; `tb-msg-id` header only; at-least-once (cross-cluster) |
| **New `destinations[].steps`** | Per-destination steps in the pipeline worker, no new hop (D17). Needed because one payload cannot be a FHIR Bundle and a table row, and exit criterion 1 needs both |
| `partitionKey` | Adds `record:key`. **Semantics fixed** per scheme in README 4.3: bytes exactly as they appear, unresolved = receiver `parse-error`, one shared evaluator. `xpath:` reserved. The pattern was *not* tightened per scheme — that would narrow a frozen contract |
| `retry-topic` | **Still refused.** README 4.1.1 now lists the nine things the spike must show to lift it, and what changes on lift (D15) |
| Refactor, no validation change | `httpParams` now references shared `$defs` (`destinationUrl`, `plaintextHttpLoopbackOnly`, `staticHeaders`, `httpAuth`, `circuitBreaker`, `timeoutMs`). The instance paths the set 1.0 pairs pin are unchanged |
| Manifest | `runnable` now covers connectors: a `connector:<kind>/<type> (task)` entry in `requires` |
| Examples | 4 valid, 16 invalid. **16 expected-failure pairs are PREDICTED, UNMEASURED** |

One schema detail worth recording because it was caught before publishing, not after: the first draft
of `remotePath` was `^/([...]{1,64}/?){0,16}$`. With the separator optional, a backtracking regex engine
can split a long run of characters across groups exponentially many ways, and this validator reads
tenant-supplied documents. The published pattern makes the separator mandatory.

### `tributary.topics/v0`

Section 3.6 gains consumer-group role `receiver`, for the `kafka` source's group on the external
cluster. No ACL change: that group runs under the source's own credentials.

### Found while writing it: two cross-contract checks

Per the rule I adopted after W1-1's contradictions, every new rule that names another contract's
artefact was traced into that contract before publishing.

- **A Kafka source cannot dead-letter.** The first draft routed a refused Kafka record to the channel's
  `dlq`. `topics/v0` `security.acls.receiver` grants WRITE on `raw` only, so that rule was
  unimplementable. Changed to halt the source partition, rather than widening an ACL shape that
  ADR-004 owns.
- **The envelope's registered content types** (`envelope/v0` section 3) are what `contentType` and
  `accept` enumerate, minus `application/dicom` (claim-check is unimplemented).

### Verified, first run

`qa-engineer`, 2026-09-24, committed at `ffc1580`: `contracts/validate.py` **122 / 0**;
`--emit-expectations` **86 pairs, all matched, 0 mismatches** (the 16 set 1.1 predictions included,
and the least certain one, `propertyNames` reporting `enum` at `$.spec.source`, among them);
`pytest tests/conformance` **127 passed**. No manifest correction needed.

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

Added after `729cdc1`, from their implementation report: a third rule, **at least one entry must be
`runnable: true`.** They had already asserted it in the test; it belongs in the manifest because a
corpus where every example is exempt no longer demonstrates that any channel spec can be executed at
all, and would go green saying so. That is the same failure shape as the other two rules, so it should
survive a rewrite of the test that currently enforces it.

**Recorded in `contracts/README.md`: the two-checker split is structural and must not be merged.**
`validate.py` and `tests/conformance` answer whether a document is well-formed and fails for its
documented reason; the corpus test in `tributary-runtime` answers whether the things a document *names*
resolve. Neither could have caught the walking-skeleton defect alone and neither can catch the next one
alone — name resolution needs a runtime, and running on a clean clone before `uv sync` is the whole
reason the standalone checker exists. Three of the four wave-1 contract defects were found by an
implementer rather than by any checker, all the same shape: internally valid, referring to something
that did not exist. **A green conformance run means the contracts are self-consistent, not that they
agree with the code.**

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
