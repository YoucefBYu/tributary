# ADR-008: Engine Kafka Connection and the Lite Stack's Transport Security

- **Status:** Proposed (architect ruling, 2026-09-29). **No invariant changes**, so no human approval is
  required to adopt it. Principal naming, rotation and key custody remain ADR-004 plus a human.
- **Date:** 2026-09-29
- **Deciders:** `architect`, on `connector-engineer`'s W2-7 security-review handoff (`e3ddee1`)
- **Contracts:** `engine-kafka/v0` (new, set 1.3); `topics/v0` 3.4

## 1. Context

`topics/v0` 3.4 has required, since contract set 1.0, that every client listener a non-local client
can reach is `SASL_SSL` or mTLS, with deny-by-default ACLs and per-component grants: the receiver
principal holds WRITE on `<env>.<channel>.raw` only. **No contract said where a data-plane process gets
the credentials to meet that.** `topic-defaults.json` `security.clients.credentials` was a `TODO`.

Evidence, all from the code on `wave/1-2`:

- **The W2-7 security review found a HIGH:** every receiver's producer to `raw` was PLAINTEXT and
  unauthenticated, so the receiver ACL could not apply. `e3ddee1` fixed it for receivers only:
  `require_secure_link` refuses PLAINTEXT unless every bootstrap host is loopback, requires verified
  TLS with the `ECDHE+AESGCM:ECDHE+CHACHA20` pin and SASL credentials, and takes them from a
  provisional `--engine-security <json>` flag.
- **Workers and loaders have the same gap.** Each builds `KafkaSettings(bootstrap_servers=...)` with
  no security (`tributary.runtime.worker`, and every `tributary.loaders.*.main`). The worker defaults
  to `localhost:9092` when `TB_BOOTSTRAP_SERVERS` is unset.
- **The worker is the most exposed role.** It reads `_tb.<env>.channels`, which invariant 3 makes it
  trust completely. An unauthenticated, unverified link there lets anyone on the path inject a
  channel spec: remote code execution across the data plane (`topics/v0` 3.4).
- **A second defect, found while ruling:** `tributary.runtime.clients.producer_config` merges
  `settings.security` **after** the contract's producer defaults, so a security map carrying `acks`
  or `enable.idempotence` silently overrides invariant 1's client profile.
- **Kafka connectors already share one secret layout** (`kafka_source/security.py`,
  `loaders/kafka/client.py`): identity under `security.secretRef`, trust bundle under
  `tls.secretRef`. The channel-spec comment said otherwise; the code is consistent with itself and
  with the PostgreSQL loader's split.
- **The Compose lite stack connects to `kafka:9092` over PLAINTEXT** from every data-plane service, so
  `e3ddee1` already stops its MLLP receiver from starting. A Compose service name is not loopback.

Recurring shape (STATUS, wave 1 exit): *a control specified and never applied to a path nobody
thought about.* Here the control is `topics/v0` 3.4, and the unapplied paths are the engine link of
every role and the lite stack.

## 2. Options — where engine credentials come from (D1)

| Option | For | Against | Complexity / cost / fit |
|---|---|---|---|
| A. Per-role CLI flag (`--engine-security`, `--bootstrap`) | Exists for receivers | Every role parses arguments differently (argparse per loader, env-only worker); two inputs for one link is how `e3ddee1`'s defect arose | Low now, drifts per role |
| B. Env vars carrying the values (`TB_KAFKA_SASL_PASSWORD`, ...) | Simple in Compose | Secret values in `docker inspect`, `/proc/<pid>/environ`, crash reporters and debug shells; invites a property passthrough | Low; fails CLAUDE.md "no secrets" in spirit |
| C. Fetch from the control plane | One place to manage | Violates invariant 3 outright, and needs credentials to fetch credentials | Rejected |
| **D. One document per process, named by `TB_ENGINE_KAFKA_FILE`, secrets by `secretRef` from local files (chosen)** | Same for every role; schema-validatable, so the loopback rule is checked on servers and protocol together; holds no secret; matches the `secretRef` resolution every connector already uses; Kubernetes ConfigMap + Secret, Compose mounted files | A new contract and a migration of every entry point | Medium, one implementation in `tributary-runtime` |

## 3. Decisions

| # | Decision | What it gives up |
|---|---|---|
| D1 | **`engine-kafka/v0`: one document per data-plane process, `TB_ENGINE_KAFKA_FILE`, bootstrap servers and security together, secrets by `secretRef` under `TB_SECRETS_DIR`.** Covers receivers, workers, loaders, the archiver; every engine client in the process. Not a Kafka connector's foreign cluster (ADR-007 D11). `--bootstrap`, `--engine-security` and `TB_BOOTSTRAP_SERVERS` are retired; a process given one after migration refuses to start | Every entry point and the Compose file change; a local run needs a small JSON file instead of a flag |
| D2 | **Every role enforces the same rules as `e3ddee1`**: PLAINTEXT only to `127.0.0.0/8`, `[::1]`, `localhost`; TLS verification and hostname check always on with the AEAD cipher pin; credentials present before connecting; `SSL` must carry a client certificate | A developer cannot point a worker at a remote plaintext broker, even briefly |
| D3 | **One implementation, in `tributary-runtime`**, used by receivers and loaders too, replacing `tributary.receivers.engine_kafka` | Receivers' `e3ddee1` code is superseded weeks after it landed |
| D4 | **No librdkafka property passthrough; contract values win.** The security map may hold only an **exact ten-key allow-list** (`engine-kafka/v0` 4 rule 4): `security.protocol`, `sasl.mechanism`/`username`/`password`, `enable.ssl.certificate.verification`, `ssl.endpoint.identification.algorithm`, `ssl.cipher.suites`, `ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location`. `KafkaSettings` rejects anything else; `topic-defaults.json` `clients.*` cannot be overridden by it. **Amended 2026-09-29 (set 1.3.1)** from `sasl.*`/`ssl.*` prefixes: a prefix admits `ssl.engine.location` and `ssl.providers` (shared-object loading) and inline PEM keys. `platform-engineer` found this and implemented the exact list first | An operator cannot tune an arbitrary librdkafka property through this document, including a PKCS#11 engine or an HSM provider; a real need becomes a contract change with a security review |
| D5 | **The Compose lite stack runs `SASL_SSL` with no opt-out.** Dev CA, broker certificate and per-component SCRAM-SHA-512 users generated at `up`; deny-by-default ACLs applied from `topic-defaults.json`; PLAINTEXT only on listeners bound to `127.0.0.1` inside the broker container. `TB_PROFILE=lite` lowers durability only (ADR-002 D14), never transport security | Slower and more complex `up` (unmeasured; the "first channel in under 15 minutes" metric, ADR-001 10.4, must be re-checked); host-side tools need a client config |

### D5 — options for the lite stack

| Option | Assessment |
|---|---|
| **(a) SASL_SSL in Compose (chosen)** | Runs the production path, including the ACL shape in `security.acls`, which nothing has executed yet. Uses the Kafka image's own tooling; no new dependency |
| (b) Dev-only opt-out flag (for example `TB_INSECURE_DEV_PLAINTEXT`) | **Rejected: no definition keeps it out of production.** A component cannot tell a Compose bridge from a production pod network; both are non-loopback. Tying it to `TB_PROFILE=lite` does not help, because lite is approved for "small sites that have accepted the risk" (`topics/v0` 3.1), which carry PHI. Tying it to "cluster has one broker" fails the same way. A CI grep on Helm catches only Helm. The only opt-out that cannot leak is one that does not exist |
| (c) Shared network namespace (`network_mode: service:kafka`), so `127.0.0.1:9092` is literally loopback | Honest and cheap, but TLS, SCRAM and ACLs would first run in Helm, the wave 1 High pattern again. Weakens isolation between containers. Kept as the fallback if (a) proves unworkable, which would come back here |

### D6-D9 — the ACL table meets a deny-by-default broker (added 2026-09-29, set 1.4)

**Evidence.** The first run of D5 found what section 4 predicted. Every loader exited with
`ChannelSpecError: channels topic _tb.dev.channels is not available` (`TOPIC_AUTHORIZATION_FAILED`,
reported through `open_spec_source`). The worker logged `GROUP_AUTHORIZATION_FAILED` (error code 30)
on its spec reader. With a temporary READ grant, MLLP → worker → loader ran end to end
(`devops-engineer`). A read of `packages/*` then found four more defects in the table. **Criteria:**
every grant has a code path; no code path lacks its grant; every prefix is delimited; the change stays
parseable by the lite `topic-bootstrap`.

| # | Decision | Rejected | What it gives up |
|---|---|---|---|
| D6 | **Receiver, worker and loader get READ on `_tb.<env>.channels` and READ on group PREFIXED `tb.<env>.<ch>.channels.`** (the spec reader's per-process group). The receiver gets it ahead of its invariant-3 migration off spec files | (a) Drop `group.id` from the reader, so no group grant is needed: cleaner, but it needs a code change, and whether `confluent-kafka` accepts a consumer without `group.id` is **unverified** here. It stays open as a later tightening. (b) Per-channel spec topics: exact isolation, one more topic per channel against the S4 budget | Every data-plane principal reads every channel spec in its environment. Specs carry no secret value and no PHI, but they reveal topology. ADR-004 may revisit |
| D7 | **Remove grants no code uses; delimit every group prefix.** Worker loses `.retry` WRITE, loader loses `.retry` READ and WRITE. `pipeline*` → `pipeline` + `pipeline.shadow-`, `loader.<dest>*` → `loader.<dest>`, `replier*` → `replier`. Add the missing transactional-id grant for transactional channels, outside the parsed string | Keeping `.retry` "for later": a grant with no code path is the one nobody reviews when the code arrives | The lite broker keeps old grants until `down` (ACLs are only ever added) |
| D8 | **Super users as implemented:** broker (`CN=kafka`), `kafka-users-init` (`CN=tb-lite-users-init`, loopback mTLS, one-shot), `topic-bootstrap`, `admin`. `channel-publish` gets `control_plane` grants only. The healthcheck principal has no ACL. Never a data-plane role, never `ANONYMOUS` | The set 1.3 text (channel-publish as a super user), which was looser than the implementation | A control-plane stand-in that needs a new grant needs a contract change |
| D9 | **Retry groups, ruled now and applied when `retry-topic` lifts:** one shared `retry` topic per channel, consumed by each destination in its own `tb.<env>.<ch>.loader.<dest>` group, filtering on `tb-destination`. A channel-wide retry group is forbidden, and the LITERAL group ACL enforces it | A per-destination retry topic: isolated by construction, but D−1 more topics per channel and a grammar change | Read amplification × destinations on a low-volume topic; a loader reads sibling destinations' retry records within one channel |

Detail: `topics/v0` 3.4.1, `engine-kafka/v0` 6 rule 3.

## 4. Consequences

- Invariant 1: the client profile can no longer be overridden through the security map (D4).
- Invariant 3: all inputs are local files read at start; no API or database call. Unchanged risk.
- Invariant 6: the document holds names, never values; errors and logs carry protocol and
  mechanism only.
- The ACL shape in `topics/v0` 3.4 gets its first execution (Compose), before ADR-004 designs the full
  role model. Expect it to find defects in the shape; that is the point.
- **Until the follow-ups land, the Compose lite stack does not start** (the receiver refuses since
  `e3ddee1`), and workers and loaders remain unauthenticated wherever they run. The `devops-engineer`
  task is on the critical path for any wave 2 demo on Compose; the exit-criterion tests use
  testcontainers on loopback and are not blocked.

## 5. Impact on invariants and contracts

**No invariant changes.** Invariant 1's client profile gains a guard; invariant 3 is respected by
construction. Contracts (set 1.3, additive): `engine-kafka/v0` new; `topics/v0` 3.4 pointer and
`topic-defaults.json` `security.clients.credentials` filled in. `CHANGELOG.md` 2026-09-29.

## 6. Follow-ups

1. `platform-engineer`: the loader in `tributary-runtime` (`TB_ENGINE_KAFKA_FILE`, schema rules,
   `engine-kafka/v0` section 4 rules 1-6, a stdlib `DirSecretResolver`), `KafkaSettings` key
   allow-list and merge order (D4), the worker and the `_tb.<env>.channels` reader migrated, the
   `localhost:9092` default removed. Tests per `engine-kafka/v0` section 5.
2. `connector-engineer`: every receiver and loader entry point on the runtime loader; retire
   `--bootstrap` and `--engine-security` and `tributary.receivers.engine_kafka`.
3. `devops-engineer`: Compose per `engine-kafka/v0` section 6, landing first or together with 1
   and 2; Helm values mount the document and Secret per role.
4. `qa-engineer`: `engine-kafka` conformance suite; measure the seven predicted pairs; an ACL negative
   test in the Compose e2e (the receiver principal cannot READ `raw` or WRITE `out.*`).
5. ADR-004: principal naming per role and channel, rotation without restart, `OAUTHBEARER`.
6. `connector-engineer` (2026-09-29, set 1.3.1): move `RawPublisher` onto the runtime loader and delete
   `tributary.receivers.engine_kafka.require_secure_link`, which still checks the `ssl.`/`sasl.`
   prefixes. From reading the source, not a run, this is **not a live gap**: `RawPublisher` then builds
   `KafkaSettings`, whose `__post_init__` runs the exact-list `check_link`, so a prefix-admitted key is
   still refused. But two rule sources for one contract rule will drift. Part of follow-up 2.
7. Set 1.4 (D6-D9): `platform-engineer` adds a `spec_reader_group` helper to `tributary.runtime.topics`;
   `connector-engineer` uses it in the loaders and moves receivers onto `open_spec_source`;
   `devops-engineer` updates three `test_bootstrap_topics.py` tests and re-runs the stack from `down`
   without the temporary grant; `qa-engineer` adds ACL negative cases (`.retry`, sibling destination
   group).
8. **Recurring problem, recorded so it is not rediscovered:** `security.acls` is English sentences
   parsed by regex in `bootstrap_topics.py`. A transactional-id target and any new conditional form
   need a parser change. Moving to a structured grant list is the fix. It is breaking for that parser,
   so it waits for ADR-004's role model rather than being done piecemeal.
