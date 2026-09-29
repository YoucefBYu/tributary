# Contract: engine Kafka connection — v0 (draft)

**Contract id:** `tributary.engine-kafka/v0`
**Status:** Draft, **new in contract set 1.3 (2026-09-29)**. Additive: it fills the `TODO` that
`topics/v0` `topic-defaults.json` `security.clients.credentials` left open, and no existing document
changes meaning. **Set 1.3.1 (2026-09-29, patch):** section 4 rule 4 narrowed from `ssl.*`/`sasl.*`
prefixes to an exact ten-key allow-list. Schema and examples unchanged. **Set 1.4 (2026-09-29):** section 1
scope table corrected to the code (`retry` unused, the loader reads `_tb.<env>.channels`); section 6
rule 3 restated to the implemented super users. README only.
**Owner:** `architect`. Propose changes by message with the exact diff and impact.
**Source:** ADR-008 (D1-D4); `topics/v0` 3.4; the W2-7 security review HIGH fixed in `e3ddee1`.
**Schema:** [`engine-kafka.schema.json`](engine-kafka.schema.json) · **Examples:**
[`examples/manifest.json`](examples/manifest.json)

Serves invariant 1 (no client setting can override `acks=all` or idempotence), invariant 3 (all
inputs are local files) and invariant 6 (the document holds names, never secret values). It exists
because `topics/v0` 3.4 required SASL_SSL or mTLS and per-component ACLs, and **nothing said where a
data-plane process gets the credentials to meet it**. Every receiver's producer to `raw` was
plaintext and unauthenticated until `e3ddee1`; the workers and loaders still are.

---

## 1. Scope

One document per **data-plane process**, used for **every** client that process builds against
**this engine's own cluster**:

| Role | Clients covered |
|---|---|
| receiver (`mllp`, `http`, `sftp`, `kafka` source) | the `raw` producer; the sync-reply consumer (`topics/v0` 3.3.1); the `_tb.<env>.channels` reader once receivers migrate off spec files (set 1.4) |
| pipeline worker | the `raw` consumer; `out`/`dlq`/`reply` producers; the `_tb.<env>.channels` reader; the events producer |
| loader | the `out.<dest>` consumer; the `dlq` producer; the `_tb.<env>.channels` reader; the events producer. `retry` only when `retry-topic` lifts (`topics/v0` 3.4.1) |
| archiver (wave 3) | the `_tb.<env>.events` consumer |

**Not covered:** a `kafka` source's or destination's connection to a **foreign** cluster. That is
always the channel spec's own explicit `kafka.connection` with its own credentials (ADR-007 D11), and
never shares this document or its principal. The control plane and the topic-creation job are not
data-plane processes and are out of scope for v0.

## 2. Where it comes from

| Input | Rule |
|---|---|
| `TB_ENGINE_KAFKA_FILE` | **Required.** Absolute path to a JSON document valid against the schema. Unset, unreadable or invalid: the process exits non-zero before building any Kafka client. There is no default and no fallback |
| `TB_SECRETS_DIR` | The existing secret root (default `/var/run/secrets/tributary`). Each `secretRef` resolves to `<TB_SECRETS_DIR>/<ref>/<key>`, the convention every connector already uses |

In Kubernetes the document is a ConfigMap and the credentials a Secret, both mounted read-only; in
Compose both are mounted files (section 6). **Invariant 3:** resolution reads local files only, at
process start, and never calls the control-plane API or PostgreSQL. A principal's credentials are
per role and per channel (for example the `adt-to-fhir` receiver); the principal naming scheme and
rotation belong to ADR-004 (human-approved scope), not to this contract.

**Retired inputs:** `--bootstrap`, `--engine-security` (the provisional flag from `e3ddee1`) and
`TB_BOOTSTRAP_SERVERS`. A process given any of them after its migration refuses to start with a
message naming the replacement, so that two sources of the same setting can never disagree. Why
bootstrap servers moved into the document: the loopback rule is a property of the servers **and** the
protocol together, and the `e3ddee1` defect lived in the gap between two inputs.

## 3. Secret layout

| Protocol | `security.secretRef` keys | `security.tls.secretRef` keys |
|---|---|---|
| `SASL_SSL` | `username`, `password` (SCRAM-SHA-512) | `ca.crt`, optional; absent `tls` means the platform trust store |
| `SSL` | `tls.crt`, `tls.key` — **required**: the certificate is the principal | `ca.crt`, as above |
| `PLAINTEXT` | none permitted | none permitted |

`security.secretRef` holds **everything about who this process is**; `tls.secretRef` holds **only**
the trust bundle. This is the layout both Kafka connector implementations already use, and the
PostgreSQL loader's `auth.secretRef` / `tls.secretRef` split (channel-spec README 3.4.3). The
channel-spec `kafkaSecurity` comments said the client certificate lived under `tls.secretRef`; that
text is corrected in set 1.3 to match (channel-spec README 3.2.5, CHANGELOG).

## 4. What every data-plane process enforces (normative)

One implementation in `tributary-runtime`, used by every role, so no entry point can build an engine
client that bypasses it (ADR-008 D2, D3).

1. **PLAINTEXT only when every bootstrap server is loopback**: `127.0.0.0/8`, `[::1]` or
   `localhost`. A Compose service name, a Kubernetes service or pod address, `host.docker.internal`
   and a host-only bridge are not loopback. **There is no opt-out**: no flag, environment variable or
   profile relaxes this. `TB_PROFILE=lite` lowers durability (ADR-002 D14) and nothing else.
2. **TLS always verifies**: `enable.ssl.certificate.verification=true`,
   `ssl.endpoint.identification.algorithm=https`, and `ssl.cipher.suites=ECDHE+AESGCM:ECDHE+CHACHA20`
   (the AEAD pin every Tributary TLS client uses; librdkafka has no minimum-version property, so the pin
   is what refuses a pre-TLS-1.2 handshake). None of the three can be configured.
3. **Credentials present and non-empty** before the first connection: `username` and `password` for
   `SASL_SSL`; `tls.crt` and `tls.key` for `SSL`. The broker is not left to discover a missing
   principal.
4. **No property passthrough, and contract values win.** The security properties this document
   produces may contain **exactly these ten keys and no others** (amended in set 1.3.1; the set 1.3
   text said `sasl.*` and `ssl.*`):

   | Key | Produced from |
   |---|---|
   | `security.protocol` | `security.protocol` |
   | `sasl.mechanism`, `sasl.username`, `sasl.password` | `saslMechanism`; `security.secretRef` keys `username`, `password` |
   | `enable.ssl.certificate.verification`, `ssl.endpoint.identification.algorithm`, `ssl.cipher.suites` | rule 2's fixed values, never the document |
   | `ssl.ca.location` | `tls.secretRef` key `ca.crt` (a path) |
   | `ssl.certificate.location`, `ssl.key.location` | `security.secretRef` keys `tls.crt`, `tls.key` (paths), both or neither |

   `KafkaSettings` rejects any other key at construction, by exact match, never by prefix. The list is
   exactly what the schema can express, so no valid document loses anything. **Why not `ssl.*` /
   `sasl.*`:** a prefix admits `ssl.engine.location` and `ssl.providers`, which load a shared object
   into the process; `ssl.key.pem`, `ssl.certificate.pem`, `ssl.ca.pem` and `ssl.key.password`, which
   put key material in the map itself rather than behind a file path; and, from librdkafka's
   configuration reference (not re-checked in this revision), `sasl.kerberos.kinit.cmd`, a shell
   command, and `sasl.oauthbearer.token.endpoint.url`, an outbound URL. Found by `platform-engineer`,
   who implemented the exact list first (`tributary.runtime.kafka_security.SECURITY_KEYS`). Adding a
   key needs an architect-approved contract change and a `security-reviewer` pass. The client settings in
   `topic-defaults.json` `clients.*` (`acks`, `enable.idempotence`, `session.timeout.ms`, ...) are
   applied from that file and cannot be overridden by the security map. **Before set 1.3 they could:**
   `producer_config` merged `settings.security` *after* the contract defaults (ADR-008 D4).
5. **Errors carry no secret value**, and startup logs the protocol and mechanism only. Never a
   password, key path content or token (invariant 6 and CLAUDE.md "no secrets").
6. Every role logs the `topics/v0` 3.8 lite warning when the profile is `lite`, as before.

**Why workers and loaders enforce it too, not only receivers.** A worker reads `raw` and writes
`out`, `dlq` and `reply` (all PHI), and it reads `_tb.<env>.channels`, which invariant 3 makes it
trust completely. An unauthenticated, unverified link to that topic lets anyone on the path inject a
channel spec, which is remote code execution across the data plane (`topics/v0` 3.4). The worker is
the most exposed role, not the least.

## 5. Acceptance (per role)

- A unit test per role: a document naming a non-loopback broker with `PLAINTEXT` is refused; `SSL`
  without `security.secretRef` is refused; a security map carrying `acks` is refused; the effective
  producer config has `acks=all` and `enable.idempotence=true` whatever the document says.
- One integration test per role against a `SASL_SSL` broker with deny-by-default ACLs: the role's
  principal can do exactly its `topic-defaults.json` `security.acls` grants. The Compose lite stack
  (section 6) is where these first run together.

## 6. The Compose lite stack (ADR-008 D5)

The lite stack runs Kafka with `SASL_SSL` like production. It does **not** get a plaintext
exception. Requirements for `deploy/compose/` (`devops-engineer`):

1. Every listener a container other than the broker can reach is `SASL_SSL`, including the
   host-published one if it is kept (its certificate then carries `IP:127.0.0.1`). `PLAINTEXT` is
   allowed only on listeners bound to `127.0.0.1` **inside the broker container** (the KRaft
   controller, a healthcheck listener).
2. A dev CA, the broker certificate and one SCRAM-SHA-512 user **per component** are generated at
   `up` by a one-shot init service. Passwords are random per `up`. Nothing is committed and no image
   bakes them in. Each component mounts only its own `secretRef` directory, read-only. The CA private
   key is not mounted into any component.
3. `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`. The topic-bootstrap job applies
   the walking-skeleton channel's grants from `topic-defaults.json` `security.acls`, read from the
   file (3.7), not restated. **Amended in set 1.4 to what `deploy/compose/kafka/tls-init.sh`
   implements**, which the security review judged acceptable and tighter than the set 1.3 text:

   | Principal | How it authenticates | Authority |
   |---|---|---|
   | `CN=kafka` (the broker) | mTLS on the loopback `CONTROLLER` and `BROKER` listeners | **super user** |
   | `CN=tb-lite-users-init` (`kafka-users-init`) | mTLS on the loopback `ADMIN` listener (`127.0.0.1:9096`), reachable only from the broker's network namespace (`network_mode: service:kafka`) | **super user**, one-shot: creates the per-component SCRAM users from the password files and exits |
   | `tb-lite-topic-bootstrap` | SCRAM over `SASL_SSL` | **super user**: creates topics and applies the ACLs |
   | `tb-lite-admin` | SCRAM over `SASL_SSL` | **super user**: the operator's principal |
   | `CN=tb-lite-healthcheck` | mTLS on the `ADMIN` listener | **no ACL at all**. It sends only `ApiVersions`, which needs no authorization, so "healthy" proves TLS, hostname verification and client-certificate authentication, and nothing more |
   | `tb-lite-channel-publish` | SCRAM over `SASL_SSL` | **not a super user**: its `control_plane` grants only (least privilege) |
   | `tb-lite-receiver`, `-worker`, `-loader` | SCRAM over `SASL_SSL` | their `security.acls` grants only |

   **Never** `User:ANONYMOUS`, which is why no listener is `PLAINTEXT`, not even the loopback
   controller: a PLAINTEXT controller's own requests would arrive as `ANONYMOUS`. **Never** a data-plane
   component. The certificate identities have no password to leak. The dev CA key is deleted after
   signing, so nothing can mint another identity. ACLs are only ever added, so a narrowed
   `security.acls` takes effect only on a fresh broker: after `docker compose down` (the storage lives
   in the container) or a credential regeneration, which wipes it.
4. Every data-plane service gets `TB_ENGINE_KAFKA_FILE` pointing at a committed document like
   `valid/compose-lite-worker.json` (it holds no secret).
5. No generated password, key or certificate is printed by the init service or reaches
   `docker compose logs`.
6. Published ports stay `127.0.0.1`-bound. Synthetic data only, as before.

## 7. Open items

- `OAUTHBEARER`: permitted by `topics/v0` 3.4, not in v0 because nothing implements it. Additive.
- Credential rotation without restart: v0 resolves once at start. Rotation is ADR-004.
- The control plane's and the topic-creation job's own connection: wave 3, likely this same shape.
