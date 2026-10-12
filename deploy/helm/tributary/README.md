# Tributary Helm chart

Render with your values and a node-sized kubelet CIDR, for example
`helm template tb deploy/helm/tributary -f deploy/helm/tributary/ci/values-ci.yaml`. No image is published
yet; override `image.registry` and `image.tag`. One release name per `env` per Kafka cluster, or StatefulSet
instance ids collide.

## Changing retention

Retention is operator-configurable (ADR-004 R1-R8). The settings are integer days, 1 to 36500, validated at
render time:

| Value | Env var | Default |
|---|---|---|
| `archiver.retention.eventsDays` | `TB_ARCHIVE_TTL_DAYS` | 90 |
| `archiver.retention.contentDays` | `TB_ARCHIVE_CONTENT_TTL_DAYS` | 30 |
| `archiver.retention.auditDays` | `TB_ARCHIVE_AUDIT_TTL_DAYS` | 2190 (six years: a conservative engineering default, not a compliance determination) |
| `archiver.retention.allowDecrease` | `TB_ARCHIVE_ALLOW_TTL_DECREASE` (migrate Job only) | false |

`archiver.ttlDays` was removed; setting it fails the render.

Procedure (R6). The values reach the archivers and the pre-upgrade migrate Job, but only the migrate Job
applies them (`ALTER TABLE ... MODIFY TTL`).

1. Lengthening: change the value and `helm upgrade`. Existing parts get the new TTL. Rows already dropped do
   not come back.
2. Shortening: the migrate Job refuses and the upgrade stops (`RETENTION_DECREASE_REFUSED`). Rows older than the
   new value are deleted irreversibly at the next TTL merge. If that is intended, set
   `archiver.retention.allowDecrease=true` for that one upgrade, then set it back to false.
3. Cost (R1): expiry drops whole month partitions, so a row can outlive its TTL by up to one month. To remove
   one message promptly, use erasure (R4, still a human decision), not a shorter TTL.
4. Kafka topic retention (`raw` 7 d, `dlq` 30 d, events 3 d, audit 30 d) is not a records-retention control (R8).
   The audit topic is a 30 d buffer; the system of record is the ClickHouse `audit_log` table.

Migrations run one at a time per ClickHouse database. The Job has `parallelism: 1`, `completions: 1` and a
fixed name `tb-archiver-migrate-<database>` (not the release name), and its hook policy is
`hook-succeeded,hook-failed`, without `before-hook-creation`. A second release migrating the same database in
the same namespace therefore fails to create its Job while the first exists, rather than running alongside it.
Releases in different namespaces are not covered: run them one after the other. The reason is that the TTL
re-read in `retention.py` (the decrease guard re-checks the live TTL before `MODIFY TTL`) is only a second line
of defence against a racing migration, not a lock. The Compose lite stack has no archiver and no migrate
service yet; when one is added it must be a one-shot service that is never scaled.

The pre-install and pre-upgrade migrate Job (`archiver.migrate`, `tributary.archiver --migrate-only`) applies
them with the migration credential only; the runtime archivers run `--skip-ddl`.

## Key custody checklist (ADR-004 H1, draft)

- Production uses customer-managed keys (KMS CMK, CMEK, or HSM or Vault on premises) on every PHI-at-rest
  surface: Kafka volumes, ClickHouse and PostgreSQL volumes, the claim-check bucket.
- The claim-check bucket has default encryption on, and the processes that read or write it hold only the KMS
  permissions they need.
- The content archiver's Secret is filled by your secrets store, rotated on your schedule (90 days suggested,
  a judgement), and never templated into values.

## Lite is synthetic data only (ADR-004 C7, `topics/v0` 3.8 rule 4)

The lite stack (`deploy/compose`) and any install without a customer-managed key, or on a host without
full-disk encryption, hold **synthetic data only**. Real PHI needs the production custody rules in this
checklist. Provider-managed keys protect a lost disk, nothing more.

## Production checklist for ADR-004 conditions C1 to C8

- C1, TLS in transit: every PHI hop uses TLS with certificate and hostname verification (Kafka, archiver to
  ClickHouse on 8443, API to PostgreSQL with `sslmode=verify-full`). The claim-check bucket has a policy that
  denies non-TLS requests (`aws:SecureTransport` false). ClickHouse plaintext ports (8123, 9000, 9004, 9005, 9009) are never
  published by a Service, pod port or NetworkPolicy rule (PostgreSQL is reached on 5432 over TLS only), and
  `archiver.clickhouse.secure=false` is refused: `.github/scripts/helm_check.py` fails the render.
- C2, the API reads its PostgreSQL credential from a `secretRef` file (`TB_DATABASE_HOST`, `_NAME`, `_USER`,
  `_SECRET_REF` under `TB_SECRETS_DIR`), never from a URL in an environment variable (`TB_DATABASE_URL` is
  refused). The chart has no API workload yet; when it gains one, `helm_check.py` already requires
  `sslmode=verify-full` wherever an sslmode is set.
- C3, revocation: the brokers set `connections.max.reauth.ms: 3600000` (`strimzi.config`). To revoke, remove
  the principal's ACLs or its `KafkaUser` first, then rotate the credential, then restart the pods that held it.
- C4, rotation: Secret files are read once at startup, so rotation and revocation need a pod restart. Never
  mount a Secret with `subPath` (it is not even refreshed in place); `helm_check.py` refuses it.
- C5, customer-managed keys also cover the Kafka tiered-storage remote tier, ClickHouse S3 disks and `BACKUP`
  targets, besides the volumes and the claim-check bucket above.
- C6, disabling a key blocks new attach and decrypt. Volumes already attached may stay readable until
  detached (provider documentation), so detach them as part of key revocation.
- INSERT-only archiver credential (ADR-004 A5). `values.yaml` only names the Secrets; the grants are yours to
  apply, as a ClickHouse administrator, once the migrate Job has created the tables (database `tributary`):

  ```sql
  CREATE USER IF NOT EXISTS tb_archiver IDENTIFIED WITH sha256_password BY '<from your secrets store>';
  GRANT INSERT ON tributary.message_events TO tb_archiver;
  GRANT INSERT ON tributary.audit_log TO tb_archiver;
  GRANT SELECT, INSERT ON tributary.archive_settings TO tb_archiver;  -- only if the archiver reads its settings row
  ```

  Pin the archiver away from the `MATERIALIZED inserted_at` column. Without this, an INSERT-only user can write
  it with `SETTINGS insert_allow_materialized_columns=1`. The archiver refuses to start without the profile:

  ```sql
  CREATE SETTINGS PROFILE IF NOT EXISTS tb_archiver_profile SETTINGS insert_allow_materialized_columns = 0 CONST;
  ALTER USER tb_archiver SETTINGS PROFILE 'tb_archiver_profile';
  ```

  Verify as the archiver user (not as an administrator): `SELECT name, value, readonly FROM system.settings
  WHERE name = 'insert_allow_materialized_columns';` returns value `0` and readonly `1`.

  Verify, and expect no ALTER, DROP, TRUNCATE, CREATE or `ON *.*` row for the user:
  `SHOW GRANTS FOR tb_archiver;` and `SELECT * FROM system.role_grants WHERE user_name = 'tb_archiver';` (must
  return no rows). Grant directly to the user: the archiver refuses privileges that arrive through a role.
  The migration user (`archiver.clickhouse.migrateSecretRef`) holds only `CREATE TABLE`, `ALTER ADD COLUMN` and
  `ALTER MODIFY TTL` on `tributary.*` (not the broad `ALTER`, which includes `ALTER DELETE` and `ALTER UPDATE`
  on `audit_log`) and is
  mounted by the migrate Job only. An archiver startup check of its own grants is the archive engineer's.
- C8, before content storage is turned on in production: (1) the ClickHouse `default` user is disabled
  (`<users><default remove="remove"/></users>` or an equivalent `users.d` override), and every client, including
  the archiver, uses a named user; confirm with `SELECT name FROM system.users`. (2) Administrator queries are
  logged outside ClickHouse (a forwarder of the server query log, or the proxy or audit layer in front of it)
  to a store that ClickHouse administrators cannot edit. Neither is automated by this chart.

## Control-plane API (`api.enabled`)

Off by default. A stateless Deployment with a ClusterIP Service on `api.port` (8080) and a NetworkPolicy:
the API port admits the kubelet CIDR (probes) and `networkPolicy.apiFrom`; nothing else.

- PostgreSQL (ADR-004 C1/C2): `api.database.*` become `TB_DATABASE_HOST/_PORT/_NAME/_USER` plus
  `TB_DATABASE_SECRET_REF` (file `<secretRef>/password`) and `TB_DATABASE_CA_SECRET_REF` (file `ca.crt`). The
  API always verifies the server certificate (verify-full), so the CA Secret is required. There is no URL and no
  password in values or environment (`helm_check.py` refuses `TB_DATABASE_URL` and any `*PASSWORD` variable).
- Ops listener (`/metrics`, `/livez`): `TB_OPS_BIND` is loopback by default. With `api.opsBindPodIP=true` it
  binds the pod IP and the NetworkPolicy admits only the OTel collector on `api.opsPort`. The API starts this
  listener only while the tamper check is configured. Liveness, readiness and startup probes use `/livez` and
  `/readyz` on the API port, which the kubelet CIDR can reach.
- Tamper check (ADR-004 R7 rev 3): `api.clickhouse.*` set `TB_CLICKHOUSE_URL` (https), `_USER`, `_SECRET_REF`
  and `TB_AUDIT_TAMPER_TARGETS`. Use a read-only user that cannot write what it grades:
  `CREATE USER tb_api_audit_ro IDENTIFIED WITH sha256_password BY '<from your secrets store>'; GRANT SELECT ON tributary.audit_log TO tb_api_audit_ro;`
  Verify with `SHOW GRANTS FOR tb_api_audit_ro;` (one SELECT row, no INSERT).
- Kafka role `api` (`kafka.roleSecretRefs.api`, default `tb-api`): the chart renders its engine-kafka ConfigMap and,
  when `strimzi.enabled` is also true, a `KafkaUser` with the 5 ACLs of the `api` role in
  `contracts/topics/v0/topic-defaults.json` (generated into `files/acls.json`): READ and WRITE on
  `_tb.<env>.channels`, WRITE on `_tb.<env>.audit`, IDEMPOTENT_WRITE on the cluster and DESCRIBE on group
  `tb.<env>.reconciler`. `helm_check.py` requires the rendered ACLs to equal that role exactly. Without Strimzi,
  create an equivalent user yourself.

## Real-scrape check (W3-8)

`deploy/scrape-check/verify_scrape.sh` starts the lite Compose stack, runs a real Prometheus (pinned by digest)
inside the worker's and the receiver's network namespaces, and requires: both `/metrics` targets up, the worker
exporting `tb_partitions_halted`, and every alert expression of the rendered `PrometheusRule` evaluating without
error. It does not make an alert fire; `partition_halted` firing needs a blocking-retry exhaust and belongs to
the chaos suite. It is Compose-based, not kind: kind is not installed on the lite-stack runner and adds nothing
to the scrape and rule-evaluation proof. Set `TB_COMPOSE_EXTRA` for local compose overrides behind a proxy.
