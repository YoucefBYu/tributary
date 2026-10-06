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

The migrate step that reads these variables is archive-engineer's W3-30. Until it lands, the variables are
rendered but nothing applies them.

## Key custody checklist (ADR-004 H1, draft)

- Production uses customer-managed keys (KMS CMK, CMEK, or HSM or Vault on premises) on every PHI-at-rest
  surface: Kafka volumes, ClickHouse and PostgreSQL volumes, the claim-check bucket.
- The claim-check bucket has default encryption on, and the processes that read or write it hold only the KMS
  permissions they need.
- The content archiver's Secret is filled by your secrets store, rotated on your schedule (90 days suggested,
  a judgement), and never templated into values.
