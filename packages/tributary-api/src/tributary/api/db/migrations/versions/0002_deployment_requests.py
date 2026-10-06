"""Deployments become request records (`openapi/v0`): kind, previous_version, pending/published/failed,
undeploy tombstones; environments gain a profile, channel versions an origin.

Revision ID: 0002
Revises: 0001

Why this reshapes 0001's `deployments` instead of adding beside it: 0001 modelled "which version is
active" (`active`/`superseded`/`rolled_back`, a mandatory image digest). The contract's record is a
request with a publish outcome, and an undeploy has no version. Two tables for one fact would drift.
`image_digest` is still stored, taken from `spec.pipeline.bundle` (digest-pinned by the channel schema).

Data: 0001 rows are mapped (active -> published, rolled_back/superseded -> superseded; a row with
`restored_from` is a rollback). The downgrade is lossy by necessity: pending, failed and undeploy rows
cannot be expressed by 0001 and are deleted.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("environments", sa.Column("profile", sa.String(16), server_default="production", nullable=False))
    op.create_check_constraint(op.f("ck_environments_profile"), "environments", "profile IN ('lite', 'production')")
    # ADD COLUMN with a default does not fire the UPDATE-only immutability trigger.
    op.add_column(
        "channel_versions",
        sa.Column(
            "origin",
            sa.dialects.postgresql.JSONB(),
            server_default=sa.text("""'{"kind": "api"}'::jsonb"""),
            nullable=False,
        ),
    )

    # `sub` is up to 255 ASCII characters (OIDC Core) and is stored as `created_by` (ADR-009 D7).
    for table in ("channels", "channel_versions", "deployments"):
        op.alter_column(table, "created_by", type_=sa.String(255), existing_nullable=False)
    op.add_column("deployments", sa.Column("kind", sa.String(16), nullable=True))
    op.add_column("deployments", sa.Column("previous_version", sa.Integer(), nullable=True))
    op.add_column("deployments", sa.Column("failure_code", sa.String(32), nullable=True))
    op.add_column("deployments", sa.Column("published_at", sa.DateTime(timezone=True), nullable=True))
    op.drop_index("uq_deployments_one_active", table_name="deployments")
    op.drop_constraint(op.f("ck_deployments_status"), "deployments", type_="check")
    op.execute("UPDATE deployments SET kind = CASE WHEN restored_from IS NULL THEN 'deploy' ELSE 'rollback' END")
    op.execute("UPDATE deployments SET status = 'published', published_at = created_at WHERE status = 'active'")
    op.execute("UPDATE deployments SET status = 'superseded' WHERE status = 'rolled_back'")
    op.alter_column("deployments", "kind", nullable=False)
    op.alter_column("deployments", "version_id", nullable=True)
    op.alter_column("deployments", "image_digest", nullable=True)
    for name, expr in (
        ("kind", "kind IN ('deploy', 'rollback', 'undeploy')"),
        ("status", "status IN ('pending', 'published', 'failed', 'superseded')"),
        ("undeploy_has_no_version", "(kind = 'undeploy') = (version_id IS NULL)"),
        ("undeploy_has_no_image", "(kind = 'undeploy') = (image_digest IS NULL)"),
        ("published_at", "(status = 'published') = (published_at IS NOT NULL)"),
        ("failure_code", "failure_code IN ('PUBLISH_FAILED', 'PUBLISH_TIMEOUT')"),
        ("failed_has_code", "(status = 'failed') = (failure_code IS NOT NULL)"),
        ("previous_version", "previous_version IS NULL OR previous_version >= 1"),
    ):
        op.create_check_constraint(op.f(f"ck_deployments_{name}"), "deployments", expr)
    op.create_index(
        "uq_deployments_one_pending",
        "deployments",
        ["channel_id", "environment_id"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("uq_deployments_one_pending", table_name="deployments")
    for table in ("channels", "channel_versions", "deployments"):
        op.alter_column(
            table, "created_by", type_=sa.String(128), existing_nullable=False, postgresql_using="left(created_by, 128)"
        )
    op.execute("DELETE FROM deployments WHERE kind = 'undeploy' OR status IN ('pending', 'failed')")
    for name in (
        "previous_version",
        "failed_has_code",
        "failure_code",
        "published_at",
        "undeploy_has_no_image",
        "undeploy_has_no_version",
        "status",
        "kind",
    ):
        op.drop_constraint(op.f(f"ck_deployments_{name}"), "deployments", type_="check")
    # Latest published row per (channel, environment) is the active one; the rest are superseded.
    op.execute(
        """
        UPDATE deployments d SET status = CASE
            WHEN d.status = 'published' AND d.seq = (
                SELECT max(x.seq) FROM deployments x
                WHERE x.channel_id = d.channel_id AND x.environment_id = d.environment_id AND x.status = 'published'
            ) THEN 'active'
            ELSE 'superseded' END
        """
    )
    op.alter_column("deployments", "image_digest", nullable=False)
    op.alter_column("deployments", "version_id", nullable=False)
    op.create_check_constraint(
        op.f("ck_deployments_status"), "deployments", "status IN ('active', 'superseded', 'rolled_back')"
    )
    op.create_index(
        "uq_deployments_one_active",
        "deployments",
        ["channel_id", "environment_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.drop_column("deployments", "published_at")
    op.drop_column("deployments", "failure_code")
    op.drop_column("deployments", "previous_version")
    op.drop_column("deployments", "kind")
    op.drop_column("channel_versions", "origin")
    op.drop_constraint(op.f("ck_environments_profile"), "environments", type_="check")
    op.drop_column("environments", "profile")
