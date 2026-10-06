"""Initial control-plane schema: environments, channels, channel_versions, deployments.

Revision ID: 0001
Revises:

Explicit DDL, not `Base.metadata.create_all`: a migration must not change when the models do.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NAME = "^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$"


def _uuid_pk() -> sa.Column[Any]:
    return sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False)


def _created_at() -> sa.Column[Any]:
    return sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False)


def upgrade() -> None:
    op.create_table(
        "environments",
        _uuid_pk(),
        sa.Column("name", sa.String(63), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_environments"),
        sa.UniqueConstraint("name", name="uq_environments_name"),
        sa.CheckConstraint("mode IN ('git-managed', 'ui-managed')", name=op.f("ck_environments_mode")),
        sa.CheckConstraint(f"name ~ '{_NAME}'", name=op.f("ck_environments_name")),
    )
    op.create_table(
        "channels",
        _uuid_pk(),
        sa.Column("name", sa.String(63), nullable=False),
        sa.Column("created_by", sa.String(128), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_channels"),
        sa.UniqueConstraint("name", name="uq_channels_name"),
        sa.CheckConstraint(f"name ~ '{_NAME}'", name=op.f("ck_channels_name")),
    )
    op.create_table(
        "channel_versions",
        _uuid_pk(),
        sa.Column("channel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("spec", postgresql.JSONB(), nullable=False),
        sa.Column("spec_sha256", sa.String(64), nullable=False),
        sa.Column("created_by", sa.String(128), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_channel_versions"),
        sa.ForeignKeyConstraint(
            ["channel_id"], ["channels.id"], name="fk_channel_versions_channel_id_channels", ondelete="CASCADE"
        ),
        sa.UniqueConstraint("channel_id", "version", name="uq_channel_versions_channel_id"),
        sa.CheckConstraint("version >= 1", name=op.f("ck_channel_versions_version")),
        sa.CheckConstraint("spec_sha256 ~ '^[0-9a-f]{64}$'", name=op.f("ck_channel_versions_spec_sha256")),
    )
    op.create_table(
        "deployments",
        _uuid_pk(),
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("channel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("image_digest", sa.String(71), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("restored_from", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_by", sa.String(128), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_deployments"),
        sa.UniqueConstraint("seq", name="uq_deployments_seq"),
        sa.ForeignKeyConstraint(
            ["channel_id"], ["channels.id"], name="fk_deployments_channel_id_channels", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["environment_id"],
            ["environments.id"],
            name="fk_deployments_environment_id_environments",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["version_id"],
            ["channel_versions.id"],
            name="fk_deployments_version_id_channel_versions",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["restored_from"], ["deployments.id"], name="fk_deployments_restored_from_deployments", ondelete="SET NULL"
        ),
        sa.CheckConstraint("status IN ('active', 'superseded', 'rolled_back')", name=op.f("ck_deployments_status")),
        sa.CheckConstraint("image_digest ~ '^sha256:[0-9a-f]{64}$'", name=op.f("ck_deployments_image_digest")),
    )
    op.create_index(
        "uq_deployments_one_active",
        "deployments",
        ["channel_id", "environment_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index("ix_deployments_channel_env_seq", "deployments", ["channel_id", "environment_id", "seq"])
    # UPDATE-only on purpose: deleting a channel cascades to its versions (intended; deployments RESTRICT
    # the delete while any exist), so a DELETE trigger would block that.
    op.execute(
        """
        CREATE FUNCTION tb_channel_versions_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'channel_versions rows are immutable';
        END $$
        """
    )
    op.execute(
        "CREATE TRIGGER channel_versions_no_update BEFORE UPDATE ON channel_versions "
        "FOR EACH ROW EXECUTE FUNCTION tb_channel_versions_immutable()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER channel_versions_no_update ON channel_versions")
    op.execute("DROP FUNCTION tb_channel_versions_immutable()")
    op.drop_table("deployments")
    op.drop_table("channel_versions")
    op.drop_table("channels")
    op.drop_table("environments")
