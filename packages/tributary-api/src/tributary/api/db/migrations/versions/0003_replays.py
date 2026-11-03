"""Replays (`openapi/v0` Replay): the API's own record of a channel replay and its outcome.

Revision ID: 0003
Revises: 0002

One row per `replay_id` (a UUIDv7 the caller mints; the idempotency key). `body_sha256` is the hash of the
canonical request, so the same id with a different body is `REQUEST_ID_REUSED`. A row stays `running` until the
background run records its end; a run the API process did not finish reads as `failed` / `INTERRUPTED` once
`deadline_at` has passed (`render`), so no sweeper is needed. Counts only: no payload, no key, no header value.
The downgrade drops the table (replay history is operational state, not a record of truth).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FAILURE_CODES = (
    "AUDIT_UNAVAILABLE", "TOPIC_NOT_FOUND", "RANGE_TOO_LARGE", "SOURCE_ERROR", "SOURCE_TIMEOUT",
    "DEADLINE_EXCEEDED", "PRODUCE_FAILED", "PRODUCE_TIMEOUT", "PRODUCE_REFUSED", "ENVELOPE_INVALID",
    "INTERRUPTED", "INTERNAL",
)  # fmt: skip


def upgrade() -> None:
    codes = ", ".join(repr(c) for c in FAILURE_CODES)
    op.create_table(
        "replays",
        sa.Column("replay_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("environment_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("body_sha256", sa.String(64), nullable=False),
        sa.Column("range_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("range_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("allow_reorder", sa.Boolean(), nullable=False),
        sa.Column("max_records", sa.Integer(), nullable=False),
        sa.Column("deadline_s", sa.Integer(), nullable=False),
        sa.Column("requested_by", sa.String(255), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("partitions_scanned", sa.Integer(), nullable=True),
        sa.Column("span", sa.BigInteger(), nullable=True),
        sa.Column("republished", sa.BigInteger(), nullable=True),
        sa.Column("skipped_undecodable", sa.BigInteger(), nullable=True),
        sa.Column("skipped_out_of_range", sa.BigInteger(), nullable=True),
        sa.Column("skipped_no_timestamp", sa.BigInteger(), nullable=True),
        sa.Column("failure_code", sa.String(32), nullable=True),
        sa.PrimaryKeyConstraint("replay_id", name=op.f("pk_replays")),
        sa.UniqueConstraint("seq", name=op.f("uq_replays_seq")),
        sa.ForeignKeyConstraint(
            ["environment_id"], ["environments.id"], name="fk_replays_environment_id_environments", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"], ["channels.id"], name="fk_replays_channel_id_channels", ondelete="RESTRICT"
        ),
        sa.CheckConstraint("status IN ('running', 'completed', 'failed')", name=op.f("ck_replays_status")),
        sa.CheckConstraint("body_sha256 ~ '^[0-9a-f]{64}$'", name=op.f("ck_replays_body_sha256")),
        sa.CheckConstraint("range_start < range_end", name=op.f("ck_replays_range")),
        sa.CheckConstraint(f"failure_code IN ({codes})", name=op.f("ck_replays_failure_code")),
        sa.CheckConstraint("(status = 'failed') = (failure_code IS NOT NULL)", name=op.f("ck_replays_failed_has_code")),
        sa.CheckConstraint("(status = 'running') = (finished_at IS NULL)", name=op.f("ck_replays_finished_at")),
    )
    op.create_index("ix_replays_channel_env_seq", "replays", ["channel_id", "environment_id", "seq"])


def downgrade() -> None:
    op.drop_table("replays")
