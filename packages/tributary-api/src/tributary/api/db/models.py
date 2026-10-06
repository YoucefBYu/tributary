"""SQLAlchemy 2 models. No PHI lives here: specs are configuration, actors are opaque ids."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

ENV_MODES = ("git-managed", "ui-managed")
ENV_PROFILES = ("lite", "production")
DEPLOYMENT_KINDS = ("deploy", "rollback", "undeploy")
#: `openapi/v0` DeploymentStatus. `pending` and `published` count as "current" (what the caller asked for,
#: and what the channels topic holds); `failed` and `superseded` never do.
DEPLOYMENT_STATUSES = ("pending", "published", "failed", "superseded")
FAILURE_CODES = ("PUBLISH_FAILED", "PUBLISH_TIMEOUT")
DIGEST_PATTERN = "^sha256:[0-9a-f]{64}$"
NAME_PATTERN = "^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING)


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class Environment(Base):
    __tablename__ = "environments"
    __table_args__ = (
        CheckConstraint(_in("mode", ENV_MODES), name="mode"),
        CheckConstraint(f"name ~ '{NAME_PATTERN}'", name="name"),
        CheckConstraint(_in("profile", ENV_PROFILES), name="profile"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(63), unique=True)
    mode: Mapped[str] = mapped_column(String(16))
    profile: Mapped[str] = mapped_column(String(16), server_default="production")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Channel(Base):
    __tablename__ = "channels"
    __table_args__ = (CheckConstraint(f"name ~ '{NAME_PATTERN}'", name="name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(63), unique=True)
    created_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ChannelVersion(Base):
    """Immutable (a database trigger rejects UPDATE; rows go only when the channel is deleted)."""

    __tablename__ = "channel_versions"
    __table_args__ = (
        UniqueConstraint("channel_id", "version"),
        CheckConstraint("version >= 1", name="version"),
        CheckConstraint("spec_sha256 ~ '^[0-9a-f]{64}$'", name="spec_sha256"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    channel_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"))
    version: Mapped[int] = mapped_column(Integer)
    spec: Mapped[dict[str, Any]] = mapped_column(JSONB)
    spec_sha256: Mapped[str] = mapped_column(String(64))
    origin: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("""'{"kind": "api"}'::jsonb"""))
    created_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Deployment(Base):
    """One row per deploy, rollback or undeploy REQUEST (`openapi/v0` Deployment). Append-only apart from
    the one transition out of `pending`. `version_id` and `image_digest` are null for an undeploy (a
    tombstone). At most one `pending` row per (channel, environment): a newer request supersedes it."""

    __tablename__ = "deployments"
    __table_args__ = (
        CheckConstraint(_in("kind", DEPLOYMENT_KINDS), name="kind"),
        CheckConstraint(_in("status", DEPLOYMENT_STATUSES), name="status"),
        CheckConstraint(f"image_digest ~ '{DIGEST_PATTERN}'", name="image_digest"),
        CheckConstraint("(kind = 'undeploy') = (version_id IS NULL)", name="undeploy_has_no_version"),
        CheckConstraint("(kind = 'undeploy') = (image_digest IS NULL)", name="undeploy_has_no_image"),
        CheckConstraint("(status = 'published') = (published_at IS NOT NULL)", name="published_at"),
        CheckConstraint(_in("failure_code", FAILURE_CODES), name="failure_code"),
        CheckConstraint("(status = 'failed') = (failure_code IS NOT NULL)", name="failed_has_code"),
        CheckConstraint("previous_version IS NULL OR previous_version >= 1", name="previous_version"),
        Index(
            "uq_deployments_one_pending",
            "channel_id",
            "environment_id",
            unique=True,
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_deployments_channel_env_seq", "channel_id", "environment_id", "seq"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    seq: Mapped[int] = mapped_column(BigInteger, Identity(always=True), unique=True)  # total order of history
    channel_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("channels.id", ondelete="RESTRICT"))
    environment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("environments.id", ondelete="RESTRICT"))
    kind: Mapped[str] = mapped_column(String(16))
    version_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("channel_versions.id", ondelete="RESTRICT"), nullable=True
    )
    image_digest: Mapped[str | None] = mapped_column(String(71), nullable=True)
    previous_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    failure_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    restored_from: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("deployments.id", ondelete="SET NULL"), nullable=True
    )
    created_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
