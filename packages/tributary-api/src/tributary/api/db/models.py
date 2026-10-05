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
DEPLOYMENT_STATUSES = ("active", "superseded", "rolled_back")
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
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(63), unique=True)
    mode: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Channel(Base):
    __tablename__ = "channels"
    __table_args__ = (CheckConstraint(f"name ~ '{NAME_PATTERN}'", name="name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(63), unique=True)
    created_by: Mapped[str] = mapped_column(String(128))
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
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Deployment(Base):
    """One row per deploy or rollback. At most one `active` row per (channel, environment)."""

    __tablename__ = "deployments"
    __table_args__ = (
        CheckConstraint(_in("status", DEPLOYMENT_STATUSES), name="status"),
        CheckConstraint(f"image_digest ~ '{DIGEST_PATTERN}'", name="image_digest"),
        Index(
            "uq_deployments_one_active",
            "channel_id",
            "environment_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
        Index("ix_deployments_channel_env_seq", "channel_id", "environment_id", "seq"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    seq: Mapped[int] = mapped_column(BigInteger, Identity(always=True), unique=True)  # total order of history
    channel_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("channels.id", ondelete="RESTRICT"))
    environment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("environments.id", ondelete="RESTRICT"))
    version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("channel_versions.id", ondelete="RESTRICT"))
    image_digest: Mapped[str] = mapped_column(String(71))
    status: Mapped[str] = mapped_column(String(16))
    restored_from: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("deployments.id", ondelete="SET NULL"), nullable=True
    )
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
