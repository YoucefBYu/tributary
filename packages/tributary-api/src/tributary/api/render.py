"""Rows to contract bodies (`openapi/v0` components)."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .db.models import Channel, ChannelVersion, Deployment, Environment


def ts(value: datetime) -> str:
    """`Timestamp`: UTC, exactly millisecond precision, `Z`."""
    v = value.astimezone(UTC)
    return v.strftime("%Y-%m-%dT%H:%M:%S.") + f"{v.microsecond // 1000:03d}Z"


def _bundle(spec: Mapping[str, Any]) -> str | None:
    bundle = ((spec.get("spec") or {}).get("pipeline") or {}).get("bundle")
    return bundle if isinstance(bundle, str) and len(bundle) <= 512 else None


def version_summary(channel: str, v: ChannelVersion) -> dict[str, Any]:
    out: dict[str, Any] = {
        "channel": channel,
        "version": v.version,
        "created_at": ts(v.created_at),
        "created_by": v.created_by,
        "origin": v.origin,
        "spec_sha256": v.spec_sha256,
    }
    if (bundle := _bundle(v.spec)) is not None:
        out["bundle"] = bundle
    return out


def version_full(channel: str, v: ChannelVersion) -> dict[str, Any]:
    return {**version_summary(channel, v), "document": v.spec}


def environment(e: Environment) -> dict[str, str]:
    return {"name": e.name, "management_mode": e.mode, "profile": e.profile}


def deployment(d: Deployment, channel: str, env: str, versions: Mapping[uuid.UUID, int]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(d.id),
        "environment": env,
        "channel": channel,
        "kind": d.kind,
        "version": None if d.version_id is None else versions[d.version_id],
        "previous_version": d.previous_version,
        "status": d.status,
        "requested_by": d.created_by,
        "requested_at": ts(d.created_at),
    }
    if d.published_at is not None:
        out["published_at"] = ts(d.published_at)
    if d.failure_code is not None:
        out["failure_code"] = d.failure_code
    return out


def channel_detail(
    ch: Channel, latest: ChannelVersion, states: list[tuple[str, Deployment]], versions: Mapping[uuid.UUID, int]
) -> dict[str, Any]:
    meta = (latest.spec.get("metadata") or {}) if isinstance(latest.spec, dict) else {}
    out: dict[str, Any] = {
        "name": ch.name,
        "latest_version": latest.version,
        "created_at": ts(ch.created_at),
        "deployments": [
            {
                "environment": env,
                "version": None if d.version_id is None else versions[d.version_id],
                "status": d.status,
            }
            for env, d in states
        ],
    }
    if isinstance(meta.get("description"), str):
        out["description"] = meta["description"]
    if isinstance(meta.get("labels"), dict):
        out["labels"] = meta["labels"]
    return out
