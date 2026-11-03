"""Rows to contract bodies (`openapi/v0` components)."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .db.models import Channel, ChannelVersion, Deployment, Environment, Replay


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


def replay(row: Replay, environment: str, channel: str, *, now: datetime) -> dict[str, Any]:
    """`Replay`. A `running` row whose deadline passed reads as `failed` / `INTERRUPTED`: the API process that
    started it is gone (or its run never recorded an end), and the contract says so (README 3.14)."""
    status, failure, finished = row.status, row.failure_code, row.finished_at
    if status == "running" and now >= row.deadline_at:
        status, failure, finished = "failed", "INTERRUPTED", row.deadline_at
    out: dict[str, Any] = {
        "replay_id": str(row.replay_id),
        "environment": environment,
        "channel": channel,
        "target_topic": f"{environment}.{channel}.raw",
        "range_start": ts(row.range_start),
        "range_end": ts(row.range_end),
        "allow_reorder": row.allow_reorder,
        "max_records": row.max_records,
        "deadline_s": row.deadline_s,
        "requested_by": row.requested_by,
        "requested_at": ts(row.requested_at),
        "status": status,
        "deadline_at": ts(row.deadline_at),
    }
    if finished is not None:
        out["finished_at"] = ts(finished)
    if status == "completed":
        for name in (
            "partitions_scanned", "span", "republished", "skipped_undecodable", "skipped_out_of_range",
            "skipped_no_timestamp",
        ):  # fmt: skip
            out[name] = int(getattr(row, name) or 0)
    elif status == "failed":
        out["republished"] = int(row.republished or 0)
        out["failure_code"] = failure
    return out
