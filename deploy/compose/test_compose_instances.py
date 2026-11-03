"""W3-28d: every Compose service that runs a receiver, worker, loader or archiver sets an explicit,
distinct instance id (the pid default is identical in every container and would fence replicas)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

COMPOSE = Path(__file__).resolve().parent / "docker-compose.yml"
RUNNERS = ("tributary.receivers.", "tributary.runtime.worker", "tributary.loaders.", "tributary.archiver")


def _instance(svc: dict[str, Any]) -> str | None:
    cmd = svc.get("command", [])
    for a in cmd:
        for flag in ("--instance=", "--instance-id="):
            if str(a).startswith(flag):
                return str(a)[len(flag) :]
    return (svc.get("environment") or {}).get("TB_INSTANCE_ID")


def test_every_runner_service_has_a_distinct_explicit_instance_id() -> None:
    services = yaml.safe_load(COMPOSE.read_text())["services"]
    ids: dict[str, str] = {}
    for name, svc in services.items():
        cmd = svc.get("command", [])
        if not any(str(a).startswith(RUNNERS) for a in cmd[:1]):
            continue
        inst = _instance(svc)
        assert inst, f"{name} runs {cmd[0]} without an explicit instance id"
        assert inst not in ids.values(), f"{name} reuses instance id {inst}"
        ids[name] = inst
    assert {"mllp-receiver", "worker", "loader"} <= set(ids)
