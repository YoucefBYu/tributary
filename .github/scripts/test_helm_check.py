"""Controls for helm_check's ADR-004 condition checks: each broken fixture must be refused."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import helm_check as h


def _run(*docs: dict[str, Any]) -> list[str]:
    by: dict[str, list[dict[str, Any]]] = {}
    for d in docs:
        by.setdefault(d["kind"], []).append(d)
    return h.check_adr004_conditions(list(docs), by)


def _sts(
    ports: list[dict[str, Any]] | None = None, env: list[dict[str, str]] | None = None, mounts: list[Any] | None = None
) -> dict[str, Any]:
    c = {"name": "c", "ports": ports or [], "env": env or [], "volumeMounts": mounts or []}
    spec = {"containers": [c], "volumes": [{"name": "s", "secret": {"secretName": "x"}}]}
    return {"kind": "StatefulSet", "metadata": {"name": "x"}, "spec": {"template": {"spec": spec}}}


def test_clean_fixture_passes() -> None:
    assert _run(_sts(env=[{"name": "TB_ARCHIVE_CLICKHOUSE_SECURE", "value": "1"}])) == []


def test_subpath_secret_named_port_range_tls_off_and_sslmode_are_refused() -> None:
    assert _run(_sts(mounts=[{"name": "s", "mountPath": "/a", "subPath": "k"}]))
    named = _sts(ports=[{"name": "chttp", "containerPort": 8123}])
    svc = {"kind": "Service", "metadata": {"name": "s"}, "spec": {"ports": [{"port": 80, "targetPort": "chttp"}]}}
    assert any("Service" in m for m in _run(named, svc))
    np = {
        "kind": "NetworkPolicy",
        "metadata": {"name": "n"},
        "spec": {"egress": [{"ports": [{"port": 8000, "endPort": 9100}]}]},
    }
    assert any("NetworkPolicy" in m for m in _run(_sts(), np))
    assert _run(_sts(env=[{"name": "TB_ARCHIVE_CLICKHOUSE_SECURE", "value": "0"}]))
    assert _run(_sts(env=[{"name": "TB_DATABASE_SSLMODE", "value": "require"}]))
    assert _run(_sts(ports=[{"containerPort": 9005}]))
    assert _run(_sts(ports=[{"containerPort": 5432}])) == []  # PostgreSQL TLS runs on 5432


def test_reauth_missing_or_too_large_is_refused() -> None:
    def kafka(cfg: dict[str, Any]) -> dict[str, Any]:
        return {"kind": "Kafka", "metadata": {"name": "k"}, "spec": {"kafka": {"config": cfg}}}

    assert _run(kafka({}))
    assert _run(kafka({"connections.max.reauth.ms": 7200000}))
    assert _run(kafka({"connections.max.reauth.ms": 3600000})) == []


def test_postgres_url_without_sslmode_and_portless_egress_are_refused() -> None:
    assert _run(_sts(env=[{"name": "TB_DB", "value": "postgresql://u@h/db"}]))
    assert _run(_sts(env=[{"name": "TB_DB", "value": "postgresql://u@h/db?sslmode=verify-full"}])) == []
    np = {"kind": "NetworkPolicy", "metadata": {"name": "n"}, "spec": {"egress": [{"to": [{"podSelector": {}}]}]}}
    assert any("no ports" in m for m in _run(_sts(), np))
