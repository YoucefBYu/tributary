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


def _api(env: dict[str, str]) -> dict[str, Any]:
    base = {
        "TB_DATABASE_HOST": "h", "TB_DATABASE_NAME": "n", "TB_DATABASE_USER": "u", "TB_DATABASE_SECRET_REF": "s",
        "TB_DATABASE_CA_SECRET_REF": "ca", "TB_OPS_BIND": "$(POD_IP)", "TB_OPS_PORT": "9180", "TB_API_BIND": "$(POD_IP)",
    }  # fmt: skip
    c = {
        "name": "api",
        "env": [{"name": k, "value": v} for k, v in {**base, **env}.items()],
        "livenessProbe": {"httpGet": {"path": "/livez", "port": "http"}},
        "securityContext": {"readOnlyRootFilesystem": True},
    }
    return {"kind": "Deployment", "metadata": {"name": "tb-api"}, "spec": {"template": {"spec": {"containers": [c]}}}}


def _api_np(frm: list[dict[str, Any]]) -> dict[str, Any]:
    sel = {"matchLabels": {"app.kubernetes.io/component": "api"}}
    rule = {"from": frm, "ports": [{"port": 9180}]}
    return {"kind": "NetworkPolicy", "metadata": {"name": "tb-api"}, "spec": {"podSelector": sel, "ingress": [rule]}}


def _check_api(*docs: dict[str, Any]) -> list[str]:
    by: dict[str, list[dict[str, Any]]] = {}
    for d in docs:
        by.setdefault(d["kind"], []).append(d)
    return h.check_api(by)


def test_api_ops_on_pod_ip_needs_a_collector_only_policy() -> None:
    collector = {"podSelector": {"matchLabels": {"app.kubernetes.io/component": "otel-collector"}}}
    assert _check_api(_api({}), _api_np([collector])) == []
    assert any("without an api NetworkPolicy" in m for m in _check_api(_api({})))
    assert any(
        "more than the OTel collector" in m
        for m in _check_api(_api({}), _api_np([{"ipBlock": {"cidr": "10.0.0.0/8"}}]))
    )
    assert any("more than the OTel collector" in m for m in _check_api(_api({}), _api_np([{"podSelector": {}}])))
    assert _check_api(_api({"TB_OPS_BIND": "127.0.0.1"})) == []


def test_api_refuses_weak_database_and_clickhouse_settings() -> None:
    assert _check_api(_api({"TB_DATABASE_CA_SECRET_REF": ""}), _api_np([]))
    assert _check_api(_api({"TB_DATABASE_PASSWORD": "x"}), _api_np([]))
    assert _check_api(_api({"TB_OPS_BIND": "0.0.0.0"}))  # noqa: S104
    assert _check_api(_api({"TB_OPS_BIND": "127.0.0.1", "TB_CLICKHOUSE_URL": "http://c", "TB_CLICKHOUSE_USER": "u"}))
    assert _check_api(
        _api({"TB_OPS_BIND": "127.0.0.1", "TB_CLICKHOUSE_URL": "https://c", "TB_CLICKHOUSE_USER": "default"})
    )


def test_api_ops_rule_without_from_is_refused() -> None:
    assert any("no `from`" in m for m in _check_api(_api({}), _api_np([])))
    np = _api_np([])
    del np["spec"]["ingress"][0]["from"]
    assert any("no `from`" in m for m in _check_api(_api({}), np))
