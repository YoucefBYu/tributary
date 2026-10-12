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


PROBES = ("startupProbe", "readinessProbe", "livenessProbe")


def _api(env: dict[str, str]) -> dict[str, Any]:
    base = {
        "TB_DATABASE_HOST": "h", "TB_DATABASE_NAME": "n", "TB_DATABASE_USER": "u", "TB_DATABASE_SECRET_REF": "s",
        "TB_DATABASE_CA_SECRET_REF": "ca", "TB_OPS_BIND": "$(POD_IP)", "TB_OPS_PORT": "9180", "TB_API_BIND": "$(POD_IP)",
        "TB_API_PORT": "8080", "TB_API_TLS_MODE": "listener", "TB_API_TLS_SECRET_REF": "tls",
    }  # fmt: skip
    c = {
        "name": "api",
        "env": [{"name": k, "value": v} for k, v in {**base, **env}.items()],
        "ports": [{"name": "https", "containerPort": 8080}],
        "volumeMounts": [{"name": "t", "mountPath": "/var/run/secrets/tributary/tls"}],
        **{p: {"httpGet": {"path": "/livez", "port": "https", "scheme": "HTTPS"}} for p in PROBES},
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


def test_api_tls_listener_and_mesh_rules() -> None:
    assert _check_api(_api({"TB_OPS_BIND": "127.0.0.1"})) == []
    # plaintext (mesh values) without TB_API_TLS_MODE=mesh is refused: probes and port name still say https
    assert _check_api(_api({"TB_OPS_BIND": "127.0.0.1", "TB_API_TLS_MODE": "plaintext"}))
    no_mount = _api({"TB_OPS_BIND": "127.0.0.1"})
    no_mount["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] = []
    assert any("TLS Secret mounted" in m for m in _check_api(no_mount))
    sub = _api({"TB_OPS_BIND": "127.0.0.1"})
    sub["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0]["subPath"] = "k"
    assert any("subPath" in m for m in _check_api(sub))
    mesh = _api({"TB_OPS_BIND": "127.0.0.1", "TB_API_TLS_MODE": "mesh"})
    c = mesh["spec"]["template"]["spec"]["containers"][0]
    c["ports"] = [{"name": "http", "containerPort": 8080}]
    for p in PROBES:
        c[p] = {"httpGet": {"path": "/livez", "port": "http", "scheme": "HTTP"}}
    assert any("mesh mode without a NetworkPolicy" in m for m in _check_api(mesh))
    sel = {"app.kubernetes.io/component": "api"}

    def np(frm: list[dict[str, Any]]) -> dict[str, Any]:
        rule = {"from": frm, "ports": [{"port": 8080}]}
        return {
            "kind": "NetworkPolicy",
            "metadata": {"name": "n"},
            "spec": {"podSelector": {"matchLabels": sel}, "ingress": [rule]},
        }

    ctl = {"podSelector": {"matchLabels": {"app": "ingress-nginx"}}}
    assert _check_api(mesh, np([{"ipBlock": {"cidr": "10.20.4.0/24"}}, ctl])) == []
    assert _check_api(mesh, np([{"podSelector": {}}]))
    assert _check_api(mesh, np([{"ipBlock": {"cidr": "0.0.0.0/0"}}, ctl]))


def test_mesh_every_api_port_rule_must_be_ingress_only() -> None:
    mesh = _api({"TB_OPS_BIND": "127.0.0.1", "TB_API_TLS_MODE": "mesh"})
    c = mesh["spec"]["template"]["spec"]["containers"][0]
    c["ports"] = [{"name": "http", "containerPort": 8080}]
    for p in PROBES:
        c[p] = {"httpGet": {"path": "/livez", "port": "http", "scheme": "HTTP"}}
    ctl = {"podSelector": {"matchLabels": {"app": "ingress-nginx"}}}

    def np(*rules: list[dict[str, Any]]) -> dict[str, Any]:
        sel = {"matchLabels": {"app.kubernetes.io/component": "api"}}
        ing = [{"from": r, "ports": [{"port": 8080}]} for r in rules]
        return {"kind": "NetworkPolicy", "metadata": {"name": "n"}, "spec": {"podSelector": sel, "ingress": ing}}

    assert _check_api(mesh, np([ctl])) == []
    assert _check_api(mesh, np([ctl], [{"podSelector": {}}]))  # one good rule must not excuse a bad one
    assert _check_api(mesh, np([ctl, {"ipBlock": {"cidr": "10.0.0.0/16"}}]))  # wider than /24


def test_api_clickhouse_ca_ref_must_match_its_mount() -> None:
    ch = {
        "TB_OPS_BIND": "127.0.0.1",
        "TB_CLICKHOUSE_URL": "https://c",
        "TB_CLICKHOUSE_USER": "ro",
        "TB_SECRETS_DIR": "/var/run/secrets/tributary",
    }
    assert _check_api(_api({**ch, "TB_CLICKHOUSE_CA_SECRET_REF": "ca"})) != []  # not mounted
    ok = _api({**ch, "TB_CLICKHOUSE_CA_SECRET_REF": "chca"})
    ok["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].append(
        {"name": "clickhouse-ca", "mountPath": "/var/run/secrets/tributary/chca"}
    )
    assert _check_api(ok) == []


def test_api_tls_and_ca_volumes_need_mode_0400() -> None:
    dep = _api({"TB_OPS_BIND": "127.0.0.1"})
    spec = dep["spec"]["template"]["spec"]
    spec["volumes"] = [{"name": "api-tls", "secret": {"secretName": "t", "defaultMode": 0o400}}]
    assert _check_api(dep) == []
    spec["volumes"] = [{"name": "api-tls", "secret": {"secretName": "t"}}]
    assert any("defaultMode 0400" in m for m in _check_api(dep))
    spec["volumes"] = [{"name": "clickhouse-ca", "secret": {"secretName": "t", "defaultMode": 0o644}}]
    assert any("defaultMode 0400" in m for m in _check_api(dep))
