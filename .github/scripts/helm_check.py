"""Assertions over `helm template` output for deploy/helm/tributary (W3-8).

Usage: helm_check.py <rendered.yaml> [--negative]
Exits 1 listing every failed check; names only, never values. Needs PyYAML (CI: `uv run --no-project --with pyyaml`).
`--negative` is the mutation control: it expects the rendered file to FAIL (CI feeds it a tampered render).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "helm" / "tributary"
FORBIDDEN_LOG_VAR = re.compile(r"\$(request|request_uri|args|query_string|http_authorization|http_cookie)\b")
METRIC = re.compile(r"\btb_[a-z][a-z0-9_]*")


def registry() -> set[str]:
    names: set[str] = set()
    for m in json.loads((ROOT / "contracts/observability/v0/metrics.json").read_text())["metrics"]:
        names.add(m["name"])
        if m["type"] == "histogram":
            names |= {m["name"] + s for s in ("_bucket", "_sum", "_count")}
    return names


def pods(docs: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for d in docs:
        if d["kind"] in ("Deployment", "Job"):
            out.append((d["metadata"]["name"], d["spec"]["template"]["spec"]))
    return out


def check(docs: list[dict[str, Any]]) -> list[str]:
    bad: list[str] = []
    known = registry()
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for d in docs:
        by_kind.setdefault(d["kind"], []).append(d)

    # Metric names in dashboards and rules exist in the contract registry.
    texts = [p.read_text() for p in sorted((CHART / "dashboards").glob("*.json"))]
    for rule in by_kind.get("PrometheusRule", []):
        texts += [r["expr"] for g in rule["spec"]["groups"] for r in g["rules"]]
    for name in {n for t in texts for n in METRIC.findall(t)} - known:
        bad.append(f"metric {name} is not in contracts/observability/v0/metrics.json")

    rules = [r for rule in by_kind.get("PrometheusRule", []) for g in rule["spec"]["groups"] for r in g["rules"]]
    if not any("tb_partitions_halted" in r["expr"] and r.get("alert") for r in rules):
        bad.append("no alert on partition_halted (tb_partitions_halted)")

    for name, spec in pods(docs):
        for c in spec["containers"]:
            sc = c.get("securityContext", {})
            if not (
                sc.get("readOnlyRootFilesystem")
                and sc.get("runAsNonRoot")
                and sc.get("allowPrivilegeEscalation") is False
            ):
                bad.append(f"{name}: container is not non-root, read-only, no-escalation")
            if sc.get("capabilities", {}).get("drop") != ["ALL"]:
                bad.append(f"{name}: capabilities not dropped")
        env = {e["name"]: e for c in spec["containers"] for e in c.get("env", [])}
        if "TB_INSTANCE_ID" in env and env["TB_INSTANCE_ID"]["valueFrom"]["fieldRef"]["fieldPath"] != "metadata.name":
            bad.append(f"{name}: TB_INSTANCE_ID is not metadata.name")

    receivers = [(n, s) for n, s in pods(docs) if n.endswith("-receiver")]
    if not receivers:
        bad.append("no receiver Deployment rendered")
    cidr_ok = False
    for n, spec in receivers:
        c = spec["containers"][0]
        args = c["args"]
        env = {e["name"]: e["valueFrom"]["fieldRef"]["fieldPath"] for e in c["env"] if "valueFrom" in e}
        if "--instance=$(POD_NAME)" not in args or env.get("POD_NAME") != "metadata.name":
            bad.append(f"{n}: --instance is not the pod name from metadata.name")
        if "--ops-bind=$(POD_IP)" not in args or env.get("POD_IP") != "status.podIP":
            bad.append(f"{n}: --ops-bind is not the pod IP")
        for probe, path in (("readinessProbe", "/readyz"), ("livenessProbe", "/livez")):
            hg = c.get(probe, {}).get("httpGet", {})
            if hg.get("path") != path or hg.get("port") != "ops":
                bad.append(f"{n}: {probe} is not {path} on the ops port")
        ops = next((p["containerPort"] for p in c["ports"] if p["name"] == "ops"), None)
        if ops != 9180:
            bad.append(f"{n}: ops port is not 9180")
    for np in by_kind.get("NetworkPolicy", []):
        for rule in np["spec"].get("ingress", []):
            if any(p["port"] == 9180 for p in rule["ports"]):
                froms = rule["from"]
                if any("ipBlock" in f and f["ipBlock"]["cidr"] not in ("0.0.0.0/0", "::/0") for f in froms) and all(
                    "ipBlock" in f or "podSelector" in f for f in froms
                ):
                    cidr_ok = True
                if any(f.get("ipBlock", {}).get("cidr") in ("0.0.0.0/0", "::/0") for f in froms):
                    bad.append("NetworkPolicy opens 9180 to the world")
    if receivers and not cidr_ok:
        bad.append("no NetworkPolicy limiting ingress on 9180 to the kubelet CIDR")

    arch = [s for n, s in pods(docs) if n.endswith("-archiver")]
    job = [s for n, s in pods(docs) if n.endswith("-archiver-migrate")]
    if not arch or "--skip-ddl" not in arch[0]["containers"][0]["args"]:
        bad.append("archiver Deployment does not run --skip-ddl")
    if not job or "--migrate-only" not in job[0]["containers"][0]["args"]:
        bad.append("no --migrate-only Job")
    else:
        hook = next(d for d in docs if d["kind"] == "Job")["metadata"]["annotations"]["helm.sh/hook"]
        if hook != "pre-install,pre-upgrade":
            bad.append("migrate Job is not pre-install,pre-upgrade")
        runtime_env = {e["name"] for e in arch[0]["containers"][0]["env"]} if arch else set()
        job_env = {e["name"] for e in job[0]["containers"][0]["env"]}
        if (
            "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF" not in job_env
            or "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF" in runtime_env
        ):
            bad.append("migration secretRef is not confined to the migrate Job")
        if arch:
            runtime_ref = next(
                e["value"] for e in arch[0]["containers"][0]["env"] if e["name"] == "TB_ARCHIVE_CLICKHOUSE_SECRET_REF"
            )
            job_ref = next(
                e["value"]
                for e in job[0]["containers"][0]["env"]
                if e["name"] == "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF"
            )
            if runtime_ref == job_ref:
                bad.append("runtime and migration secretRef are the same")

    bad += check_network(docs, by_kind, receivers)
    for name, spec in pods(docs):
        if spec.get("securityContext", {}).get("seccompProfile", {}).get("type") != "RuntimeDefault":
            bad.append(f"{name}: seccompProfile is not RuntimeDefault")
        want = "otel-collector" in name
        if spec.get("automountServiceAccountToken") is not want:
            bad.append(f"{name}: automountServiceAccountToken must be {str(want).lower()}")
    for svc in by_kind.get("Service", []):
        for p in svc["spec"]["ports"]:
            if p["port"] == 9180 or p.get("targetPort") in (9180, "ops"):
                bad.append(f"Service {svc['metadata']['name']} exposes the ops port")

    if not by_kind.get("ScaledObject"):
        bad.append("no KEDA ScaledObject")
    for so in by_kind.get("ScaledObject", []):
        if so["spec"]["triggers"][0]["type"] != "kafka":
            bad.append("ScaledObject trigger is not kafka lag")
    if not by_kind.get("PodDisruptionBudget"):
        bad.append("no PodDisruptionBudget")

    for cm in by_kind.get("ConfigMap", []):
        fmt = cm.get("data", {}).get("log-format-upstream")
        if fmt is not None:
            for v in FORBIDDEN_LOG_VAR.findall(fmt):
                bad.append(f"proxy log format contains ${v} (ADR-009 D1)")
    return bad


def check_network(
    docs: list[dict[str, Any]], by_kind: dict[str, list[dict[str, Any]]], receivers: list[Any]
) -> list[str]:
    bad: list[str] = []
    nps = by_kind.get("NetworkPolicy", [])
    if not any(n["spec"]["podSelector"] == {} and "ingress" not in n["spec"] for n in nps):
        bad.append("no namespace default-deny NetworkPolicy")
    labels = {
        d["metadata"]["name"]: d["spec"]["template"]["metadata"]["labels"] for d in docs if d["kind"] == "Deployment"
    }
    for name, _ in receivers:
        sel = [n["spec"]["podSelector"].get("matchLabels", {}) for n in nps if n["metadata"]["name"] == name]
        if not sel or not sel[0] or not sel[0].items() <= labels[name].items():
            bad.append(f"{name}: no NetworkPolicy whose selector matches the receiver pods")
    for n in nps:
        for rule in n["spec"].get("ingress", []):
            ports = {p["port"] for p in rule.get("ports", [])}
            if 9180 in ports:
                for f in rule["from"]:
                    sel = f.get("podSelector", f.get("namespaceSelector"))
                    if ("podSelector" in f or "namespaceSelector" in f) and not sel:
                        bad.append("NetworkPolicy allows 9180 from podSelector {} (all pods)")
                    if "podSelector" in f and "namespaceSelector" not in f and not f["podSelector"].get("matchLabels"):
                        bad.append("NetworkPolicy allows 9180 from an unlabelled podSelector")
            for name, _ in receivers:
                if n["metadata"]["name"] == name:
                    spec_ports = {
                        p["containerPort"]
                        for d in docs
                        if d["metadata"]["name"] == name and d["kind"] == "Deployment"
                        for c in d["spec"]["template"]["spec"]["containers"]
                        for p in c["ports"]
                        if p["name"] == "ingest"
                    }
                    if ports & spec_ports and not rule.get("from"):
                        bad.append(f"{name}: ingest port is open to everyone (must be default-deny)")
    return bad


def main() -> int:
    docs = [d for d in yaml.safe_load_all(Path(sys.argv[1]).read_text()) if d]
    bad = check(docs)
    for b in bad:
        print("FAIL:", b)
    if "--negative" in sys.argv:
        return 0 if bad else 1
    print("helm_check: ok" if not bad else f"helm_check: {len(bad)} failure(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
