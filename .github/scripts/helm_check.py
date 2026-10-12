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
CA_EXPECTED = True  # ci/values-ci.yaml sets archiver.clickhouse.caCertSecretRef
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
        if d["kind"] in ("Deployment", "StatefulSet", "Job"):
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
                froms = rule.get("from", [])
                if any("ipBlock" in f and f["ipBlock"]["cidr"] not in ("0.0.0.0/0", "::/0") for f in froms) and all(
                    "ipBlock" in f or "podSelector" in f for f in froms
                ):
                    cidr_ok = True
                if any(f.get("ipBlock", {}).get("cidr") in ("0.0.0.0/0", "::/0") for f in froms):
                    bad.append("NetworkPolicy opens 9180 to the world")
    if receivers and not cidr_ok:
        bad.append("no NetworkPolicy limiting ingress on 9180 to the kubelet CIDR")

    for n, spec in pods(docs):
        if not n.endswith("-worker"):
            continue
        c = spec["containers"][0]
        if "--ops-bind=$(POD_IP)" not in c["args"]:
            bad.append(f"{n}: worker --ops-bind is not the pod IP")
        if c.get("livenessProbe", {}).get("httpGet", {}).get("path") != "/livez":
            bad.append(f"{n}: worker has no /livez liveness probe")
        if not any(
            x["metadata"]["name"] == n and any(9180 in [p["port"] for p in r["ports"]] for r in x["spec"]["ingress"])
            for x in by_kind.get("NetworkPolicy", [])
        ):
            bad.append(f"{n}: no NetworkPolicy limiting ops port 9180 for the worker")
    arch = [s for n, s in pods(docs) if n.endswith("-archiver")]
    job = [s for n, s in pods(docs) if "archiver-migrate" in n]
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

    bad += check_adr004_conditions(docs, by_kind)
    bad += check_api(by_kind)
    bad += check_workloads(docs, by_kind)
    bad += check_retention(docs, by_kind)
    bad += check_acls(by_kind)
    bad += check_network(docs, by_kind, receivers)
    for name, spec in pods(docs):
        if spec.get("securityContext", {}).get("seccompProfile", {}).get("type") != "RuntimeDefault":
            bad.append(f"{name}: seccompProfile is not RuntimeDefault")
        want = "otel-collector" in name
        if spec.get("automountServiceAccountToken") is not want:
            bad.append(f"{name}: automountServiceAccountToken must be {str(want).lower()}")
    for svc in by_kind.get("Service", []):
        for p in svc["spec"].get("ports", []):
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


def check_workloads(docs: list[dict[str, Any]], by_kind: dict[str, list[dict[str, Any]]]) -> list[str]:
    """StatefulSets (ruling H3), no PVCs, no contracts volume (ruling H4), archiver CA path (W3-8 follow-up)."""
    bad: list[str] = []
    for d in by_kind.get("Deployment", []):
        if "otel-collector" not in d["metadata"]["name"] and not d["metadata"]["name"].endswith("-api"):
            bad.append(f"{d['metadata']['name']}: data-plane workload is a Deployment, must be a StatefulSet")
    sets = by_kind.get("StatefulSet", [])
    if not sets:
        bad.append("no StatefulSet rendered")
    names = {d["metadata"]["name"] for d in by_kind.get("Service", []) if d["spec"].get("clusterIP") == "None"}
    for d in sets:
        n = d["metadata"]["name"]
        if d["spec"].get("podManagementPolicy") != "Parallel":
            bad.append(f"{n}: podManagementPolicy is not Parallel")
        if d["spec"].get("volumeClaimTemplates"):
            bad.append(f"{n}: has volumeClaimTemplates")
        if d["spec"].get("serviceName") not in names:
            bad.append(f"{n}: serviceName is not a rendered headless Service")
    for so in by_kind.get("ScaledObject", []):
        if so["spec"]["scaleTargetRef"].get("kind") != "StatefulSet":
            bad.append("ScaledObject scaleTargetRef.kind is not StatefulSet")
    for name, spec in pods(docs):
        for v in spec.get("volumes", []):
            if v["name"] == "contracts" or v.get("configMap", {}).get("name", "").endswith("contracts"):
                bad.append(f"{name}: has a contracts volume (contracts/ ships in the image)")
        for c in spec["containers"]:
            for e in c.get("env", []):
                if e["name"] == "TB_CONTRACTS_DIR":
                    bad.append(f"{name}: sets TB_CONTRACTS_DIR (the image sets it)")
                if e["name"] == "TB_ARCHIVE_CLICKHOUSE_CA_CERT" and not re.fullmatch(
                    r"/var/run/secrets/tributary-ca/[^/]+", e["value"]
                ):
                    bad.append(f"{name}: TB_ARCHIVE_CLICKHOUSE_CA_CERT is not a mounted file path (empty key?)")
    ca = [
        e
        for _, s2 in pods(docs)
        for c in s2["containers"]
        for e in c.get("env", [])
        if e["name"] == "TB_ARCHIVE_CLICKHOUSE_CA_CERT"
    ]
    if CA_EXPECTED and not ca:
        bad.append("archiver CA cert env not rendered although caCertSecretRef is set in the CI values")
    return bad


# ClickHouse plaintext HTTP, native, MySQL, PostgreSQL-compat, interserver. 5432 is allowed: PostgreSQL TLS runs on it.
PLAINTEXT_DB_PORTS = {8123, 9000, 9004, 9005, 9009}
MAX_REAUTH_MS = 3_600_000


def _uses_secret(spec: dict[str, Any], volume: str) -> bool:
    for v in spec.get("volumes", []):
        if v["name"] != volume:
            continue
        if "secret" in v:
            return True
        return any("secret" in src for src in v.get("projected", {}).get("sources", []))
    return False


def check_adr004_conditions(docs: list[dict[str, Any]], by_kind: dict[str, list[dict[str, Any]]]) -> list[str]:
    """ADR-004 C1 (no plaintext ClickHouse port exposed, TLS on, sslmode verify-full where set), C3 (broker re-authentication), C4 (no subPath Secret mounts).

    A `subPath` mount never receives a rotated Secret, so a revoked or rotated credential would stay in the
    container until it restarts (C4). Plaintext ClickHouse and PostgreSQL ports must not be published by a
    Service, a pod port or a NetworkPolicy rule: the chart only ever talks to them over TLS.
    """
    bad: list[str] = []
    named: dict[str, int] = {}  # container port names that resolve to a plaintext database port
    for _, spec in pods(docs):
        for c in [*spec["containers"], *spec.get("initContainers", [])]:
            for p in c.get("ports", []):
                if p.get("containerPort") in PLAINTEXT_DB_PORTS and "name" in p:
                    named[p["name"]] = p["containerPort"]

    def plain(port: Any, end: Any = None) -> bool:
        if isinstance(port, str):
            return port in named or (port.isdigit() and int(port) in PLAINTEXT_DB_PORTS)
        if not isinstance(port, int):
            return False
        return any(port <= x <= (end if isinstance(end, int) else port) for x in PLAINTEXT_DB_PORTS)

    for name, spec in pods(docs):
        for c in [*spec["containers"], *spec.get("initContainers", [])]:
            for m in c.get("volumeMounts", []):
                if ("subPath" in m or "subPathExpr" in m) and _uses_secret(spec, m["name"]):
                    bad.append(f"{name}: Secret volume {m['name']} is mounted with subPath (never rotated, C4)")
            for p in c.get("ports", []):
                for key in ("containerPort", "hostPort"):
                    if plain(p.get(key)):
                        bad.append(f"{name}: pod exposes plaintext database port {p[key]} (C1)")
            for e in c.get("env", []):
                val = str(e.get("value", ""))
                if e["name"].endswith("_PORT") and plain(val):
                    bad.append(f"{name}: {e['name']} points at plaintext database port {val} (C1)")
                if e["name"] == "TB_ARCHIVE_CLICKHOUSE_SECURE" and val != "1":
                    bad.append(f"{name}: ClickHouse TLS is off (TB_ARCHIVE_CLICKHOUSE_SECURE != 1, C1)")
                if "sslmode" in e["name"].lower() and val != "verify-full":
                    bad.append(f"{name}: {e['name']} is not verify-full (C1)")
                if "sslmode=" in val and "sslmode=verify-full" not in val:
                    bad.append(f"{name}: {e['name']} sets an sslmode other than verify-full (C1)")
                if val.startswith(("postgresql://", "postgres://", "postgresql+asyncpg://")) and "sslmode=" not in val:
                    bad.append(f"{name}: {e['name']} is a PostgreSQL URL without sslmode=verify-full (C1)")
    for svc in by_kind.get("Service", []):
        for p in svc["spec"].get("ports", []):
            if plain(p.get("port")) or plain(p.get("targetPort")):
                bad.append(f"Service {svc['metadata']['name']} exposes plaintext database port (C1)")
    for np in by_kind.get("NetworkPolicy", []):
        for rule in [*np["spec"].get("ingress", []), *np["spec"].get("egress", [])]:
            for p in rule.get("ports", []):
                if plain(p.get("port"), p.get("endPort")):
                    bad.append(f"NetworkPolicy {np['metadata']['name']} opens plaintext database port {p['port']} (C1)")
    for np in by_kind.get("NetworkPolicy", []):
        for i, rule in enumerate(np["spec"].get("egress", [])):
            if not rule.get("ports"):
                bad.append(f"NetworkPolicy {np['metadata']['name']}: egress rule {i} has no ports (opens every port)")
    for j in by_kind.get("Job", []):
        spec = j["spec"]
        if "archiver-migrate" in j["metadata"]["name"]:
            if spec.get("parallelism") != 1 or spec.get("completions") != 1:
                bad.append("archiver migrate Job must set parallelism 1 and completions 1")
            if "before-hook-creation" in j["metadata"].get("annotations", {}).get("helm.sh/hook-delete-policy", ""):
                bad.append("archiver migrate Job must not use before-hook-creation (it would replace a live migration)")
    for k in by_kind.get("Kafka", []):
        v = k["spec"]["kafka"].get("config", {}).get("connections.max.reauth.ms")
        if not isinstance(v, int) or isinstance(v, bool) or not 0 < v <= MAX_REAUTH_MS:
            bad.append(f"Kafka {k['metadata']['name']}: connections.max.reauth.ms must be 1..{MAX_REAUTH_MS} (C3)")
    return bad


def _envs(dep: dict[str, Any]) -> dict[str, str]:
    c = dep["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: str(e.get("value", "$(valueFrom)")) for e in c.get("env", [])}


def check_api(by_kind: dict[str, list[dict[str, Any]]]) -> list[str]:
    """tributary-api (W3-34, ADR-004 C1/C2, ops/v0 O2): PostgreSQL by secretRef with verify-full inputs, ops
    listener on loopback or on the pod IP behind a collector-only NetworkPolicy, a kubelet probe on the API
    port, a read-only ClickHouse user, a ClusterIP Service and a NetworkPolicy."""
    apis = [d for d in by_kind.get("Deployment", []) if d["metadata"]["name"].endswith("-api")]
    bad: list[str] = []
    for dep in apis:
        n = dep["metadata"]["name"]
        env = _envs(dep)
        spec = dep["spec"]["template"]["spec"]
        c = spec["containers"][0]
        for k in ("TB_DATABASE_HOST", "TB_DATABASE_NAME", "TB_DATABASE_USER", "TB_DATABASE_SECRET_REF"):
            if not env.get(k):
                bad.append(f"{n}: {k} is not set (ADR-004 C2)")
        if not env.get("TB_DATABASE_CA_SECRET_REF"):
            bad.append(f"{n}: no database CA secretRef, so sslmode verify-full cannot be met (C1)")
        for k in env:
            if k == "TB_DATABASE_URL" or "PASSWORD" in k:
                bad.append(f"{n}: {k} carries a database credential in the environment (C2)")
        if env.get("TB_DATABASE_SSLMODE", "verify-full") != "verify-full":
            bad.append(f"{n}: TB_DATABASE_SSLMODE is not verify-full (C1)")
        ops = env.get("TB_OPS_BIND", "")
        if ops not in ("127.0.0.1", "$(POD_IP)"):
            bad.append(f"{n}: TB_OPS_BIND must be loopback or the pod IP, got {ops!r}")
        if env.get("TB_API_BIND") not in ("$(POD_IP)", "127.0.0.1"):
            bad.append(f"{n}: TB_API_BIND is not the pod IP")
        if ops == "$(POD_IP)":
            ops_port = int(env["TB_OPS_PORT"])
            nps = [
                np
                for np in by_kind.get("NetworkPolicy", [])
                if np["spec"].get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "api"
            ]
            if not nps:
                bad.append(f"{n}: ops bound to the pod IP without an api NetworkPolicy")
            for np in nps:
                for rule in np["spec"].get("ingress", []):
                    if any(p.get("port") == ops_port for p in rule.get("ports", [])):
                        if not rule.get("from"):
                            bad.append(f"{n}: an ops-port ingress rule with no `from` admits everyone")
                        for f in rule.get("from", []):
                            comp = f.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component")
                            if "ipBlock" in f or comp != "otel-collector":
                                bad.append(f"{n}: the ops port admits more than the OTel collector")
                    elif not rule.get("ports"):
                        bad.append(f"{n}: an ingress rule without ports opens the ops port")
        live = c.get("livenessProbe", {}).get("httpGet", {})
        if live.get("path") != "/livez" or live.get("port") != "http":
            bad.append(f"{n}: liveness must be kubelet httpGet /livez on the API port")
        if env.get("TB_CLICKHOUSE_URL"):
            if not env["TB_CLICKHOUSE_URL"].startswith("https://"):
                bad.append(f"{n}: TB_CLICKHOUSE_URL is not https (C1)")
            if env.get("TB_CLICKHOUSE_USER", "default") == "default":
                bad.append(f"{n}: the tamper check uses the ClickHouse default user (needs a SELECT-only user)")
        if not c["securityContext"].get("readOnlyRootFilesystem"):
            bad.append(f"{n}: root filesystem is writable")
    for svc in by_kind.get("Service", []):
        if svc["metadata"]["name"].endswith("-api") and svc["spec"].get("type", "ClusterIP") != "ClusterIP":
            bad.append(f"Service {svc['metadata']['name']} is not ClusterIP")
    return bad


def check_retention(docs: list[dict[str, Any]], by_kind: dict[str, list[dict[str, Any]]]) -> list[str]:
    """ADR-004 R2/R3: the three TTL vars on archiver and migrate Job with the R2 defaults; the decrease guard
    on the Job only; the audit topic buffer read from the contract."""
    bad: list[str] = []
    want = {"TB_ARCHIVE_TTL_DAYS": "90", "TB_ARCHIVE_CONTENT_TTL_DAYS": "30", "TB_ARCHIVE_AUDIT_TTL_DAYS": "2190"}
    for name, spec in pods(docs):
        if not (name.endswith("-archiver") or "archiver-migrate" in name):
            continue
        env = {e["name"]: e.get("value") for c in spec["containers"] for e in c.get("env", [])}
        for k, v in want.items():
            if env.get(k) != v:
                bad.append(f"{name}: {k} is {env.get(k)!r}, expected the R2 default {v!r}")
        guard = env.get("TB_ARCHIVE_ALLOW_TTL_DECREASE")
        if name.endswith("-archiver") and guard is not None:
            bad.append(f"{name}: the decrease guard must reach the migrate Job only")
        if "archiver-migrate" in name and guard != "false":
            bad.append(f"{name}: TB_ARCHIVE_ALLOW_TTL_DECREASE is not 'false' by default")
    topics = by_kind.get("KafkaTopic", [])
    if by_kind.get("KafkaUser"):
        audit = [t for t in topics if t["spec"]["topicName"].endswith(".audit")]
        contract = json.loads((ROOT / "contracts/topics/v0/topic-defaults.json").read_text())["classes"]["audit"]
        if len(audit) != 1:
            bad.append("no _tb.<env>.audit KafkaTopic")
        elif audit[0]["spec"]["config"].get("retention.ms") != str(contract["configs"]["retention.ms"]):
            bad.append("audit topic retention.ms differs from topic-defaults.json")
        elif audit[0]["spec"]["partitions"] != contract["partitions"]:
            bad.append("audit topic partitions differ from topic-defaults.json")
    return bad


def check_acls(by_kind: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Rendered KafkaUser ACLs against the contract (re-parsed independently of the template)."""
    users = {u["metadata"]["labels"]["tributary.dev/role"]: u for u in by_kind.get("KafkaUser", [])}
    if not users:
        return []  # strimzi.enabled=false: nothing rendered
    bad: list[str] = []
    for role in ("receiver", "worker", "loader", "archiver", "lag-reader"):
        if role not in users:
            bad.append(f"no KafkaUser for role {role}")
    rendered = {
        role: {
            (a["resource"]["type"], a["resource"].get("name", ""), a["resource"].get("patternType", ""), op)
            for a in u["spec"]["authorization"]["acls"]
            for op in a["operations"]
        }
        for role, u in users.items()
    }
    for role, acls in rendered.items():
        for t, name, _pat, op in acls:
            if name in ("*", "") and t != "cluster":
                bad.append(f"KafkaUser {role}: wildcard or empty resource name")
            if op in ("Alter", "Delete", "All") or (t == "cluster" and op not in ("IdempotentWrite", "Describe")):
                bad.append(f"KafkaUser {role}: forbidden operation {op} on {t}")
            if op == "Write" and name.endswith(".channels") and role != "api":
                bad.append(f"KafkaUser {role}: WRITE on the channels topic")
    # Sync-reply grants (set 1.2): present for the syncReply channel, absent for the other one.
    rcv, wrk = rendered.get("receiver", set()), rendered.get("worker", set())
    if ("topic", "prod.adt-passthrough.reply", "literal", "Read") not in rcv or (
        "group",
        "tb.prod.adt-passthrough.replier",
        "literal",
        "Read",
    ) not in rcv:
        bad.append("receiver lacks the sync-reply READ on reply or the replier group (every sync request would AR)")
    if ("topic", "prod.adt-passthrough.reply", "literal", "Write") not in wrk:
        bad.append("worker lacks WRITE on the sync-reply topic")
    if any(".reply" in n or ".replier" in n for _t, n, _p, op in rcv | wrk if "lab-results" in n):
        bad.append("sync-reply grants rendered for a channel without syncReply")
    if any(op == "Write" and n.endswith(".reply") for _t, n, _p, op in rcv):
        bad.append("receiver can WRITE reply (it could forge an answer)")
    lag = rendered.get("lag-reader", set())
    if any(op != "Describe" for *_, op in lag):
        bad.append("lag_reader holds more than DESCRIBE")
    if not any(op == "Write" for *_, op in rendered.get("receiver", set())):
        bad.append("receiver has no WRITE (raw)")
    if any(t == "topic" and ".out." in n and op == "Write" for t, n, _p, op in rendered.get("receiver", set())):
        bad.append("receiver can WRITE out topics")
    # Two-way against the generated file: every contract grant for the cluster-independent roles appears.
    gen = json.loads((CHART / "files" / "acls.json").read_text())["roles"]
    for role, key in (("archiver", "archiver"), ("worker", "worker")):
        want = {
            (g["resourceType"].lower(), g["name"].replace("@ENV@", "prod"))
            for g in gen[key]
            if "@CHANNEL@" not in g["name"]
        }
        have = {(t, n) for t, n, _p, _o in rendered.get(role, set())}
        for t, n in want:
            if t != "cluster" and (t, n) not in have:
                bad.append(f"KafkaUser {role}: contract grant on {n} missing from the render")
    if "api" in rendered:  # rendered only with api.enabled: exactly the contract's `api` role, nothing more
        camel = lambda op: "".join(w.capitalize() for w in op.split("_"))  # noqa: E731
        want_api = {
            (
                g["resourceType"].lower(),
                "" if g["resourceType"] == "CLUSTER" else g["name"].replace("@ENV@", "prod"),
                "" if g["resourceType"] == "CLUSTER" else g["patternType"].lower(),
                camel(g["operation"]),
            )
            for g in gen["api"]
        }
        have_api = {(t, n, p, op) for t, n, p, op in rendered["api"]}
        if have_api != want_api:
            bad.append(
                f"KafkaUser api: ACLs differ from acls.json (missing {sorted(want_api - have_api)}, extra {sorted(have_api - want_api)})"
            )
    return bad


def check_network(
    docs: list[dict[str, Any]], by_kind: dict[str, list[dict[str, Any]]], receivers: list[Any]
) -> list[str]:
    bad: list[str] = []
    nps = by_kind.get("NetworkPolicy", [])
    if not any(n["spec"]["podSelector"] == {} and "ingress" not in n["spec"] for n in nps):
        bad.append("no namespace default-deny NetworkPolicy")
    labels = {
        d["metadata"]["name"]: d["spec"]["template"]["metadata"]["labels"]
        for d in docs
        if d["kind"] in ("Deployment", "StatefulSet")
    }
    for name, _ in receivers:
        sel = [n["spec"]["podSelector"].get("matchLabels", {}) for n in nps if n["metadata"]["name"] == name]
        if not sel or not sel[0] or not sel[0].items() <= labels[name].items():
            bad.append(f"{name}: no NetworkPolicy whose selector matches the receiver pods")
    for n in nps:
        for rule in n["spec"].get("ingress", []):
            ports = {p["port"] for p in rule.get("ports", [])}
            if 9180 in ports:
                if not rule.get("from"):
                    bad.append(
                        f"NetworkPolicy {n['metadata']['name']}: a 9180 ingress rule with no `from` admits everyone"
                    )
                for f in rule.get("from", []):
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
                        if d["metadata"]["name"] == name and d["kind"] in ("Deployment", "StatefulSet")
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
