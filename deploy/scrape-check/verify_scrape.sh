#!/usr/bin/env bash
# W3-8 real-scrape check. A real Prometheus (not `helm template`) scrapes /metrics from the running lite
# stack's worker and receiver, loads the chart's rendered PrometheusRule alerts, and must evaluate every
# alert expression without error. Compose-based on purpose: kind is not available on the CI runner image
# used for the lite stack and is far heavier than this proof needs.
#
#   deploy/scrape-check/verify_scrape.sh            # builds nothing; brings the lite stack up first
#
# The stack is torn down on exit (volumes included); set TB_KEEP_STACK=1 to keep it.
# Env: TB_COMPOSE_PROJECT (default tbscrape), TB_COMPOSE_EXTRA (extra `-f` files, space separated),
#      TB_PROM_IMAGE (pinned by digest), TB_HELM (helm binary).
# Prometheus shares each target's network namespace (`--network container:<name>`), so it reaches the ops
# listener on that container's loopback (the default bind): nothing is published or exposed for this check.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROJECT=${TB_COMPOSE_PROJECT:-tbscrape}
PROM=${TB_PROM_IMAGE:-quay.io/prometheus/prometheus@sha256:2659f4c2ebb718e7695cb9b25ffa7d6be64db013daba13e05c875451cf51b0d3}
HELM=${TB_HELM:-helm}
WORK=$(mktemp -d)
FILES=(-f "$ROOT/deploy/compose/docker-compose.yml")
for f in ${TB_COMPOSE_EXTRA:-}; do FILES+=(-f "$f"); done
cleanup() {
  if [ "${TB_KEEP_STACK:-0}" != "1" ]; then docker compose -p "$PROJECT" "${FILES[@]}" down -v >/dev/null 2>&1 || true; fi
  docker rm -f "${PROJECT}-prom-worker" "${PROJECT}-prom-receiver" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# 1. The chart's alert rules, exactly as rendered.
CHART=$ROOT/deploy/helm/tributary
"$HELM" template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true >"$WORK/rendered.yaml"
uv run --no-project --with pyyaml==6.0.3 python -I - "$WORK/rendered.yaml" "$WORK/rules.yml" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
rule = next(d for d in docs if d["kind"] == "PrometheusRule")
yaml.safe_dump({"groups": rule["spec"]["groups"]}, open(sys.argv[2], "w"))
PY

# 2. The lite stack.
docker compose -p "$PROJECT" "${FILES[@]}" up -d --wait >/dev/null

scrape() { # <container> <label>
  local target=$1 label=$2 name="${PROJECT}-prom-$2"
  cat >"$WORK/prom-$label.yml" <<CFG
global: {scrape_interval: 2s, evaluation_interval: 2s}
rule_files: [/etc/prometheus/rules.yml]
scrape_configs:
  - job_name: $label
    static_configs: [{targets: ["127.0.0.1:9180"]}]
CFG
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker create --name "$name" --network "container:$target" "$PROM" \
    --config.file=/etc/prometheus/prometheus.yml --web.listen-address=127.0.0.1:9091 \
    --storage.tsdb.path=/prometheus >/dev/null
  docker cp "$WORK/prom-$label.yml" "$name:/etc/prometheus/prometheus.yml"
  docker cp "$WORK/rules.yml" "$name:/etc/prometheus/rules.yml"
  docker start "$name" >/dev/null
  local api="http://127.0.0.1:9091" out
  for _ in $(seq 1 40); do
    out=$(docker exec "$name" wget -qO- "$api/api/v1/targets" 2>/dev/null || true)
    if grep -q '"health":"up"' <<<"$out"; then break; fi
    sleep 1
  done
  grep -q '"health":"up"' <<<"$out" || { echo "FAIL: $label target is not up"; echo "$out" | head -c 400; return 1; }
  echo "ok: Prometheus scraped $label /metrics (target up)"
  sleep 6   # two evaluation cycles
  docker exec "$name" wget -qO- "$api/api/v1/rules" >"$WORK/rules-$label.json"
  docker exec "$name" wget -qO- "$api/api/v1/label/__name__/values" >"$WORK/names-$label.json"
}
scrape tb-worker worker
scrape tb-mllp-receiver receiver

# 3. Verdicts.
uv run --no-project python -I - "$WORK" <<'PY'
import json, sys
w = sys.argv[1]
rules = json.load(open(f"{w}/rules-worker.json"))["data"]["groups"]
alerts = [r for g in rules for r in g["rules"] if r["type"] == "alerting"]
bad = [a["name"] for a in alerts if a["health"] != "ok" or a.get("lastError")]
assert alerts, "no alerting rules loaded"
assert not bad, f"alert expressions that failed to evaluate: {bad}"
print(f"ok: {len(alerts)} alert expressions evaluated without error")
names = set(json.load(open(f"{w}/names-worker.json"))["data"])
assert "tb_partitions_halted" in names, "worker did not export tb_partitions_halted"
print("ok: worker exports tb_partitions_halted")
r = set(json.load(open(f"{w}/names-receiver.json"))["data"])
assert any(n.startswith("tb_") for n in r), "receiver exported no tb_ series"
print(f"ok: receiver exports {len([n for n in r if n.startswith('tb_')])} tb_ series")
halted = next(a for a in alerts if a["name"] == "TributaryPartitionHalted")
print(f"info: TributaryPartitionHalted state={halted['state']} (firing is demonstrated by the chaos suite, not here)")
PY
echo "scrape-check: ok"
