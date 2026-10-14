#!/usr/bin/env bash
# Local CI (docs/agent-team/decisions/ci-alternatives.md, option C). The ONE place the CI commands live:
# .github/workflows/ci.yml and helm.yml call this script and nothing else (test_ci_parity.py enforces it),
# so the local gate and CI cannot drift.
#
#   ci_local.sh [--sha REV] [--report] <job>...
#
# jobs:  contracts  lint  python  integration  supply-chain  image-check  ui  helm
#        fast  (contracts, python, supply-chain)        all  (every job, in CI order)
# other: install-helm-tools   list-jobs   ui-present   (used by the workflows; see below)
#
# Default (and --sha REV): run REV (default HEAD) in a clean, detached git worktree under a temp dir,
# removed afterwards. Uncommitted changes are NOT tested, on purpose: this is the committed state a push
# publishes. `uv sync` is the bare `uv sync` CI runs, with UV_FROZEN=1; it is safe here because the worktree has
# its own .venv and never touches the shared checkout's environment.
#
# --here   run in the current checkout (what the workflows do). Refused unless CI=true or TB_CI_LOCAL_HERE=1:
#          a bare `uv sync` in a shared checkout drops workspace packages other agents need.
# --report post a `local/<job>` commit status (success or failure) for the sha. The token is read ONLY from the
#          environment variable TB_STATUS_TOKEN; it is never echoed, logged, written to disk or passed to a job.
#          Repo: TB_STATUS_REPO=owner/repo, else the origin remote. Self-attested: it proves this script ran on
#          that sha on this machine (ci-alternatives.md option C).
#
# Each job prints `PASS <job>: <summary line>` or `FAIL <job>: ...` with the last lines of its output, and the
# script exits non-zero if any job failed. Full logs: $TB_CI_LOGS (default a temp dir, path printed).
set -euo pipefail

ALL_JOBS=(contracts python supply-chain image-check integration helm ui)
FAST_JOBS=(contracts python supply-chain)
ALL_NAMES="contracts lint python integration supply-chain image-check ui helm fast all install-helm-tools list-jobs ui-present"

HELM_VERSION=v3.16.2
HELM_SHA256=9318379b847e333460d33d291d4c088156299a26cd93d570a7f5d0c36e50b5bb
KUBECONFORM_VERSION=v0.6.7
KUBECONFORM_SHA256=95f14e87aa28c09d5941f11bd024c1d02fdc0303ccaa23f61cef67bc92619d73
CHART=deploy/helm/tributary
export UV_FROZEN=1   # fail if uv.lock is out of date, never re-resolve (same as ci.yml)

SELF="${BASH_SOURCE[0]}"

# ---------------------------------------------------------------------------------------------- job bodies
# Run by the inner process (`ci_local.sh __run <job>`) with `set -eu`, cwd = the checkout root. Every command
# here is the command ci.yml / helm.yml used to carry inline.

step() {  # step <name>: a folding group in the Actions log, a plain banner elsewhere
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::group::$1"; else echo "== $1"; fi
}
endstep() { if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::endgroup::"; fi; }

job_contracts() {
  # architect-owned, dependency-free by design: jsonschema plus the standard library only, run with
  # --no-project so it proves contracts/ is valid on a clean clone before the workspace exists.
  step 'contracts validate (without openapi-spec-validator the default run fails by design)'
  uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py
  endstep
  step 'pinned failure pairs have not drifted'
  uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py --check-expectations
  endstep
}

job_lint() {
  step 'uv sync'; uv sync; endstep
  step 'ruff check and format'; uv run ruff check .; uv run ruff format --check .; endstep
  step 'mypy packages'; uv run mypy packages; endstep
}

job_python() {
  job_lint
  step 'fast tests'; uv run pytest -m "not integration and not chaos and not bench"; endstep
  # Explicit so a contracts/** change can never skip it, even if testpaths change.
  step 'contract conformance'; uv run pytest tests/conformance -q; endstep
  # Named so a marker, testpaths or collection change cannot silently drop the sync-reply property tests.
  step 'sync-reply property tests'
  uv run pytest -q \
    packages/tributary-receivers/tests/reply/test_properties.py \
    packages/tributary-runtime/tests/test_reply_properties.py
  endstep
}

job_integration() {
  step 'uv sync'; uv sync; endstep
  # Collection check first (no Docker needed): the -m integration selection must equal
  # tests/support/integration_expected.json. Then the run itself: passed == expected count and skipped == 0,
  # so Docker missing or a skip/deselect cannot pass.
  step 'integration collection matches expected'; uv run python -m tests.support.integration_expected --check; endstep
  step 'integration tests'; uv run pytest -m integration -ra --junitxml=integration-junit.xml; endstep
  step 'integration passed count equals expected, none skipped'
  uv run python .github/scripts/check_integration_counts.py integration-junit.xml
  endstep
}

job_supply_chain() {
  step 'uv sync'; uv sync; endstep
  step 'gitignore still protects sensitive paths'; uv run python .github/scripts/gitignore_guard.py; endstep
  step 'secret scan'; uv run python .github/scripts/secret_scan.py; endstep
  step 'licence check (runtime, dev and build-system requires) and SBOM'
  uv run python .github/scripts/licence_check.py --sbom sbom.cdx.json
  endstep
}

job_image_check() {
  # ADR-007 follow-up 18 / D24, W3-13: builds each package image and runs image_env_check.py inside it, plus a
  # negative control per image. TB_BUILD_CA_BUNDLE=/path/ca.crt passes an extra CA to the build (proxies only).
  step 'image environment check'; .github/scripts/image_check.sh; endstep
}

job_ui() {
  if [ ! -f ui/package.json ]; then echo "skipped: ui/ does not exist yet"; return 0; fi
  step 'ui lint, typecheck, test'
  pnpm --dir ui install --frozen-lockfile
  pnpm --dir ui lint
  pnpm --dir ui typecheck
  pnpm --dir ui test --run
  endstep
}

# --- helm: official release tarballs, verified against a pinned checksum before extraction ------------------
tools_dir() { echo "${TB_CI_TOOLS:-${XDG_CACHE_HOME:-$HOME/.cache}/tributary-ci-tools}"; }

install_helm_tools() {  # prints the directories to put on PATH, one per line (workflows append them to $GITHUB_PATH)
  local d; d="$(tools_dir)/helm-${HELM_VERSION}-kubeconform-${KUBECONFORM_VERSION}"
  if [ ! -x "$d/linux-amd64/helm" ] || [ ! -x "$d/kubeconform" ]; then
    mkdir -p "$d"
    (
      set -euo pipefail
      cd "$d"
      curl -sSfLO "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz"
      echo "${HELM_SHA256}  helm-${HELM_VERSION}-linux-amd64.tar.gz" | sha256sum -c - >&2
      tar xzf "helm-${HELM_VERSION}-linux-amd64.tar.gz" linux-amd64/helm
      curl -sSfLO "https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/kubeconform-linux-amd64.tar.gz"
      echo "${KUBECONFORM_SHA256}  kubeconform-linux-amd64.tar.gz" | sha256sum -c - >&2
      tar xzf kubeconform-linux-amd64.tar.gz kubeconform
    )
  fi
  echo "$d/linux-amd64"; echo "$d"
}

have_helm_tools() {
  command -v helm >/dev/null 2>&1 && command -v kubeconform >/dev/null 2>&1 \
    && [ "$(helm version --short 2>/dev/null | cut -d+ -f1)" = "$HELM_VERSION" ] \
    && [ "$(kubeconform -v 2>/dev/null)" = "$KUBECONFORM_VERSION" ]
}

job_helm() {
  if ! have_helm_tools; then
    step 'install helm and kubeconform (checksum-verified)'
    local p; p="$(install_helm_tools | paste -sd: -)"; export PATH="$p:$PATH"
    endstep
  fi
  trap 'rm -f rendered.yaml tampered*.yaml mesh*.yaml ops*.yaml' EXIT
  step 'KafkaUser ACLs match topic-defaults.json (drift check)'
  (
python3 deploy/helm/gen_acls.py --check
  )
  endstep

  step 'helm lint'
  (
helm lint "$CHART" -f "$CHART/ci/values-ci.yaml" --strict
  )
  endstep

  step 'helm template (all optional parts on)'
  (
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true > rendered.yaml
  )
  endstep

  step 'kubeconform (CRDs without a published schema are skipped)'
  (
kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version 1.30.0 rendered.yaml
  )
  endstep

  step 'chart assertions (instance, probes, ops NetworkPolicy, skip-ddl, metric names, log format)'
  (
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py rendered.yaml
  )
  endstep

  step 'mutation control, a tampered render must fail the assertions'
  (
sed 's/--ops-bind=\$(POD_IP)/--ops-bind=127.0.0.1/' rendered.yaml > tampered.yaml
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py tampered.yaml --negative
  )
  endstep

  step 'mutation controls, ADR-004 C1/C3/C4 (subPath Secret, plaintext DB port, missing re-auth) must fail'
  (
py="uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py"
# C4: a subPath on the ClickHouse Secret mount
sed 's/{name: clickhouse, mountPath: \([^}]*\)}/{name: clickhouse, mountPath: \1, subPath: x}/' rendered.yaml > tampered-subpath.yaml
grep -q 'subPath: x' tampered-subpath.yaml
$py tampered-subpath.yaml --negative
# C1: a plaintext ClickHouse port in the archiver env
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true --set archiver.clickhouse.port=8123 > tampered-port.yaml
$py tampered-port.yaml --negative
# C3: re-auth interval above the cap, and removed
# (the reauth check applies only to a render with strimzi.enabled=true, which is how rendered.yaml is made)
sed 's/connections.max.reauth.ms: 3600000/connections.max.reauth.ms: 7200000/' rendered.yaml > tampered-reauth.yaml
grep -q 7200000 tampered-reauth.yaml
$py tampered-reauth.yaml --negative
grep -v 'connections.max.reauth.ms' rendered.yaml > tampered-reauth-removed.yaml
$py tampered-reauth-removed.yaml --negative
# C1: ClickHouse TLS off
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true --set archiver.clickhouse.secure=false > tampered-tls.yaml
$py tampered-tls.yaml --negative
  )
  endstep

  step 'mutation controls, API (database CA, ops bind, liveness port, ClickHouse user and TLS) must fail'
  (
py="uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py"
mut() { sed "$1" rendered.yaml > tampered-api.yaml; cmp -s rendered.yaml tampered-api.yaml && exit 1; $py tampered-api.yaml --negative; }
mut 's/{name: TB_DATABASE_CA_SECRET_REF, value: "tb-api-db-ca"}/{name: TB_X, value: "1"}/'
mut 's/{name: TB_OPS_BIND, value: "\$(POD_IP)"}/{name: TB_OPS_BIND, value: "0.0.0.0"}/'
mut '/livenessProbe/,+1 s#port: http#port: ops#'
mut 's/value: "tb_api_audit_ro"/value: "default"/'
mut 's#value: "https://clickhouse.example.internal:8443"#value: "http://clickhouse.example.internal:8123"#'
  )
  endstep

  step 'API TLS, ADR-004 C1a (mesh render passes; plaintext, missing mount, plain ingress and a missing mesh policy fail)'
  (
py="uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py"
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true --set api.tls.mode=mesh \
  --set 'api.tls.ingressControllerFrom[0].podSelector.matchLabels.app\.kubernetes\.io/name=ingress-nginx' > mesh.yaml
$py mesh.yaml
python3 - <<'PY'
import re
t = open("mesh.yaml").read()
docs = t.split("\n---\n")
kept = [d for d in docs if not (re.search(r"kind: NetworkPolicy", d) and re.search(r"name: tb-api\n", d))]
assert len(kept) < len(docs), "no api NetworkPolicy to remove"
open("mesh-nopolicy.yaml", "w").write("\n---\n".join(kept))
PY
$py mesh-nopolicy.yaml --negative
mut() { sed "$1" rendered.yaml > tampered-tls.yaml; cmp -s rendered.yaml tampered-tls.yaml && exit 1; $py tampered-tls.yaml --negative; }
mut 's/{name: TB_API_TLS_MODE, value: "listener"}/{name: TB_API_TLS_MODE, value: "plaintext"}/'
mut 's/scheme: HTTPS/scheme: HTTP/'
mut '/name: api-tls, mountPath/d'
mut 's/backend-protocol: "HTTPS"/backend-protocol: "HTTP"/'
mut 's/proxy-ssl-verify: "on"/proxy-ssl-verify: "off"/'
if helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set api.tls.mode=mesh > /dev/null 2>&1; then
  echo "mesh without ingressControllerFrom must not render" >&2; exit 1
fi
mesh_set=(--set api.tls.mode=mesh --set 'api.tls.ingressControllerFrom[0].podSelector.matchLabels.app=x')
# mesh with the NetworkPolicy off (even with the ops opt-out) is refused, with no opt-out
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" "${mesh_set[@]}" --set channels=null --set ingress.enabled=false --set api.opsBindPodIP=false \
  --set networkPolicy.enabled=false --set networkPolicy.allowUnprotectedOpsPort=true 2>&1 | grep -q "networkPolicy.enabled must be true"
# open or empty controller peers are refused at render time
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set api.tls.mode=mesh --set 'api.tls.ingressControllerFrom[0].ipBlock.cidr=0.0.0.0/0' 2>&1 | grep -q "ipBlock peer is not allowed"
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set api.tls.mode=mesh --set 'api.tls.ingressControllerFrom[0].namespaceSelector=null' 2>&1 | grep -q "podSelector.matchLabels is required"
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set api.tls.mode=mesh --set 'api.tls.ingressControllerFrom[0].podSelector.matchLabels=null' 2>&1 | grep -q "podSelector.matchLabels is required"
# L1: a namespaceSelector-only peer would admit every pod in that namespace
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set api.tls.mode=mesh --set 'api.tls.ingressControllerFrom[0].namespaceSelector.matchLabels.team=ingress' 2>&1 | grep -q "a namespaceSelector alone admits every pod"
printf 'api:\n  tls:\n    mode: mesh\n    ingressControllerFrom:\n      - namespaceSelector: {matchExpressions: [{key: kubernetes.io/metadata.name, operator: Exists}]}\n' > mesh-expr.yaml
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" -f mesh-expr.yaml 2>&1 | grep -q "matchExpressions is not allowed"
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" "${mesh_set[@]}" --set networkPolicy.kubeletCIDR=10.20.0.0/16 2>&1 | grep -q "/24 or tighter"
  )
  endstep

  step 'an API ops listener on the pod IP without a NetworkPolicy is refused'
  (
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set channels=null --set networkPolicy.enabled=false 2>&1 | grep -q "unprotected"
# the archiver alone (no channels, no API) also binds the pod IP
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set channels=null --set ingress.enabled=false --set api.enabled=false --set networkPolicy.enabled=false 2>&1 | grep -q "unprotected"
# tampered renders: an archiver policy that selects nothing, and a second --ops-bind
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true > ops.yaml
python3 - <<'PY'
s = open("ops.yaml").read()
a = s.replace("app.kubernetes.io/component: archiver\n  policyTypes: [Ingress]", "app.kubernetes.io/component: archiverx\n  policyTypes: [Ingress]")
assert a != s
open("ops-nosel.yaml", "w").write(a)
i = s.index("tributary.archiver\n")
j = s.index("- --ops-bind=$(POD_IP)", i)
open("ops-twobind.yaml", "w").write(s[:j] + "- --ops-bind=0.0.0.0\n            " + s[j:])
PY
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py ops-nosel.yaml --negative
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py ops-twobind.yaml --negative
  )
  endstep

  step 'default values refuse to render without a kubelet CIDR'
  (
helm template tb "$CHART" 2>&1 | grep -q "networkPolicy.kubeletCIDR is required"
  )
  endstep

  step 'too-short and open CIDRs, a disabled policy and plain-HTTP ingress are refused'
  (
v="-f $CHART/ci/values-ci.yaml"
helm template tb "$CHART" $v --set networkPolicy.kubeletCIDR=0.0.0.0/0 2>&1 | grep -q "is too short"
helm template tb "$CHART" $v --set networkPolicy.kubeletCIDR=10.0.0.0/8 2>&1 | grep -q "is too short"
helm template tb "$CHART" $v --set networkPolicy.enabled=false 2>&1 | grep -q "allowUnprotectedOpsPort"
helm template tb "$CHART" $v --set ingress.tlsSecretName= 2>&1 | grep -q "tlsSecretName is required"
helm template tb "$CHART" $v --set archiver.ttlDays=365 2>&1 | grep -q "archiver.ttlDays was removed"
helm template tb "$CHART" $v --set archiver.retention.eventsDays=0 2>&1 | grep -q "must be an integer from 1 to 36500"
helm template tb "$CHART" $v --set archiver.retention.auditDays=36501 2>&1 | grep -q "must be an integer from 1 to 36500"
  )
  endstep
}

# ---------------------------------------------------------------------------------------------- inner entry
if [ "${1:-}" = "__run" ]; then  # a fresh process per job, so `set -e` really applies to every command
  set -eu
  set +o pipefail   # ci.yml / helm.yml ran under `bash -e` (no pipefail); `cmd | grep -q` would otherwise die on SIGPIPE
  case "$2" in
    contracts) job_contracts ;;
    lint) job_lint ;;
    python) job_python ;;
    integration) job_integration ;;
    supply-chain) job_supply_chain ;;
    image-check) job_image_check ;;
    ui) job_ui ;;
    helm) job_helm ;;
    *) echo "unknown job: $2" >&2; exit 2 ;;
  esac
  exit 0
fi

# ---------------------------------------------------------------------------------------------- outer
usage() { sed -n '2,/^set -euo/p' "$SELF" | sed '$d' | sed 's/^# \{0,1\}//'; }
die() { echo "ci_local: $*" >&2; exit 2; }

sha=HEAD; here=0; report=0; want=()
while [ $# -gt 0 ]; do
  case "$1" in
    --sha) [ $# -ge 2 ] || die "--sha needs a revision"; sha="$2"; shift 2 ;;
    --here) here=1; shift ;;
    --report) report=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) want+=("$1"); shift ;;
  esac
done
[ ${#want[@]} -gt 0 ] || { usage >&2; exit 2; }

# Subcommands the workflows use that are not jobs.
if [ ${#want[@]} -eq 1 ]; then
  case "${want[0]}" in
    install-helm-tools) install_helm_tools; exit 0 ;;
    list-jobs) printf '%s\n' $ALL_NAMES; exit 0 ;;
    ui-present) echo "present=$([ -f ui/package.json ] && echo true || echo false)"; exit 0 ;;
  esac
fi

jobs=()
for w in "${want[@]}"; do
  case "$w" in
    all) jobs+=("${ALL_JOBS[@]}") ;;
    fast) jobs+=("${FAST_JOBS[@]}") ;;
    contracts|lint|python|integration|supply-chain|image-check|ui|helm) jobs+=("$w") ;;
    *) die "unknown job '$w' (jobs: $ALL_NAMES)" ;;
  esac
done

# --report: refuse up front, before spending an hour on the jobs. The token is moved out of the environment
# at once, so no job (arbitrary test code) can read it; only the reporter gets it, for the one request.
report_token=""
if [ "$report" = 1 ]; then
  [ -n "${TB_STATUS_TOKEN:-}" ] || die "--report needs the environment variable TB_STATUS_TOKEN (a fine-grained token, this repo only, commit statuses: write). It is read from the environment only. Nothing was run."
  report_token="$TB_STATUS_TOKEN"
fi
unset TB_STATUS_TOKEN

if [ "$here" = 1 ]; then
  [ "${CI:-}" = true ] || [ "${TB_CI_LOCAL_HERE:-}" = 1 ] || die "--here runs a bare 'uv sync' in this checkout; refused outside CI (set TB_CI_LOCAL_HERE=1 to override)"
  root="$(pwd)"
else
  top="$(git rev-parse --show-toplevel)"
  full_sha="$(git -C "$top" rev-parse --verify "${sha}^{commit}")" || die "cannot resolve $sha"
  if [ -n "$(git -C "$top" status --porcelain --untracked-files=no)" ]; then
    echo "note: uncommitted changes are not tested; this runs ${full_sha:0:12}" >&2
  fi
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/tb-ci-local.XXXXXX")"
  wt="$tmp/wt"
  cleanup() {
    git -C "$top" worktree remove --force "$wt" >/dev/null 2>&1 || true
    git -C "$top" worktree prune >/dev/null 2>&1 || true
    rm -rf "$tmp/wt"
  }
  trap cleanup EXIT
  git -C "$top" worktree add --detach --quiet "$wt" "$full_sha"
  root="$wt"
fi
logs="${TB_CI_LOGS:-${tmp:-$(mktemp -d)}/logs}"
mkdir -p "$logs"
[ -x "$root/.github/scripts/ci_local.sh" ] || die "$sha has no .github/scripts/ci_local.sh"
cd "$root"

strip_ansi() { sed 's/\x1b\[[0-9;]*[A-Za-z]//g'; }
summary_of() {  # the real summary lines: up to 3 pytest/ruff/helm results if there are any, else the last output line
  local log="$1" line
  line="$(strip_ansi <"$log" | grep -E '[0-9]+ passed|All checks passed|files? already formatted|[0-9]+ problems|Summary: |helm_check: ok|Success: no issues|skipped: |[0-9]+ failed' | sed 's/^[= ]*//; s/[= ]*$//' | tail -n 3 | paste -sd'|' | sed 's/|/ | /g' || true)"
  [ -n "$line" ] || line="$(strip_ansi <"$log" | grep -v '^\s*$' | tail -n 1 || true)"
  echo "$line" | cut -c1-200
}

post_status() {  # post_status <job> <success|failure> <description>   (token only in this python process)
  local repo="${TB_STATUS_REPO:-}"
  if [ -z "$repo" ]; then
    repo="$(git remote get-url origin 2>/dev/null | sed -E 's#^(git@|https?://)([^/:]+)[:/]##; s#\.git$##' || true)"
  fi
  TB_STATUS_TOKEN="$report_token" python3 -I - "$repo" "${full_sha:-$(git rev-parse HEAD)}" "local/$1" "$2" "$3" <<'PY'
import json, os, sys, urllib.error, urllib.request

repo, sha, context, state, description = sys.argv[1:6]
token = os.environ.pop("TB_STATUS_TOKEN", "")
if not repo or repo.count("/") != 1:
    print(f"report: cannot work out owner/repo (set TB_STATUS_REPO)", file=sys.stderr)
    sys.exit(1)
body = json.dumps({"state": state, "context": context, "description": description[:140]}).encode()
req = urllib.request.Request(
    f"https://api.github.com/repos/{repo}/statuses/{sha}",
    data=body,
    method="POST",
    headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "tributary-ci-local",
    },
)
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"report: {context} {state} -> HTTP {resp.status}")
except urllib.error.HTTPError as exc:  # GitHub's message never contains the token
    print(f"report: {context} not posted, HTTP {exc.code}", file=sys.stderr)
    sys.exit(1)
except Exception as exc:  # class name only
    print(f"report: {context} not posted ({type(exc).__name__})", file=sys.stderr)
    sys.exit(1)
PY
}

failed=0; results=(); python_failed=0
for job in "${jobs[@]}"; do
  if [ "$job" = integration ] && [ "$python_failed" = 1 ]; then  # ci.yml: integration needs python
    echo "SKIP integration (python failed in this run)"; failed=1; results+=("SKIP integration"); continue
  fi
  log="$logs/$job.log"
  echo "##### $job"
  set +e
  bash "$root/.github/scripts/ci_local.sh" __run "$job" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  set -e
  summary="$(summary_of "$log")"
  if [ "$rc" = 0 ]; then
    results+=("PASS $job: $summary"); state=success
    for f in sbom.cdx.json integration-junit.xml; do [ -f "$f" ] && cp "$f" "$logs/" || true; done
  else
    results+=("FAIL $job: $summary (exit $rc, log $log)"); state=failure; failed=1
    [ "$job" = python ] && python_failed=1
    echo "----- last lines of $job"; strip_ansi <"$log" | tail -n 25
  fi
  if [ "$report" = 1 ]; then
    post_status "$job" "$state" "$([ "$rc" = 0 ] && echo PASS || echo FAIL): $summary" || { echo "ci_local: status for $job was not posted" >&2; failed=1; }
  fi
done

echo
echo "===== ci_local summary (${full_sha:-here}) ====="
printf '%s\n' "${results[@]}"
exit "$failed"
