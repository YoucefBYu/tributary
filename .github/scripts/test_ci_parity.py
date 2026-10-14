"""Parity between the workflows and `ci_local.sh` (ci-alternatives.md, option C).

Every `run:` step in every gating workflow must be exactly one call to `.github/scripts/ci_local.sh --here <sub>`
(optionally redirected into $GITHUB_OUTPUT or $GITHUB_PATH). Any other command is a check that exists in CI but
not in the local gate, which is the drift this design removes. The test also closes the other ways to hide work:
only pinned, allow-listed actions; no `continue-on-error`, `shell:`, `working-directory`, `defaults.run`, step
`env:`, job-level `if:`; step `if:` only for the ui probe (and `always()` on the SBOM upload); `persist-credentials:
false` on every checkout. chaos-rf3.yml and bench-linux.yml are scheduled/manual measurement jobs and are exempt
by name. A new workflow file is gated by default.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "ci_local.sh"
WORKFLOW_DIR = ROOT / ".github" / "workflows"
EXEMPT = {"chaos-rf3.yml", "bench-linux.yml"}
CALL = re.compile(r'^\.github/scripts/ci_local\.sh --here (?P<sub>[a-z][a-z-]*)( >> "\$GITHUB_(OUTPUT|PATH)")?$')
JOB_SUBCOMMAND = {"chart": "helm"}  # workflow job id -> subcommand; every other job id is its own subcommand
HELPER_SUBCOMMANDS = {"install-helm-tools", "ui-present", "list-jobs"}
NOT_WORKFLOW_JOBS = {"lint", "fast", "all"}  # convenience aliases for local use
ALLOWED_USES = {
    "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
    "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "astral-sh/setup-uv@c18668ad3cf93ea998bef934396af7bb5c839dc7",
    "pnpm/action-setup@d9184bf108216479bc5a137cc391f4d7b14c870b",
}
HAS_UI_IF = "steps.has_ui.outputs.present == 'true'"
STEP_KEYS = {"name", "uses", "with", "run", "if", "id"}
JOB_KEYS = {"name", "runs-on", "needs", "steps", "timeout-minutes"}
WORKFLOW_ENV = {"UV_FROZEN"}


def subcommands() -> set[str]:
    out = subprocess.run(["bash", str(SCRIPT), "list-jobs"], capture_output=True, text=True, check=True).stdout
    return set(out.split())


def gated_workflows() -> list[str]:
    return sorted(p.name for p in WORKFLOW_DIR.glob("*.yml") if p.name not in EXEMPT)


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((WORKFLOW_DIR / name).read_text())  # type: ignore[no-any-return]


def violations(name: str, doc: dict[str, Any], known: set[str]) -> list[str]:
    """Everything wrong with one parsed workflow, as readable strings."""
    bad: list[str] = []
    if "defaults" in doc:
        bad.append(f"{name}: defaults (run shell or working-directory) is not allowed")
    if set(doc.get("env") or {}) - WORKFLOW_ENV:
        bad.append(f"{name}: workflow env other than {sorted(WORKFLOW_ENV)}")
    for job_id, job in doc["jobs"].items():
        for key in set(job) - JOB_KEYS:
            bad.append(f"{name}:{job_id}: job key {key!r} is not allowed")
        called: set[str] = set()
        for i, step in enumerate(job["steps"]):
            where = f"{name}:{job_id} step {i + 1}"
            for key in set(step) - STEP_KEYS:
                bad.append(
                    f"{where}: step key {key!r} is not allowed (shell, working-directory, continue-on-error, env, ...)"
                )
            if "uses" in step:
                action = str(step["uses"]).split("#")[0].strip()
                if action not in ALLOWED_USES:
                    bad.append(f"{where}: action {action!r} is not on the pinned allow-list")
                if (
                    action.startswith("actions/checkout@")
                    and (step.get("with") or {}).get("persist-credentials") is not False
                ):
                    bad.append(f"{where}: checkout without persist-credentials: false")
            if "if" in step:
                cond = " ".join(str(step["if"]).split())
                ok = cond == HAS_UI_IF or (
                    cond == "always()" and str(step.get("uses", "")).startswith("actions/upload-artifact@")
                )
                if not ok:
                    bad.append(f"{where}: step if: {cond!r} is not allowed")
            if "run" not in step:
                continue
            m = CALL.match(" ".join(str(step["run"]).split()))
            if m is None:
                bad.append(f"{where}: run is not a single ci_local.sh --here call: {str(step['run']).strip()[:60]!r}")
                continue
            if m["sub"] not in known:
                bad.append(f"{where}: ci_local.sh has no subcommand {m['sub']!r}")
            called.add(m["sub"])
        want = JOB_SUBCOMMAND.get(job_id, job_id)
        if want not in called:
            bad.append(f"{name}:{job_id} never calls ci_local.sh {want}")
    return bad


def test_the_gated_set_is_what_we_expect() -> None:
    assert {"ci.yml", "helm.yml"} <= set(gated_workflows())


@pytest.mark.parametrize("name", gated_workflows())
def test_every_gated_workflow_only_calls_the_script(name: str) -> None:
    assert violations(name, load(name), subcommands()) == []


def test_every_job_subcommand_is_called_by_a_workflow_job() -> None:
    called = {JOB_SUBCOMMAND.get(j, j) for n in gated_workflows() for j in load(n)["jobs"]}
    assert subcommands() - HELPER_SUBCOMMANDS - NOT_WORKFLOW_JOBS - called == set()


CHECKOUT = {
    "uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7",
    "with": {"persist-credentials": False},
}
GOOD_RUN = {"run": ".github/scripts/ci_local.sh --here contracts"}


def _doc(*steps: dict[str, Any], **top: Any) -> dict[str, Any]:
    return {"jobs": {"contracts": {"steps": [dict(CHECKOUT), *steps]}}, **top}


def test_negative_baseline_is_clean() -> None:
    assert violations("t.yml", _doc(dict(GOOD_RUN)), subcommands()) == []


def test_negative_a_raw_run_step_is_a_violation() -> None:
    known = subcommands()
    assert violations("t.yml", _doc({"run": "echo x"}), known)
    for run in (
        ".github/scripts/ci_local.sh --here contracts && echo x",
        ".github/scripts/ci_local.sh --here contracts; rm -rf .",
        "echo x\n.github/scripts/ci_local.sh --here contracts",
        ".github/scripts/ci_local.sh contracts",  # missing --here
        ".github/scripts/ci_local.sh --here contracts | tee x",
    ):
        assert violations("t.yml", _doc({"run": run}), known), run


def test_negative_an_unknown_subcommand_and_a_job_that_skips_its_subcommand_are_violations() -> None:
    known = subcommands()
    assert violations("t.yml", _doc({"run": ".github/scripts/ci_local.sh --here nonesuch"}), known)
    other = _doc({"run": ".github/scripts/ci_local.sh --here python"})  # job `contracts` calls the wrong one
    assert any("never calls" in v for v in violations("t.yml", other, known))


@pytest.mark.parametrize(
    "extra",
    [
        {"continue-on-error": True},
        {"shell": "bash {0}"},
        {"working-directory": "/tmp"},  # noqa: S108,  # noqa: S108
        {"env": {"PATH": "/evil"}},
        {"if": "github.ref == 'refs/heads/main'"},
        {"if": "false"},
    ],
)
def test_negative_step_level_escape_hatches_are_violations(extra: dict[str, Any]) -> None:
    assert violations("t.yml", _doc({**GOOD_RUN, **extra}), subcommands()), extra


def test_negative_job_and_workflow_level_escape_hatches_are_violations() -> None:
    known = subcommands()
    doc = _doc(dict(GOOD_RUN))
    doc["jobs"]["contracts"]["continue-on-error"] = True
    assert violations("t.yml", doc, known)
    doc = _doc(dict(GOOD_RUN))
    doc["jobs"]["contracts"]["if"] = "false"
    assert violations("t.yml", doc, known)
    assert violations("t.yml", _doc(dict(GOOD_RUN), defaults={"run": {"shell": "true {0}"}}), known)
    assert violations("t.yml", _doc(dict(GOOD_RUN), env={"BASH_ENV": "/tmp/x"}), known)  # noqa: S108


def test_negative_unpinned_and_unknown_actions_are_violations() -> None:
    known = subcommands()
    assert violations("t.yml", _doc(dict(GOOD_RUN), {"uses": "evil/action@v1"}), known)
    assert violations(
        "t.yml", _doc(dict(GOOD_RUN), {"uses": "actions/checkout@v7"}), known
    )  # a tag, not the pinned sha


def test_negative_checkout_must_not_persist_credentials() -> None:
    known = subcommands()
    doc = _doc(dict(GOOD_RUN))
    doc["jobs"]["contracts"]["steps"][0] = {"uses": CHECKOUT["uses"]}
    assert any("persist-credentials" in v for v in violations("t.yml", doc, known))
    doc["jobs"]["contracts"]["steps"][0] = {"uses": CHECKOUT["uses"], "with": {"persist-credentials": True}}
    assert any("persist-credentials" in v for v in violations("t.yml", doc, known))


def test_the_ui_probe_condition_and_artifact_always_are_the_only_allowed_ifs() -> None:
    known = subcommands()
    ui = _doc(dict(GOOD_RUN), {"if": HAS_UI_IF, "uses": "pnpm/action-setup@d9184bf108216479bc5a137cc391f4d7b14c870b"})
    assert violations("t.yml", ui, known) == []
    up = _doc(
        dict(GOOD_RUN), {"if": "always()", "uses": "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"}
    )
    assert violations("t.yml", up, known) == []
    assert violations("t.yml", _doc(dict(GOOD_RUN), {"if": "always()", **GOOD_RUN}), known)  # always() on a run step
