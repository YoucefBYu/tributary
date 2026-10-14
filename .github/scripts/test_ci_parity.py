"""Parity between the workflows and `ci_local.sh` (ci-alternatives.md, option C).

Every `run:` step in ci.yml and helm.yml must be exactly one call to `.github/scripts/ci_local.sh --here <sub>`
(optionally redirected into $GITHUB_OUTPUT or $GITHUB_PATH). Any other command is a check that exists in CI but
not in the local gate, which is the drift this design removes. Also: every subcommand a workflow calls exists, every
job subcommand is called by some workflow job, and each workflow job calls the subcommand named for it.
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
WORKFLOWS = ("ci.yml", "helm.yml")
CALL = re.compile(r'^\.github/scripts/ci_local\.sh --here (?P<sub>[a-z][a-z-]*)( >> "\$GITHUB_(OUTPUT|PATH)")?$')
JOB_SUBCOMMAND = {"chart": "helm"}  # workflow job id -> subcommand; every other job id is its own subcommand
HELPER_SUBCOMMANDS = {"install-helm-tools", "ui-present", "list-jobs"}
NOT_WORKFLOW_JOBS = {"lint", "fast", "all"}  # convenience aliases for local use


def subcommands() -> set[str]:
    out = subprocess.run(["bash", str(SCRIPT), "list-jobs"], capture_output=True, text=True, check=True).stdout
    return set(out.split())


def violations(name: str, doc: dict[str, Any], known: set[str]) -> list[str]:
    """Everything wrong with one parsed workflow, as readable strings."""
    bad: list[str] = []
    for job_id, job in doc["jobs"].items():
        called: set[str] = set()
        for i, step in enumerate(job["steps"]):
            if "run" not in step:
                continue
            m = CALL.match(" ".join(str(step["run"]).split()))
            where = f"{name}:{job_id} step {i + 1}"
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


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text())  # type: ignore[no-any-return]


@pytest.mark.parametrize("name", WORKFLOWS)
def test_every_run_step_only_calls_the_script(name: str) -> None:
    assert violations(name, load(name), subcommands()) == []


def test_every_job_subcommand_is_called_by_a_workflow_job() -> None:
    called = {JOB_SUBCOMMAND.get(j, j) for n in WORKFLOWS for j in load(n)["jobs"]}
    assert subcommands() - HELPER_SUBCOMMANDS - NOT_WORKFLOW_JOBS - called == set()


def _doc(*steps: dict[str, Any]) -> dict[str, Any]:
    return {"jobs": {"contracts": {"steps": [{"uses": "actions/checkout@x"}, *steps]}}}


def test_negative_a_raw_run_step_is_a_violation() -> None:
    known = subcommands()
    ok = _doc({"run": ".github/scripts/ci_local.sh --here contracts"})
    assert violations("t.yml", ok, known) == []
    assert violations("t.yml", _doc({"run": "echo x"}), known)
    # appended to a legitimate call, chained, or piped: still not a single call
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
