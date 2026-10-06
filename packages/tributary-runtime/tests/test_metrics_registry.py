"""The runtime's in-process counters against `contracts/observability/v0/metrics.json` (README section 3).

Metric export is not built yet (W2-5 ruling: it waits on this contract). What exists are the counters
the registry was seeded from (`StepStats`, `LoopStats`, `ReplyStats`), and what must not drift is the
mapping: every closed `outcome` list in the registry for a worker metric has a source counter, and no
counter is an outcome the registry lacks. Every worker-emitted metric is either mapped here or listed
in `NOT_TRACKED_YET` with the reason, so adding a worker metric to the registry fails this file until
someone decides which.
"""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path
from typing import Any, get_args

import pytest
from tributary.runtime import topics
from tributary.runtime.consumer import LoopStats
from tributary.runtime.executor import PIPELINE_SCOPE, StepOutcome
from tributary.runtime.reply import ReplyStats

REGISTRY_PATH = Path(__file__).resolve().parents[3] / "contracts" / "observability" / "v0" / "metrics.json"

#: registry metric -> outcome label value -> `LoopStats` / `ReplyStats` field.
WORKER_RECORDS = {
    "handled": "handled",
    "retried": "retried",
    "dead_lettered": "dead_lettered",
    "quarantined": "quarantined",
    "rejected": "rejected",
}
COMMITS = {"ok": "commits", "refused": "commits_refused", "failed": "commits_failed"}

#: Worker metrics with no in-process source today, and why. Not a contract mismatch: the registry is
#: ahead of the code until the exporter lands. Each needs a source before `/metrics` ships.
NOT_TRACKED_YET = {
    "tb_build_info": "static; written by the exporter",
    "tb_spec_version": "the applied spec version is known (PipelineHandler) but not published as a gauge",
    "tb_spec_source_unproven": "ChannelsCache.available exists; not published as a gauge",
    "tb_messages_total": "no per-(stage, status, destination) counter at emit time in events.py",
    "tb_stage_duration_seconds": "ExecutionResult.duration_ms exists per execution; no bucketed histogram",
    "tb_partitions_paused": "LoopStats counts pauses and resumes (cumulative); no current gauge per destination",
    "tb_partitions_halted": "LoopStats counts halts (cumulative); no current gauge per destination",
}
#: Counted in-process, not exported under these names. Not registered; a registry entry is a contract change.
UNREGISTERED_COUNTERS = {"consumed", "pauses", "resumes", "halts", "aborts", "pending"}


@pytest.fixture(scope="module")
def registry() -> dict[str, Any]:
    assert REGISTRY_PATH.is_file(), "metrics registry not found: the drift tests would pass vacuously"
    data: dict[str, Any] = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    return data


def metric(registry: dict[str, Any], name: str) -> dict[str, Any]:
    [entry] = [m for m in registry["metrics"] if m["name"] == name]
    return dict(entry)


def int_fields(cls: type) -> set[str]:
    return {f.name for f in dataclasses.fields(cls) if f.type in ("int", int)}


def test_every_worker_metric_is_mapped_or_explicitly_untracked(registry: dict[str, Any]) -> None:
    mapped = {"tb_worker_records_total", "tb_commits_total", "tb_worker_replies_total", "tb_step_duration_seconds"}
    worker = {m["name"] for m in registry["metrics"] if "worker" in m["emitters"]}
    assert worker == mapped | set(NOT_TRACKED_YET), "a worker metric was added or removed in the registry"


def test_worker_records_outcomes_have_a_loop_counter_each(registry: dict[str, Any]) -> None:
    entry = metric(registry, "tb_worker_records_total")
    assert entry["labels"] == ["channel", "outcome"]
    assert set(entry["label_values"]["outcome"]) == set(WORKER_RECORDS)
    assert set(WORKER_RECORDS.values()) <= int_fields(LoopStats)


def test_commit_outcomes_have_a_loop_counter_each(registry: dict[str, Any]) -> None:
    entry = metric(registry, "tb_commits_total")
    assert set(entry["label_values"]["outcome"]) == set(COMMITS)
    assert set(COMMITS.values()) <= int_fields(LoopStats)


def test_no_loop_counter_is_an_outcome_the_registry_lacks() -> None:
    counted = int_fields(LoopStats) | {"pending"}
    assert counted == set(WORKER_RECORDS.values()) | set(COMMITS.values()) | UNREGISTERED_COUNTERS


def test_reply_outcomes_are_exactly_the_reply_counters(registry: dict[str, Any]) -> None:
    entry = metric(registry, "tb_worker_replies_total")
    assert entry["labels"] == ["channel", "outcome"]
    assert set(entry["label_values"]["outcome"]) == int_fields(ReplyStats)


def test_step_outcomes_and_scope_match_step_stats(registry: dict[str, Any]) -> None:
    entry = metric(registry, "tb_step_duration_seconds")
    assert entry["labels"] == ["channel", "scope", "step", "outcome"]
    assert set(entry["label_values"]["outcome"]) == set(get_args(StepOutcome))
    scope = re.compile(registry["labels"]["scope"]["pattern"])
    assert scope.fullmatch(PIPELINE_SCOPE) and scope.fullmatch("fhir-server")


def test_name_labels_use_the_runtime_s_own_name_grammar(registry: dict[str, Any]) -> None:
    """`channel` and `destination` label values come from the same spec names the topic builder validates."""
    labels = registry["labels"]
    assert labels["channel"]["pattern"] == topics.CHANNEL.pattern
    assert labels["destination"]["pattern"] == f"^({topics.DESTINATION.pattern[1:-1]})?$"
