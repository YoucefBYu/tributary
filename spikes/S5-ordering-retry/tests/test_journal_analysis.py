"""Unit tests for the rebalance-journal analysis.

This logic is what makes the `rebalance` and recovery-gap claims falsifiable, so
it needs tests of its own rather than being trusted because a cluster run looked
plausible. Two bugs it exists to prevent, both of which shipped once:

* counting a worker's **shutdown** revoke as evidence that a rebalance happened,
  which made the `rebalance` scenario unable to fail;
* deriving "partitions this worker owned" from every partition it ever delivered
  on, which includes partitions it handed over long before the kill and turned a
  2 s outage into a reported 3 ms recovery.

No cluster needed: the journal is a file, so these are pure functions of it.
"""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest
from s5.runner import owned_at, read_journal, rebalance_evidence


def _write(tmp_path: Path, entries: list[dict[str, object]]) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    with (run_dir / "rebalances.jsonl").open("wb") as fh:
        for entry in entries:
            fh.write(orjson.dumps(entry) + b"\n")
    return run_dir


def _entry(
    ts: float, worker: str, kind: str, partitions: list[int], shutting_down: bool = False
) -> dict[str, object]:
    return {
        "ts": ts,
        "worker": worker,
        "kind": kind,
        "partitions": partitions,
        "shutting_down": shutting_down,
    }


def test_owned_at_reflects_handovers_not_delivery_history(tmp_path: Path) -> None:
    """worker-a starts alone with everything, then gives half away."""
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0, 1, 2, 3]),
            _entry(110.0, "worker-a", "revoke", [2, 3]),
            _entry(110.5, "worker-b", "assign", [2, 3]),
        ],
    )
    assert owned_at(run_dir, "worker-a", 105.0) == {0, 1, 2, 3}
    # After the handover it owns half, even though it delivered on all four.
    assert owned_at(run_dir, "worker-a", 120.0) == {0, 1}
    assert owned_at(run_dir, "worker-b", 120.0) == {2, 3}


def test_owned_at_ignores_events_after_the_instant_asked_about(tmp_path: Path) -> None:
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0]),
            _entry(200.0, "worker-a", "revoke", [0]),
        ],
    )
    assert owned_at(run_dir, "worker-a", 150.0) == {0}
    assert owned_at(run_dir, "worker-a", 250.0) == set()


def test_a_lost_partition_is_no_longer_owned(tmp_path: Path) -> None:
    """Membership loss removes ownership just as a revoke does."""
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0, 1]),
            _entry(150.0, "worker-a", "lost", [0, 1]),
        ],
    )
    assert owned_at(run_dir, "worker-a", 200.0) == set()


def test_shutdown_revokes_are_not_evidence_of_a_rebalance(tmp_path: Path) -> None:
    """The exact bug that made the `rebalance` scenario unfalsifiable.

    Every worker revokes its partitions when it closes. Counting those meant the
    scenario reported a rebalance whether or not one occurred.
    """
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0, 1]),
            _entry(300.0, "worker-a", "revoke", [0, 1], shutting_down=True),
        ],
    )
    evidence = rebalance_evidence(run_dir, since_wall=50.0)
    assert evidence["revokes_while_running_after"] == []
    assert evidence["partitions_moved"] == []


def test_a_revoke_by_a_running_worker_is_evidence(tmp_path: Path) -> None:
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0, 1]),
            _entry(200.0, "worker-a", "revoke", [1]),
            _entry(201.0, "worker-c", "assign", [1]),
            _entry(300.0, "worker-a", "revoke", [0], shutting_down=True),
        ],
    )
    evidence = rebalance_evidence(run_dir, since_wall=150.0)
    assert evidence["partitions_moved"] == [1]
    assert [e["worker"] for e in evidence["revokes_while_running_after"]] == ["worker-a"]
    assert {e["worker"] for e in evidence["assigns_after"]} == {"worker-c"}


def test_evidence_before_the_join_is_excluded(tmp_path: Path) -> None:
    """Only movement after the new member started counts as caused by it."""
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0, 1]),
            _entry(120.0, "worker-a", "revoke", [1]),  # earlier rebalance
            _entry(200.0, "worker-a", "revoke", [0]),  # after the join
        ],
    )
    evidence = rebalance_evidence(run_dir, since_wall=150.0)
    assert evidence["partitions_moved"] == [0]


def test_a_truncated_final_line_is_skipped(tmp_path: Path) -> None:
    """A killed worker can leave a half-written line; it must not crash analysis."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    path = run_dir / "rebalances.jsonl"
    with path.open("wb") as fh:
        fh.write(orjson.dumps(_entry(100.0, "worker-a", "assign", [0])) + b"\n")
        fh.write(b'{"ts": 101.0, "worker": "worker-a", "kind": "rev')
    assert len(read_journal(run_dir)) == 1
    assert owned_at(run_dir, "worker-a", 200.0) == {0}


def test_a_missing_journal_is_empty_not_an_error(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    assert read_journal(run_dir) == []
    assert owned_at(run_dir, "worker-a", 1.0) == set()


@pytest.mark.parametrize("kind", ["revoke", "lost"])
def test_ownership_is_order_dependent_not_set_arithmetic(tmp_path: Path, kind: str) -> None:
    """Re-assignment after a revoke restores ownership."""
    run_dir = _write(
        tmp_path,
        [
            _entry(100.0, "worker-a", "assign", [0]),
            _entry(110.0, "worker-a", kind, [0]),
            _entry(120.0, "worker-a", "assign", [0]),
        ],
    )
    assert owned_at(run_dir, "worker-a", 130.0) == {0}
