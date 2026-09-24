"""Each headline check against a journal it must pass and one it must fail.

A check that has never been seen to fail proves nothing (PHASE-0-DECISION section 4). The cluster
runs add a broken loader per check on top of these.
"""

from __future__ import annotations

from typing import Any

from s7 import checks

OUT, RETRY = "x.out", "x.retry"


def attempt(ts: float, msg_id: str, n: int, ok: bool, t: str, o: int, *, nb: float | None = None) -> dict[str, Any]:
    return {"ts": ts, "w": "a", "id": msg_id, "k": 0, "s": 0, "n": n, "nb": nb, "ok": ok, "src": t, "t": t, "p": 0,
            "o": o}


def ack(ts: float, msg_id: str, n: int, *, dlq: bool = False) -> dict[str, Any]:
    return {"ts": ts, "w": "a", "acked": msg_id, "n": n, "dlq": dlq}


def commit(ts: float, t: str, offset: int) -> dict[str, Any]:
    return {"ts": ts, "w": "a", "commit": [[t, 0, offset]]}


# -- commit order ---------------------------------------------------------------------------------


def test_commit_after_the_retry_record_is_acknowledged_passes() -> None:
    lines = [attempt(1.0, "m", 1, False, OUT, 0), ack(1.01, "m", 1), commit(1.02, OUT, 1)]
    evidence = checks.Evidence.from_lines(lines)
    assert checks.commit_violations(checks.resolution_times(evidence), evidence.commits) == []


def test_commit_before_the_retry_record_is_acknowledged_fails() -> None:
    lines = [attempt(1.0, "m", 1, False, OUT, 0), commit(1.005, OUT, 1), ack(1.01, "m", 1)]
    evidence = checks.Evidence.from_lines(lines)
    assert len(checks.commit_violations(checks.resolution_times(evidence), evidence.commits)) == 1


def test_commit_past_a_record_never_seen_fails() -> None:
    """No evidence for offset 1 at all: committing 2 must be a violation, not a pass."""
    lines = [attempt(1.0, "m", 1, True, OUT, 0), commit(2.0, OUT, 2)]
    evidence = checks.Evidence.from_lines(lines)
    violations = checks.commit_violations(checks.resolution_times(evidence), evidence.commits)
    assert [v[4] for v in violations] == [1]


def test_watermark_commit_below_a_held_record_passes_and_above_it_fails() -> None:
    lines = [
        attempt(1.0, "a", 2, True, RETRY, 0),
        # offset 1 is held (not attempted), offset 2 is done
        attempt(1.1, "c", 2, True, RETRY, 2),
        commit(1.2, RETRY, 1),
    ]
    evidence = checks.Evidence.from_lines(lines)
    resolved = checks.resolution_times(evidence)
    assert checks.commit_violations(resolved, evidence.commits) == []
    assert len(checks.commit_violations(resolved, [(1.2, RETRY, 0, 3)])) == 1


def test_a_reenqueued_record_resolves_when_its_copy_is_acknowledged() -> None:
    moved = {"ts": 2.0, "w": "a", "moved": "b", "n": 1, "t": RETRY, "p": 0, "o": 0}
    before = [ack(0.5, "b", 1), moved, commit(2.05, RETRY, 1), ack(2.1, "b", 1)]
    after = [ack(0.5, "b", 1), moved, ack(2.1, "b", 1), commit(2.2, RETRY, 1)]
    for lines, expected in ((before, 1), (after, 0)):
        evidence = checks.Evidence.from_lines(lines)
        assert len(checks.commit_violations(checks.resolution_times(evidence), evidence.commits)) == expected


def test_dlq_ack_resolves_the_last_attempt() -> None:
    lines = [attempt(1.0, "p", 5, False, RETRY, 0), ack(1.1, "p", 4, dlq=True), commit(1.2, RETRY, 1)]
    evidence = checks.Evidence.from_lines(lines)
    assert checks.commit_violations(checks.resolution_times(evidence), evidence.commits) == []


# -- ordering -------------------------------------------------------------------------------------


def test_never_failed_order() -> None:
    produced = {"a": (0, 0), "b": (0, 1), "c": (0, 2)}
    in_order = [attempt(1, "a", 1, True, OUT, 0), attempt(2, "b", 1, True, OUT, 1), attempt(3, "c", 1, True, OUT, 2)]
    swapped = [attempt(1, "b", 1, True, OUT, 1), attempt(2, "a", 1, True, OUT, 0), attempt(3, "c", 1, True, OUT, 2)]
    assert checks.never_failed_inversions(checks.Evidence.from_lines(in_order), produced) == 0
    assert checks.never_failed_inversions(checks.Evidence.from_lines(swapped), produced) == 1


def test_a_retried_message_arriving_late_is_displacement_not_a_never_failed_inversion() -> None:
    produced = {"a": (0, 0), "b": (0, 1), "c": (0, 2)}
    lines = [
        attempt(1, "a", 1, False, OUT, 0),
        attempt(2, "b", 1, True, OUT, 1),
        attempt(3, "c", 1, True, OUT, 2),
        attempt(5, "a", 2, True, RETRY, 0, nb=5),
    ]
    evidence = checks.Evidence.from_lines(lines)
    assert checks.never_failed_inversions(evidence, produced) == 0
    moved = checks.displacement(evidence, produced)
    assert moved["displaced"] == 1 and moved["overtaken_by_max"] == 2 and moved["behind_s_max"] == 3.0


# -- scheduling lag -------------------------------------------------------------------------------


def test_resume_lag_measures_from_due_time_to_the_next_attempt() -> None:
    lines = [
        {"ts": 10.001, "w": "a", "resume": 1, "t": RETRY, "p": 0, "at": 10.0},
        attempt(10.3, "m", 2, True, RETRY, 0, nb=10.0),
    ]
    evidence = checks.Evidence.from_lines(lines)
    assert [round(v, 3) for v in checks.resume_lag(evidence)] == [0.3]
    assert [round(v, 3) for v in checks.scheduling_lag(evidence, "pause")] == [0.3]
    assert [round(v, 3) for v in checks.scheduling_lag(evidence, "buffer")] == [0.3]
