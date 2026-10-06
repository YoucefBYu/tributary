"""The shared sync-reply oracle's own controls (W3-20): it passes the model over random interleavings, and it goes
red on each of the seven broken `SyncRequest`s. Fast, no Docker. The receiver's real `PendingMap` runs through the
same `violations` in `packages/tributary-receivers/tests/reply/test_properties.py`."""

from __future__ import annotations

import pytest
from hypothesis import find, given, settings
from tributary.runtime.sync_answer import Resolution, SyncRequest

from tests.support.sync_reply_oracle import (
    BROKEN_REQUESTS,
    AnswersHeldReplyOnFailedCommit,
    LastReplyWins,
    SwapsAeCodes,
    TrustsUnrecognisedStatus,
    revoke_gaps,
    traces,
    violations,
)


@given(traces())
@settings(max_examples=2000, deadline=None)
def test_the_model_satisfies_the_oracle(trace: list[str]) -> None:
    assert violations(trace, SyncRequest) == []


@pytest.mark.parametrize("broken", BROKEN_REQUESTS)
def test_each_broken_request_is_caught(broken: type[SyncRequest]) -> None:
    counterexample = find(traces(), lambda trace: bool(violations(trace, broken)), settings=settings(max_examples=5000))
    assert violations(counterexample, broken)


@pytest.mark.parametrize(
    ("broken", "trace"),
    [
        (LastReplyWins, ["reply_rejected", "reply_accepted", "commit_ok", "timeout"]),
        (SwapsAeCodes, ["commit_ok", "timeout"]),
        (SwapsAeCodes, ["commit_ok", "revoke", "timeout"]),
        (TrustsUnrecognisedStatus, ["commit_ok", "reply_unknown", "timeout"]),
        (AnswersHeldReplyOnFailedCommit, ["reply_accepted", "commit_failed", "timeout"]),
    ],
)
def test_each_control_fails_on_its_minimal_trace_and_the_model_passes_it(
    broken: type[SyncRequest], trace: list[str]
) -> None:
    assert violations(trace, broken)
    assert violations(trace, SyncRequest) == []


def test_the_revoke_check_passes_the_model_and_catches_a_revoke_that_never_answers() -> None:
    class IgnoresRevoke(SyncRequest):
        def partition_revoked(self) -> Resolution | None:
            return None

    assert revoke_gaps(["commit_ok", "revoke", "timeout"], SyncRequest) == []
    assert revoke_gaps(["commit_ok", "revoke", "timeout"], IgnoresRevoke)
