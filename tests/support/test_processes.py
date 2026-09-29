"""Regression guard: tearing down a spawned process must never signal pytest or pytest's process group.

Written after CI on 917c46d ended `pytest -m integration` with no summary while the e2e test's four
children were still alive. No code path in the test tree was found that signals the runner, so the
hardening is structural: every `ManagedProcess` runs in its own session and teardown only ever signals
that child's own group (`ManagedProcess._signal_group`), including any grandchild it started.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

from tests.support.processes import ManagedProcess

CHILD = (
    "import json, os, subprocess, sys, time\n"
    "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
    "print(json.dumps({'event': 'ready', 'grandchild': g.pid, 'pgid': os.getpgrp()}), flush=True)\n"
    "time.sleep(300)\n"
)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:  # a zombie of a reparented grandchild still answers signal 0
        with open(f"/proc/{pid}/stat", encoding="ascii") as stat:
            return stat.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False


@pytest.mark.skipif(not hasattr(os, "killpg"), reason="POSIX process groups")
@pytest.mark.parametrize("teardown", ["terminate_and_wait", "kill_and_wait"])
def test_teardown_signals_only_the_childs_own_group_and_leaves_pytest_alone(teardown: str) -> None:
    runner_pid, runner_pgid = os.getpid(), os.getpgrp()
    proc = ManagedProcess([sys.executable, "-c", CHILD])
    ready = proc.wait_for_event("ready", timeout_s=20.0)
    assert ready["pgid"] == proc.process.pid != runner_pgid, "the child must lead its own group"
    grandchild = int(ready["grandchild"])
    assert _alive(grandchild)

    getattr(proc, teardown)()

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and _alive(grandchild):
        time.sleep(0.1)
    assert not _alive(grandchild), "teardown must reach the child's whole group"
    assert (os.getpid(), os.getpgrp()) == (runner_pid, runner_pgid)  # and we are still here to assert it


def test_a_group_that_could_be_the_runners_is_refused() -> None:
    proc = ManagedProcess([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        real = proc.process
        proc.process = type("Fake", (), {"poll": lambda self: None, "pid": os.getpgrp()})()  # type: ignore[assignment]
        with pytest.raises(RuntimeError, match="test runner"):
            proc._signal_group(15)
    finally:
        proc.process = real
        proc.terminate_and_wait()
