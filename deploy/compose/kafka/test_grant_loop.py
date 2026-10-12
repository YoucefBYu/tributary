"""grant-loop.sh against a stub kafka-acls: grant, TTL revoke, withdrawn, replaced and stale requests."""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

LOOP = Path(__file__).resolve().parent / "grant-loop.sh"


@pytest.fixture
def env(tmp_path: Path) -> dict[str, Path]:
    grant = tmp_path / "grant"
    grant.mkdir()
    stub = tmp_path / "acls"
    state = tmp_path / "acl-present"
    stub.write_text(
        f'#!/bin/bash\ncase " $* " in *" --remove "*) rm -f {state} ;; *" --add "*) touch {state} ;; esac\nexit 0\n'
    )
    stub.chmod(0o755)
    return {"grant": grant, "stub": stub, "state": state}


def _start(env: dict[str, Path], ttl: int = 120) -> tuple[subprocess.Popen[bytes], subprocess.Popen[bytes]]:
    broker = subprocess.Popen(["sleep", "60"])  # noqa: S607
    e = {
        **os.environ,
        "GRANT": str(env["grant"]),
        "TB_ACLS_BIN": str(env["stub"]),
        "TB_ACL_ARGS": "--x",
        "TB_GRANT_TTL_SECONDS": str(ttl),
        "TB_GRANT_POLL_SECONDS": "0.1",
    }
    loop = subprocess.Popen(["bash", str(LOOP), str(broker.pid)], env=e)  # noqa: S603,S607
    assert _wait(lambda: (env["grant"] / "loop-started").exists())
    return broker, loop


def _wait(cond, timeout: float = 8.0) -> bool:  # type: ignore[no-untyped-def]
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _stop(*ps: subprocess.Popen[bytes]) -> None:
    for p in ps:
        p.kill()
        p.wait()


def test_grant_echoes_nonce_then_withdraw_revokes(env: dict[str, Path]) -> None:
    broker, loop = _start(env)
    try:
        (env["grant"] / "request").write_text("n1")
        assert _wait(lambda: (env["grant"] / "granted").exists())
        assert (env["grant"] / "granted").read_text() == "n1" and env["state"].exists()
        (env["grant"] / "request").unlink()
        assert _wait(lambda: not env["state"].exists() and not (env["grant"] / "granted").exists())
    finally:
        _stop(loop, broker)


def test_ttl_revokes_while_the_request_stays(env: dict[str, Path]) -> None:
    broker, loop = _start(env, ttl=1)
    try:
        (env["grant"] / "request").write_text("n1")
        assert _wait(lambda: env["state"].exists())
        assert _wait(lambda: not env["state"].exists(), timeout=10)
        time.sleep(0.5)
        assert not env["state"].exists(), "the same nonce must not be granted twice"
    finally:
        _stop(loop, broker)


def test_new_nonce_replaces_the_grant(env: dict[str, Path]) -> None:
    broker, loop = _start(env)
    try:
        (env["grant"] / "request").write_text("n1")
        assert _wait(lambda: (env["grant"] / "granted").exists())
        (env["grant"] / "request").write_text("n2")
        assert _wait(lambda: (env["grant"] / "granted").exists() and (env["grant"] / "granted").read_text() == "n2")
        assert env["state"].exists()
    finally:
        _stop(loop, broker)


def test_a_request_left_before_the_loop_started_is_never_granted(env: dict[str, Path]) -> None:
    (env["grant"] / "request").write_text("stale")
    broker, loop = _start(env)
    try:
        time.sleep(1.0)
        assert not env["state"].exists() and not (env["grant"] / "granted").exists()
        (env["grant"] / "request").write_text("fresh")
        assert _wait(lambda: env["state"].exists())
    finally:
        _stop(loop, broker)
