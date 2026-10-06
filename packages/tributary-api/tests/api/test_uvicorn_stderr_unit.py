"""Invariant 6 under uvicorn: an unexpected exception becomes the fixed 500 and uvicorn logs NOTHING about it.

Starlette's ServerErrorMiddleware re-raises after sending its 500, and uvicorn prints that traceback (exception
text included) through `uvicorn.error`, outside the PHI guard. The app's outermost layer must absorb it. This runs
the real app under a real uvicorn in a subprocess and scans everything it writes for a marker the failing call
carries in its exception message.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time

import httpx
from api_kit import Idp, bearer

MARK = "ZZ-EXC-MARKER-ZZ"

PROGRAM = """
import json, os, sys
import uvicorn
from api_kit import ROLE_MAP, AUDIENCE
from tributary.api.app import create_app
from tributary.api.auth.jwks import FileJwksFetcher, KeySet
from tributary.api.config import DatabaseSettings, OidcSettings, Settings
from tributary.runtime.logs import JsonLogger

port = int(sys.argv[1])
oidc = OidcSettings(issuer=os.environ["ISS"], audience=AUDIENCE, role_map=ROLE_MAP, jwks_file=os.environ["JWKS"])
settings = Settings(database=DatabaseSettings("h", "n", "u", "r"), oidc=oidc, profile="lite")
keys = KeySet(FileJwksFetcher(os.environ["JWKS"]), refresh_s=300, max_stale_s=3600, timeout_s=5)

class Boom:
    def __call__(self):
        raise RuntimeError(os.environ["MARK"])

class Pub:
    degraded = False
    async def publish(self, *a): ...

app = create_app(settings=settings, sessions=Boom(), publisher=Pub(), keys=keys, log=JsonLogger("api"))
uvicorn.run(app, host="127.0.0.1", port=port, access_log=False, log_config=None)
"""


def test_unexpected_exception_is_a_fixed_500_and_nothing_reaches_stderr(idp: Idp, tmp_path) -> None:  # type: ignore[no-untyped-def]
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    jwks = idp.jwks_file(tmp_path / "jwks.json")
    env = {
        "ISS": idp.issuer,
        "JWKS": str(jwks),
        "MARK": MARK,
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(__import__("pathlib").Path(__file__).parent),
    }
    proc = subprocess.Popen(  # noqa: S603 - fixed argv built here
        [sys.executable, "-c", PROGRAM, str(port)], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"http://127.0.0.1:{port}/livez", timeout=1).status_code == 200:
                    break
            except httpx.TransportError:
                time.sleep(0.2)
        else:
            raise AssertionError("the app under uvicorn did not start")
        time.sleep(0.5)  # let the key file load
        r = httpx.get(
            f"http://127.0.0.1:{port}/v1/environments", headers=bearer(idp.mint(roles=["tb-admin"])), timeout=10
        )
        assert r.status_code == 500 and r.json()["code"] == "INTERNAL"
        assert MARK not in r.text
        again = httpx.get(f"http://127.0.0.1:{port}/livez", timeout=5)
        assert again.status_code == 200  # the server survived
    finally:
        proc.terminate()
        out, _ = proc.communicate(timeout=15)
    text = out.decode("utf-8", "replace")
    assert MARK not in text, "exception text reached the process output"
    assert "Traceback" not in text
    lines = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
    assert any(line.get("event") == "unhandled_error" and line.get("error") == "RuntimeError" for line in lines)


def test_a_refused_startup_exits_2_with_a_startup_refused_line() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "tributary.api"],
        env={"PATH": "/usr/bin:/bin", "TB_DATABASE_URL": "postgresql://u:p@h/d"},
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 2
    lines = [json.loads(line) for line in result.stderr.decode().splitlines() if line.startswith("{")]
    assert {"event": "startup_refused", "code": "DATABASE_URL_REFUSED"}.items() <= lines[-1].items()
    assert b"postgresql://" not in result.stderr + result.stdout  # the value is never echoed
