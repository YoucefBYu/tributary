"""kafka-users-init reads passwords from files and never takes one as an argument."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import add_scram_users as asu


def _secret(root: Path, ref: str, user: str, password: str) -> None:
    (root / ref).mkdir()
    (root / ref / "username").write_text(user)
    (root / ref / "password").write_text(password + "\n")


def test_reads_each_component_password_from_its_own_file(tmp_path: Path) -> None:
    _secret(tmp_path, "tb-lite-a", "tb-lite-a", "pw-a")
    _secret(tmp_path, "tb-lite-b", "tb-lite-b", "pw-b")
    assert asu.read_users(tmp_path, ["tb-lite-a", "tb-lite-b"]) == [("tb-lite-a", "pw-a"), ("tb-lite-b", "pw-b")]


def test_refuses_empty_missing_and_duplicate(tmp_path: Path) -> None:
    _secret(tmp_path, "tb-lite-a", "tb-lite-a", "  ")
    with pytest.raises(SystemExit, match="empty"):
        asu.read_users(tmp_path, ["tb-lite-a"])
    with pytest.raises(SystemExit, match="cannot read"):
        asu.read_users(tmp_path, ["absent"])
    _secret(tmp_path, "tb-lite-x", "same", "p1")
    _secret(tmp_path, "tb-lite-y", "same", "p2")
    with pytest.raises(SystemExit, match="same user"):
        asu.read_users(tmp_path, ["tb-lite-x", "tb-lite-y"])


def test_admin_link_is_verified_mutual_tls_with_no_password() -> None:
    cfg = asu.admin_config(Path("/s"), "127.0.0.1:9096")
    assert cfg["security.protocol"] == "SSL"
    assert cfg["enable.ssl.certificate.verification"] is True
    assert cfg["ssl.endpoint.identification.algorithm"] == "https"
    assert cfg["ssl.certificate.location"] == "/s/users-init/client.crt"
    assert not any("password" in k for k in cfg)


def test_the_script_has_no_command_line_password_surface() -> None:
    """No argparse/sys.argv: a password can only come from a file."""
    tree = ast.parse(Path(asu.__file__).read_text())
    imported = {n.names[0].name for n in ast.walk(tree) if isinstance(n, ast.Import)}
    assert "argparse" not in imported
    assert "sys.argv" not in Path(asu.__file__).read_text()


def test_grant_window_is_requested_then_withdrawn(tmp_path: Path) -> None:
    import threading
    import time

    (tmp_path / "granted").write_text("stale-nonce")  # an answer to an earlier request must not count

    def broker() -> None:
        while not (tmp_path / "request").exists():
            time.sleep(0.01)
        time.sleep(0.05)
        (tmp_path / "granted").write_text((tmp_path / "request").read_text())

    t = threading.Thread(target=broker)
    t.start()
    asu.request_grant(tmp_path, timeout=5)
    t.join()
    assert (tmp_path / "request").exists()
    asu.withdraw_request(tmp_path)
    assert not (tmp_path / "request").exists()
    asu.withdraw_request(tmp_path)  # re-run safe


def test_sigterm_becomes_an_exit_so_finally_revokes() -> None:
    with pytest.raises(SystemExit) as e:
        asu._terminate(15, None)
    assert e.value.code == 143


def test_no_grant_in_time_fails_and_withdraws(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="did not grant"):
        asu.request_grant(tmp_path, timeout=0.2)
    assert not (tmp_path / "request").exists()


def test_revoke_failure_is_reported_not_raised() -> None:
    class _Failing:
        def delete_acls(self, _f: object) -> dict[str, object]:
            raise RuntimeError("boom")

    assert asu.revoke_own_alter(_Failing()) is False
