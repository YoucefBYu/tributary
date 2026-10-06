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
