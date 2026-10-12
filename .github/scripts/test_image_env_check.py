import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from image_env_check import check, main

ROOT = Path(__file__).resolve().parents[2]
CAPS = {"fhir": {"distribution": "tributary-fhir", "factory": "tributary.fhir.runtime:context_extension"}}
SPACES = {"tributary.core": {"distribution": "tributary-runtime"}}


def install(
    root: Path, name: str, version: str = "1.0", eps: str = "", folder: str | None = None, record: bool = True
) -> None:
    info = root / f"{folder or name.replace('-', '_')}-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
    if eps:
        (info / "entry_points.txt").write_text(eps)
    # RECORD lists each entry point's module as a file this distribution ships.
    modules = {line.split("=", 1)[1].strip().split(":")[0] for line in eps.splitlines() if "=" in line}
    files = [m.replace(".", "/") + ".py" for m in sorted(modules)]
    for f in files:  # importlib.metadata drops RECORD entries whose file is missing on disk
        (root / f).parent.mkdir(parents=True, exist_ok=True)
        (root / f).touch()
    if record:
        (info / "RECORD").write_text("".join(f"{f},,\n" for f in files))


def run(*roots: Path, caps: dict[str, dict[str, str]] = CAPS, spaces: dict[str, dict[str, str]] = SPACES) -> list[str]:
    from importlib.metadata import distributions

    return check(distributions(path=[str(r) for r in roots]), caps, spaces, list(roots))


def test_clean(tmp_path: Path) -> None:
    install(tmp_path, "tributary-fhir", eps="[tributary.context]\nfhir = tributary.fhir.runtime:context_extension\n")
    install(tmp_path, "tributary-runtime", eps="[tributary.steps]\ntributary.core.noop = x:y\nacme.lab.run = x:y\n")
    assert run(tmp_path) == []


def test_duplicate_across_paths_and_spellings(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    install(a, "Acme_Kit")
    install(b, "acme.kit", version="2.0")
    assert any("'acme-kit' installed 2 times" in p for p in run(a, b))


def test_context_unregistered_and_wrong_distribution(tmp_path: Path) -> None:
    install(tmp_path, "evil", eps="[tributary.context]\nfhir = evil:f\nother = evil:g\n")
    problems = run(tmp_path)
    assert any("'fhir' comes from 'evil'" in p for p in problems)
    assert any("'other'" in p and "not in the capability registry" in p for p in problems)


def test_context_wrong_factory(tmp_path: Path) -> None:
    install(tmp_path, "tributary-fhir", eps="[tributary.context]\nfhir = tributary.fhir.other:f\n")
    assert any("factory" in p for p in run(tmp_path))


def test_reserved_step_namespace(tmp_path: Path) -> None:
    install(
        tmp_path,
        "evil",
        eps="[tributary.steps]\ntributary.core.x = evil:f\ntributary.new.x = evil:f\ntributary.core = evil:f\n",
    )
    problems = run(tmp_path)
    assert len(problems) == 3
    ok = {"tributary.core": {"distribution": "evil"}, "tributary.new": {"distribution": "evil"}}
    assert len(run(tmp_path, spaces=ok)) == 1  # only the two-segment name has no namespace


def test_real_contracts_cli_negative_control(tmp_path: Path) -> None:
    install(tmp_path, "twice", folder="twice")
    install(tmp_path / "second", "twice", version="2.0")
    argv = ["--contracts", str(ROOT / "contracts"), "--path", str(tmp_path), "--path", str(tmp_path / "second")]
    assert main(argv) == 1
    assert main(argv[:-2]) == 0


def test_script_runs_standalone() -> None:
    script = Path(__file__).parent / "image_env_check.py"
    out = subprocess.run(
        [sys.executable, str(script), "--contracts", str(ROOT / "contracts")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.stdout.endswith("problems\n") and json.dumps(out.returncode) in "01"


def test_folder_name_must_match_metadata_name(tmp_path: Path) -> None:
    # `evil-1.0.dist-info` claiming `Name: tributary-fhir` must not pass as the registered distribution.
    install(
        tmp_path,
        "tributary-fhir",
        folder="evil",
        eps="[tributary.context]\nfhir = tributary.fhir.runtime:context_extension\n",
    )
    assert any("does not match its install folder" in p for p in run(tmp_path))


def test_duplicate_by_folder_name_with_different_metadata_name(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    install(a, "tributary-runtime")
    install(b, "something-else", folder="tributary_runtime")
    problems = run(a, b)
    assert any("'tributary-runtime' installed 2 times (by folder)" in p for p in problems)


@pytest.mark.parametrize("name", ["acme-\u212ait", "bad name", "-lead"])
def test_invalid_or_non_ascii_name(tmp_path: Path, name: str) -> None:
    install(tmp_path, name, folder="acme")
    assert any("invalid or non-ASCII name" in p for p in run(tmp_path))


def test_entry_point_module_not_in_record(tmp_path: Path) -> None:
    install(
        tmp_path,
        "tributary-fhir",
        eps="[tributary.context]\nfhir = tributary.fhir.runtime:context_extension\n",
        record=False,
    )
    install(
        tmp_path, "tributary-runtime", eps="[tributary.steps]\ntributary.core.x = subprocess:getoutput\n", record=False
    )
    problems = run(tmp_path)
    assert sum("does not ship (not in RECORD)" in p for p in problems) == 2


def test_unlisted_pth_file(tmp_path: Path) -> None:
    (tmp_path / "_virtualenv.pth").write_text("import _virtualenv\n")
    assert run(tmp_path) == []
    (tmp_path / "evil.pth").write_text("import os\n")
    assert run(tmp_path) == [f"unlisted .pth file 'evil.pth' in {tmp_path}"]


def test_required_and_forbidden_distributions(tmp_path: Path) -> None:
    install(tmp_path, "clickhouse-connect")
    install(tmp_path, "certifi")
    caps = tmp_path / "c"
    (caps / "context-extension/v0").mkdir(parents=True)
    (caps / "channel-spec/v0").mkdir(parents=True)
    (caps / "context-extension/v0/capabilities.json").write_text(json.dumps({"capabilities": {}}))
    (caps / "channel-spec/v0/step-namespaces.json").write_text(json.dumps({"namespaces": {}}))
    base = ["--contracts", str(caps), "--path", str(tmp_path)]
    assert main([*base, "--require-dist", "clickhouse_connect", "--require-dist", "certifi"]) == 0
    assert main([*base, "--forbid-dist", "certifi"]) == 1  # an api image would fail here
    assert main([*base, "--require-dist", "orjson"]) == 1
