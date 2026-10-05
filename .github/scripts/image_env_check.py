"""Image-build check (ADR-007 follow-up 18, D24). Standard library only, so it runs in any image's Python.

Fails (exit 1) when the environment it runs in has:

1. two installed distributions with the same PEP 503 name, by METADATA Name or by install-folder name (the
   runtime deliberately does not refuse this); a folder whose name disagrees with its METADATA Name; an
   invalid or non-ASCII name (the runtime's `provenance.py` rules);
2. a `tributary.context` entry point that is not in `contracts/context-extension/v0/capabilities.json`, or
   that comes from another distribution or names another factory than the registry pins;
3. a `tributary.steps` entry point in a reserved `tributary.*` namespace that is not in
   `contracts/channel-spec/v0/step-namespaces.json`, or that comes from another distribution;
4. a `tributary.context` or reserved step entry-point module that the distribution's RECORD does not list;
5. a `.pth` file in an import directory other than `_virtualenv.pth` (it can run code at interpreter start).

Reads metadata only and imports nothing. Output is names only. Usage, in an image:

    docker run --rm -v "$PWD/contracts:/c:ro" -v "$PWD/.github/scripts:/s:ro" --entrypoint python IMAGE \
        /s/image_env_check.py --contracts /c

`--path DIR` (repeatable) scans those directories instead of `sys.path` (negative-control fixtures).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.metadata import Distribution, distributions
from pathlib import Path

CONTEXT_GROUP = "tributary.context"
STEPS_GROUP = "tributary.steps"
RESERVED = "tributary"


def normalise(name: str) -> str:
    """PEP 503 normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


#: A distribution name is PEP 508: ASCII letters, digits, `.`, `_`, `-` (same rule as the runtime's provenance.py).
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", re.ASCII)

#: `.pth` files that may sit in an import directory. Anything else can run code at interpreter start.
ALLOWED_PTH = frozenset({"_virtualenv.pth"})


def canonical(name: str) -> str | None:
    """The PEP 503 form of a valid ASCII name, or `None` (never matches a registry binding)."""
    if not name.isascii() or not _NAME.fullmatch(name):
        return None
    return normalise(name)


def _dist_name(dist: Distribution) -> str:
    return str(dist.metadata["Name"] or "")


def _folder_name(dist: Distribution) -> str | None:
    """The name the install folder (`name-1.0.dist-info`, `name.egg-info`) carries, canonical."""
    path = getattr(dist, "_path", None)
    if not isinstance(path, Path) or path.suffix not in {".dist-info", ".egg-info"}:
        return None
    return canonical(path.stem.split("-", 1)[0])


def _ships_module(dist: Distribution, module: str) -> bool:
    """Whether `module` is one of the distribution's own installed files (RECORD), or lies under a directory
    one of its `.pth` files names (editable installs)."""
    parts = module.split(".")
    if not parts or not all(part.isidentifier() for part in parts):
        return False
    stem = "/".join(parts)
    candidates = {f"{stem}.py", f"{stem}/__init__.py", *(f"{stem}{suffix}" for suffix in EXTENSION_SUFFIXES)}
    files = [str(entry).replace("\\", "/") for entry in (dist.files or ())]
    if candidates.intersection(files):
        return True
    for entry in (f for f in files if f.endswith(".pth")):
        try:
            located = Path(str(dist.locate_file(entry)))
            lines = located.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for line in (raw.strip() for raw in lines):
            if not line or line.startswith(("#", "import ", "import\t")):
                continue
            root = Path(line) if Path(line).is_absolute() else located.parent / line
            if any((root / candidate).is_file() for candidate in candidates):
                return True
    return False


def identity_problems(dists: Iterable[Distribution]) -> list[str]:
    """Invalid names and a folder that disagrees with METADATA (the runtime's `install_folder_name` rule)."""
    problems = []
    for dist in dists:
        name = _dist_name(dist)
        if canonical(name) is None:
            problems.append(f"distribution with an invalid or non-ASCII name {name!a} installed")
        elif _folder_name(dist) != canonical(name):
            problems.append(
                f"distribution {canonical(name)!r} has metadata whose Name does not match its install folder"
            )
    return problems


def duplicate_problems(dists: Iterable[Distribution]) -> list[str]:
    """Count by METADATA Name and by install-folder name: either spelling of a second installation counts."""
    by_name: dict[str, int] = defaultdict(int)
    by_folder: dict[str, int] = defaultdict(int)
    for dist in dists:
        by_name[normalise(_dist_name(dist))] += 1
        folder = _folder_name(dist)
        if folder is not None:
            by_folder[folder] += 1
    problems = []
    for kind, counts in (("name", by_name), ("folder", by_folder)):
        problems += [
            f"duplicate distribution {name!r} installed {count} times (by {kind})"
            for name, count in sorted(counts.items())
            if count > 1
        ]
    return problems


def pth_problems(directories: Iterable[Path], allowed: frozenset[str] = ALLOWED_PTH) -> list[str]:
    problems = []
    for directory in directories:
        if directory.is_dir():
            problems += [
                f"unlisted .pth file {pth.name!r} in {directory}"
                for pth in sorted(directory.glob("*.pth"))
                if pth.name not in allowed
            ]
    return problems


def context_problems(dists: Iterable[Distribution], capabilities: Mapping[str, Mapping[str, str]]) -> list[str]:
    problems = []
    for dist in dists:
        for point in dist.entry_points.select(group=CONTEXT_GROUP):
            owner = normalise(_dist_name(dist))
            pinned = capabilities.get(point.name)
            if pinned is None:
                problems.append(f"{CONTEXT_GROUP} {point.name!r} from {owner!r} is not in the capability registry")
            elif normalise(pinned["distribution"]) != owner:
                problems.append(
                    f"{CONTEXT_GROUP} {point.name!r} comes from {owner!r}, registry pins {pinned['distribution']!r}"
                )
            elif pinned.get("factory") and point.value != pinned["factory"]:
                problems.append(f"{CONTEXT_GROUP} {point.name!r} names a factory other than the registry's")
            if not _ships_module(dist, point.value.split(":", 1)[0].strip()):
                problems.append(
                    f"{CONTEXT_GROUP} {point.name!r} targets a module {owner!r} does not ship (not in RECORD)"
                )
    return problems


def _namespace(uses: str) -> str | None:
    segments = uses.split(".")
    return ".".join(segments[:2]) if len(segments) >= 3 else None


def step_problems(dists: Iterable[Distribution], namespaces: Mapping[str, Mapping[str, str]]) -> list[str]:
    problems = []
    for dist in dists:
        for point in dist.entry_points.select(group=STEPS_GROUP):
            if point.name.split(".", 1)[0] != RESERVED:
                continue
            owner = normalise(_dist_name(dist))
            namespace = _namespace(point.name)
            pinned = namespaces.get(namespace) if namespace else None
            if pinned is None:
                problems.append(f"{STEPS_GROUP} {point.name!r} from {owner!r} is in no registered tributary namespace")
            elif normalise(pinned["distribution"]) != owner:
                problems.append(
                    f"{STEPS_GROUP} {point.name!r} comes from {owner!r}, registry pins {pinned['distribution']!r}"
                )
            if not _ships_module(dist, point.value.split(":", 1)[0].strip()):
                problems.append(
                    f"{STEPS_GROUP} {point.name!r} targets a module {owner!r} does not ship (not in RECORD)"
                )
    return problems


def check(
    dists: Iterable[Distribution],
    capabilities: Mapping[str, Mapping[str, str]],
    namespaces: Mapping[str, Mapping[str, str]],
    directories: Iterable[Path] = (),
) -> list[str]:
    installed = list(dists)
    return (
        identity_problems(installed)
        + duplicate_problems(installed)
        + context_problems(installed, capabilities)
        + step_problems(installed, namespaces)
        + pth_problems(directories)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contracts", type=Path, required=True)
    parser.add_argument("--path", action="append", help="scan these directories instead of sys.path")
    args = parser.parse_args(argv)
    caps = json.loads((args.contracts / "context-extension/v0/capabilities.json").read_text())["capabilities"]
    spaces = json.loads((args.contracts / "channel-spec/v0/step-namespaces.json").read_text())["namespaces"]
    dists = list(distributions(path=args.path) if args.path else distributions())
    directories = [Path(p) for p in (args.path or sys.path) if p]
    problems = check(dists, caps, spaces, directories)
    for problem in problems:
        print(f"FAIL: {problem}")
    print(f"image_env_check: {len(dists)} distributions, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
