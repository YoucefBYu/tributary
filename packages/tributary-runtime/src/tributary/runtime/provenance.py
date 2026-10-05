"""Where an entry point really comes from, read from installed metadata without importing anything.

Shared by the two code-loading paths of the data plane: `uses` steps (`steps.py`, channel-spec README
3.3.5 N4) and context extensions (`extensions.py`, `context-extension/v0` L2). Both pin a name to one
distribution. Matching the *claimed* distribution name is not enough on its own, and these checks close
three ways around it (security review of a1219af, Lows 1 and 3):

* **A look-alike name.** `str.lower()` folds some non-ASCII letters into ASCII (U+212A KELVIN SIGN
  becomes `k`), so `acme-\\u212ait` used to normalise to `acme-kit`. A distribution name is PEP 508:
  ASCII letters, digits, `.`, `_`, `-`. Anything else never matches (`canonical_distribution`).
* **Metadata that lies about its name.** `METADATA` can claim `Name: tributary-runtime` from a folder
  called `evil-1.0.dist-info`. The install folder's name is the installer's record of what it
  installed, so the two must agree (`install_folder_name`).
* **A target outside the distribution.** `acme.lab.run = subprocess:getoutput` names the standard
  library, so the bound distribution's own `entry_points.txt` would hand any importable callable to the
  loader. The target module must be one of the distribution's own installed files, from its `RECORD`.
  For an editable install (the uv workspace) the `RECORD` lists a `.pth` file instead, and the module
  must then lie under a directory that `.pth` names (`ships_module`).

**Shadowing** (set 1.7, ADR-007 D24, channel-spec README 3.3.5 N4, context-extension L5).
`importlib.metadata.entry_points()` keeps only the first installation per distribution name on
`sys.path`, so a same-named package earlier on the path hides the real one without a trace.
`scan_entry_points` reads every distribution instead, and `InstalledEntryPoints.split` separates what
`importlib.metadata` would show (`visible`, which is what resolution counts and what `import` loads) from
the hidden rest (`shadowed`, never loaded, logged as `duplicate` by each loader).

None of this detects a package that **replaces** a bound distribution's files under the same name; the
controls for that are the locked, hash-pinned dependency set and the digest-pinned image.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.metadata import EntryPoint, distributions
from pathlib import Path
from typing import Any

from .capabilities import normalise_distribution

__all__ = [
    "InstalledEntryPoints",
    "canonical_distribution",
    "install_folder_name",
    "provenance_problem",
    "scan_entry_points",
    "ships_module",
]

#: PEP 508 distribution name, ASCII only (`re.ASCII` keeps `\w`-style shortcuts out of it altogether).
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", re.ASCII)


def canonical_distribution(name: str) -> str | None:
    """The PEP 503 form of a valid ASCII distribution name, or `None`, which never matches a binding."""
    if not name.isascii() or not _NAME.fullmatch(name):
        return None
    return normalise_distribution(name)


def install_folder_name(dist: Any) -> str | None:
    """The distribution name the install folder carries (`name-1.0.dist-info`, `name.egg-info`), canonical.

    Read from `PathDistribution._path`, the folder `importlib.metadata` found the metadata in. A
    distribution that is not on a path (a custom finder) has none, and so never matches.
    """
    path = getattr(dist, "_path", None)
    if not isinstance(path, Path) or path.suffix not in {".dist-info", ".egg-info"}:
        return None
    return canonical_distribution(path.stem.split("-", 1)[0])


def ships_module(dist: Any, module: str) -> bool:
    """Whether `module` is one of `dist`'s own installed files (`RECORD`), or lies under a directory
    that a `.pth` file listed in that `RECORD` names (an editable install). Imports nothing."""
    parts = module.split(".")
    if not parts or not all(part.isidentifier() for part in parts):
        return False
    stem = "/".join(parts)
    candidates = {f"{stem}.py", f"{stem}/__init__.py", *(f"{stem}{suffix}" for suffix in EXTENSION_SUFFIXES)}
    files = [str(entry).replace("\\", "/") for entry in (dist.files or ())]
    if candidates.intersection(files):
        return True
    return any(_under_pth(dist, entry, candidates) for entry in files if entry.endswith(".pth"))


def _under_pth(dist: Any, entry: str, candidates: Iterable[str]) -> bool:
    """A `.pth` path line is a directory added to `sys.path`; `import` lines (custom finders) are not."""
    try:
        located = Path(str(dist.locate_file(entry)))
        text = located.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError, AttributeError):
        return False
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith(("import ", "import\t")):
            continue
        root = Path(line) if Path(line).is_absolute() else located.parent / line
        if any((root / candidate).is_file() for candidate in candidates):
            return True
    return False


def provenance_problem(dist: Any, value: str, expected: str) -> str | None:
    """`None` when an entry point (its `dist` and `value`) comes from `expected`, PEP 503 normalised.

    Otherwise a fixed phrase, safe to log and to put in a `ChannelSpecError`: names only, no file content.
    """
    if dist is None:
        return "has no distribution metadata"
    claimed = canonical_distribution(str(dist.name))
    if claimed is None:
        return "names its distribution with characters outside PEP 508 ASCII"
    if claimed != expected:
        return "comes from another distribution"
    if install_folder_name(dist) != claimed:
        return "has metadata whose Name does not match its install folder"
    module = value.split(":", 1)[0].strip()
    if not ships_module(dist, module):
        return "targets a module its distribution does not ship"
    return None


def scan_entry_points(group: str) -> list[EntryPoint]:
    """Every `group` entry point of every installed distribution, **without** de-duplication.

    `importlib.metadata.entry_points()` keeps only the first distribution per name on `sys.path`, so a
    same-named package earlier on the path would hide the real one without a trace (security review of
    a1219af, Low 2). Reading every distribution lets a loader see the hidden one and say so.
    """
    return [point for dist in distributions() for point in dist.entry_points.select(group=group)]


@dataclass(frozen=True, slots=True)
class InstalledEntryPoints:
    """What one metadata scan found: the entry points `importlib.metadata` would show, and the hidden rest.

    `visible` keeps the first installation per distribution name in `sys.path` order, which is what
    `entry_points()` returns and what N4 and L2 count. `shadowed` are entry points of a later
    installation with the same name: never loaded, and logged as `duplicate` (N5, L5).
    """

    visible: tuple[EntryPoint, ...]
    shadowed: tuple[EntryPoint, ...]

    @classmethod
    def split(cls, points: Iterable[EntryPoint]) -> InstalledEntryPoints:
        first: dict[str, object] = {}
        visible: list[EntryPoint] = []
        shadowed: list[EntryPoint] = []
        for point in points:
            dist = point.dist
            if dist is None:
                visible.append(point)
                continue
            name = install_folder_name(dist) or canonical_distribution(str(dist.name)) or str(dist.name)
            install = getattr(dist, "_path", None) or id(dist)
            if first.setdefault(name, install) == install:
                visible.append(point)
            else:
                shadowed.append(point)
        return cls(visible=tuple(visible), shadowed=tuple(shadowed))
