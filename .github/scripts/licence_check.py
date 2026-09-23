"""Licence and SBOM check (Phase 0 requirement 15, invariant 10).

Covers everything that can end up on a build or run path:
  * the locked runtime and dev groups (uv.lock, via ``uv export``), and
  * every ``[build-system] requires`` entry in the root and workspace pyprojects, resolved
    transitively. Phase 0 found pathspec (MPL-2.0) only there: a lock-file-only check misses it.

Fails on anything outside Apache-2.0, MIT, BSD, ISC, PSF, except the human-approved unmodified
MPL-2.0 dependencies in MPL_APPROVED. A new or modified MPL dependency needs fresh human approval:
do not just extend that set.

Writes a CycloneDX 1.5 SBOM to --sbom (default sbom.cdx.json).
"""

from __future__ import annotations

import argparse
import importlib.metadata as md
import json
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Human-approved 2026-09-22 (docs/agent-team/gates/phase-0-spikes.md section 8), plus certifi,
# human-approved 2026-09-23: dev-only, unmodified, via testcontainers[kafka] -> docker -> requests ->
# certifi. Never shipped in the default distribution. Unmodified only.
MPL_APPROVED = {"orjson", "hypothesis", "pathspec", "certifi"}

ALLOWED = ("apache", "mit", "bsd", "isc", "psf", "python software foundation", "zlib", "0bsd")
DENIED = re.compile(
    r"\b(a?gpl|lgpl|sspl|bsl|busl|business source|server side public|"
    r"gnu (lesser |affero )?general public)",
    re.I,
)
# Packages whose metadata carries no usable licence field; verified by hand, record the evidence.
OVERRIDES: dict[str, str] = {}


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _terms_ok(expr: str) -> bool:
    """SPDX-ish evaluation: OR needs one allowed branch, AND needs all terms allowed."""
    expr = expr.strip().strip("()")
    if re.search(r"\s+OR\s+", expr):
        return any(_terms_ok(p) for p in re.split(r"\s+OR\s+", expr))
    if re.search(r"\s+AND\s+", expr):
        return all(_terms_ok(p) for p in re.split(r"\s+AND\s+", expr))
    return _term_ok(expr)


def _term_ok(term: str) -> bool:
    t = term.lower()
    if DENIED.search(t):
        return False
    return any(a in t for a in ALLOWED)


def licences(dist: md.Distribution) -> list[str]:
    meta = dist.metadata
    out: list[str] = []
    if expr := meta.get("License-Expression"):
        out.append(expr)
    out += [c.split("::", 1)[1].strip() for c in meta.get_all("Classifier") or [] if c.startswith("License ::")]
    lic = meta.get("License")
    if lic and len(lic) < 200:  # a full licence text pasted into the field is not a name
        out.append(lic)
    return out


def classify(name: str, dist: md.Distribution) -> tuple[str, str]:
    """Return (status, detail): status is ok, mpl-approved or fail."""
    found = [OVERRIDES[name]] if name in OVERRIDES else licences(dist)
    detail = " | ".join(found) or "NO LICENCE METADATA"
    if any(DENIED.search(x) for x in found):
        return "fail", detail
    if any("mpl" in x.lower() or "mozilla" in x.lower() for x in found):
        # MPL as one term of an AND expression (orjson) still needs the approval.
        if name in MPL_APPROVED:
            return "mpl-approved", detail
        return "fail", detail + " (MPL not approved for this package)"
    if not found:
        return "fail", detail
    ok = any(_terms_ok(x) for x in found)
    return ("ok" if ok else "fail"), detail


def build_requires() -> list[str]:
    reqs: list[str] = []
    for p in [ROOT / "pyproject.toml", *sorted((ROOT / "packages").glob("*/pyproject.toml"))]:
        data = tomllib.loads(p.read_text(encoding="utf-8"))
        reqs += data.get("build-system", {}).get("requires", [])
    return sorted(set(reqs))


def resolve(target: Path) -> None:
    export = subprocess.run(
        ["uv", "export", "--frozen", "--all-groups", "--no-emit-workspace", "--no-hashes", "--no-header"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    reqs = [ln for ln in export.splitlines() if ln and not ln.startswith(("#", " "))]
    reqs += build_requires()
    req_file = target / "requirements.in"
    req_file.write_text("\n".join(reqs) + "\n", encoding="utf-8")
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--quiet",
            "--python-version",
            "3.12",
            "--target",
            str(target / "site"),
            "-r",
            str(req_file),
        ],
        cwd=ROOT,
        check=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sbom", default="sbom.cdx.json")
    args = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        tdir = Path(tmp)
        resolve(tdir)
        components, failures = [], []
        for dist in sorted(md.distributions(path=[str(tdir / "site")]), key=lambda d: _norm(d.metadata["Name"])):
            name, version = _norm(dist.metadata["Name"]), dist.version
            status, detail = classify(name, dist)
            print(f"{status:12} {name}=={version}  [{detail}]")
            components.append(
                {
                    "type": "library",
                    "name": name,
                    "version": version,
                    "purl": f"pkg:pypi/{name}@{version}",
                    "licenses": [{"license": {"name": detail}}],
                }
            )
            if status == "fail":
                failures.append(f"{name}=={version}: {detail}")
    sbom = {"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1, "components": components}
    Path(args.sbom).write_text(json.dumps(sbom, indent=1), encoding="utf-8")
    if failures:
        print("\nLICENCE CHECK FAILED (invariant 10):", *failures, sep="\n  ", file=sys.stderr)
        return 1
    print(f"\nlicence check passed: {len(components)} components, SBOM at {args.sbom}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
