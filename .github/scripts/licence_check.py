"""Licence and SBOM check (Phase 0 requirement 15, invariant 10).

Covers everything that can end up on a build or run path:
  * the locked runtime and dev groups (uv.lock, via ``uv export``), and
  * every ``[build-system] requires`` entry in the root and workspace pyprojects, resolved
    transitively. Phase 0 found pathspec (MPL-2.0) only there: a lock-file-only check misses it.

Fails on anything outside Apache-2.0, MIT, BSD, ISC, PSF, except the human-approved unmodified
MPL-2.0 dependencies in MPL_APPROVED and the human-approved unmodified LGPL dependencies in
LGPL_APPROVED. Each entry names a *scope*, not just a package: "runtime" means the approval covers it
shipping in the default distribution (invariant 10's actual concern), "non-runtime" means the opposite
-- approved only for staying out of it (dev group or `[build-system] requires`). The check enforces the
scope, not just the licence: a package approved non-runtime that starts appearing in the runtime-only
dependency closure fails, even though its name is on the list, because that closure is a different
question than the one that was answered. A new or modified MPL or LGPL dependency, or an existing one
arriving by a new path or into a new scope, needs fresh human approval: do not just extend these sets.
LGPL_APPROVED names a package, not a licence family: every other LGPL (and AGPL) dependency still
fails via DENIED, approved or not, unless its own name is added deliberately. An LGPL entry also names
the exact workspace members the approval covers (`importers_of`): the same package pulled in by some
other, unapproved member fails even though its name and scope are unchanged.

`resolve()` exports every workspace member's own dependency closure (`--all-packages`), not just
whatever the root dev group already reaches, so a brand new member is covered by this gate from the
moment its own pyproject.toml exists -- registering it in the root dev group is a separate, later step
(see pyproject.toml, devops-engineer) that this check must not wait on.

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

# Each approval is scoped: "runtime" means human-approved to ship in the default distribution;
# "non-runtime" means human-approved to be a dev-group or build-system-only dependency and NOT to
# ship in it. All four are unmodified dependencies.
#
# orjson: human-approved 2026-09-22 (docs/agent-team/gates/phase-0-spikes.md section 8). A
#   tributary-runtime dependency: ships in the distribution, so scope is "runtime".
# hypothesis, pathspec: same approval, same date. Test framework and a mypy/hatchling dependency
#   respectively: neither ships, so scope is "non-runtime".
# certifi: SEPARATE approval, human-approved 2026-09-23 (ADR-001 2.3; wave-1 gate report 5.3) -- not
#   folded into the 2026-09-22 decision, which was written to cover exactly those three packages.
#   Current path is testcontainers[kafka] -> docker -> requests -> certifi, dev group only, so scope
#   is "non-runtime". If certifi (or any of these) ever arrives in the runtime closure or by a
#   different path, that is a different dependency and needs its own fresh approval -- it must not be
#   silently covered by broadening this comment.
MPL_APPROVED: dict[str, str] = {
    "orjson": "runtime",
    "hypothesis": "non-runtime",
    "pathspec": "non-runtime",
    "certifi": "non-runtime",
}

# paramiko: LGPL-2.1, human-approved 2026-09-24, unmodified dependency, for SFTP support -- but only
# as pulled in by these two workspace members (MEDIUM on 1c01120, security-reviewer): the approval
# named exactly this import path, not "paramiko anywhere in the workspace". Both ship it in the
# default distribution, so scope is "runtime". No other LGPL package is approved: LGPL_PATTERN below
# is checked by name against this set only, never as a blanket allowance for the licence family.
LGPL_APPROVED: dict[str, tuple[str, frozenset[str]]] = {
    "paramiko": ("runtime", frozenset({"tributary-loaders", "tributary-receivers"})),
}

ALLOWED = ("apache", "mit", "bsd", "isc", "psf", "python software foundation", "zlib", "0bsd")
DENIED = re.compile(
    r"\b(a?gpl|lgpl|sspl|bsl|busl|business source|server side public|"
    r"gnu (lesser |affero )?general public)",
    re.I,
)
# Narrower than DENIED: LGPL only, never AGPL ("gnu affero general public" does not match). Checked
# ahead of DENIED so an approved package's LGPL text does not fail before LGPL_APPROVED is consulted.
LGPL_PATTERN = re.compile(r"lgpl|gnu lesser general public", re.I)
MPL_PATTERN = re.compile(r"mpl|mozilla", re.I)
# Packages whose metadata carries no usable licence field; verified by hand, record the evidence.
OVERRIDES: dict[str, str] = {}


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _strip_approved_terms(text: str, pattern: re.Pattern[str]) -> str:
    """Remove every AND/OR-delimited term of an SPDX-ish expression that matches `pattern`, instead of
    treating the whole (possibly compound) string as a single unit.

    Fixes HIGH on 1c01120 (security-reviewer): `classify` used to search a whole descriptor string
    for "does it contain lgpl/mpl anywhere", then approve the *entire* string on that one hit -- so
    "LGPL-2.1 AND AGPL-3.0", "LGPL-2.1 AND GPL-3.0", "LGPL-2.1 AND MPL-2.0" and "LGPL-2.1 OR SSPL-1.0"
    all passed for paramiko, and the same shape of bug was possible on the MPL branch it was modelled
    on (see `test_mpl_and_expression_needs_the_other_term_too` for the "MPL-2.0 AND GPL-3.0" case
    architect and the lead asked to have checked). Survivors are rejoined with AND, the conservative
    reading when the original connective (AND or OR) is discarded: every surviving term becomes
    mandatory rather than optional, so an approval can never accidentally launder a denied OR-sibling
    through this rewrite. A textual edit, not a full parser -- a parenthesised group is an opaque term,
    the same limitation `_terms_ok` below already has for grouping."""
    terms = re.split(r"\s+(?:AND|OR)\s+", text)
    survivors = [t for t in terms if not pattern.search(t)]
    return " AND ".join(survivors)


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


def classify(
    name: str,
    dist: md.Distribution,
    *,
    runtime_names: frozenset[str] = frozenset(),
    importer_names: frozenset[str] = frozenset(),
) -> tuple[str, str]:
    """Return (status, detail): status is ok, mpl-approved, lgpl-approved or fail.

    `runtime_names` is the set of packages in the runtime-only dependency closure (every workspace
    package's own `dependencies`, none of the dev group or `[build-system] requires`) -- the actual
    default-distribution surface invariant 10 is about. An MPL or LGPL package's approval scope is
    checked against it: "runtime" must be in that closure, "non-runtime" must not be. A scope mismatch
    fails even for an approved name, because the approval answered a different question than the one
    this resolution is now asking.

    `importer_names` is the set of workspace members whose own runtime-only closure actually pulls in
    `name` (see `importers_of`). An LGPL approval also names the importers it covers (MEDIUM on
    1c01120): a package approved only for specific importers still fails if some other, unapproved
    member starts pulling it in, even though the package's own name and scope are unchanged -- that is
    a different question than the one the approval answered, same reasoning as the runtime/non-runtime
    scope check.
    """
    found = [OVERRIDES[name]] if name in OVERRIDES else licences(dist)
    detail = " | ".join(found) or "NO LICENCE METADATA"
    if not found:
        return "fail", detail

    status = "ok"

    if any(LGPL_PATTERN.search(x) for x in found):
        approval = LGPL_APPROVED.get(name)
        if approval is None:
            return "fail", detail + " (LGPL not approved for this package)"
        scope, allowed_importers = approval
        in_runtime = name in runtime_names
        if scope == "runtime" and not in_runtime:
            return "fail", detail + f" (approved runtime-only, but not in the runtime closure: {name!r} moved)"
        if scope == "non-runtime" and in_runtime:
            return "fail", detail + f" (approved non-runtime only, but now in the runtime closure: {name!r} moved)"
        extra_importers = importer_names - allowed_importers
        if extra_importers:
            return "fail", (
                detail + f" (approved only as imported by {sorted(allowed_importers)},"
                f" but also imported by {sorted(extra_importers)})"
            )
        # Strip only the LGPL term(s): an AND/OR-ed extra term (AGPL, GPL, SSPL, an unapproved MPL...)
        # must still be checked below, not waved through by this one approval (HIGH on 1c01120).
        found = [_strip_approved_terms(x, LGPL_PATTERN) for x in found]
        found = [x for x in found if x]
        status = "lgpl-approved"
        if not found:
            return status, detail

    if any(DENIED.search(x) for x in found):
        return "fail", detail

    if any(MPL_PATTERN.search(x) for x in found):
        mpl_scope = MPL_APPROVED.get(name)
        if mpl_scope is None:
            return "fail", detail + " (MPL not approved for this package)"
        in_runtime = name in runtime_names
        if mpl_scope == "runtime" and not in_runtime:
            return "fail", detail + f" (approved runtime-only, but not in the runtime closure: {name!r} moved)"
        if mpl_scope == "non-runtime" and in_runtime:
            return "fail", detail + f" (approved non-runtime only, but now in the runtime closure: {name!r} moved)"
        # Same reasoning as the LGPL strip above: an AND/OR-ed extra term still needs checking.
        found = [_strip_approved_terms(x, MPL_PATTERN) for x in found]
        found = [x for x in found if x]
        if status == "ok":
            status = "mpl-approved"
        if not found:
            return status, detail
        if any(DENIED.search(x) for x in found):
            return "fail", detail

    ok = any(_terms_ok(x) for x in found)
    return (status if ok else "fail"), detail


def build_requires() -> list[str]:
    reqs: list[str] = []
    for p in [ROOT / "pyproject.toml", *sorted((ROOT / "packages").glob("*/pyproject.toml"))]:
        data = tomllib.loads(p.read_text(encoding="utf-8"))
        reqs += data.get("build-system", {}).get("requires", [])
    return sorted(set(reqs))


def _export(*extra: str) -> list[str]:
    export = subprocess.run(
        ["uv", "export", "--frozen", "--no-emit-workspace", "--no-hashes", "--no-header", *extra],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [ln for ln in export.splitlines() if ln and not ln.startswith(("#", " "))]


def runtime_closure_names() -> frozenset[str]:
    """Every package in the runtime-only dependency closure: each workspace member's own
    `dependencies`, with the dev group excluded. This is the default-distribution surface, as
    opposed to the full `--all-groups` export `resolve()` installs to check everything at once."""
    reqs = _export("--no-dev", "--all-packages")
    names = {re.split(r"[<>=!~\[; ]", r, maxsplit=1)[0] for r in reqs}
    return frozenset(_norm(n) for n in names if n)


def workspace_members() -> list[str]:
    """Every workspace member's own package name (not the root pseudo-package), read straight from
    `packages/*/pyproject.toml` rather than the lockfile -- `uv export --package` needs the name a
    member declares for itself, and a brand new member's name must be visible here even before
    anything else in the workspace depends on it."""
    return sorted(
        tomllib.loads(p.read_text(encoding="utf-8"))["project"]["name"]
        for p in (ROOT / "packages").glob("*/pyproject.toml")
    )


def importers_of(pkg: str) -> frozenset[str]:
    """Which workspace members pull `pkg` into their own runtime-only dependency closure (no dev
    group). Narrower than `runtime_closure_names`, which only answers "is it in the closure somewhere"
    -- a scoped LGPL approval (MEDIUM on 1c01120, security-reviewer) names the exact importers a human
    approved, so a package pulled in by some other, unapproved member must still fail even though the
    package's own name and runtime/non-runtime scope are unchanged."""
    target = _norm(pkg)
    out: set[str] = set()
    for member in workspace_members():
        reqs = _export("--package", member, "--no-dev")
        names = {_norm(re.split(r"[<>=!~\[; ]", r, maxsplit=1)[0]) for r in reqs if r}
        if target in names:
            out.add(member)
    return frozenset(out)


def resolve(target: Path) -> None:
    # --all-packages: every workspace member's own dependencies, not just whichever ones the root dev
    # group already reaches. Without it a brand new member is invisible to this gate even though its
    # own dependencies are already in uv.lock (architect/healthcare-standards-engineer: fhir.resources
    # and fhir-core, tributary-fhir's runtime dependencies, did not appear in the scan output until the
    # package was added to the root dev group -- this closure should not depend on that registration
    # step at all).
    reqs = _export("--all-groups", "--all-packages")
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
        runtime_names = runtime_closure_names()
        # Only computed for names an LGPL approval actually scopes by importer: a `uv export
        # --package` call per workspace member per name would be wasted work for the rest of the
        # closure, which never consults `importer_names`.
        lgpl_importers = {name: importers_of(name) for name in LGPL_APPROVED}
        components, failures = [], []
        for dist in sorted(md.distributions(path=[str(tdir / "site")]), key=lambda d: _norm(d.metadata["Name"])):
            name, version = _norm(dist.metadata["Name"]), dist.version
            status, detail = classify(
                name, dist, runtime_names=runtime_names, importer_names=lgpl_importers.get(name, frozenset())
            )
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
