# Gate reference

Load this only when a step needs it. Commands mirror `.github/workflows/ci.yml` and `.github/workflows/helm.yml`; if they drift, the workflows win.

## Helm

Run when `deploy/helm/**`, `contracts/topics/**`, `contracts/observability/**` or `deploy/compose/topic-bootstrap/bootstrap_topics.py` changed (the helm workflow's path filter). From the worktree root, with `CHART=deploy/helm/tributary` and `R=/tmp/tb-gate-$SHA-rendered.yaml` (keep renders out of the repo):

```bash
python3 deploy/helm/gen_acls.py --check; echo "exit=$?"
helm lint "$CHART" -f "$CHART/ci/values-ci.yaml" --strict; echo "exit=$?"
helm template tb "$CHART" -f "$CHART/ci/values-ci.yaml" --set strimzi.enabled=true > "$R"; echo "exit=$?"
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py "$R"; echo "exit=$?"
```

Mutation control (must fail the assertions, so `--negative` exits 0):

```bash
sed 's/--ops-bind=\$(POD_IP)/--ops-bind=127.0.0.1/' "$R" > "$R.tampered"
uv run --no-project --with pyyaml==6.0.3 python .github/scripts/helm_check.py "$R.tampered" --negative; echo "exit=$?"
```

`kubeconform` runs in CI only if it is not installed locally; say so in the report instead of claiming a pass. The workflow also asserts that several invalid `--set` values refuse to render; read `helm.yml` from line 61 when the change touches chart validation.

If `gen_acls.py --check` fails, regenerate with `python3 deploy/helm/gen_acls.py` (no flag) in the owner's checkout. Never hand-edit `deploy/helm/tributary/files/acls.json`.

## Other CI-only checks worth running when relevant

| When | Command |
|---|---|
| `.gitignore` or new top-level paths changed | `uv run python .github/scripts/gitignore_guard.py` |
| HL7/FHIR mapping changed | `uv run pytest tests/conformance -q -p no:cacheprovider` |
| Full integration run | `uv run pytest -m integration -ra --junitxml=/tmp/tb-int.xml -p no:cacheprovider` then `uv run python .github/scripts/check_integration_counts.py /tmp/tb-int.xml` |
| `ui/` changed | `pnpm --dir ui install --frozen-lockfile && pnpm --dir ui lint && pnpm --dir ui typecheck && pnpm --dir ui test --run` |

`licence_check.py` rewrites `sbom.cdx.json`; that is expected inside the gate worktree and must not be copied back by hand.

## Regenerating pinned files

| File | Regenerate with |
|---|---|
| `tests/support/integration_expected.json` | `uv run python -m tests.support.integration_expected --write` (author of the test change, then tell `qa-engineer`) |
| `deploy/helm/tributary/files/acls.json` | `python3 deploy/helm/gen_acls.py` |
| `uv.lock` | `uv lock` (one agent per batch, see `dev-env`) |
| UI API client | the `ui/` generator script, owned by `ui-engineer` |

## Troubleshooting

- Docker or testcontainers errors: see the `dev-env` skill (dockerd restart, unique container names).
- Long logs: hand them to the `scout` subagent and ask for the failing test ids and first error lines only.
- A failure that also fails on the previous wave commit is pre-existing. Report it with both SHAs, and do not mask it.
