# Wave 2 CI confirmation (STATUS backlog item 2 / "#8")

qa-engineer, 2026-10-05, tree at `ec4b9d1`, run locally against the commands in `.github/workflows/ci.yml`.

| Check | Command | Exit | Result | Expected |
|---|---|---|---|---|
| Contract validation | `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py` | 0 | `156 checks passed, 0 failed` | 156 / 0 : met |
| Pinned pairs | same, `--emit-expectations` | 0 | 110 pairs, 110 `pinned_pair_matches: true`, 0 false | all match : met |
| Conformance | `uv run pytest tests/conformance -q` (also with `--no-project --with pytest --with jsonschema`) | 0, 0 | `150 passed` both ways | 150 : met |
| Reply property test, fast suite | `uv run pytest -m "not integration and not chaos and not bench" packages/tributary-receivers/tests/reply/test_properties.py -v` | 0 | `16 passed in 9.21s`, none deselected (no `integration` marker) | runs in the CI fast suite : met |
| Controls go red | scratch copy in the session scratchpad (not in the repo): `AnswersOnReplyAlone`, `NeverArmsTheClock`, `IgnoresRevoke`, `SettlesOnTheReplyAlone` each neutralised to a correct implementation (override method renamed), run with `-c pyproject.toml` | 1 | `6 failed, 10 passed`: the failures are exactly the control tests for those four (`test_each_broken_request_is_caught_through_the_map[AnswersOnReplyAlone]`, `test_each_broken_map_is_caught[NeverArmsTheClock]`, `...[SettlesOnTheReplyAlone]`, `test_the_revoke_control_is_caught`, and both parametrisations of `test_the_map_controls_fail_on_their_minimal_traces...`) | controls red : met |

The `python` CI job runs `uv run pytest -m "not integration and not chaos and not bench"` with `testpaths` containing `packages`, so the property test is collected there; a second explicit step runs `tests/conformance`.

## Gaps for devops-engineer (`.github/workflows/ci.yml`)

1. **`--emit-expectations` is not run anywhere in CI**, and no test in `tests/conformance` calls it. The `contracts` job runs `validate.py` only. A schema edit that drifts a pinned (path, validator) pair is not caught by CI. Fix: add `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py --emit-expectations` plus a check that no line has `"pinned_pair_matches": false` (for example `... | grep -c '"pinned_pair_matches": false'` expecting 0, or have `validate.py` exit non-zero on a mismatch, which is the architect's file).
2. The reply property test has no dedicated CI step; it is protected only by living in the default fast suite. A deselecting marker or a `testpaths` change would drop it silently. Optional: an explicit step `uv run pytest packages/tributary-receivers/tests/reply -q`, like the conformance step.
3. The `integration` job tolerates exit 5 (no tests collected) with `|| [ $? -eq 5 ]`. There are now 100+ integration tests, so that guard can hide a collection failure; it should be removed. Also the lead's "105-112 green" has not been seen in a CI log by QA.
