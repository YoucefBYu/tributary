# Local CI and the self-hosted runner (W3-CI-1)

Decision: `docs/agent-team/decisions/ci-alternatives.md`, options C plus A. This file is the operator side
(section 7 of that decision, kept here because `docs/` is not devops-engineer's).

## The script is the CI

`.github/scripts/ci_local.sh` holds every CI command. `ci.yml` and `helm.yml` only check out, set up uv and call
it; `.github/scripts/test_ci_parity.py` fails if any workflow `run:` does anything else.

    .github/scripts/ci_local.sh all --sha HEAD          # every job in a clean detached worktree, removed afterwards
    .github/scripts/ci_local.sh fast                    # contracts, python (lint, types, fast tests), supply-chain
    .github/scripts/ci_local.sh helm image-check        # any subset: contracts lint python integration
                                                        #   supply-chain image-check ui helm

It prints `PASS <job>: <summary line>` or `FAIL <job>: ...` per job and exits non-zero on any failure. Uncommitted
changes are not tested (it runs the commit). Behind a TLS-intercepting proxy set
`TB_BUILD_CA_BUNDLE=/path/ca.crt` for `image-check`. Install the advisory pre-push hook with
`uv run pre-commit install --hook-type pre-push` (`git push --no-verify` bypasses it).

## Posting statuses (`--report`)

`ci_local.sh --report <jobs>` posts a `local/<job>` commit status (success or failure) for the sha.

- The token is a fine-grained token limited to this repository with "Commit statuses: write" and nothing else.
- It lives in a **file**, `TB_STATUS_TOKEN_FILE=/path/to/file`, owned by you with mode 0600. The script refuses a
  symlink, a file owned by someone else, or one readable by group or others. Keep the file outside the repository and
  out of any synced folder. The old `TB_STATUS_TOKEN` environment variable is no longer read (the script refuses to
  run if it is set), because every child process can read the environment of its parent.
- Only the reporter opens the file, just before its one HTTPS request. The token is never in a process's
  environment, a command line, a log or a status description, and jobs do not even get the file's path.
- **Limit:** a job runs as you, so test code you run could read a 0600 file in your home directory. This
  protects against other users and accidental exposure, not against hostile code in the repository. Run untrusted
  code under a separate account, keep the token's scope minimal, and rotate it if in doubt.

A status is self-attested (it proves this script ran on that sha on that machine); on a private GitHub Free repo it
cannot be made a required check. The pre-push hook tests the ref being pushed when pre-commit exposes it
(`PRE_COMMIT_TO_REF`), otherwise HEAD; `--sha` overrides both.

## Self-hosted runner (option A)

Only after the human's 10-minute test shows self-hosted jobs are dispatched during the billing hold.

0. Before attaching anything: Settings > Actions > General > "Fork pull request workflows" and confirm that
   workflows from fork pull requests cannot run (for a private repo, leave "Run workflows from fork pull requests"
   off). Settings > Secrets and variables > Actions > Variables: restrict who can edit repository variables to
   admins, because whoever can set `TB_RUNNER` chooses which machine runs the jobs. Write access to the repository
   already lets someone push a workflow that runs on the runner, so only trusted people get write access.
1. Settings > Actions > Runners > New self-hosted runner (Linux x64). Install it as a service and register it
   with **`--ephemeral` (required)**: a fresh runner per job, so no job inherits files, caches or processes
   from the previous one.
2. Install on the host: Docker, `uv`, Node 22 and pnpm (only when `ui/` exists), `curl`, `git`, `python3`.
3. Set the repository variable `TB_RUNNER` to the runner's label. Every job uses
   `runs-on: ${{ vars.TB_RUNNER || 'ubuntu-24.04' }}`; unset, jobs go to GitHub-hosted runners. (Verify on the
   first run that `vars` resolves in `runs-on`.)

**Risk, accept it knowingly.** Any workflow code pushed to the repository runs on that machine with access to
its Docker socket, which is root-equivalent on the host. This is acceptable only for a **private repository with
trusted pushers**. Never attach the runner to a public repository or one that runs fork pull requests. Run it as a
dedicated unprivileged user on a machine without other secrets, keep `permissions: contents: read` (as the
workflows have), and remove the runner (and unset `TB_RUNNER`) if the repo ever becomes public. GitHub has postponed
a per-minute charge for self-hosted runners in private repos (unverified as of 2026-10-14); spend is the human's call.
