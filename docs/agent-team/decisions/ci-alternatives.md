# CI alternatives while GitHub-hosted Actions is unavailable

- **Status:** Proposed. The human decides. Nothing here has been implemented.
- **Author:** architect, 2026-10-14
- **Escalations inside:** cloud spend (option A's possible future charge, and fixing billing), credentials (a status token), and publishing (option F). These are the human's calls (`CLAUDE.md`, "Escalate to a human").

## 1. Problem

`youcefbyu/tributary` is private. Every GitHub Actions job is refused within about 3 s with "recent
account payments have failed or your spending limit needs to be increased". Today the only
verification is the lead's manual gate (`.claude/skills/gate/SKILL.md`, `reference.md`), run in a clean
worktree.

**The gate has already drifted from `ci.yml`.** Comparing `.github/workflows/ci.yml` with `SKILL.md`
steps 1-10:

| CI step (`ci.yml` / `helm.yml`) | Gate today |
|---|---|
| `uv sync` (`ci.yml:47`) | `uv sync --all-packages` (step 1). This is a different install set. |
| `contracts/validate.py` default run, then `--check-expectations` (`ci.yml:33,36`) | Only `--check-expectations` (step 6) |
| `pytest tests/conformance` and the sync-reply property tests (`ci.yml:52-60`) | Conditional, or missing |
| Full integration suite, then `check_integration_counts.py` (`ci.yml:80-82`) | Only "affected dirs" by default; the count check is listed in the CI-only table |
| `gitignore_guard.py`, `licence_check.py --sbom` (`ci.yml:94,98`) | The guard is conditional; the licence check runs without `--sbom` |
| `image_check.sh`, the image environment check (`ci.yml:116`) | **Missing entirely** |
| `ui` job (`ci.yml:118-133`) | Conditional |
| Helm: kubeconform plus about 25 negative render assertions (`helm.yml:53-155`) | One mutation control; kubeconform is "if installed" |

Whatever we choose must remove this drift by construction, not by care.

## 2. Criteria

1. **Fidelity:** it runs what `ci.yml`/`helm.yml` run, including testcontainers (which needs a Docker socket or DinD) and `image_check.sh` (`docker build` plus `docker run -v $root/...` bind mounts).
2. **Works today,** despite the billing hold.
3. **Visible on the PR,** as a status check or not at all.
4. **Licence:** permissive, or clearly outside the distribution (invariant 10).
5. **Effort and maintenance** for a small agent team.
6. **Risk:** security (the Docker socket is root-equivalent), drift, and anything needing human sign-off.

## 3. Options

### A. GitHub self-hosted runner on the human's machine
- **Cost:** $0 today. GitHub's billing docs say "GitHub Actions usage is free for self-hosted runners". In Dec 2025 GitHub announced a $0.002/min platform charge for self-hosted runners in private repos from 2026-03-01, then **postponed** it "to re-evaluate". I found no later decision (unverified as of 2026-10-14). If it is revived: minutes per push x pushes x $0.002. For example, 30 min x 100 pushes = $6/month. Spend is the human's call.
- **Does the billing hold still block jobs? Unverified, and likely yes.** The docs do not say. A community report shows a staff-applied `billing.lock` left **both** hosted and self-hosted jobs stuck at "Waiting for a runner". Our message names failed payments, which looks like an account-level hold, not a minutes quota. A 10-minute test settles it (section 5, step 1).
- **Licence:** `actions/runner` is MIT (from memory, not re-fetched). It is not distributed with Tributary.
- **Setup:** about 1 h. Install and register the runner as a service, install Docker, `uv`, Node 22 and pnpm. Jobs say `runs-on: ubuntu-24.04`, which routes to hosted runners, so devops-engineer changes it to `${{ vars.TB_RUNNER || 'ubuntu-24.04' }}` (the `vars` context is allowed in `runs-on`; verify on first run).
- **Fidelity:** high. These are the real workflows and the real expression engine. Host Docker means testcontainers and `image_check.sh` bind mounts work natively. The differences are a host OS that is not the hosted image, and a runner that is not ephemeral (leftover containers, caches) unless it runs `--ephemeral`.
- **PR result:** real Actions check runs, the same as hosted.
- **Maintenance:** low to medium. Runner auto-updates, host patching, disk cleanup. CI stops when the machine sleeps.
- **Risks:** any pushed workflow code runs on the human's machine with a root-equivalent Docker socket. That is acceptable for a private repo with trusted pushers, and **never acceptable for a public repo** (fork PRs). It depends on GitHub billing state we do not control.

### B. nektos/act, running the workflows locally in Docker
- **Cost:** $0. **Licence:** MIT. Latest is v0.2.89, released 2026-06-01. It is local dev tooling and not distributed.
- **Setup:** about 2-4 h, plus some iteration. Choose a runner image (`catthehacker/ubuntu:act-latest`; act's docs say the images "do **not** contain **all** the tools" of hosted runners) and mount the socket.
- **Fidelity:** medium. act documents `concurrency`, `timeout-minutes`, `continue-on-error`, annotations and step summaries as ignored, the `github` context as incomplete, and says it cannot be "completely compatible". testcontainers and `image_check.sh` talk to the host daemon from inside a job container. The `-v "$root/contracts:/c:ro"` mounts in `image_check.sh:34` then resolve against **host** paths, so we would need `act --bind` (my inference, unverified). `upload-artifact` needs `--artifact-server-path`.
- **PR result:** none.
- **Maintenance:** medium. Image pulls on every run unless `--pull=false`, and debugging act-only differences.
- **Risks:** false greens and false reds from emulation gaps. It is good for debugging workflow YAML, but weak as a gate.

### C. Repo-owned local CI script, pre-push hook and parity check
- **Design:** one script, `.github/scripts/ci_local.sh <job>` (owned by devops-engineer), holds the commands for every job (`contracts`, `python`, `integration`, `supply-chain`, `image-check`, `ui`, `helm`, plus `fast` and `all`). **The workflows become thin wrappers**: after `checkout` and `setup-uv`, each job's only `run:` is `.github/scripts/ci_local.sh <job>`, with `::group::` markers so the Actions log keeps per-step folding. A parity test (`.github/scripts/test_ci_parity.py`) fails if any workflow `run:` is not a `ci_local.sh <job>` call (outside a short allow-list, such as the `has_ui` probe), or if a workflow job has no script subcommand or the reverse. `--sha <sha>` runs in a clean detached worktree, as the gate does today.
- **PR status (optional):** `--report` posts one commit status per job, with context `local/<job>`, through `POST /repos/{owner}/{repo}/statuses/{sha}` (limit 1000 per sha and context). It uses a fine-grained token, scoped to this repo with commit-statuses write, read from an environment variable and **never stored in the repo**. Whether the Statuses API works during the Actions billing hold is unverified. It is a separate API, so I expect it to.
- **Cost:** $0. **Licence:** our own code (Apache-2.0). `pre-commit` (MIT) is already used.
- **Setup:** about 1 day for devops-engineer, plus about 1 h for the lead to point the gate skill at the script.
- **Fidelity:** high for commands, because the commands are the same file. It runs on the host, so testcontainers and `image_check.sh` behave as they do in the gate today. The parity test makes drift a red test, not a review comment. Not covered: the Actions runtime itself (expressions, `uses:` versions), and the host differs from `ubuntu-24.04`.
- **PR result:** none by default. With `--report`, a status on the PR. It is **self-attested**: it proves the script ran on that SHA on the reporter's machine, not that an independent runner did. Statuses on a private repo on GitHub Free **cannot be made required**, because branch protection and rulesets on private repos need Pro or higher.
- **Pre-push hook:** a `pre-commit` hook at the `pre-push` stage runs `ci_local.sh fast` (contracts, lint, types, fast tests, supply-chain; integration is too slow for a hook). It is advisory, since `--no-verify` bypasses it.
- **Maintenance:** low. A new check is one script edit, and the parity test enforces it.
- **Risks:** self-attestation, and the reporter's environment leaks into results (mitigated by the clean worktree and `UV_FROZEN=1`). The script is reused unchanged by A, B and F, so no choice here is wasted.

### D. Self-hosted forge CI: Woodpecker, Gitea Actions or Forgejo Actions
- **Licences:** Woodpecker is Apache-2.0 (3.18.0, 2026-08-24). Gitea is MIT, and its runner (now `gitea/runner`) is MIT. **Forgejo is GPL-3.0-or-later from v9.0, and its runner is GPL-3.0-or-later** (v12.5.2). It would run as a separate service and never be distributed, but our stated preference for permissive dev tooling makes Forgejo a human sign-off item. Gitea is the permissive equivalent.
- **Cost:** $0 in software. It needs an always-on host, and for Woodpecker a **publicly reachable HTTPS endpoint** for GitHub webhooks (`WOODPECKER_HOST`), which means a tunnel or exposed listener. That is network exposure, needs a security review, and possibly cloud spend.
- **Setup:** 1-2 days. Server, database, agent or runner, a GitHub OAuth app (Woodpecker), or a push mirror (Gitea).
- **Fidelity:** Woodpecker needs a **second pipeline definition** (`.woodpecker/*.yaml`), which means drift. Gitea Actions runs GitHub-Actions-syntax workflows with few changes. Its runner supports DinD (privileged) or a socket mount, but act-style gaps apply.
- **PR result:** Woodpecker posts commit statuses to GitHub PRs. Gitea shows results on Gitea only, unless a bridge posts statuses.
- **Maintenance:** high: a server to patch, back up and upgrade, plus credential handling.
- **Risks:** the largest attack surface of any option, an inbound listener, and OAuth secrets to manage.

### E. Dagger, a portable pipeline as code
- **Licence:** Apache-2.0. It runs locally with Docker; Dagger Cloud is optional. **Cost:** $0.
- **Setup:** 3-5 days to rewrite every job in the Dagger Python SDK.
- **Fidelity:** it becomes the new source of truth, not a mirror of `ci.yml`. testcontainers needs the host socket passed through `with_unix_socket`. The engine runs as a privileged container (third-party report, unverified).
- **PR result:** none on its own. It needs a host CI or a status script, as in C.
- **Maintenance:** medium. A new dependency, abstraction and engine version to track.
- **Risks:** a large rewrite that solves the same drift problem C solves in a day, plus a privileged engine.

### F. Make the repo public
- **Cost:** $0. GitHub's docs say "The use of standard GitHub-hosted runners is free ... In public repositories". Branch protection, and so **required** status checks, are also free for public repos. Whether the current account-level hold also blocks public-repo jobs is **unverified**.
- **Licence:** no change to Tributary's licence. It makes the Apache-2.0 core visible.
- **Setup:** a few clicks, after a pre-publication audit taking **days**:
  - a full-history secret and PHI scan (the current scans check the tree);
  - confirming `samples/mirth/` never entered history;
  - the codename, naming and trademark question;
  - reviewing ADRs and agent docs that become public;
  - fork-PR approval settings.
- **Fidelity:** exact. These are the real hosted runners.
- **PR result:** real checks, which can be required.
- **Maintenance:** none for CI. There is new community and security-report surface, and a `SECURITY.md` is needed.
- **Risks:** this is **publishing, so it is the human's decision**. It is irreversible in practice, since forks and caches persist. A self-hosted runner (A) must then never be attached.

### Not listed as an option: fix the billing hold
Updating the payment method restores hosted runners with the plan's private-repo minutes. That is spend, so it is the human's call. It is the simplest route if acceptable, and it composes with C.

## 4. Comparison

| | Cost | Licence | Effort | Fidelity | PR status | Maintenance | Works under hold |
|---|---|---|---|---|---|---|---|
| A self-hosted runner | $0 (charge postponed) | MIT | ~1 h | High | Real checks | Low-Med | **Unverified, likely no** |
| B act | $0 | MIT | 2-4 h | Medium | None | Medium | Yes |
| C local script and parity | $0 | Ours | ~1 day | High (same file) | Optional, self-attested | Low | Yes |
| D Woodpecker / Gitea / Forgejo | $0 plus host and endpoint | Apache / MIT / **GPL** | 1-2 days | Med (rewrite or act-like) | Woodpecker: yes | High | Yes |
| E Dagger | $0 | Apache-2.0 | 3-5 days | Rewrite | None | Medium | Yes |
| F go public | $0 | n/a | Clicks plus audit days | Exact | Real, requirable | None | Unverified |

## 5. Recommendation

**Primary: C.** It is the only option that works regardless of GitHub billing, fixes the existing
gate drift by construction, needs no new service or licence decision, and is reused unchanged by A, B
and F. **We give up** independent verification (results are self-attested) and enforceable required
checks.

**Fallback and upgrade: A.** Run it on the same script if the billing test shows self-hosted jobs are
dispatched. It adds real check runs at about 1 h of setup. **We give up** the machine's isolation
(root-equivalent Docker socket) and depend on GitHub's billing state and the postponed charge.

**Not recommended:**
- B: use it only as an optional aid for debugging workflow YAML.
- D: high maintenance and an inbound listener; Forgejo is also GPL.
- E: a rewrite for no gain over C.

F is the human's publishing decision, independent of this choice.

### First steps
1. **Human, about 10 min:** test option A.
   - Add a runner under Settings > Actions > Runners > New self-hosted runner.
   - On a scratch branch, push a one-job workflow (`on: push`, `runs-on: self-hosted`, `run: echo ok`).
   - Record whether the job is dispatched or refused with the billing message. The architect logs it in STATUS.
2. **devops-engineer, task W3-CI-1:** create `.github/scripts/ci_local.sh` with the subcommands above and `--sha`. Convert the `ci.yml` and `helm.yml` `run:` steps to call it. Verify:
   - `.github/scripts/ci_local.sh all --sha HEAD` exits 0 and prints PASS per job with real pytest summary lines.
   - `uv run pytest .github/scripts/test_ci_parity.py -q` passes.
   - Adding a raw `run: echo x` to `ci.yml` makes the parity test fail.
3. **devops-engineer:** add a `pre-push` hook to `.pre-commit-config.yaml` running `ci_local.sh fast`. Install it with `uv run pre-commit install --hook-type pre-push`.
4. **Lead:** reduce the gate skill to `ci_local.sh all --sha <sha>`, plus the push rules.
5. **Human, a credential decision:** optionally create a fine-grained token (this repo only, commit statuses write), held in an environment variable outside the repo, so `--report` posts `local/<job>` statuses.
6. **If step 1 shows dispatch:** devops-engineer sets `runs-on: ${{ vars.TB_RUNNER || 'ubuntu-24.04' }}`, and the human sets the repo variable `TB_RUNNER` to the runner's label.

## 6. Sources
- [GitHub docs: Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions)
- [Techzine: GitHub delays paid self-hosting of runners](https://www.techzine.eu/news/devops/137396/github-bends-to-criticism-and-delays-paid-self-hosting-of-runners/)
- [WZ-IT: Actions pricing change 2026](https://wz-it.com/en/blog/github-actions-pricing-change-2026-open-source-alternatives/)
- [devactivity: billing lock blocked hosted and self-hosted jobs](https://devactivity.com/posts/development-integrations/when-your-ci-cd-grinds-to-a-halt-lessons-from-a-github-actions-billing-lock)
- [GitHub community discussion 166876](https://github.com/orgs/community/discussions/166876)
- [nektos/act](https://github.com/nektos/act), [act releases](https://github.com/nektos/act/releases), [act runners](https://nektosact.com/usage/runners.html), [act not supported](https://nektosact.com/not_supported.html), [act usage](https://nektosact.com/usage/index.html)
- [Woodpecker](https://github.com/woodpecker-ci/woodpecker), [Woodpecker GitHub forge](https://woodpecker-ci.org/docs/administration/configuration/forges/github), [Woodpecker releases](https://github.com/woodpecker-ci/woodpecker/releases), [OneUptime: Woodpecker webhook trace](https://oneuptime.com/blog/post/2026-08-11-woodpecker-pipeline-never-starts-after-push/markdown)
- [Gitea](https://github.com/go-gitea/gitea), [Gitea runner](https://gitea.com/gitea/act_runner), [Forgejo runner on pkg.go.dev](https://pkg.go.dev/code.forgejo.org/forgejo/runner/v12)
- [Dagger](https://github.com/dagger/dagger), [Dagger host services](https://docs.dagger.io/manuals/user/host-services), [Atlassian thread on Dagger privileged mode](https://community.atlassian.com/forums/Bitbucket-questions/Please-support-me-to-run-dagger-CI-CD/qaq-p/2445872)
- [GitHub REST: commit statuses](https://docs.github.com/en/rest/commits/statuses), [About protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches), [Rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
