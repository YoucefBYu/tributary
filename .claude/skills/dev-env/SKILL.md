---
name: dev-env
description: "Sandbox and shared-checkout facts for Tributary agents: Docker/dockerd restart, testcontainers naming, uv sync rules, ports, proxy CA for image builds, gh CLI limits. Use when Docker or testcontainers fail, before uv sync/uv lock, before building images, or when spawning receivers in tests."
---

Facts about this environment. Follow them exactly; each one cost a past wave time.

## Docker
- Docker daemon down (`Cannot connect to the Docker daemon`): restart it with `setsid nohup dockerd >/tmp/dockerd.log 2>&1 &`, then confirm with `docker info`. Do not use `systemctl` or `service`.
- Testcontainers and `docker run`: give every container a **unique name** (include your role and task id, e.g. `tb-<role>-<task>-kafka`). Several agents share one daemon.
- **Never stop, remove or prune containers you did not start.** No `docker rm -f $(docker ps -q)` and no `docker system prune`.
- Image builds behind the proxy: set `TB_BUILD_CA_BUNDLE=/root/.ccr/ca-bundle.crt` (e.g. `TB_BUILD_CA_BUNDLE=/root/.ccr/ca-bundle.crt .github/scripts/image_check.sh`; it becomes a `--secret id=ca` build secret). Never disable TLS verification.

## Python and uv
- **Never run a bare `uv sync`**: it drops workspace packages other agents need. Use `uv run --no-sync <cmd>` to run against the current env, or `uv sync --all-packages` when you really need to sync.
- Only **one agent per batch** edits root `pyproject.toml` and `uv.lock` (normally `devops-engineer`). Others message that owner with the exact dependency change and its licence.
- One-off tools without touching the env: `uv run --no-project --with <pkg> python ...`.

## Processes and ports
- Spawned receivers and workers in tests: set `TB_OPS_PORT=0` so the ops/metrics listener binds an ephemeral port and parallel agents do not collide.
- Bind test listeners to `127.0.0.1` and port `0`; read the bound port back.

## Git and GitHub
- `gh` here is api-only: use `gh api <REST path>` (GET by default). `gh pr view`, `gh run list` and similar do not exist.
- Shared checkout: never switch branches, stash, reset or rebase. Use `git -C <path>` for other worktrees.

## Token hygiene for these tools
- `docker logs --tail 200 <name>` rather than full logs; hand long logs to the `scout` subagent.
- `uv run pytest ... -q` and report only the summary line and failing test ids.
