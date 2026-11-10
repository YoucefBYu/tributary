# Phase 1, wave 4: console, hardening, MVP release candidate

- **Status:** DRAFT by `architect`, 2026-11-10, on `wave/1-3` at `aeb0e27`. **Not started: waits for the
  human's go-ahead** and for PR #2 (`wave/1-3`) to merge. The lead reviews and commits this file.
- **Source:** `phase-1.md` "Wave 4: UI and hardening" (W4-1..W4-8, MVP exit criteria), the items wave 3
  carried (`wave-3.md` sections 4.2 and 6), and the open human items (STATUS).
- **Constitution:** `CLAUDE.md` and ADR-001. No task here changes an invariant.

---

## 1. Goals

1. **The console (ADR-001 roadmap item 11).** A web UI on the `openapi/v0` contract: dashboard, message
   browser with PHI masked by default and an audited reveal, channel view, deploy and rollback.
2. **Close what wave 3 deferred** where it is cheap and contract-ready: replay cancel, audited `401`/`403`,
   the `received`/`sent` residuals in stats, and the W3-30 follow-ups. Destination-scoped reprocess is
   **designed** this wave (ADR) and built only if the human signs H7.
3. **MVP release candidate:** signed images and chart with an SBOM and licence gate (W4-7), measured
   numbers (W4-6), and docs a new user can follow in under 15 minutes (W4-8).
4. **CI that cannot drift:** GitHub Actions runs again because the repo is public. `ci_local.sh` stays
   the single source of the commands (parity test), and the checks become **required** on `main`.

### 1.1 Scope decisions (architect, recorded so they are not relitigated)

- **Mirth/OIE importer and `mirth_compat` shim: NOT in wave 4.** They are Phase 2 (`phase-2.md`
  P2.2-4, owner `migration-engineer`), and no Phase 1 exit criterion needs them. Phase 0 S6 is their
  input. **Give up:** no migration story in the MVP release candidate; the docs say so.
- **Kubernetes operator (kopf): NOT in wave 4.** Phase 2 (`phase-2.md` P2.1-3). The Helm chart (W3-8)
  is the Phase 1 deployment path. **Give up:** no drift reconciliation of Kubernetes objects beyond
  Helm and the W3-5 channel reconciler.
- **KEDA scaling IS in scope as a verify, not new design.** The chart renders `ScaledObject`s on lag
  (W3-8) under the verified `lag_reader` role. What has never been shown is a scale-out on real lag
  (W4-14).
- The human may override any of these (section 7, Q6).

---

## 2. Tasks

Verify lines are exact. "Same command" means `.github/scripts/ci_local.sh <job>` for the named job.

### 2.1 From `phase-1.md` (amended where wave 3 changed the ground)

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W4-1 | `ui-engineer` | `ui/`: React, TypeScript, Vite, `pnpm`; API client **generated** from `contracts/openapi/v0/openapi.json` (no hand-written types for API shapes); OIDC authorization-code + PKCE login against the D9 lite issuer; token held in memory only (no `localStorage`), sent as `Authorization: Bearer` (ADR-009 D1). A CI check regenerates the client and fails on diff | W3-1a/b (done) | `pnpm --dir ui lint && pnpm --dir ui typecheck && pnpm --dir ui test --run` exit 0; `.github/scripts/ci_local.sh ui` PASS; a test asserts no token is written to `localStorage` or `sessionStorage` |
| W4-2 | `ui-engineer` | Dashboard: channel list with deployed version per environment, `getChannelStats`, live events via a **fetch-based** SSE reader (EventSource cannot send the header; `openapi/v0` 3.16), `reset` handled by a "gap, search to fill" notice. Lag is a Grafana link, not an API value (3.15) | W4-1 | Playwright against Compose: a synthetic message sent over MLLP appears in the live view within 10 s; a forced `reset` shows the gap notice |
| W4-3 | `ui-engineer` | Message browser: search, timeline, destination statuses, content per stage (**masked by default**; reveal calls `getMessageContent`, which is the audited operation; no client cache, honours `no-store`), stage diff, reprocess, replay with the `allow_reorder` prompt on per-key channels | W4-1; W4-9 for replay cancel | Playwright: viewer, developer and auditor see no reveal control and get `403` on the content route; operator reveal produces exactly one `message.content.view` record on `_tb.dev.audit` and one row in `audit_log`; no payload byte in the DOM before reveal |
| W4-4 | `ui-engineer` | Channel view: spec diagram from the stored document (source, steps, destinations), version history, deploy and rollback with `expected_current_version`, rollback default target as `openapi/v0` 3.3 states | W4-1 | Playwright deploy flow: deploy v2, the dashboard shows v2 on `tb_spec_version` within 30 s, roll back, v1 again |
| W4-5 | `control-plane-engineer` | API gaps the UI finds, through contract proposals to `architect`; per-role authorization test matrix over **every** operation in `openapi.json` | W4-1 | `uv run pytest packages/tributary-api -k role_matrix -q`: one case per (operation, role), each `2xx` or `403` exactly as `x-tributary-roles` says; a test fails if an operation lacks a case |
| W4-6 | `qa-engineer` | Chaos and performance on 3 brokers. **Amended:** on the free RF=3 cluster (W3-33a/b) for correctness and ratios; absolute numbers on dedicated capacity only if the human approves spend (Q5) | W3-33a/b | Report `docs/spikes/wave-4-mvp-numbers.md`: ACK p99, sustained msg/s, end-to-end p50/p99 against ADR-001 targets, method, environment, raw rows; chaos suite 0 ACKed lost, 0 order violations, fault fired in every run |
| W4-7 | `devops-engineer` | Release pipeline: versioned images and chart, CycloneDX SBOM per image, keyless signing (Sigstore `cosign`, licence checked first), `licence_check.py` gate, SBOM **file-content** scan for bundled licence files (STATUS risk "licence data bundled inside a package"). Nothing is pushed to a registry until the human decides release (Q4) | W3-8, W3-13 | `ci_local.sh supply-chain` and `image-check` PASS; `cosign verify` succeeds on a locally built image; a fixture wheel carrying a GPL file under a non-`cacert.pem` name fails the scan |
| W4-8 | `architect` | Docs: quickstart, concepts, channel-spec reference, operations guide (retention, custody checklist, TLS modes, replay limits). ADR-004 exists; ADR-005 (bundles and trust model) drafted | W4-6 for numbers | A person who has not seen the repo follows the quickstart on a clean Linux host to a running channel and a found message; time recorded (MVP criterion 1) |

### 2.2 Carried from wave 3

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W4-9 | `architect`, then `control-plane-engineer` | **Replay cancel** (ruled 2026-10-13, `openapi/v0` README 6): `POST .../replays/{replay_id}/cancel`, status `cancelled`, a PostgreSQL flag the running replica polls between flushes; idempotent; `channel.replay` audit phase `cancelled` with the acknowledged count. Contract first (`openapi/v0` 0.3.0, `audit/v0` additive) | none | `validate.py` N / 0 with new examples; integration: a replay over 50k records cancelled mid-run stops within one flush interval, ends `cancelled` with `republished` equal to what `raw` gained, and a second cancel is `200` with no change |
| W4-10 | `architect`, then `control-plane-engineer` | **Audit refused requests** (`audit/v0` rule 8, today counted only): a new action `request.refused` for `401`/`403` on PHI-content operations only, carrying route **template**, method, status and `actor` when a valid token gave one; never the token, claims or path values. Best effort, never fail closed (a refusal already did nothing). Rate-limited per subject so a token flood cannot fill `audit_log` | none | `validate.py` N / 0; integration: a viewer's `GET .../content` yields `403` and one `request.refused` record with no path value; 1,000 refused requests in 10 s from one subject yield at most the configured cap plus one `suppressed` count record |
| W4-11 | `archive-engineer` | **Stats residuals:** `channel_stats` counts **distinct messages** per stage for `sent` (absorbs the accepted attempt over-count, `events/v0` 2 field table) and excludes `tb-parent-id` records from latency (`received.ts` is consume time for re-injections, `events/v0` 2.1). `audit_log` assertion added to the archive e2e (W3-9b finding) | none | `uv run pytest -m integration packages/tributary-archiver -k "stats and (distinct or parent)" -q`: a message that succeeded on try 3, then redelivered after a crash, counts once in `sent`; a replayed record is absent from latency percentiles |
| W4-12 | `archive-engineer` | **W3-30 follow-ups:** the `unsupported_version` and `invalid` alerts shown firing on a real scrape (as `partition_halted` was); the tamper-check alert shown firing on a forged duplicate in `audit_log`; the `retention` read-only `GET` checked against `archive_settings` after a decrease with the flag | W3-30 (done) | Integration plus a Compose scrape: each alert's expression returns a series after its fault is injected and none before (`assert_fault_fired`) |
| W4-13 | `architect` | **Destination-scoped reprocess: design only.** ADR-003 amendment for the two re-entry points (`wave-3.md` 2.6 H5): `out.<dest>` for loader exhaustion, an additive `raw` header for destination-step failures; the `reprocessor` grants it needs. **Build only after the human signs H7** (Q1) | none | ADR merged as Proposed; `security-reviewer` read it; grants listed as a `topic-defaults.json` diff, not applied |
| W4-14 | `devops-engineer` | **KEDA on real lag:** a Compose-free kind cluster run where `raw` lag on one channel scales the worker StatefulSet from 1 to N and back, under the `lag_reader` principal alone | W3-8 | Script plus log in `docs/spikes/wave-4-keda.md`: replicas observed 1 → N → 1, lag below threshold within the documented window, no rebalance loop (static membership ids from pod names) |

### 2.3 CI and governance

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W4-15 | `devops-engineer` | **CI on public GitHub Actions:** every job green on hosted runners through `ci_local.sh <job>` (parity test unchanged); fork pull requests run without secrets and need approval; **no self-hosted runner is ever attached** to the public repo (`ci-alternatives.md` A, risks). Required checks configured on `main` once the human sets branch protection (Q3) | none | A PR from a branch shows every `ci.yml`/`helm.yml` job as a check; `uv run pytest .github/scripts/test_ci_parity.py -q` passes; repository settings screenshot or `gh api repos/{owner}/{repo}/actions/permissions` output recorded in STATUS |
| W4-16 | `devops-engineer` | **Post-publication audit** (the repo went public before the `ci-alternatives.md` F audit was recorded as done): full-history secret scan and PHI scan, confirm `samples/mirth/` never entered history, `SECURITY.md` with a private report channel | none | Scan commands and their summary lines in STATUS; `git log --all -- samples/mirth` empty; `SECURITY.md` present. **A finding is escalated to the human the same day**; nothing is rewritten in shared history without the human |

---

## 3. Execution sequence

Seats per batch are capped at six. `security-reviewer` and `code-reviewer` run as subagents after each
task. Priority: blocks exit, then security, then contract, then deferrable.

### Batch 1 — small and unblocked (every dependency exists)

| Order | ID | Owner | Why now |
|---|---|---|---|
| 1 | W4-16 | `devops-engineer` | Security: the repo is already public |
| 2 | W4-1 | `ui-engineer` | Blocks W4-2..W4-4 and MVP criteria 1 and 5 |
| 3 | W4-9 and W4-10 contracts | `architect` | Contract-first for W4-3 and the audit change |
| 4 | W4-11 | `archive-engineer` | Small, self-contained, makes the dashboard's numbers honest |

### Batch 2 — after W4-1 and the W4-9/W4-10 contracts

W4-2, then W4-4 (`ui-engineer`); W4-5 role matrix and the W4-9, W4-10 builds (`control-plane-engineer`);
W4-15 (`devops-engineer`, after Q3 for the required-checks part); W4-12 (`archive-engineer`); W4-13
(`architect`).

### Batch 3 — after W4-2 and W4-9

W4-3 (`ui-engineer`); W4-6 (`qa-engineer`); W4-7 and W4-14 (`devops-engineer`).

### Batch 4 — release candidate

W4-8 (`architect`, after W4-6); `security-reviewer` on the UI (token handling, reveal, CSP) and the
release pipeline; `code-reviewer` on the wave diff; architect gate report; human sign-off (Q4).

---

## 4. Exit criteria

The MVP criteria of `phase-1.md`, verbatim, with the reading this wave is gated on.

| # | Criterion | What counts |
|---|---|---|
| 1 | Time to first working channel on the lite stack is under 15 minutes for someone following the quickstart | W4-8: a person who has not worked on the repo, clean Linux host, timer from `git clone` to a message found in the console; time and host recorded. Lite holds synthetic data only (`topics/v0` 3.8 rule 4) |
| 2 | Chaos suite: zero acknowledged-message loss; ordering preserved on ordered channels; data plane unaffected by control-plane or archive outages | W4-6 on the RF=3 free cluster plus wave 3's criterion 2 result: 0 ACKed lost, 0 order violations, every fault shown to have fired, mutation controls red |
| 3 | Measured ACK p99, sustained msg/s and end-to-end latency reported against the ADR targets | W4-6 report with raw rows. Free-cluster numbers are labelled as shared-runner ratios (`wave-3.md` 2.7 D5); absolute claims only from dedicated capacity (Q5) |
| 4 | `security-reviewer` and `code-reviewer` have no unresolved Critical or High findings | Both reviews committed under `docs/` (wave 3 lesson: a relayed pass is not a citable file) |
| 5 | Docs let a new user install, build a channel, test it, deploy it and find a message in the browser | W4-8 quickstart walk-through, plus W4-3 and W4-4 Playwright flows green |
| 6 | Human sign-off and a decision on public release | Q4. Release means publishing images, chart and a tag; the repo being public is not a release |

**Also gate-tracked:** `ci_local.sh all` green on the release-candidate SHA and the same jobs green on
GitHub Actions; no image published before W4-7 and W3-13 pass; K2/H7 recorded (signed or explicitly
deferred by the human).

---

## 5. Contracts this wave

Set 1.9, additive unless an ADR says otherwise: `openapi/v0` 0.3.0 (replay cancel; refused-request audit
has no API surface), `audit/v0` (`cancelled` phase, `request.refused` action), `observability/v0` only if
W4-10's cap needs a counter. Contract changes during the wave: message the architect with the exact diff
and the reason. Every count is a prediction until `qa-engineer` or the lead measures it.

---

## 6. Carried, not scheduled

| Item | Owner | Why not scheduled |
|---|---|---|
| Mirth/OIE importer, `mirth_compat` | `migration-engineer` | Phase 2 P2.2-4 (1.1) |
| Kubernetes operator | `devops-engineer` | Phase 2 P2.1-3 (1.1) |
| Destination-scoped reprocess **build** | `archive-engineer`, `platform-engineer` | Waits on H7 (Q1); W4-13 designs it |
| Claim-check fetch through the API | `architect` | Custody of bucket credentials in the API (ADR-004 H1, A3) |
| Operator clear or skip for a halted partition | none | Needs a contract and an audit action first |
| Environment- and channel-scoped roles | `architect` | ADR-001 roadmap item 13; roles stay global (ADR-009 D14 give-up) |
| `_hmac` identifier search | `architect` | Deferred (ADR-004 H3) until per-patient search is scheduled |
| W2-2 item 4, FHIR R4 schema validation | `healthcare-standards-engineer` | Waits on the network allow-list (Q7) |

---

## 7. Open decisions — human only

| # | Decision | Blocks | Default until answered |
|---|---|---|---|
| Q0 | **Go-ahead for wave 4**, after PR #2 (`wave/1-3`) merges | Everything | Not started |
| Q1 | **ADR-004 H7 / K2:** sign the Kafka role model, including the content archiver reading every channel's PHI and the `reprocessor` READ on `raw` | W4-13 build; MVP gate record | Roles as published; W4-13 design only |
| Q2 | **`LICENSE` file:** the repo has none. Apache-2.0 is stated (CLAUDE.md invariant 10), but adding the file, the copyright holder line and any `NOTICE` are governance (ADR-006, open) | Public use of the code; W4-7 SBOM licence field | No file; packages declare Apache-2.0 in metadata |
| Q3 | **CI governance now that the repo is public:** enable branch protection with required checks on `main`; fork-PR approval policy; whether the optional `local/<job>` statuses token (`ci-alternatives.md` C) is still wanted (it is not needed now that hosted runners run). **The self-hosted runner (option A) must not be attached to a public repo**, so that test is withdrawn | W4-15 required checks | Hosted runners, checks not required |
| Q4 | **Release decision** (MVP criterion 6): publish images, chart and a tag; project name and trademark check of the codename, which is now public | MVP exit | Nothing published |
| Q5 | **Dedicated capacity for absolute numbers** (W4-6; cloud spend, was 7.6 / W4-6 in wave 3) | Absolute claims in MVP criterion 3 | Free cluster, ratios only |
| Q6 | **Scope:** confirm importer and operator stay in Phase 2 (1.1) | none | As 1.1 |
| Q7 | Network allow-list for `hl7.org` (carried 7.5) | W2-2 item 4 | Golden files unchecked against R4 |
| Q8 | ADR-004 **H6 erasure** (delete, crypto-shred, or neither) | Operations guide section on erasure (W4-8) | Documented as undecided |
