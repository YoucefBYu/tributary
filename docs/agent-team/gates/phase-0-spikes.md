# Gate report: Phase 0 (spikes)

**GO WITH CONDITIONS — SIGNED by the human on 2026-09-22.** All four blocking conditions were met; the
conditions that remain are the Phase 1 requirement table in section 7.1 and three non-blocking open
items. **Signing this gate does not start Phase 1: that needs the human's separate go-ahead.**

Compiled by `architect`, 2026-09-21, from `docs/agent-team/phases/phase-0.md` exit criteria.
Technical verdict behind it: `docs/spikes/PHASE-0-DECISION.md` (**GO WITH CHANGES**).
Branch `wave/0`. **The architect has no shell: every test result below was run and reported by the
lead and is attributed as such, not verified by me.**

---

## 1. Findings

### 1.1 Reviewer findings

| Reviewer | Scope | Result |
|---|---|---|
| `code-reviewer` | cumulative `wave/0` diff, `641e8c9..HEAD`, read-only, nothing run | **No Critical. 1 High, 6 Medium, 3 Low** — below |
| `security-reviewer` | Phase 0 wave, source-based, read-only; **no scanners available**; contracts v0 excluded (reviewed separately) | **No Critical. 2 High, 5 Medium, 4 Low** — below. Clean on the things that matter most: no PHI and no secrets in tracked files, the 10k corpus and all fixtures verified `TRIBUTARY-SYNTH`, tracked `docs/spikes/data` holds counts, latencies, ids and broker logs only, MLLP de-framing sound, invariant 6 upheld in logs |
| `code-reviewer` | Phase 0 evidence (S2, S3, S5, S4 harnesses) | Closed earlier in the wave: no Critical, nothing UNSUPPORTED, one LOW open (`docs/spikes/reviews/phase-0-evidence-review.md`) |

#### High

Both reviewers are counted together. **High 1 was found independently by both** — one finding, two
reviewers, which is why it leads the conditions.

| # | Finding | Status |
|---|---|---|
| **1** | **The root `.gitignore` was damaged by commit `3fb0049`** — 31 lines cut to 2. HEAD ignores only `runs/` and `__pycache__/`. It no longer ignores `samples/mirth/*` (**which CLAUDE.md mandates and which is meant to hold sensitive anonymized Mirth exports**), `.env`, `.env.*`, `secrets/`, `.claude/settings.local.json`, `benchmarks/results/`, `spikes/**/out/`, `.venv/`, the tool caches, `dist/`, `build/`, `*.egg-info`, `node_modules/` and `ui/dist/`. `security-reviewer` verified the current behaviour rather than inferring it: `.env`, `secrets/x`, `samples/mirth/foo.xml` and `benchmarks/results/*` are **not** ignored at HEAD. Untracked and now unignored: `.claude/settings.local.json`, `benchmarks/results/` (the generated corpus), `.claude/agent-memory/`, and a root `Dockerfile` of unknown origin. **A single `git add -A` would commit credentials or a sensitive Mirth export, and the Write/Edit content hook does not run on commits** | **FIXED 2026-09-22**, human commit `325cef1` (31 lines restored, stray root `Dockerfile` removed; lead verified `samples/mirth/*`, `.env`, `secrets/` and `benchmarks/results/` are ignored again). Corroborated a third way while it was open: S2 follow-up 4 reported the damage from the other direction — its raw run files are missing from git because of it (my G6) |
| **2** | **Unauthenticated, all-interfaces spike data plane.** `spikes/common/docker-compose.yml:41` runs `PLAINTEXT` listeners with no authorizer; lines 67, 79 and 91 publish 19092/29092/39092 on `0.0.0.0`; `spikes/S2-mllp-kafka/server.py:41` defaults the MLLP listener to `0.0.0.0` with no peer authentication; lines 102-111 run `kafka-ui` on 8080 with `DYNAMIC_CONFIG_ENABLED` and no login. While the cluster is up, **any host on the LAN has read, write and admin on it**, can inject into spike topics, and can forge S3 replies — `sync_server.py:112` accepts any reply matching a pending correlation id in broadcast mode | **FIXED 2026-09-21/22.** `e553fd7` (`connector-engineer`): MLLP listeners default to `127.0.0.1`. `71364c7` (`platform-engineer`): all four published ports bound to `127.0.0.1`, `DYNAMIC_CONFIG_ENABLED` removed. The lead had already stopped the cluster (`docker compose down`, 2026-09-21), so the live exposure closed before the config fix landed. The auth/TLS requirement itself remains a Phase 1 requirement, already specified in `contracts/topics/v0` 3.4 |

**On High 2 and the integrity of the Phase 0 evidence** — the part that touches my synthesis rather
than the infrastructure. The exposure was real for the duration of the runs, and a LAN attacker could
in principle have injected records or forged replies, which would falsify measurements silently. My
assessment: **no reason to doubt the numbers, and I am not going to pretend the window did not exist.**
Three reasons the evidence holds. The runs were on a single developer laptop, not a shared network
service. Each scenario creates fresh topics with run-unique ids and compares produced ids against the
sink set exactly, so injected records would appear as unexplained extras or loss rather than as clean
passes — and S2's and S5's loss checks did come out clean. And the failure modes an attacker would have
had to fake (a 12-message replay that preserves order, 50 records inside an open transaction, exactly 2
of 6 partitions moving) are more specific than anything random interference produces. **What I cannot
do is prove absence of tampering**, and no Phase 0 artefact can. The Linux re-run on a dedicated
cluster (human decision #7) is what closes this properly, and it is another reason to do it before any
number is published.

**Why this is the most serious finding in the gate, despite nothing being leaked yet:** no secrets are
tracked and none ever were (`spikes/S2-mllp-kafka/certs/*.pem` are covered by S2's own `.gitignore` and
were never in history), so this is not an incident. It is a **removed control** on exactly the paths
CLAUDE.md names as sensitive — the Mirth export directory and the secrets patterns — at the moment when
the human is being asked to supply real anonymized Mirth exports (decision #9). The next `git add -A`
by anyone, with a corpus of exports on disk, is the failure mode. Restore before Phase 1, not after.

**Process defect, same commit:** `3fb0049` bundled the `.gitignore` deletion with an unrelated docs fix
and a `server.py` edit. CLAUDE.md requires committing only files you own, and `.gitignore` belongs to
`devops-engineer`. A one-line destructive change rode in on an unrelated commit, which is why it was
not noticed for a whole wave.

#### Medium — `code-reviewer`

| # | Finding | Status |
|---|---|---|
| CR-2 | **`tests/conformance/test_contracts_v0.py:53-58, 84-90` only require an invalid example to produce *some* error**, while the manifest says it fails for "exactly the documented violation". An example that fails for the wrong reason still passes — **including the five PHI guards**. `qa-engineer` verified each reason by inspection, not by the test | **FIXED 2026-09-22.** `qa-engineer` measured the expected `(json_path, keyword)` pair for every invalid example against a real validator run and made the test assert it; I merged all 46 pairs into the three manifests I own (`83a1fd2`). **The suite now reports 76 passed.** It had directly weakened a guard I cited as evidence all wave |
| 3 | **`spikes/common/src/tbcommon/envelope.py` drifts from `contracts/envelope/v0` and nothing validates it against the schema**: `to_headers` always emits `tb-attempt` "0", the stage goes out as `tb-x-stage` rather than the registered `tb-stage`, no `tb-destination` or `tb-retry-not-before`, and the docstring still says "future `contracts/`" | **OPEN, Phase 1 condition.** Same defect as my G1/section 6. The reviewer's sharpest point: **the spikes never exercised the real envelope contract** |
| 4 | **`spikes/S2-mllp-kafka/server.py:96` uses `uuid4()` for `tb-msg-id`; `spikes/S3-sync-reply/sync_server.py:124,131` uses `uuid4().bytes` (raw bytes) and `uuid4().hex`** (unhyphenated). The contract requires a UUIDv7 message id and canonical UUID text. Does not affect the spike numbers; S3's raw topic would fail conformance | **Spike code FIXED 2026-09-21** (`e553fd7`: canonical UUID ids in S2/S3). The **Phase 1 condition stands** regardless — the point was never the spike numbers but that nothing validated an emitter against the schema, which requirement 10 fixes. Independently found as my G1; two reviewers converging is why it is an acceptance criterion rather than a cleanup note |
| 5 | **`benchmarks/gen.py:71`: invalid escape `\&` in an f-string.** SyntaxWarning on 3.12, `SyntaxError` under `-W error` and on a future Python | **OPEN, not blocking.** One-line fix, owner `qa-engineer`: make the HL7 encoding-characters literal a raw string (`r"MSH\|^~\&\|"`) or escape it as `\\&`. It is in the corpus generator, so it will outlive the spikes |
| 6 | **No unit test that AA is withheld until the publish future resolves** — `test_server.py` covers only the failure-to-`AR` case. Invariant 1 rests on the broker chaos runs | **FIXED 2026-09-21** (`a5df2c7`): the AA-withheld-until-publish-resolves unit test now exists. Invariant 1 no longer rests on the 26 chaos trials alone, which is the cheapest evidence improvement in the whole wave |
| 7 | **Ownership cannot be verified from history** — every commit has the same git author, so roles cannot be attributed. `.gitignore` (devops-owned) was edited in a spike commit, and `docs/spikes/**` (architect-owned) was written by the spike owners | **Explained, partially accepted.** See below |

**On finding 7, the ownership question, answered plainly.** Single git author is an artefact of the
agent-team setup: all agents commit through one human git identity, so `git log` cannot attribute
roles and this is not fixable within the wave. Of the two concrete instances:

- **`docs/spikes/**` written by spike owners: coordinated and correct.** The lead instructed each spike
  owner to file their report there, and the hook permitted it. As the owner of `docs/spikes/` I confirm
  this was intended, and I have since reviewed every report and stamped each with an acceptance verdict.
  Ownership of the directory did what it is for: one role decides what the reports *mean*.
- **`.gitignore` edited in a spike commit: a real violation**, and it caused High 1. This is the
  finding, not the authorship ambiguity.

#### Medium — `security-reviewer`

| # | Finding | Status |
|---|---|---|
| SR-3 | **Queues are bounded in items, not bytes.** Worst case the S2 server retains 512 × 256 × 1 MB, and S3 1024 × 256 | **OPEN, Phase 1 requirement.** A per-item bound is not a memory bound when the item is a 1 MB message. Phase 1 receivers need a **global byte budget**, and it interacts with claim-check (untested) since 1 MB is the inline ceiling |
| SR-4 | **`orjson` metadata declares `MPL-2.0 AND (Apache-2.0 OR MIT)`**, while `spikes/common/pyproject.toml:8` annotates it as Apache-2.0/MIT. `orjson` is a **shipping runtime** dependency, so invariant 10's MPL clause applies. `pathspec` (MPL-2.0, build-time via hatchling) is also in the tree | **Human-approved 2026-09-22; annotation corrected in `71364c7`.** It corrected my own invariant-10 assessment: I had accepted the pyproject annotation, which is precisely the risk my G5 named, now realised. See section 5.1, including the build-backend inventory gap `platform-engineer` then found |
| SR-5 | **`xml.etree` from the stdlib parses untrusted channel XML in S6** (billion-laughs / entity expansion) | **OPEN, Phase 1 requirement before any upload path exists.** Use `defusedxml` plus a size cap. Today the analyzer only runs on files a human placed locally, so it is not reachable remotely — but the importer's whole purpose is to consume files from outside |
| SR-6 | **S2 `Dockerfile`: root user, base image pinned by tag not digest, unpinned and unhashed pip installs.** `apache/kafka:4.0.0` and `kafbat/kafka-ui:v1.1.0` are tag-pinned only | **OPEN, Phase 1 requirement.** Digest-pin and run as non-root in the product images; supply-chain integrity for the default distribution is not a spike concern but is a release blocker |
| SR-7 | **S1 has no `pyproject` or lock file** — it runs via `uv run --with`, so the measured versions are unrecorded. From cache: `hl7apy` 1.3.5 (MIT), `hl7` 0.4.5 (BSD) | **OPEN, affects reproducibility of the S1 numbers.** Not a correctness problem for the verdict (margins are 12x-182x) but the Linux re-run must pin and record versions, or the comparison is not like-for-like |

#### Low — `code-reviewer`

| # | Finding | Status |
|---|---|---|
| CR-8 | `docs/spikes/data/S2/dry.json` came from the non-compliant dry-run publisher (ACK without commit) and should not be mistaken for evidence | **FIXED 2026-09-21** (`e553fd7`): renamed to `dry-not-evidence.json`. I had also added `docs/spikes/data/S2/README.md` documenting every raw file, since I could not rename without a shell; that README is updated for the new name and remains the durable warning |
| CR-9 | `benchmarks/harness.py:38` divides by zero on an empty corpus (cosmetic) | OPEN, owner `qa-engineer`. Cosmetic |
| CR-10 | `spikes/S2-mllp-kafka/server.py:37` defaults to `0.0.0.0` with no auth; the **product** default needs an explicit decision | **OPEN, Phase 1 design requirement**, and `security-reviewer` High 2 raises the same default as a live exposure. ADR-001 section 8 already requires TLS on all listeners including MLLP; what it does not say is that the bind address must be explicit. Phase 1 receiver: no wildcard bind default, and refuse to start a plaintext listener on a non-loopback address |

#### Low — `security-reviewer`

| # | Finding | Status |
|---|---|---|
| SR-8 | **No mTLS on the MLLP TLS listener** — the server does not authenticate the client. The client side verifies correctly | **OPEN, Phase 1 requirement.** Client-cert auth for MLLP; ADR-001 section 8 says mTLS, the spike only did server-side TLS |
| SR-9 | **`hypothesis` (MPL-2.0) had no recorded approval, and `ADR-001:92` listed MPL as generally allowed — looser than CLAUDE.md** | **Both addressed.** The approval is recorded in section 5, invariant 10; ADR-001 2.3 is corrected to state that MPL is not in the default allow-list and is approval-gated, with the three MPL items in the tree named. CLAUDE.md governs and was not edited |
| SR-10 | `echo_worker.py:77` calls `int()` on an untrusted header — a malformed `tb-x-reply-partition` crash-loops the worker | **Spike code FIXED 2026-09-22** (`71364c7`, malformed-header fix). **Requirement 13 stands** for Phase 1: header parsing must be total for *every* integer header in the envelope contract (`tb-attempt`, `tb-channel-version`), not just the one the reviewer found |
| SR-11 | `sync_server.py:151` re-frames the reply without validating it; an embedded `\x1c\x0d` could inject a frame boundary. Not reachable today, but it is an undocumented assumption | **Spike code FIXED 2026-09-21** (`e553fd7`, frame-byte rejection on worker replies). **Requirement 14 stands** for the product reply path. It was worth fixing early: with reply forgery possible on the then-open cluster, an unvalidated re-frame was the second half of a chain |

**Reviewer coverage gaps, stated so they are not read as passes.** Both reviews were read-only and
**nothing was executed** — no linters, no tests, no scanners.

`code-reviewer` did not review: `s4.py` past line 140, `spikes/S1`, `spikes/S6-mirth-import`,
`tests/support/synthetic`, most of `tbcommon` (`admin`, `clients`, `config`, `smoke`), ADR content,
`contracts/topics` and `contracts/events` content, `.claude/hooks`, or per-commit authorship.

`security-reviewer` did not: run any CVE or image scan (**`bandit`, `pip-audit`, `gitleaks`, `trivy`,
`hadolint`, `semgrep` and `syft` are all missing from this environment**), observe live listener, ACL or
TLS behaviour, verify the native libraries bundled in the `confluent-kafka` wheel from artefacts, or do
more than grep `.claude/agent-memory`. Contracts v0 was out of scope here because it had its own two
rounds.

**So the security position of this wave is source-review-only.** Every licence conclusion, including the
MPL ones, rests on declared metadata rather than on a tool; no dependency has been checked for known
vulnerabilities. That is acceptable for a spike wave whose output is documents, and it is **not**
acceptable for Phase 1 — hence the CI conditions in section 7.

**Both reviews are now complete.** An earlier revision of this report listed "`security-reviewer` has
not reviewed the wave diff" as a blocking condition; that is obsolete and has been replaced by the
findings above.

**One correction to the reviewer's context note:** it flags that S3's runner may still record a false
`lost: 7,974` in `s3.jsonl`. That was addressed — the runner now passes `--id-from msh10` (commit
`ad5a655`), the false record is retained **and annotated** in place, and `s3-restart-verify.json`
(7,974 acked, 7,974 in topic, 0 lost) is the source the report quotes. The rerun was not redone, which
the S3 report states.

Already closed in-wave, for context: the two contract security rounds (14 findings — 12 fixed, 1
escalated as unresolved-by-design, 1 referred to the human, none disputed;
`docs/security/contracts-v0-review.md`) and the Phase 0 evidence review (no Critical, nothing
UNSUPPORTED, one LOW open; `docs/spikes/reviews/phase-0-evidence-review.md`).

### 1.2 Findings raised by this gate audit

Mine, from the invariant and contract-conformance audit in sections 5 and 6. None is Critical.
**G1 and `code-reviewer`'s Medium 3 and 4 are the same defect, found independently from different
angles** — I came at it from "does the contract match what the code emits", the reviewer from "does the
code match the contract". G6 and the reviewer's High 1 are the same damaged `.gitignore` seen from two
directions: it lost the spikes' raw run data, and it lost the sensitive-path controls. Convergence is
why both are Phase 1 conditions rather than notes.

| # | Sev | Finding | Action |
|---|---|---|---|
| G1 | **High** | **Spike code emits envelopes that the published contract would reject.** Concretely, `spikes/S3-sync-reply/sync_server.py:131` produces `tb-msg-id` as **raw UUIDv4 bytes** (`uuid.uuid4().bytes`) — binary, not text, and v4 not v7 — and line 124 mints the correlation id as `uuid.uuid4().hex`, a 32-character unhyphenated string. Both fail `contracts/envelope/v0`; the binary header also breaks invariant 8's "UTF-8 text, never binary" rule and v4 loses the time-ordering `tb-msg-id` is specified for | Not a Phase 0 blocker — spike code is throwaway and predates the contract. **Must be a named Phase 1 task** (ADR-002 follow-up 3, currently scoped to `envelope.py` only; widen it to S2/S3/S5 patterns and to the receiver being written from them) |
| G2 | Medium | **The S5 event emitter would fail `contracts/events/v0`** on at least three required fields: no `v`, no `kind`, and `ts` is an epoch float (`spikes/S5-ordering-retry/src/s5/events.py:59`) rather than RFC 3339 text; it also uses `worker` where the contract says `instance`. Known and recorded in `contracts/CHANGELOG.md`, but no task owns the reconciliation | Add to the Phase 1 runtime task as acceptance criteria. The contract is the target, not the spike |
| G3 | Medium | **S5 topic names do not satisfy the topic grammar.** `spike.s5.<run>.raw` is four segments against `env.channel.kind[.dest]`, and `spike.s5.<run>._tb.events` embeds `_tb.events` as a suffix with an underscore (`spikes/S5-ordering-retry/src/s5/topics.py:39-47`); `topic-name.schema.json` rejects both. By contrast S4's `s4.c0001.raw` conforms | Spike-local, no action in Phase 0. Phase 1 topic creation must go through the contract, and the conformance suite already guards the grammar |
| G4 | Medium | **`ruff check .` as documented in CLAUDE.md does not pass**: 59 findings, and `ruff format --check` reports 44 files would be reformatted (lead-reported). CLAUDE.md's command list applies `ruff` repo-wide while scoping `mypy` to `packages/`, so the documented command is currently untrue | Decide the scope: either exclude throwaway `spikes/` in the ruff config, or fix the findings. A documented command that fails teaches the team to ignore it. Owner: lead / `devops-engineer` (not the architect — config is not mine) |
| G5 | Low | **Licence compliance is asserted from source annotations, not verified by tooling.** No scanner was available in this environment (`pip-audit`, `trivy`, `gitleaks`, `bandit`, `hadolint` all missing, per the security review). `hypothesis` is **MPL-2.0**, which CLAUDE.md permits only "as an unmodified dependency approved by the architect" — see section 5, invariant 10, where I record that approval | Add a licence check to Phase 1 CI. Until then, invariant 10 rests on declarations |
| G6 | Low | **Per-message raw data for S2, S3 and S5 is not in git** — the `runs/` directories are ignored and, per S2 follow-up 4, the root `.gitignore` is damaged. Tracked aggregates in `docs/spikes/data/` plus the report tables are the durable record, and every run is regenerable from a documented command | Accepted for a spike wave; the aggregates are sufficient to audit the claims. Note it against exit criterion 1 |

---

## 2. Checks run

All reported by the lead, on `wave/0` at HEAD. I have no shell and did not reproduce any of them.

| Check | Result |
|---|---|
| `tests/conformance` (contracts v0) | **76 passed** at `83a1fd2`, after the expected-failure assertions landed (CR-2). Was 74 before it; 70 at `ae4d3bd`; 62 on the first run. Envelope 7 valid / 18 invalid, events 8 / 16, topic names 10 / 12, plus the guard and reporting tests. No schema bugs at any point |
| `spikes/common` unit tests | 13 passed |
| S5 fast suite | 50 passed (13 integration deselected) |
| S2 unit tests | 30 passed |
| S6 tests | 7 passed |
| `tests/support` | 1 passed |
| `benchmarks.gen` determinism | Same corpus sha256 (`a0bdd1c4ea082835…`) on two runs — the P0-1 verify line |
| `ruff check` on spikes/tests/benchmarks | **59 findings** — see G4 |
| `ruff format --check` | **44 files would be reformatted** — see G4 |

### 2.1 Checks not run, and why

The lead reports these were not run because **there is nothing to run them against**: no root
`pyproject.toml`, no `packages/`, no `ui/`.

| Check | Status |
|---|---|
| `uv sync` | not run — no root workspace yet |
| `mypy packages` | not run — `packages/` does not exist |
| root `pytest` (fast markers) | not run — no root test config; the per-spike suites above are what exists |
| `pytest -m integration` | not run — testcontainers-based integration suites belong to Phase 1; S5's 13 integration tests were deselected in the fast run and were exercised during the spike itself |
| `pnpm` UI lint / typecheck / test | not run — `ui/` does not exist |

**These are not Phase 0 exit criteria** — Phase 0's output is evidence, not product code, and the
plan's ground rules say spike code is throwaway and must not be imported from `packages/`. They are
recorded here so nobody reads their absence as a pass. **All of them must pass at the Phase 1 wave-1
gate**, where `packages/` and `ui/` exist.

---

## 3. Exit criteria

From `docs/agent-team/phases/phase-0.md` section "Exit criteria". No criterion is marked PASS without
evidence.

| # | Criterion | Evidence | Verdict |
|---|---|---|---|
| 1 | "All **seven** spike reports exist with methodology, raw data and verdicts." | **The plan defines six spikes, S1-S6, not seven** (section 4). All six reports exist, each with question, method, environment, raw results, thresholds, verdict and follow-ups, and each now carries an architect acceptance stamp: `S1-hl7-throughput.md`, `S2-mllp-kafka.md`, `S3-sync-reply.md`, `S4-topic-scale.md`, `S5-ordering-retry.md`, `S6-mirth-import-corpus.md`. Raw **aggregates** are tracked in `docs/spikes/data/`; per-message dumps are not in git (G6). P0-1 and P0-2 filed no reports — correctly, since the plan gives them a command as their verify line, not a report | **PASS** against the six spikes the plan defines. The word "seven" corresponds to nothing in the plan and is a defect in `phase-0.md` — most likely it counted `PHASE-0-DECISION.md` as a seventh document, or an S1b that was never needed. **Not papered over: the phase doc should be corrected rather than this criterion being read as a missing deliverable** |
| 2 | "Each threshold is reported as passed or failed with numbers. No claim without a measurement." | Every plan threshold has a number and an explicit verdict — full table in `PHASE-0-DECISION.md` section 3, reproduced in section 4 below. The two misses are stated as misses: S2's ACK p99 (**NOT MET as measured**, 33-125 ms against 50 ms, cause unresolved) and S4 at 12 partitions per topic (**FAIL**, cluster lost at ~425-450 channels). S6 makes **no** claim: its estimate is labelled a guess, and `PHASE-0-DECISION.md` section 6 lists eleven things Phase 0 did not establish | **PASS** |
| 3 | "`PHASE-0-DECISION.md` has a recommendation, updated risks, and a revised Phase 1 scope if needed." | Recommendation in section 1 (GO WITH CHANGES, with the five reasons it is not a plain GO); risks with the evidence column filled in section 9, including four new risks; revised Phase 1 scope in section 8, seven items; nine human decisions with recommendations in section 7 | **PASS** |
| 4 | "ADR-001 updated, ADR-002 and ADR-003 drafted, contracts v0 published." | ADR-001 updated at 5.3 (partition default 12 → 3), 6.3 (per-core estimate replaced by measurement), 7.1, 7.3 (failure modes, each row marked measured or not), 13 (risk table) and 14 (roadmap). ADR-002 and ADR-003 went past "drafted": every `TODO(Sn)` is filled from a filed report or explicitly recorded as unmeasured (ADR-003 D2, deferred with its reason). Contracts v0 published for envelope, topics and events, with **76** conformance tests passing at `83a1fd2` — each invalid example now asserted to fail at its documented path and keyword — and two security rounds closed | **PASS** |
| 5 | "Human sign-off recorded in `docs/agent-team/STATUS.md`." | **Signed 2026-09-22**, recorded in the STATUS sign-offs table and decisions log: *"Approve orjson, hypothesis and pathspec; record Phase 0 GO WITH CONDITIONS signed"*. The nine memo decisions were recorded on 2026-09-21 (section 8) | **PASS** |

**5 of 5 PASS.** Criterion 1 passes on substance with a documentation discrepancy recorded (the plan says
"seven" where it defines six spikes). Criterion 5 was FAIL (pending) in every earlier revision of this
report and passed only when the human signed — it was never an agent's to close.

---

## 4. Thresholds, with numbers

Condensed from `PHASE-0-DECISION.md` section 3, which carries the evidence ratings quoted verbatim
from the reviewer.

| Threshold | Number | Verdict |
|---|---|---|
| S1: ≥ 200 msg/s/core, parse plus transform | lazy **36,322**, `python-hl7` **2,446**, `hl7apy` **100** (solo host) | **PASS** (12.2x on the conservative candidate); `hl7apy` **FAIL**, off the hot path |
| S2: ACK p99 < 50 ms at 1k msg/s | **33 to 125 ms** across 11 cell runs; 2 under 50 ms | **NOT MET as measured, cause unresolved** |
| S2: zero acknowledged-message loss, 20 leader-kill trials | **0 lost / 20 trials**, 244,710 acknowledged | **MET** ("no loss observed"; small sample bounds per-trial loss only to ~14% at 95%) |
| S2: no ACK while Kafka is unavailable | **0 ACKs in the outage window, 6/6 trials** | **MET** |
| S3: four items to establish (no numeric threshold) | +40 ms p50 at 100 msg/s; 4.6 KB per in-flight request; timeout accurate to ~5 ms; 0 lost across a worker restart with 1,026 timeouts | **ESTABLISHED** against a stand-in worker |
| S4: 500 channels normal, rebalance < 30 s | **3 partitions/topic: 1,000 channels healthy, 100 groups assigned in 4.3 s.** 12 partitions/topic: lost at **~425-450 channels** | **PASS at 3; FAIL at the plan's 12.** Static-member removal 43.8 s is session-timeout-bound, not topic-count-bound |
| S5: per-key order preserved across rebalances and kills | **0 violations** in 2,000 model schedules, 12 cluster scenarios, 13 integration tests, including a 12-message replay | **PASS** |
| S5: duplicate rate reported | **0.60%** / 0.10% / 0% without a fault | **PASS** (reported, not thresholded) |
| S5: commit-after-delivery verified | **0 violations**, 144 sampled through the kill window | **PASS** |
| S6: estimate the share importable with no manual work | **no measurement** — "5 to 20 percent" is a labelled guess | **INCONCLUSIVE** (no corpus) |

---

## 5. Invariant audit, 1-10

Per CLAUDE.md. "Not exercised" is a valid and honest result for a spike wave; it is recorded so Phase 1
does not inherit an assumption.

| # | Invariant | Evidence | Assessment |
|---|---|---|---|
| 1 | **ACK after commit** | **Measured and code-checked.** `spikes/S2-mllp-kafka/server.py`: `enable.idempotence=True`, `acks=all`, and `AA` written only from a successful delivery report; any error, timeout or full queue returns `AR` (lines 75-146). Measured: 0 acknowledged loss in 20/20 leader-kill and 6/6 outage trials, 244,710 messages; 0 ACKs inside the outage window. The contract side was where the real bug was: `min.insync.replicas` and `unclean.leader.election.enable` were set per profile but not per topic, so a topic created from the class defaults satisfied `acks=all` with **one** replica — found by `security-reviewer` (HIGH 5) and fixed by pinning the durability floor in all eight classes | **Upheld.** Best-evidenced invariant in the wave. Caveats: small sample; the lite RF=1 profile **cannot** satisfy it and says so (human decision #3) |
| 2 | **Kafka hops only at durability boundaries** | S5's layout creates exactly `raw`, `out.<dest>`, `retry`, `dlq` and an events topic, and pipeline steps run in-process within the loader (`spikes/S5-ordering-retry/src/s5/topics.py`). `contracts/topics/v0` fixes the classes and the grammar | **Upheld in substance.** No extra hop was introduced anywhere. Physical S5 topic names do not match the grammar (G3) — a naming divergence, not a boundary violation |
| 3 | **The data plane is independent** | **NOT EXERCISED.** No spike ran with PostgreSQL, the API or ClickHouse down, because none of them exists yet; there is no control plane and no `_tb.<env>.channels` producer. Contract-level support only: the compacted channels topic is specified, and `contracts/topics/v0` 3.4 forbids any worker or loader `WRITE` on it — because invariant 3 makes workers trust it completely, so write access there is data-plane-wide RCE | **Not exercised.** Must be a Phase 1 chaos test: kill PostgreSQL and the API, assert message flow continues |
| 4 | **At-least-once, idempotency keyed by `tb-msg-id`** | Partly measured. S5 reports duplicates rather than asserting zero (0.60% after a kill at `commit_every=20`, 0.10% on broker kill, 0% without a fault), and its ordering checker deliberately treats a redelivery as a duplicate and not a reorder — so `1,2,3,1,3,2` passes, which is correct under at-least-once and is exactly why the sink must be idempotent. S3 sharpened it: 1,026 timed-out requests were already committed, so a sender retry duplicates them downstream | **Upheld on the producer side; the consumer side is untested.** No real loader exists, so **no loader idempotency technique has been tested** — the contract requires each to document and test one. ADR-003 follow-up 2 makes it a per-sink integration table in Phase 1 |
| 5 | **Ordering is explicit** | **Measured, strongest positive result of the wave.** 0 order violations across 2,000 randomised fault schedules, 12 cluster fault-injection scenarios and 13 integration tests; order held through a kill that replayed 12 messages; a cooperative joiner moved exactly 2 of 6 partitions with 0 duplicates. Three mutation controls prove the checks can fail, including a deliberately reordering loader. `contracts/topics/v0` closes the silent path: `out.<dest>` must match `raw`'s partition count, or the key-to-partition mapping differs | **Upheld.** Gap: the `retry-topic` policy — the one that does **not** preserve order — was never implemented or measured, so no destination may select it until it is spiked (ADR-003 D2) |
| 6 | **No PHI in logs, metric labels, trace attributes, exception messages or test output** | Strong at contract level, partial at runtime. S5 scans **every** `_tb.events` body per run for `TRIBUTARY-SYNTH`, `MSH\|` and `PID\|`: nothing found in any run. `tbcommon.envelope` never holds the payload by construction. The conformance suite's failure reporter prints `json_path` and `validator` only, never the instance. Five of the fourteen security findings were PHI-surface defects and all are fixed: the record key removed from the validation projection, enforceable `attrs` rules, claim-check URIs constrained, `tb-correlation-id` reduced to a UUID so it cannot carry payload-derived content, and the record-key prohibition widened to URLs, browser storage and sender-facing errors | **Upheld, and independently confirmed.** `security-reviewer` checked the tracked artefacts and the log statements by hand: no PHI and no secrets in tracked files, the 10k corpus and every fixture carry `TRIBUTARY-SYNTH`, `docs/spikes/data` holds counts, latencies, ids and broker logs only, the S2 server logs counters and control ids with fixed ACK text, and the conformance suite is PHI-safe. Remaining gap: the **automated** scan covers event bodies only, no spike scanned its own log output as a test, and the place it will eventually leak (exception text from a parser) has no product code yet. Phase 1: make it a test over logs, not only events |
| 7 | **Channels are declarative; steps are `(message, ctx)`, unit-testable without Kafka** | **Largely not exercised.** There is no channel-spec contract yet (planned for Phase 1 wave 1) and no SDK. One relevant positive: S5's property tests run the **real** `BlockingLoader` against `s5.fakekafka`, an in-process model, with the model's own behaviours pinned by tests so it cannot drift into being easier than Kafka — evidence that the "testable without Kafka" pattern is achievable for the hardest part of the runtime | **Not exercised as an invariant.** The pattern is demonstrated; the declarative spec and the `ctx` seam are Phase 1 |
| 8 | **Envelope: metadata in `tb-*` headers, payload opaque bytes, claim-check over 1 MB** | Headers implemented across all spikes and specified in `contracts/envelope/v0`; no spike inspects a payload to make a routing, retry or ordering decision. Two defects: **S3 writes `tb-msg-id` as raw UUIDv4 bytes**, violating "UTF-8 text, never binary" and the UUIDv7 rule (G1), and **claim-check is entirely untested** — no spike exercised a payload over 1 MB, though it is the largest single PHI store in the design | **Partially upheld.** The header model holds; one concrete violation in spike code, and the over-1-MB path is an untested assumption carried into Phase 1 |
| 9 | **The archive is off the critical path** | Demonstrated in code: `spikes/S5-ordering-retry/src/s5/events.py` emits fire-and-forget, catches `BufferError` and **drops the event rather than the message**, swallows every other exception with a debug log, and calls `poll(0)` in a `finally`. The events contract makes the same trade explicit: events may be lost or duplicated, and consumers must be idempotent on `(message_id, stage, destination, attempt)` | **Upheld by construction.** Not exercised under load: there is no archiver, and whether it keeps up at roughly four events per message is still unmeasured (`_tb.<env>.events` partition count remains a planning value) |
| 10 | **Clean-room and licensing** | S6 states it used public documentation and general format knowledge only, copied nothing from Mirth or OIE, and its element names are flagged as unverified recollection rather than transcription — clean-room upheld. Permissive dependencies: `confluent-kafka` (Apache-2.0), `uvloop` (MIT), `pydantic` (MIT), `python-hl7` 0.4.5 (BSD), `hl7apy` 1.3.5 (MIT), `jsonschema` (MIT). **Three MPL-2.0 items are in the tree**, one of which I previously mis-stated — see the correction below | **Clean-room upheld. Licensing is conditionally upheld and now needs a human**, per the table below |

### 5.1 Invariant 10: correction and the three MPL items

**I got `orjson` wrong in the first version of this report.** I wrote "Apache-2.0/MIT", taking it from
the annotation in `spikes/common/pyproject.toml:8`. `security-reviewer` (SR-4) read the package metadata:
it declares **`MPL-2.0 AND (Apache-2.0 OR MIT)`** — the MPL conjunct is not optional, and `orjson` is a
**shipping runtime** dependency, not a test one. This is exactly the failure mode my own finding G5
predicted ("licence compliance is asserted from source annotations, not verified by tooling") landing on
the very next line of the same table. The annotation should be corrected where it lives, which is
`platform-engineer`'s file.

**Status: HUMAN-APPROVED 2026-09-22.** The human instructed, verbatim: **"Approve orjson, hypothesis and
pathspec; record Phase 0 GO WITH CONDITIONS signed".** All three are approved as **unmodified** MPL-2.0
dependencies. Two limits on that approval, recorded so it is not read wider than it is: it covers **these
three packages, unmodified**, and **any new or modified MPL dependency needs fresh approval**; and the
inventory behind it is incomplete, per the build-backend gap below.

| Package | Licence | Role | Status |
|---|---|---|---|
| `orjson` | `MPL-2.0 AND (Apache-2.0 OR MIT)` | **Shipping runtime** dependency, hot path (ADR-001 6.3) | **Human-approved 2026-09-22**, architect-approved as unmodified. Unvendored, used through its public API; MPL-2.0 is file-level copyleft, so an unmodified dependency does not reach the Apache-2.0 core. It ships in the default distribution, which is why it needed the human |
| `hypothesis` | MPL-2.0 | Test-only | **Human-approved 2026-09-22.** Already named in CLAUDE.md's stack table; not distributed with the runtime |
| `pathspec` | MPL-2.0 | Build-time, via `hatchling` | **Human-approved 2026-09-22.** Build-time only; does not reach a distributed artefact |

**Why this went to the human at all.** CLAUDE.md gives the architect the MPL approval and separately
escalates "a dependency with a non-permissive license" to a human. Those two clauses overlap here, and
the conservative reading was the right one: I recorded the technical approval so work was not blocked,
and the human confirmed the licensing posture. Had it been rejected, `orjson` was the painful one — it is
on the hot path and the alternatives (`ujson`, stdlib `json`) cost throughput that S1 measured as
headroom rather than surplus.

**A gap in this evidence, raised by `platform-engineer`: `pathspec` appears in none of the three spike
lockfiles.** `[build-system] requires` resolves separately from the runtime and dev dependency groups, so
**a licence inventory taken from lockfiles alone misses every build-backend dependency** — `pathspec`
arrived through `hatchling` and was found only because a reviewer read the tree rather than a lock file.
Two consequences, both recorded rather than waved away:

- **`pathspec` may not be the only one.** The MPL list above is "the MPL items we found", not "the MPL
  items that exist". I am not going to imply a complete inventory from an incomplete method.
- **Phase 1 CI requirement (requirement 15):** the licence and SBOM check must cover **build-system
  requirements as well as the locked runtime and dev groups**. A check that reads only `uv.lock` would
  reproduce exactly this blind spot and report a clean result.

This is the same lesson as G5 and SR-4 arriving a third time: every licence conclusion in this wave came
from reading declarations, and each time someone looked somewhere new, the picture changed.

**No invariant needs to change.** That is the same conclusion as `PHASE-0-DECISION.md` and it is
reached here independently, from the code and the contracts rather than from the reports. Invariant 5
was the one at genuine risk and it came out with zero measured violations. Invariant 10 is the one that
moved during the gate: it is upheld on clean-room grounds and **conditionally** upheld on licensing,
pending the human confirmation above.

---

## 6. Contract conformance: spike code versus `contracts/v0`

Asked for explicitly, because the spikes were written before the contracts and the Phase 1 runtime
will be written from the spikes. **None of these is a Phase 0 failure** — spike code is throwaway by
the plan's own ground rules, and `tb-x-*` is the contract's reserved space for exactly this. They
matter because they are what Phase 1 must not copy.

| Item | Spike | `contracts/v0` | Verdict |
|---|---|---|---|
| Stage header | `tb-x-stage` (`tbcommon/envelope.py:34`) | `tb-stage`, registered and required on `retry`/`dlq` | **Legal as an experiment, must not survive.** The contract reserves `tb-x-*` for spike-local use and says it must not reach a released package; the CHANGELOG records that it ratified this header as `tb-stage` |
| Sequence header | `tb-x-seq` | unregistered; `tb-x-*` permitted | **Conformant.** Measurement affordance, never load-bearing |
| Reply partition | `tb-x-reply-partition` (S3) | unregistered; `tb-x-*` permitted | **Conformant** as an experiment. If targeted reply mode ships, it needs a registered header |
| `tb-msg-id` | `uuid.uuid4().bytes` in S3 (`sync_server.py:131`); canonical UUIDv7 text in `tbcommon.envelope` | UUIDv7, canonical lowercase text | **S3 violates** (G1): binary and v4. `tbcommon.envelope.uuid7()` is conformant — correct version and variant nibbles |
| `tb-correlation-id` | `uuid.uuid4().hex` (S3), 32 chars, no hyphens | canonical hyphenated UUID, engine-minted | **Non-conformant encoding, conformant intent** (G1). It is engine-minted and not payload-derived, which is what security finding 9 and ADR-002 D10 actually require |
| `tb-received-at` | `datetime.now(UTC).isoformat(timespec="milliseconds")` → `+00:00` | RFC 3339 UTC, 3-9 fractional digits, `Z` or `+00:00` | **Conformant** |
| `traceparent` | `00-<32 hex>-<16 hex>-01`, trace id kept and span id re-minted per hop | same, with all-zero trace and span ids rejected | **Conformant**, and propagation is implemented (`child_traceparent`) |
| `tb-source` | `spike/<pid>` | `^[A-Za-z0-9._:@/=\[\]-]{1,253}$` | **Conformant** |
| Headers absent from spike code | no `tb-destination`, `tb-error-code`, `tb-retry-not-before` | required on `out`/`retry`/`dlq` | **Expected**: the contract added them after the spikes, precisely so ADR-003 could express retry and DLQ triage |
| Event shape | `ts` epoch float, no `v`, no `kind`, `worker` (`s5/events.py:59-71`) | RFC 3339 `ts`, required `v` and `kind`, `instance` | **Would fail the schema** (G2). Recorded in the CHANGELOG as a deliberate contract improvement over the spike shape |
| Event key and PHI rules | keyed by `msg_id`; ids and sizes only; scanned for payload markers | message events keyed by `tb-msg-id`; no payload bytes | **Conformant** |
| Topic names | `spike.s5.<run>.raw`, `spike.s5.<run>._tb.events` | `env.channel.kind[.dest]`, `_tb.<env>.<kind>` | **Non-conformant** (G3). S4's `s4.c0001.raw` conforms, which shows the grammar is workable |
| Broker and client settings | `acks=all`, `enable.idempotence`, `enable.auto.commit=false`, cooperative-sticky, static membership, `read_committed`, RF=3, `min.insync.replicas=2` | same, as required configuration | **Conformant** — the spikes are where these values were validated |

**The single most useful output of this section:** the Phase 1 reconciliation task (ADR-002 follow-up 3)
is currently scoped to `spikes/common/src/tbcommon/envelope.py`. That is too narrow. It must also cover
the S2/S3 receiver patterns, the S5 event emitter and topic naming, because those are the files Phase 1
will be written from. I have widened the follow-up's wording to say so.

---

## 7. Conditions for GO

**Blocking sign-off:**

1. ~~**Restore the root `.gitignore` and remove the stray `Dockerfile`** (High 1).~~ **MET 2026-09-22**,
   human commit `325cef1`: the 31-line file is back and the stray root `Dockerfile` is gone. Verified by
   the lead — `samples/mirth/*`, `.env`, `secrets/` and `benchmarks/results/` are ignored again.
   **Reported by the lead; I have no shell and did not verify it myself.** This mattered most because
   decision #9 invites real anonymized Mirth exports into `samples/mirth/`; that directory is now
   protected again **before** any export lands on disk, which was the ordering risk.
2. ~~**Bind the spike data plane to localhost** (High 2).~~ **MET 2026-09-21.** `connector-engineer`
   (`e553fd7`): MLLP listeners default to `127.0.0.1`. `platform-engineer` (`71364c7`): all four
   published ports bound to `127.0.0.1` and `DYNAMIC_CONFIG_ENABLED` removed. The lead had already stopped
   the cluster (`docker compose down`, 2026-09-21), so the live exposure was closed before the fix and the
   config is now safe for the next start. **Reported by the lead; I have not re-read the code.**
3. ~~**Human confirmation of the MPL licensing posture.**~~ **MET 2026-09-22.** The human instructed,
   verbatim: **"Approve orjson, hypothesis and pathspec; record Phase 0 GO WITH CONDITIONS signed".**
   All three are approved as unmodified MPL-2.0 dependencies — see section 5.1 for the terms and the
   caveat that the inventory is incomplete.
4. ~~**Human sign-off** on the nine decisions and exit criterion 5.~~ **MET 2026-09-22** by the same
   instruction. The nine memo decisions were recorded on 2026-09-21 (section 8); the gate itself is now
   signed as **GO WITH CONDITIONS**.

**All four blocking conditions are met. The gate is signed.** What remains is the Phase 1 requirement
table below plus three open items that were never blockers: audit retention, the Mirth corpus, and the
keyed-hash mechanism. **Signing the Phase 0 gate does not start Phase 1** — that needs the human's
separate go-ahead.

### 7.1 Phase 1 requirements from the two reviews

One list, both reviewers, each traceable to its finding. None blocks this gate; all of them are
acceptance criteria on Phase 1 tasks rather than a cleanup backlog, because every one of them is a
control that Phase 0 showed is not self-enforcing.

| # | Requirement | From | Owner |
|---|---|---|---|
| 5 | **Byte-budgeted queues.** Bound receiver queues in bytes, not items — 512 × 256 × 1 MB is not a memory bound. Interacts with claim-check, since 1 MB is the inline ceiling | SR-3 | `connector-engineer` |
| 6 | **`defusedxml` plus a size cap** on any channel-XML parsing, before an upload path exists | SR-5 | `migration-engineer` |
| 7 | **Digest-pinned base images and non-root containers**; pin and hash pip installs | SR-6 | `devops-engineer` |
| 8 | **mTLS on receivers, and an explicit bind-address rule**: no wildcard default, and refuse to start a plaintext listener on a non-loopback address | SR-8, CR-10 | `connector-engineer` |
| 9 | **Lock files for anything measured.** S1 ran via `uv run --with`, so the versions behind its numbers are unrecorded; the Linux re-run must pin and record them or it is not a like-for-like comparison | SR-7 | `qa-engineer` |
| 10 | **Schema-projection tests over the real emitters.** Phase 1 code must be validated against `contracts/envelope/v0` and `contracts/events/v0` — the check Phase 0 never performed, which is why G1/G2/G3 and CR-3/CR-4 went unnoticed. Spike helpers stay out of `packages/` per the Phase 0 ground rule | CR-3, CR-4, G1, G2, G3 | `platform-engineer`, `connector-engineer` |
| 11 | ~~**Expected-keyword conformance assertions**~~ — **DONE 2026-09-22** (`83a1fd2`, 76 passed). Every invalid example is now asserted to fail at its documented `json_path` and keyword, the five PHI guards included | CR-2 | `qa-engineer` (test) + architect (manifests) |
| 12 | ~~**A unit test that AA is withheld until the publish future resolves.**~~ **DONE 2026-09-21** (`a5df2c7`). Invariant 1 no longer rests on the 26 chaos trials alone | CR-6 | `connector-engineer` |
| 13 | **Total header parsing.** A malformed integer header must be a reject, never a crash. Generalise beyond `tb-x-reply-partition` to every integer header in the envelope contract (`tb-attempt`, `tb-channel-version`) | SR-10 | `platform-engineer`, `connector-engineer` |
| 14 | **Validate the reply before re-framing it** — an embedded `\x1c\x0d` must not be able to inject a frame boundary. Pairs with High 2: reply forgery plus an unvalidated re-frame is a chain | SR-11 | `connector-engineer` |
| 15 | **CI: licence/SBOM check, CVE/image scan, and the PHI scan extended from event bodies to log output.** No scanner existed in this environment, so the whole wave's security position is source-review-only. **The licence check MUST cover `[build-system] requires` as well as the locked runtime and dev groups** — `pathspec` (MPL-2.0) is in none of the three spike lockfiles, so a lock-file-only check would report clean and miss it (section 5.1) | G5, SR-4, SR-6, invariant 6, `platform-engineer` | `devops-engineer` |
| 16 | Fix `benchmarks/gen.py:71` (invalid `\&` escape — a future `SyntaxError`, and it is the corpus generator, so it outlives the spikes) and `benchmarks/harness.py:38` | CR-5, CR-9 | `qa-engineer` |

**Requirement 11 is already half done.** `qa-engineer` measured the expected `(json_path, keyword)`
pairs against a real validator run, hand-checked them against each `violates` line, and staged them in
`tests/conformance/expected_failures.json`. I have merged all 46 pairs into the manifests I own —
`contracts/envelope/v0/examples/manifest.json` (18), `contracts/events/v0/examples/manifest.json` (16)
and `contracts/topics/v0/examples/topic-names.json` (12) — so the sidecar can be deleted. Two things
recorded there rather than silently accepted:

- `invalid/claim-check-with-inline-payload.json` fails on **two** correct pairs (`$.payload_present` and
  `$.payload_size`, both `const`). One is pinned; the test asserts the pinned pair is *among* the
  failures, so the second is not lost.
- All twelve invalid topic names pin `anyOf` at `$`, which is **weak evidence of why they fail, and is
  accepted as weak.** A topic name is one string against one grammar, so every rejection is "matched no
  branch"; splitting the schema into named branches would not change that, because a bad `env` charset
  and an unknown `kind` would still fail the same `anyOf`. Not restructuring the schema for negligible
  gain — decision recorded in the file so it is not relitigated.

### 7.2 Other items before Phase 1 code starts

| # | Item | From |
|---|---|---|
| 17 | Linux re-run of S1 and S2 on a dedicated cluster as the first Phase 1 CI task | human decision #7, **APPROVED** |
| 18 | Spike `retry-topic` delay tiers and claim-check before implementing either | ADR-003 D2, memo section 6 |
| 19 | Chaos coverage for **invariant 3** (kill PostgreSQL and the API, assert flow continues) and a tested **loader-idempotency table per sink** for invariant 4 — the two invariants Phase 0 could not exercise | section 5 |
| 20 | Decide the lint and format scope for throwaway spike code so the command in CLAUDE.md is true. My recommendation: exclude `spikes/` rather than lint throwaway code, and keep the repo-wide command honest | G4 |
| 21 | Correct the "seven spike reports" wording in `docs/agent-team/phases/phase-0.md` | criterion 1 |
| 22 | Process reminder at Phase 1 kickoff: `.gitignore` is `devops-engineer`'s and must not ride along in a spike commit. CLAUDE.md already says commit only files you own — this is High 1's cause, not a new rule | High 1 |

### 7.3 Why this verdict

**Why GO WITH CONDITIONS rather than GO:** all five exit criteria now pass, no invariant needs to change,
and the architecture's two load-bearing properties were measured and held — but Phase 0 left real gaps
that Phase 1 must close, not inherit. **Two invariants (3 and 7) were never exercised**, `retry-topic` and
claim-check are untested assumptions with contracts written against them, every number came from one
laptop, and the whole wave's security position is source-review-only because no scanner exists here. The
conditions are how those gaps stay visible. None of them is a reason to redesign anything, which is the
distinction between a condition and a defect.

**Everything that was blocking has been closed, and none of it by an agent alone**: the human restored the
`.gitignore` and approved the MPL posture, `platform-engineer` and `connector-engineer` closed the
exposure, `qa-engineer` made the conformance suite assert *why* an example fails, and `code-reviewer` and
`security-reviewer` found the two Highs that neither I nor the spike owners had noticed in a whole wave.

**Both Highs are the same class of problem, and it is worth naming:** a control that was specified and
then quietly not applied. The `.gitignore` protected the sensitive paths until a commit removed it; the
topics contract requires SASL_SSL, a deny-by-default authorizer and least-privilege ACLs, while the
spike cluster it was written alongside runs `PLAINTEXT` on `0.0.0.0` with no authorizer. Neither was a
wrong decision — both were correct decisions that nothing enforced. That is the argument for the CI
conditions in this section over more documentation.

**The finding that should change behaviour, not just the backlog:** `code-reviewer` Medium 2. The
conformance suite asserts that invalid examples fail, not that they fail for the documented reason — so
"74 passed" is weaker than I have been treating it, including for the five PHI guards I cited as
regression protection in two security rounds. It is the same failure mode the S5 harness had (a green
check that cannot distinguish the right failure from a lucky one), found in my own contract tests this
time. Requirement 11 fixes it — the manifests now pin the failing path and keyword for all 46 invalid
examples — and the pattern is now recorded twice in this wave, which is the reason it is worth stating
as a habit rather than a ticket: **assert that the check failed for the reason you claim.**

**Why not NO-GO:** nothing measured in Phase 0 contradicted an invariant, and the two threshold misses
both have known, bounded causes with a change already made (partition default) or a named follow-up
(Linux re-run).

---

## 8. Human decisions, 2026-09-21

On 2026-09-21 the human instructed: **"proceed with the recommendations of the decisions"**, relayed by
the lead. Each line below is attributed to that instruction and to the corresponding recommendation in
`PHASE-0-DECISION.md` section 7. Nothing is recorded here beyond it.

| # | Decision | Status |
|---|---|---|
| 1 | Invariants 1-10 confirmed unchanged | **ADOPTED** |
| 2 | `_tb.<env>.*` platform topic naming (ADR-002 D2) confirmed as a refinement of ADR-001 5.3 prose, **not** an invariant-2 change | **ADOPTED** |
| 3 | Lite mode with RF=1 | **ADOPTED** as a documented profile with a startup warning and **no RPO=0 promise.** Recorded honestly for what it is: an explicit, human-approved **profile exception to invariant 1's durability promise**. The invariant itself is **not** edited; the exception lives in ADR-002, `contracts/topics/v0` and the documentation |
| 4 | `_hmac` keyed-hash mechanism | **Recommendation ADOPTED**: leave it **UNRESOLVED** until before the archiver is built. No agent designs it. Per-patient search in the message browser **stays blocked** on it |
| 5 | Audit retention | **STILL OPEN.** The recommendation was only to answer before the `_tb.<env>.audit` contract is written, so the question stands: what retention duty applies to the audit trail, given the legal-interpretation question and the six-year hint in HIPAA §164.316(b)(2)(i). **It blocks the audit contract. No number is written** |
| 6 | ADR-004 scope | **APPROVED**: Kafka role/ACL model, credential rotation, key custody for encryption at rest and the `_hmac` key, claim-check object lifecycle. Needed before the archiver or production |
| 7 | Linux re-run of S1 and S2 | **APPROVED** as the first Phase 1 CI task, before the receiver is built |
| 8 | Revised Phase 1 scope (`PHASE-0-DECISION.md` section 8) | **APPROVED** |
| 9 | Mirth corpus | **NOT decided.** Still needs the human either to supply anonymized exports or to accept that the estimate stays a guess |

**The Phase 0 gate sign-off itself is NOT recorded.** The human has not signed exit criterion 5, and the
`.gitignore` restore, the MPL confirmations (`orjson`, `hypothesis`, `pathspec`) and the spike-infra
localhost binding remain conditions.

**Also on 2026-09-21: the lead stopped the running spike cluster** (`docker compose down`) because of the
all-interfaces exposure in High 2. Nothing was lost — storage was ephemeral.

---

## 9. Sign-off

| Item | Status |
|---|---|
| Exit criteria 1-4 | **PASS** (criterion 1 with a documentation discrepancy recorded) |
| Exit criterion 5, human sign-off | **FAIL (pending human)** |
| Invariants 1-10 | **No change needed.** 1, 2, 5, 9 upheld with evidence; 4, 6, 8, 10 upheld in part with named gaps; 3 and 7 **not exercised** |
| `code-reviewer`, wave diff | No Critical; 1 High, 6 Medium, 3 Low. Coverage gaps listed in 1.1 |
| `security-reviewer`, Phase 0 | No Critical; 2 High, 5 Medium, 4 Low. **No PHI and no secrets in tracked files; every fixture and the 10k corpus verified `TRIBUTARY-SYNTH`.** Source-review only — no scanner was available |
| Unresolved Critical | **none** |
| Unresolved High | **none.** Both are fixed: the `.gitignore` in human commit `325cef1`, the all-interfaces exposure in `e553fd7` and `71364c7` |
| Invariant 10 | **Upheld.** Clean-room fine; the three MPL-2.0 items are **human-approved 2026-09-22**, with the approval scoped to those packages unmodified and the inventory's build-backend gap recorded (5.1) |
| Reviewer findings closed since the first draft | CR-2, CR-5 partially, CR-6, CR-8, SR-4, SR-10 and SR-11 in spike code; the Phase 1 requirements they motivated stand |
| Gate recommendation | **GO WITH CONDITIONS** (section 7): 4 blocking conditions, **all met**; 16 items remain before Phase 1 code |
| Human decisions on the nine memo items | **Recorded 2026-09-21** (section 8): 7 adopted or approved, 1 still open (audit retention), 1 not decided (Mirth corpus) |
| Phase 0 gate sign-off (exit criterion 5) | **SIGNED 2026-09-22** — "Approve orjson, hypothesis and pathspec; record Phase 0 GO WITH CONDITIONS signed". **Does not start Phase 1**, which needs a separate go-ahead |
