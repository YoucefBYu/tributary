# Phase 2: v1.0

**Goal:** the differentiating features (shadow deploys, visual editor, Mirth migration), production operations (RBAC, alerts, operator, autoscaling), more connectors, and a security-audited release.

**Prerequisite:** MVP exit criteria met and signed off. Re-check priorities with design-partner feedback before starting.

**Working mode:** same as Phase 1. Run `/run-phase 2 wave-<n>` and `/phase-gate 2 wave-<n>`, one wave per session, branch `wave/2-<n>`.

---

## Wave 1: Production operations

**Team (5):** `control-plane-engineer`, `devops-engineer`, `platform-engineer`, `qa-engineer`, `architect`. `security-reviewer` as a subagent.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| P2.1-1 | `control-plane-engineer` | RBAC with channel-level permissions, API tokens with scope and expiry, audit completeness | MVP | Authorization test matrix per role; `security-reviewer` clean |
| P2.1-2 | `control-plane-engineer` | Alert rules: model, storage, rendering to Alertmanager rules; alert history | MVP | Rules load in Prometheus; test alert fires |
| P2.1-3 | `devops-engineer` | Kubernetes operator (kopf): CRDs `Channel` and `Pipeline`, status conditions, drift handling | MVP | Tests on a local `kind` cluster: apply CRD, workers appear, delete cleans up |
| P2.1-4 | `devops-engineer` | KEDA autoscaling on consumer lag; PodDisruptionBudgets; NetworkPolicies; HA PostgreSQL and ClickHouse options | P2.1-3 | Load test scales workers up and down without message loss |
| P2.1-5 | `platform-engineer` | Key-level parallelism inside a partition with commit watermark | MVP | Property tests: per-key order preserved, no commit past unfinished work; throughput gain measured |
| P2.1-6 | `qa-engineer` | Expanded chaos suite (network partitions, disk full, slow consumers, clock skew); soak test of 24 hours | P2.1-3, P2.1-5 | Report with zero acknowledged-message loss; duplicate and lag statistics |
| P2.1-7 | `architect` | ADR-006 (licensing, governance, OIE relationship) draft for human decision; updated risk register | none | Draft ready for the human |

**Exit criteria:** RBAC and alerts working end to end, operator and autoscaling verified on `kind`, key-level parallelism with measured gain, soak test passed.

---

## Wave 2: Differentiators

**Team (6):** `platform-engineer`, `archive-engineer`, `ui-engineer`, `migration-engineer`, `connector-engineer`, `qa-engineer`. `architect` as a subagent for contract questions.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| P2.2-1 | `platform-engineer` | Shadow deploy: second consumer group on the same `raw` topic writing to a shadow topic; canary by key range | MVP | Shadow run has no effect on real destinations; test proves it |
| P2.2-2 | `archive-engineer` | Output diffing between production and shadow; time-travel replay of a time range through a new version; diff report API | P2.2-1 | Diff of known-different versions shows the expected differences only |
| P2.2-3 | `ui-engineer` | Visual pipeline editor with YAML round-trip and Monaco for step code; shadow and diff views; alert screens | P2.2-2, P2.1-2 | Playwright: edit, validate, deploy, shadow, diff flow |
| P2.2-4 | `migration-engineer` | Mirth/OIE importer (channels and code templates) and `mirth_compat` shim | Phase 0 S6 | Import the sample corpus: report share converted with no manual work and list of gaps |
| P2.2-5 | `migration-engineer` | Assisted porting of JavaScript steps to Python drafts with generated tests and shadow-mode comparison; parity checklist and migration guide | P2.2-4, P2.2-1 | On sample channels: the ported step matches original output on the test corpus, or the differences are listed |
| P2.2-6 | `connector-engineer` | DICOM (C-STORE receiver, DICOMweb loader) with claim-check for large objects; database CDC source (Debezium-based) | MVP | Integration tests with synthetic DICOM objects; large payloads never inline in Kafka |
| P2.2-7 | `qa-engineer` | E2E for shadow, replay and migration flows; performance regression suite in CI | P2.2-1..5 | CI fails on defined regression thresholds |

**Exit criteria:** shadow deploy and replay diff work on live-like traffic; a real Mirth channel migrates with tests and a diff report; DICOM and CDC connectors pass tests; UI editor round-trips specs.

---

## Wave 3: Release

**Team (4):** `architect`, `devops-engineer`, `qa-engineer`, `ui-engineer`. `security-reviewer` and `code-reviewer` as subagents.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| P2.3-1 | `security-reviewer` (via subagent) with `architect` filing the report | Full security review: threat model, PHI handling, Python execution trust model, containers, dependencies, licenses | Waves 1 and 2 | Report in `docs/security/`; all Critical and High resolved or accepted by the human |
| P2.3-2 | `qa-engineer` | Published performance report on the reference cluster (methodology, raw data, reproduction script) | Wave 1 and 2 | Third party can reproduce with one command |
| P2.3-3 | `devops-engineer` | Release engineering: versioning policy, changelog automation, signed and SBOM-attached artifacts, upgrade tests from the MVP release | Wave 2 | Upgrade test passes with in-flight messages |
| P2.3-4 | `architect` | Final ADRs (002 to 006), documentation site content, security whitepaper draft, compliance mapping notes (GDPR, HIPAA); legal review items listed for the human | P2.3-1 | Human review checklist complete |
| P2.3-5 | `ui-engineer` | Accessibility audit fixes, empty and error states, onboarding flow | Wave 2 | Accessibility checks pass in CI |

**Exit criteria (v1.0)**
1. No unresolved Critical or High security findings.
2. Performance report published and reproducible.
3. Upgrade from the MVP release verified.
4. Docs, migration guide and quickstart complete.
5. Human decisions recorded: license and governance (ADR-006), trademark, release announcement, support model.
