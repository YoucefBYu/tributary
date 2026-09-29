# Architecture decision records

Written with the `/new-adr` skill. ADR-001 is the source of truth for architecture; read it before any
design work. An ADR that changes one of the invariants in `CLAUDE.md` stays **Proposed** until a human
approves it.

| ADR | Title | Status | Contracts |
|---|---|---|---|
| [001](ADR-001-kafka-centric-python-integration-engine.md) | Kafka-centric, Python-native open-source integration engine | Proposed | — |
| [002](ADR-002-topic-topology-and-envelope.md) | Topic topology and envelope specification | Proposed (skeleton, TODOs pending spikes) | `envelope/v0`, `topics/v0` |
| [003](ADR-003-ordering-retry-and-dlq.md) | Ordering, retry and DLQ policy | Proposed (skeleton, TODOs pending spikes) | `envelope/v0`, `events/v0` |
| [007](ADR-007-channel-spec-shape-and-refusals.md) | Channel spec shape and what it refuses | Proposed (W1-1, no invariant changes; amended for sets 1.1 and 1.3) | `channel-spec/v0`, `envelope/v0`, `topics/v0` |
| [008](ADR-008-engine-kafka-connection-and-lite-transport-security.md) | Engine Kafka connection and the lite stack's transport security | Proposed (2026-09-29, no invariant changes) | `engine-kafka/v0`, `topics/v0` |

Numbers 004 to 006 stay reserved for the planned ADRs below, so the channel-spec decision took 007
rather than renumbering them. ADR numbers are identifiers, not an ordering.

## Planned

| ADR | Title | Trigger |
|---|---|---|
| 004 | Archive design, PHI handling and key management | Phase 1, before the archiver ships |
| 005 | Bundle format, dependency isolation and trust model | Phase 1, before bundles build in CI |
| 006 | Licensing, governance and OIE relationship | Human decision, before public launch |

## Conventions

- Number and title: `ADR-00N-kebab-title.md`.
- Sections: title, status, date, deciders; context with evidence; at least two options with pros, cons
  and an assessment of complexity, cost, scalability and team familiarity; decision; consequences;
  impact on invariants and contracts; follow-ups.
- Mark unverified claims as assumptions. Never present an unmeasured number as a result. Placeholders
  are written `TODO(Sn)` so they are greppable before a gate.
