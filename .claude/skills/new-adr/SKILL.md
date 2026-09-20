---
name: new-adr
description: "Write a new architecture decision record for Tributary in the project's ADR format."
argument-hint: "<short decision title>"
disable-model-invocation: true
---

Write a new ADR for: $ARGUMENTS

## Steps

1. List `docs/adr/` and pick the next number (`ADR-00N-kebab-title.md`). Read ADR-001 and any ADR that this one relates to. Read spike reports in `docs/spikes/` that the decision depends on.
2. Write the ADR with these sections:
   - **Title, Status** (Proposed, Accepted, Superseded), **Date**, **Deciders**
   - **Context:** the forces at play, with evidence (link spike reports and measurements)
   - **Options considered:** at least two, each with pros, cons and a short assessment of complexity, cost, scalability and team familiarity
   - **Decision:** what was chosen and why
   - **Consequences:** what becomes easier, what becomes harder, what to revisit and when
   - **Impact on invariants and contracts:** name any invariant or contract affected
   - **Follow-ups:** concrete tasks
3. Mark unverified claims as assumptions. Never present an unmeasured number as a result.
4. If the ADR changes an invariant, set Status to **Proposed** and escalate to the human. Do not edit `CLAUDE.md`'s invariants yourself.
5. Update the ADR index in `docs/adr/README.md` (create it if missing).
