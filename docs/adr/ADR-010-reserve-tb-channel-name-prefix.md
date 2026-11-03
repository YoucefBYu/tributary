# ADR-010: Reserve the `tb-` Channel-Name Prefix

- **Status:** Accepted (architect, 2026-10-06). **No invariant changes**, so no human approval is
  required. It is the first **non-additive** change to a frozen set-1.0 grammar, and this ADR records
  that exception.
- **Date:** 2026-10-06
- **Deciders:** `architect`, on the coordinator's request (W3-27 archiver metrics, lead-relayed)
- **Contracts:** `channel-spec/v0` (`metadata.name`), set 1.8

## 1. Context

`tb_commits_total` carries a `channel` label for every emitter (`observability/v0` `metrics.json`).
The archiver consumes every channel's events, so it labels its own series `channel="tb-archiver"`
(`archiver/exposition.py` `ARCHIVER_CHANNEL`). The label grammar equals the channel-name grammar
`^[a-z][a-z0-9-]{0,63}$`, so a real channel named `tb-archiver` would merge its series with the
archiver's, and dashboards and alerts would misattribute commits.

## 2. Options

| Option | For | Against |
|---|---|---|
| **A. Reserve `tb-` in `channel.schema.json` `metadata.name` (chosen)** | One pattern; covers every future platform stand-in (`tb-api`, `tb-reprocessor`); the label grammar, `tb-channel` header and topic grammar are untouched, so stand-ins stay valid label values | Narrows a frozen set-1.0 grammar: a spec named `tb-*` that was valid is now refused |
| B. A separate archiver metric without `channel` (`tb_archiver_commits_total`) | Additive | Fixes one metric only; the next stand-in repeats the problem; a W3-27 code change and a dashboard change |
| C. `channel-spec/v1` with the narrower grammar | Follows the letter of `contracts/README.md` | Moves every channel-spec path (conformance, ADRs, runtime corpus test) for one lookahead that affects zero documents |

**Criteria:** no collision possible by construction; smallest blast radius; honesty about the
versioning rule.

## 3. Decision

Option A. `metadata.name` becomes `^(?!tb-)[a-z][a-z0-9-]{0,63}$`. The control plane refuses such a
document with `422 SPEC_INVALID`, and a worker refuses it on load like any schema-invalid spec (**unverified**: follow-up 1).
Recorded in `contracts/CHANGELOG.md` as the one non-additive item of set 1.8, with an invalid example
pinned at `$.metadata.name` / `pattern`.

**Evidence that nothing breaks (grep, 2026-10-06):** no channel example in `contracts/`, no Compose or
Helm channel, and no test fixture uses a `tb-` channel name. Nothing has been released or published.

**Give up:** the promise that every 1.0-valid document stays valid under 1.x now has one recorded
exception. A site that names a channel `tb-*` must rename it before upgrading; none exists today.

## 4. Follow-ups

1. `platform-engineer`: if the runtime checks channel names outside the schema (topic builders), no
   change is needed there; the reservation is a spec rule. Confirm the runtime validates published
   specs against the schema on load (corpus test).
2. `archive-engineer`: keep `ARCHIVER_CHANNEL = "tb-archiver"`; new stand-ins use `tb-<component>`.
3. `qa-engineer`: the conformance suite picks up `channel-name-reserved-prefix.json`.
