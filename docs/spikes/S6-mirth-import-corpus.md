# S6: Mirth/OIE corpus analysis and importer prototype

**Status: DOCUMENTATION-ONLY, LOW CONFIDENCE. No real corpus was available. Prototype and analyzer were executed against synthetic XML only (see Environment).**

**ACCEPTED 2026-09-21 by architect as INCONCLUSIVE** at the P0-3 synthesis (`docs/spikes/PHASE-0-DECISION.md`): accepted as an honest null result, not as an estimate. The 43-60+ person-days and the "5 to 20 percent importable" figure are **not** carried into any plan as numbers. Consequence for Phase 1: `mirth-import` is **timeboxed with a review point rather than committed**, and migration cost stays the highest-likelihood, highest-impact open risk in ADR-001 section 13 — the one risk Phase 0 could not reduce. Unblocking it is a human decision (supply anonymized exports in `samples/mirth/`).

## Question
How much of a typical Mirth/OIE channel estate can the importer convert with no manual work, and how big is the effort for the importer and `mirth_compat`?

## Method
- `samples/mirth/` contains only its README (checked with a glob): no channel exports. Per the phase plan, fell back to channel XML structure from public documentation and general knowledge of the export format. Nothing copied from Mirth/OIE source (invariant 10).
- Built a corpus analyzer, `spikes/S6-mirth-import/analyze.py`, that reports aggregate counts only (connector types, script LOC p50/p90/max, per-feature channel counts, no-manual-work count). It prints no property values or script text, so its output is safe to paste. It is ready to run when a real corpus arrives: `python analyze.py samples/mirth`.
- Built an importer prototype for one connector type, the TCP/MLLP listener source: `importer.py`, tests in `test_importer.py` (synthetic, hand-authored XML). It maps host, port, max connections, idle timeout and charset, forces `ack: after-commit`, drops credential-like properties into `secretRef` TODOs, wraps JavaScript step bodies as commented source beside a Python skeleton, and lists items needing human attention.

## Environment
Run with system Python 3.11 (no `uv` or pytest installed on this machine): the tests in `test_importer.py` pass when invoked directly, and a committed test exercises `analyze.py` on synthetic channels. This validates the code paths only, not the format. Element and property names (`listenerConnectorProperties/host`, `TcpReceiverProperties`, `receiveTimeout`, `serverMode` and so on) come from recollection of the public format and are unverified against a real export.

Audit fix: the importer originally ignored destination connectors, so a channel with destinations could be reported `fullyAutomatic`. Destinations, non-trivial preprocessing/postprocessing/deploy/undeploy scripts and destination-level scripts are now listed in `needsAttention` (regression tests added). Analyzer and importer now agree: only the TCP receiver is supported and any destination needs work. Templated numeric properties (`${port}`) become attention items instead of aborting. The analyzer reports `skipped_unparseable` and `skipped_non_channel` counts, and a committed test covers it. Caveat: custom connector class names in analyzer output could identify an organization; review before pasting. Known analyzer limit: "no manual work" counts script bodies only, so channels with non-script transformer steps (mapper, message builder) are still counted clean; the importer flags those. Reconcile when a corpus exists.

## Raw results
None measured. There are no statistics for connector mix, script LOC or feature usage, and I am not inventing any.

## Assumption-based estimates (not measurements)
Structure knowledge that is safe to state: a channel has one source connector and N destinations; each has a filter and transformer of ordered steps (mapper, message builder, JavaScript, rule builder); plus preprocessing, postprocessing, deploy and undeploy scripts; plus map usage and code-template libraries.

| Item | Mechanical? | Rough effort (person-days, unvalidated) |
|---|---|---|
| Channel and connector settings for MLLP, HTTP, file/SFTP, DB reader/writer, Kafka | Yes | 3 to 5 per connector type; the prototype covers ~1 |
| Mapper, rule-builder and message-builder steps | Mostly | 5 to 8 total |
| JavaScript bodies | No, draft only | LLM-assisted, human review per script |
| Lifecycle scripts to hooks | Skeleton only | 2 |
| `mirth_compat`: HL7 accessor (`msg["PID"]["PID.5"]`), map objects, logger | Yes | 8 to 12 |
| `mirth_compat`: E4X-style XML, DB helpers, Java calls | E4X partial, Java calls no | E4X 10+; Java calls stay flagged as gaps |
| Code templates to packages | Partial | 3 |

Estimated importer plus compat core: summing the table (five connector types at 3 to 5 each, plus the other rows, plus E4X at 10+) gives about 43 to 60 person-days or more, with E4X open-ended. This supersedes the earlier 35 to 50 figure. Still a guess. Java calls (`Packages.`, `java.*`) and E4X are the likely long tail; propose flagging rather than emulating Java (no sensible equivalent, document the gap).

**Share of channels importable with no manual work: no measurement. Reasoned guess is 5 to 20 percent**, limited to pass-through or routing channels whose only steps are mappers or none, with supported connectors. Most production channels contain at least one non-trivial JavaScript step, so the realistic target is "automatic skeleton, manual port of bodies" for the rest. Confidence: low; this could be wrong in either direction by 2x or more.

## Verdict
INCONCLUSIVE for the estimate (no corpus). The prototype approach (mechanical settings, commented JS plus skeleton, attention list, secrets never copied) is sound in design but unverified in execution. Nothing here should feed a go/no-go threshold. S6 has no numeric threshold beyond producing the estimate, which is documented as low confidence.

## Follow-ups
1. Human: supply anonymized exports in `samples/mirth/` (credentials, hosts, PHI stripped); then run `analyze.py` and replace the estimates above.
2. Check element names against one real export (tests already pass on synthetic XML).
3. Correct the "no manual work" definition once the corpus exists (currently: no script bodies and supported connector types only).
4. Create the `packages/tributary-mirth-import/` and `docs/migration/` parity checklist in Phase 1; the ADR-001 section 9.1 table is the starting checklist (all items currently Missing).
5. Phase 1 requirement (from architect): channel names must slugify to topic segments matching `^[a-z][a-z0-9-]{0,63}$` (`contracts/topics/v0`; also the `tb-channel` value). The slugifier must be collision-free: names differing only in case or punctuation get a numeric suffix or an error, never a silent collision. Record the original name in the spec `metadata`. Note: the current prototype does not slugify.
