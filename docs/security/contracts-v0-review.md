# Security review: contracts v0 (manual, read-only)

Reviewer: `security-reviewer`. Scope: commits 2d1639d and 7a4e620 (contracts/envelope|topics|events v0, ADR-002/003 skeletons).
Filed by the lead from the reviewer's message because the original was lost in a session restart. Wording of findings 7-14 is the reviewer's; findings 1-6 are condensed.
No scanners were available (gitleaks, trivy, bandit, pip-audit, hadolint all missing). Nothing was run against Kafka. Schemas were read, not executed.

## Status

| # | Sev | Finding | Status |
|---|---|---|---|
| 1 | HIGH | Strict validation of a projection containing the PHI `key` leaks it via validator errors | fixed in ae4d3bd (key removed from projection: `key_present`, `key_size`) |
| 2 | HIGH | Clear-text `attrs` guard fails only on key casing; `{"mrn": ...}` passes | fixed in ae4d3bd (64-hex `_hmac`, deny-list, short-code values) |
| 3 | HIGH | Claim-check URIs may carry credentials or identifiers | fixed in ae4d3bd (opaque locator, no query/userinfo) |
| 4 | HIGH | No transport security or authorization in the topics contract | fixed in ae4d3bd (mechanisms and ACL shape; role model and keys go to ADR-004) |
| 5 | HIGH | `min.insync.replicas` and `unclean.leader.election.enable` missing from per-class topic configs (invariant 1) | fixed in ae4d3bd |
| 6 | HIGH | `_hmac` keyed-hash mechanism unspecified | **OPEN by design, escalated.** Marked UNRESOLVED in `events/v0/README.md` 3.1; shape kept, mechanism not designed. ADR-004 plus a human. Interim: producers SHOULD NOT emit identifier-derived `attrs`, and `_hmac` values are PHI in ClickHouse |
| 7 | MEDIUM | `attrs` payload smuggling channel; the "cannot" claim is false | **fixed.** Claim dropped; 16 entries, ≤64-char values, delimiter/CRLF exclusion, 1024-byte producer cap. Examples `attrs-too-many-entries`, `attrs-hl7-delimiters` |
| 8 | MEDIUM | Record-key rule has scope holes; kcat/Kafka UI promoted as an unaudited PHI viewer | **fixed.** Envelope section 4 extended to URLs, browser storage, sender-facing errors and other headers; anything that can display a key or a raw record must be authn/authz/audited (section 2) |
| 9 | MEDIUM | `correlation_id` clear text with unconstrained provenance | **fixed structurally.** UUID-only in both schemas, so a payload-derived value is unrepresentable. Envelope 3.2; example `correlation-id-sender-token` |
| 10 | MEDIUM | `tb-usr-*` documented as reaching events with no representable field | **fixed by decision:** `tb-usr-*` does NOT reach `_tb.events` in v0 (envelope 2.2, events rule 6). Explicit `attrs` emission instead of an invented mapping |
| 11 | MEDIUM | Shared topics collapse the channel isolation boundary | **fixed.** Now a restriction: a share group must not span a tenant or trust boundary; control plane refuses; group inherits the union of member sensitivity (topics 1.3) |
| 12 | MEDIUM | Audit retention 30 d versus a six-year expectation | **human decision, not made.** Paragraph in topics 2.1, questions in `docs/agent-team/STATUS.md`, value unchanged and marked `TODO(human)` |
| 13 | LOW | 16 KiB header budget unenforced (~32 KiB reachable) | **fixed.** `tb-usr-*`/`tb-x-*` capped at 256 bytes; worst case now under 7 KiB. Example `usr-header-over-length` |
| 14 | LOW | No encryption-at-rest requirement | **fixed.** Topics 3.5 and `topic-defaults.json.security.encryption_at_rest`, stated with its limits. Key custody remains ADR-004 |

Nothing disputed. Conformance expectation moves 70 → 74 tests; not run by the architect (no shell).

## HIGH 6: `contracts/events/v0/README.md:104`

The keyed-hash search mechanism is unspecified cryptography. The `_hmac` convention names no algorithm, no input canonicalization, no tenant scoping, no truncation and no rotation; only "key management is ADR-004" is deferred. Consequences: two producers compute different values and search silently returns nothing; a single global key makes each hash a stable cross-tenant pseudonym in a shared ClickHouse; an unkeyed SHA-256 passes the schema. Per CLAUDE.md this needs human escalation (designing cryptography or key management).
FIX (reviewer): pin HMAC-SHA256, a per-tenant key, domain separation over the attr name, full-length hex, and a rotation/re-index story, or mark the mechanism explicitly unresolved rather than stating it as settled.
LEAD DIRECTION: do not design it. Mark UNRESOLVED, keep the fixed-length-hex schema shape, put the open questions in ADR-004 scope, escalate to the human.

## MEDIUM 7: `events/v0/README.md:108-109` and `event.schema.json:57-63`

The "cannot become a payload smuggling channel" claim is false. 32 attrs entries x 256 chars is about 8 KiB of channel-controlled text per event; a typical ADT message is 1-2 KiB, so a buggy or hostile step can reassemble a whole payload inside the archive while passing validation.
FIX: drop the claim, cap total attrs bytes, and constrain value shape to exclude segment delimiters.

## MEDIUM 8: `envelope/v0/README.md:145-152` and `:77-79`

The record-key rule has scope holes. Section 4 covers logs, metric labels, trace attributes, exception messages and test output, but not the UI, URLs and browser storage, error responses returned to senders, or HTTP loader/lookup URLs built from the key. Section 2's rationale actively promotes kcat and the Kafka UI reading records, with no requirement that those tools be access-controlled and audited: an unaudited PHI viewer. Nothing forbids copying the key into `tb-usr-*` or `tb-source` either.
FIX: extend the section 4 prohibition list and add an access-control/audit requirement wherever readable headers and keys are browsable.

## MEDIUM 9: `envelope/v0/README.md:108` and `event.schema.json:37`

`correlation_id` is clear text into ClickHouse and the UI with unconstrained provenance. It may be set from payload content (it also serves as the `reply` record key), and unlike `key` no rule forbids logging or storing it.
FIX: state that correlation ids must not be derived from patient identifiers, or treat them exactly as the key is treated.

## MEDIUM 10: `envelope/v0/README.md:88-90` versus `event.schema.json:61`

`tb-usr-*` is documented as reaching `_tb.events`, ClickHouse and the browser, but has no representable field there. `additionalProperties: false` plus attrs keys of `^[a-z][a-z0-9_]{0,31}$` cannot hold header names containing hyphens or longer than 31 characters, so implementers will invent a mapping, exactly the ad-hoc path where unvetted tenant metadata leaks.
FIX: define the mapping normatively, or state that `tb-usr-*` does not reach events.

## MEDIUM 11: `topics/v0/README.md:67-74`

Shared topics collapse the channel isolation boundary. "No per-channel ACL" plus consumers reading and discarding other channels' records means, in a multi-tenant deployment, that one channel's credentials read another's PHI. It is listed as a cost, not a restriction.
FIX: forbid sharing across tenants or trust boundaries, not merely "opt-in only".

## MEDIUM 12: `topic-defaults.json:86-92` and `events/v0/README.md:141`

`_tb.<env>.audit` retains 30 days with no long-term sink and no written contract. HIPAA 164.316(b)(2) expects six-year retention of such records, so audit completeness is currently unverifiable.
LEAD DIRECTION: human decision. Do not set the target; write a paragraph for the human.

## LOW 13: `envelope.schema.json:206-255` versus `README.md:75`

The 16 KiB total-header limit and the per-value budget are unenforced (the schema admits roughly 32 KiB via `tb-usr-*`/`tb-x-*`). The 1 MB payload plus headers still fits under `message.max.bytes`, so this is a conformance gap, not a broker failure.

## LOW 14: `topics/v0`

No encryption-at-rest requirement for topics or the claim-check bucket, and unlike the ACL item it is not even listed as deferred in section 5.

## Checked and found sound

- Error headers carry codes only: `tb-error-code` is `^[A-Z][A-Z0-9_]{0,63}$`, no free-text error header exists, and `raw`/`out`/`reply` are forbidden from carrying it. ADR-003:86-88 and the changelog give the correct reason (parser exception text quotes field values).
- No payload field exists in the event schema and `additionalProperties: false` blocks one being added by a producer.
- `dlq` is keyed by `tb-msg-id`, so the 30-day topic holds no PHI record keys.
- `traceparent` all-zero trace-id and span-id rejection matches W3C.
- `acks=all`, `enable.idempotence`, `enable.auto.commit=false`, `read_committed`, `unclean.leader.election.enable=false` and `auto.create.topics.enable=false` are correctly specified; nothing weakens ACK-after-commit apart from HIGH 5.
- No secrets, credentials or non-synthetic identifiers in any changed file. No dependency or license changes in 2d1639d or 7a4e620.
