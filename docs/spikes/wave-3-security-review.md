# Wave 3: security pass for exit criterion 4

Summary relayed by the lead, 2026-10-14. It records the verdict and the findings; the review itself is the reviewer's. Synthetic data only.

**Scope.** `packages/tributary-api` at aba9ac5: the W3-6 archive, content and stream endpoints; W3-32 audit; W3-37a/b TLS and the mesh
NetworkPolicy; authentication and RBAC.

**Verdict.** No Critical, High or Medium finding.

**Low findings, all fixed.**

| Id | Finding | Fixed in |
|---|---|---|
| L1 | Mesh ingress `podSelector` | f453f75 |
| L2 | Connection cap | 54e96cd |
| L3 | Replay restart by a different subject | 54e96cd |
| L4 | Literal loopback only | 54e96cd |

**Accepted by design.** 401 and 403 refusals are counted, not audited (`audit/v0` rule 8).

**Archive audit path (W3-30 part 2).**

| Finding | Fixed in |
|---|---|
| H1: crash loop on non-string fields | 416b4a0 |
| Medium: `ts` and partition stall | dbccffb, 12227f6 |
| Writer-set timestamps (LogAppendTime) | 6f48df8, 86486c2, 50db095 |

**Not verified.** Integration and live ClickHouse inserts near the 2299 cap, and the topic-config alter against a live broker.
