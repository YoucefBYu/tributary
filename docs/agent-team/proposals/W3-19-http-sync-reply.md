# W3-19 proposal: HTTP synchronous reply, status mapping (contract, phase 1)

**Status:** Proposed by `connector-engineer` for `architect` ruling. No code until ruled and published.
**Resolves:** the "HTTP mapping: not fixed in set 1.2" paragraph of `channel-spec/v0` 3.2.6.
**Binding constraint (3.2.6, kept):** not-committed outcomes are exactly `503` with `Retry-After`; no
committed outcome uses a status that clients or proxies retry on their own (`5xx`, `408`, `429`).
**Contract change:** additive. No new error code, no new Kafka header, no worker change (the `reply`
record, `tb-correlation-id`, `tb-reply-partition`, `tb-reply-deadline` and the `REPLY_` codes are the
MLLP ones). `platform-engineer` is not needed for `body: ack`.

## 1. Meaning of the three answers (same as MLLP)

`AR` = commit not confirmed, a resend is safe. `AE` = committed, outcome not `accepted` or not known: the
engine holds the message, and a resend is a new message with a new `tb-msg-id` that loader idempotency does
not absorb. `AA` = committed and routed (not delivered).

## 2. Status mapping

Without `source.reply` nothing changes: `202` after commit, and the 3.2.2 table stands.

| Case | Committed | Status | `Tb-Reply-Code` | MLLP twin |
|---|---|---|---|---|
| Reply `accepted` (routed; a whole-message `Drop` too) | yes | **`200 OK`** | none | `AA` |
| Reply `rejected` (a `Reject` code) | yes | **`202 Accepted`** | the reject code | `AE` + code |
| Reply `failed` (`TRANSFORM_ERROR`, `REPLY_BODY_MISSING`, ...) | yes | `202` | the code | `AE` + code |
| `tb-reply-status` absent or unrecognised (fail-safe) | yes | `202` | the reply's code, else none; never `200` | `AE` |
| No reply by the deadline | yes | `202` | `REPLY_TIMEOUT` | `AE` |
| Reply partition revoked with the request in flight | yes | `202` | `REPLY_ROUTE_LOST` | `AE` |
| Receiver stopping, or spec undeployed, with a committed request pending | yes | `202` | `REPLY_ROUTE_LOST` | `AE` |
| Kafka down; `raw` publish failed or timed out | not confirmed | **`503` + `Retry-After`** | `PRODUCE_FAILED` or `PRODUCE_TIMEOUT` | `AR` |
| Pending map full, or this process holds no reply partition | no, nothing published | `503` + `Retry-After` | `REPLY_BUSY` / `REPLY_UNAVAILABLE` | `AR` |
| Before any commit: parse, content type, auth, size, queue full | no | unchanged: `400`, `415`, `401`/`403`, `413`, `422`, `503` | as today | `AR` |

Why `202` for every `AE`: it is the one standard status that says "received, outcome not (yet) known", it is
not retried by clients or proxies, and it keeps `200` meaning only "committed and routed". A sender must read
`Tb-Reply-Code` or the body to tell `AE` from a plain async `202`; a sync channel never sends an uncoded `202`.
**Rejected alternatives:** `500`/`502`/`504`/`408`/`429` (auto-retried: a duplicate); `422` or `409` for a
pipeline `Reject` (`422` already means "not committed, retrying cannot help", and reusing it would make
`AR` and `AE` indistinguishable by status); `200` for `AE` (a sender that tests only `2xx==ok` would treat a
rejected message as accepted).

Notes the contract text must carry: (a) the sender's own client timeout must exceed `reply.timeoutMs` plus a
margin, or it will give up on a committed request and resend (a duplicate); (b) a timeout or `504` produced by
a proxy in front of the receiver is outside the engine's control and is the same duplicate risk; (c) the
`AE` rule "do not resend blindly" and the control-plane duplicate warning (3.2.6) apply unchanged.

## 3. Response shape and headers

- Body, `Content-Type: application/json`: `{"v":0,"status":"accepted|rejected|failed|timeout","tb-msg-id":"..."}`
  and, on `202`/`503`, `"code":"<registered code>"`. Ids and codes only, never payload or exception text
  (invariant 6). Header `Cache-Control: no-store` on every sync answer.
- Response headers: `Tb-Msg-Id` (id only), `Tb-Reply-Code` (a code registered in `envelope/v0` 2.3.2; absent on
  `200`), `Retry-After` on every `503` (integer seconds, as today).
- **No new request header.** Kafka headers are the MLLP ones; the receiver mints `tb-correlation-id` and sets
  `tb-reply-partition` and `tb-reply-deadline`.
- `body: step` is **refused for `http` at startup in the first build** (a step body needs a response content
  type, which `ctx.reply` does not carry: a `platform-engineer` SDK change). `body: ack` only.
- Metrics: `tb_receiver_sync_answers_total{answer, code}` maps `200`->`AA`, sync `202`->`AE`, `503`->`AR`;
  no change to `metrics.json`.

## 4. Bounds on the pending map

- `maxPending` (1-50000, default 10000, about 4.6 KB per entry) per receiver process. Over it: `503`
  `REPLY_BUSY` with `Retry-After`, nothing published.
- Per-connection fairness `max(1, maxPending // 4)` as for MLLP. An HTTP connection also reads one request at
  a time (no pipelining): the next request is not read until the previous answer is written, so one connection
  holds at most one entry. Open sockets stay bounded by the existing `max_conns`.
- Deadline: `timeoutMs` (1000-60000) from the successful `raw` callback. Every entry has one; the reaper
  answers `202 REPLY_TIMEOUT`. A late reply is dropped as unknown and counted (`replies_unmatched`).
- A client that disconnects while pending: the entry is removed and the slot freed; the message is still
  processed (sync mode never cancels work).
- An entry resolves only when the correlation id is in the map **and** `tb-msg-id` matches. The answer never
  precedes the successful `raw` callback (`AE` before it would claim a commit that may fail; the entry waits,
  then answers `202` on success or `503` on failure).
- No process holding a reply partition is not ready (`reply_consumer` check, as MLLP); the receiver keeps
  serving non-sync refusals (`503` `REPLY_UNAVAILABLE`), never ACKs without a commit (invariant 1).

## 5. Contract edits requested (architect owns)

1. `channel-spec/v0` 3.2.6: replace the "HTTP mapping: not fixed" paragraph with section 2 and 3 above.
2. 3.2.2 table, `http` row: add "sync mode: see 3.2.6".
3. Source-support table (line ~140 and ~576): `http` `reply` from "refused at startup" to "accepted; `body: step`
   refused until a content type is carried" once ruled and published; CHANGELOG entry.

## 6. Open questions for the ruling

1. Confirm `202` (with code) for all `AE`, `200` for `AA`. Alternative if you prefer status-level separation of
   `Reject`: `409`, at the price of `AE` splitting across two statuses.
2. Should an optional sender-supplied `Idempotency-Key` ever dedupe a resend after `AE`? Not proposed (edge
   dedupe is keyed on MSH-10 for MLLP only); noted because HTTP senders often send one.
3. `body: step` for http: defer to a later set (needs `ctx.reply` content type), as proposed here?

## 7. Tests I will write after the ruling (as W2-8)

Status for each table row against a real broker; no `5xx`/`408`/`429` on any committed outcome; latency and
timeout tests; map full and per-connection bound; client disconnect frees the slot; late and forged replies
dropped; `AE` never before the `raw` callback; no payload or exception text in any body, header or log.
