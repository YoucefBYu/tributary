"""The MLLP `asyncio` server: framing in, publish to `<env>.<channel>.raw`, ACK after commit.

Invariant 1: `AA` is written only after the delivery report for that exact record says success. Any
publish failure, timeout or full producer queue returns `AR` — never a dropped-and-ACKed message.
If the peer disappears after the commit but before the `AA` reaches it, the sender resends: a
duplicate downstream, never a loss (invariant 4; dedupe is downstream, keyed by `tb-msg-id`).

Per connection, frames are published concurrently (a sender may pipeline) but ACKs are written back
in arrival order, matching what a pipelining MLLP sender expects. The receiver-wide `ByteBudget`
(`backpressure.py`) is the real backpressure: a full budget delays reading the next frame on every
connection until room frees up, and a wait that outlives `ack.timeoutMs` becomes a `queue-full` NACK
rather than an unbounded buffer (gate requirement 5).

A reservation is held for a frame's whole lifetime in this process, including the time spent
writing its ACK to the peer: `_ack_writer` releases it only after that write attempt finishes
(success or a detected-dead connection), never as soon as the publish outcome is known. Releasing
early would let a stalled peer's connection keep admitting and publishing new frames — each one
freeing its own budget the instant Kafka acks it — while a backlog of already-decided ACKs queues
up behind the one write that is blocked in `w.drain()`, an unbounded-item-count bypass of a
byte-count budget (code review, W1-5 second round). `conn_pipeline` bounds `q` itself for the same
reason, defense in depth: even if a future change reordered the release again, a full `q` still
backpressures the reader independently of `ByteBudget`.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import time
from dataclasses import dataclass
from typing import Any

from tributary.runtime import load_engine_kafka_or_exit
from tributary.runtime.logs import JsonLogger
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT
from tributary.sdk.keys import HL7_CONTENT_TYPE

from ..backpressure import BudgetExceeded, ByteBudget
from ..errors import ConfigError
from ..keys import resolve_key
from ..metrics import Metrics
from ..net import require_safe_bind
from ..reply.feed import ReplyFeed, open_reply_feed
from ..reply.pending import Outcome, Pending, PendingMap
from ..secrets import DirSecretResolver, SecretResolver
from ..tls import AEAD_CIPHER_PIN, build_ssl_context, log_peer_refused, peer_san_allowed
from .ack import build_ack
from .config import MllpSettings
from .framing import CR, EB, SB, Fatal, FrameParser, Oversize, control_id, looks_like_hl7
from .kafka_publisher import RawPublisher, build_raw_envelope
from .kafka_publisher import topic_name as raw_topic_name
from .reload import restart_required_fields

__all__ = ["MllpServer", "MllpSettings"]

READ_CHUNK = 65536

#: (control id, outcome, reserved byte count or None). Outcome is a completed `asyncio.Task[bool]` for a
#: message that reached the publisher, an `asyncio.Task[Outcome]` for a synchronous-reply request (it
#: resolves once the answer is known), or a `str` NACK reason decided before publishing.
_QueueItem = tuple[str, "asyncio.Task[bool] | asyncio.Task[Outcome] | str", int | None]


@dataclass(slots=True)
class _Conn:
    """Per-connection state a live spec update needs: the TLS object to re-check against a new allow-list,
    whether the peer has been revoked, how many committed-or-committing frames still await their ACK write,
    and whether the handler is parked in `read` (idle)."""

    ssl_object: Any
    revoked: bool = False
    pending: int = 0
    reading: bool = False


class MllpServer:
    def __init__(
        self,
        cfg: MllpSettings,
        publisher: RawPublisher,
        *,
        metrics: Metrics | None = None,
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        require_safe_bind(cfg.bind, tls_configured=cfg.tls is not None)
        self.cfg = cfg
        self.publisher = publisher
        self.metrics = metrics if metrics is not None else Metrics()
        self.budget = ByteBudget(cfg.queue_max_bytes, cfg.queue_max_messages)
        self._inflight = asyncio.Semaphore(cfg.max_inflight)
        self._ssl_context: ssl.SSLContext | None = None
        self._listener: asyncio.base_events.Server | None = None
        self._conns: dict[asyncio.StreamWriter, _Conn] = {}
        self._refusal: str | None = None  # set by `stop_accepting`: the fixed NACK text for later frames
        self._reply_feed: ReplyFeed | None = None
        self._pending: PendingMap | None = None
        self._background: set[asyncio.Future[None]] = set()
        if cfg.tls is not None:
            self._ssl_context = build_ssl_context(
                cfg.tls, secret_resolver or DirSecretResolver(), cipher_pin=AEAD_CIPHER_PIN
            )

    def use_reply_feed(self, feed: ReplyFeed) -> None:
        """The process's reply consumer, for a spec with `source.reply`. Before `start`; the feed outlives the
        server (the runner owns it: a redeploy must not build a second consumer on the same
        `group.instance.id`, which would fence the first)."""
        self._reply_feed = feed

    @property
    def pending(self) -> PendingMap | None:
        return self._pending

    @property
    def open_connections(self) -> int:
        return len(self._conns)

    @property
    def accepting(self) -> bool:
        return self._refusal is None

    async def apply_settings(self, new: MllpSettings) -> None:
        """Apply a live spec update (`mllp/reload.py` lists what may change). `bind`, `port` and the TLS
        context cannot: the caller must have checked `restart_required_fields` and refuses here too.

        Everything up to the final `await` is one step on the event loop, so no frame is ever read
        with a message-size limit that the byte budget cannot hold."""
        if restart_required_fields(self.cfg, new):
            raise ValueError("settings changed a field that needs a restart")
        self.budget.set_limits(new.queue_max_bytes, new.queue_max_messages)
        if self._pending is not None and new.reply is not None:
            self._pending.configure(new.reply)
        self.cfg = new
        self._revoke_disallowed_peers(new)
        await self.budget.wake()

    def _revoke_disallowed_peers(self, cfg: MllpSettings) -> None:
        """Re-check every open connection's peer certificate against the new allow-list. The list is
        otherwise only checked at accept, so a SAN removed from it would keep publishing on a session that
        was already open. A revoked connection that is idle closes now; one with frames in flight gets their
        ACKs, then closes (`_ack_writer`); a frame it sends meanwhile is NACKed, never published."""
        allowed = cfg.tls.allowed_client_sans if cfg.tls is not None else ()
        if not allowed:
            return
        for w, conn in list(self._conns.items()):
            if conn.revoked or peer_san_allowed(conn.ssl_object, allowed):
                continue
            conn.revoked = True
            self.metrics.rejected_peer_not_allowed += 1
            log_peer_refused(cfg.channel, conn.ssl_object)  # TLS_PEER_NOT_ALLOWED, certificate hash only
            if conn.reading and conn.pending == 0:
                w.close()

    def _refusal_for(self, conn: _Conn) -> str | None:
        if self._refusal is not None:
            return self._refusal
        return "peer-not-allowed" if conn.revoked else None

    def stop_accepting(self, reason: str) -> None:
        """Close the listener and NACK (`AR`, text `reason`, a fixed string) every frame that arrives
        afterwards on a connection still open. Frames already read and published are untouched: their ACK
        is still written (invariant 1). Idempotent; the first reason wins."""
        if self._refusal is None:
            self._refusal = reason
        if self._pending is not None:
            self._pending.retire()
        if self._listener is not None:
            self._listener.close()

    async def drain(self, timeout_s: float) -> bool:
        """Wait, bounded, until every frame this server admitted has had its ACK written, then close every
        connection. True when nothing was left in flight."""
        deadline = time.monotonic() + timeout_s
        while self.budget.used_messages and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        for w in list(self._conns):
            w.close()
        if self._pending is not None:
            self._pending.abandon_all()  # whatever outlived the budget: nothing is left to answer
        return self.budget.used_messages == 0

    async def _publish(self, frame: bytes, remote: str, deadline: float, cfg: MllpSettings, key: bytes | None) -> bool:
        try:
            async with self._inflight:
                timeout = max(deadline - time.monotonic(), 0.001)
                message = build_raw_envelope(
                    channel=cfg.channel,
                    channel_version=cfg.channel_version,  # the spec this frame was admitted under
                    source=f"mllp:{cfg.instance}@{remote}",
                    payload=frame,
                    key=key,  # `ordering: none` -> None; `per-key` -> the evaluated key, record key only
                )
                t0 = time.monotonic()
                ok = await self.publisher.publish(message, timeout_s=timeout)
                self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
                return ok
        except Exception:
            return False

    async def _publish_sync(
        self, frame: bytes, remote: str, deadline: float, cfg: MllpSettings, key: bytes | None, entry: Pending
    ) -> Outcome:
        """A synchronous-reply request: publish to `raw` with the reply route, report the raw callback to the
        pending map (which starts the reply clock on success), then wait for the one answer. The answer is
        `SyncRequest`'s: nothing here decides it (`reply/pending.py`)."""
        pending = self._pending
        if pending is None:
            raise RuntimeError("synchronous reply without a pending map")
        ok = False
        try:
            try:
                async with self._inflight:
                    timeout = max(deadline - time.monotonic(), 0.001)
                    message = build_raw_envelope(
                        channel=cfg.channel,
                        channel_version=cfg.channel_version,
                        source=f"mllp:{cfg.instance}@{remote}",
                        payload=frame,
                        key=key,
                        reply_headers=entry.routing_headers(ack_timeout_s=cfg.ack_timeout_s),
                    )
                    entry.bind(message.require(h.TB_MSG_ID))  # before the produce: no reply can name it earlier
                    t0 = time.monotonic()
                    ok = await self.publisher.publish(message, timeout_s=timeout)
                    self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
            except Exception:
                ok = False
        except asyncio.CancelledError:
            pending.commit(entry, ok=False)  # never leave an entry no one will settle
            raise
        pending.commit(entry, ok=ok)
        return await entry.outcome

    def _sync_ack(self, result: Outcome) -> bytes | None:
        """The frame to write for a settled synchronous request, or `None` when the sender is gone.

        `MSA-2` is the sender's own control id; `MSA-3` is `<CODE> <tb-msg-id>` and never payload or
        exception text. `AR` is the commit that was not confirmed, so a resend is safe; `AE` is a commit, so
        a resend duplicates; `AA` is a commit and an `accepted` reply (a step body replaces the built ACK)."""
        resolution = result.resolution
        cid, msg_id = result.sender_control_id, result.msg_id or ""
        if resolution.answer is None:
            return None
        if resolution.answer == "AA":
            self.metrics.acked += 1
            self.metrics.reply_aa += 1
            if result.body:
                return SB + result.body + EB + CR
            return build_ack("AA", cid, msg_id)
        if resolution.answer == "AE":
            self.metrics.reply_ae += 1
            if resolution.code == REPLY_TIMEOUT:
                self.metrics.reply_timeouts += 1
            elif resolution.code == REPLY_ROUTE_LOST:
                self.metrics.reply_route_lost += 1
            return build_ack("AE", cid, f"{resolution.code or ''} {msg_id}".strip())
        self.metrics.publish_failures += 1
        self.metrics.nacked_unavailable += 1
        return build_ack("AR", cid, "unavailable")

    def _release_body_when_done(self, task: asyncio.Task[Any]) -> None:
        """A queued answer that will never be written (its writer was cancelled): hand back its body bytes."""
        if task.cancelled() or task.exception() is not None:
            return
        result = task.result()
        if isinstance(result, Outcome) and (charged := result.take_charge()):
            release = asyncio.ensure_future(self.budget.release_bytes(charged))
            self._background.add(release)
            release.add_done_callback(self._background.discard)

    async def _discard_queue(self, q: asyncio.Queue[_QueueItem | None], conn: _Conn) -> None:
        """After a writer was cancelled: give back what its unanswered items still hold."""
        while True:
            try:
                item = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            if item is None:
                continue
            _cid, outcome, reserved = item
            if reserved is not None:
                await self.budget.release(reserved)
                conn.pending -= 1
            if isinstance(outcome, asyncio.Task):
                outcome.add_done_callback(self._release_body_when_done)

    async def _ack_writer(
        self, q: asyncio.Queue[_QueueItem | None], w: asyncio.StreamWriter, conn: _Conn | None = None
    ) -> None:
        conn = conn if conn is not None else _Conn(None)
        dead = False
        while (item := await q.get()) is not None:
            cid, outcome, reserved = item
            charged = 0
            out: bytes | None
            if isinstance(outcome, str):
                out = build_ack("AR", cid, outcome)
            else:
                result = await outcome
                if isinstance(result, Outcome):
                    charged = result.take_charge()
                    out = self._sync_ack(result)
                elif result:
                    out = build_ack("AA", cid)
                    self.metrics.acked += 1
                else:
                    out = build_ack("AR", cid, "unavailable")
                    self.metrics.publish_failures += 1
                    self.metrics.nacked_unavailable += 1
            try:
                if not dead and out is not None:  # keep draining so the reader never blocks on a dead peer
                    w.write(out)
                    await asyncio.wait_for(w.drain(), self.cfg.write_timeout_s)
            except (ConnectionError, OSError, TimeoutError):
                dead = True
                if self._pending is not None:
                    self._pending.abandon(conn)  # nothing more can be written to this sender
            finally:
                if charged:
                    await self.budget.release_bytes(charged)
                # Released only now, once the write attempt (or the decision to skip it for a
                # dead peer) is over: while a stalled peer's drain() is pending, this frame's
                # bytes stay charged, so the budget cannot be bypassed by piling up written-but-
                # unflushed ACKs (code review, W1-5 second round).
                if reserved is not None:
                    await self.budget.release(reserved)
                    conn.pending -= 1
                if conn.revoked and conn.pending == 0 and q.empty():
                    w.close()  # revoked while frames were in flight: their ACKs/NACKs are written, now close

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        peer = w.get_extra_info("peername")
        remote = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        if self._refusal is not None or self.metrics.connections_open >= self.cfg.max_conns:
            self.metrics.connections_refused += 1
            w.close()
            return
        if self.cfg.tls is not None and self.cfg.tls.allowed_client_sans:
            ssl_object = w.get_extra_info("ssl_object")
            if not peer_san_allowed(ssl_object, self.cfg.tls.allowed_client_sans):
                # README 3.2.7: close without reading a frame and without an ACK (there is no message to ACK).
                self.metrics.rejected_peer_not_allowed += 1
                log_peer_refused(self.cfg.channel, ssl_object)
                w.close()
                return
        self.metrics.connections_total += 1
        self.metrics.connections_open += 1
        conn = _Conn(w.get_extra_info("ssl_object"))
        self._conns[w] = conn
        parser = FrameParser(self.cfg.max_message_bytes)
        # Bounded independently of ByteBudget (defense in depth: see the module docstring). A full
        # q means the ack_writer has fallen behind flushing ACKs to this peer; q.put then
        # backpressures the reader directly, on top of whatever ByteBudget itself is doing.
        q: asyncio.Queue[_QueueItem | None] = asyncio.Queue(self.cfg.conn_pipeline)
        writer_task = asyncio.create_task(self._ack_writer(q, w, conn))
        try:
            while not parser.fatal:
                if conn.revoked and conn.pending == 0:
                    break  # revoked between two reads (a revocation only closes a connection parked in read)
                conn.reading = True
                try:
                    chunk = await asyncio.wait_for(r.read(READ_CHUNK), self.cfg.idle_timeout_s)
                except TimeoutError:
                    if conn.pending:
                        continue  # not idle: the sender is waiting for an answer (a reply can take up to timeoutMs)
                    raise
                finally:
                    conn.reading = False
                if not chunk:
                    break  # peer closed; any partial frame is discarded, never ACKed
                parser.set_max_size(self.cfg.max_message_bytes)  # live spec update
                refused = False
                for frame in parser.feed(chunk):
                    cfg = self.cfg  # one spec for this whole frame, from admission to the stamped envelope
                    refusal = self._refusal_for(conn)
                    if refusal is not None and not isinstance(frame, Fatal):
                        # The channel stopped accepting after this connection opened. Nothing of this frame
                        # is published; the sender is told, then the connection closes.
                        self.metrics.nacked_not_accepting += 1
                        await q.put((frame.cid if isinstance(frame, Oversize) else control_id(frame), refusal, None))
                        refused = True
                        break
                    if isinstance(frame, Fatal):
                        self.metrics.nacked_oversize += 1
                        await q.put(("", "oversize", None))
                        break  # unterminated/hostile oversize sender: drop the connection
                    if isinstance(frame, Oversize):
                        self.metrics.nacked_oversize += 1
                        await q.put((frame.cid, "oversize", None))
                        continue
                    self.metrics.messages_received += 1
                    cid = control_id(frame)
                    deadline = time.monotonic() + cfg.ack_timeout_s
                    if "parse-error" in cfg.nack_on and not looks_like_hl7(frame):
                        self.metrics.nacked_parse_error += 1
                        await q.put((cid, "parse-error", None))
                        continue
                    # Per-key channels: the key is evaluated from the message before anything is queued. An
                    # unresolved key is a parse-error NACK (README 4.3), never a null key or a default: that
                    # would put one patient's messages on different partitions and reorder them (invariant 5).
                    outcome = resolve_key(cfg.key_expression, payload=frame, content_type=HL7_CONTENT_TYPE)
                    if outcome.reason is not None:
                        self.metrics.nacked_parse_error += 1
                        await q.put((cid, "parse-error", None))
                        continue
                    try:
                        reserved = await self.budget.reserve(len(frame), max(deadline - time.monotonic(), 0))
                    except BudgetExceeded:
                        # Only reachable if a live update shrank the queue below a frame already admitted
                        # by the parser; `apply_settings` makes that a single step, so treat it as oversize.
                        self.metrics.nacked_oversize += 1
                        await q.put((cid, "oversize", None))
                        continue
                    if not reserved:
                        self.metrics.nacked_queue_full += 1
                        await q.put((cid, "queue-full", None))
                        continue
                    refusal = self._refusal_for(conn)
                    if refusal is not None:
                        # Tombstoned or revoked while this frame waited for the queue: not published.
                        await self.budget.release(len(frame))
                        self.metrics.nacked_not_accepting += 1
                        await q.put((cid, refusal, None))
                        refused = True
                        break
                    waiting: Pending | None = None
                    if cfg.reply is not None and self._pending is not None:
                        # Refused before anything is published: no reply partition held, or maxPending reached.
                        opened = self._pending.open(sender_control_id=cid, connection=conn)
                        if isinstance(opened, str):
                            await self.budget.release(len(frame))
                            await q.put((cid, opened, None))  # AR, MSA-3 = the REPLY_ code
                            continue
                        waiting = opened
                    conn.pending += 1
                    task: asyncio.Task[Any]
                    if waiting is None:
                        task = asyncio.ensure_future(self._publish(frame, remote, deadline, cfg, outcome.key))
                    else:
                        task = asyncio.ensure_future(
                            self._publish_sync(frame, remote, deadline, cfg, outcome.key, waiting)
                        )
                    await q.put((cid, task, len(frame)))
                if refused:
                    break
        except (TimeoutError, ConnectionError, OSError, ssl.SSLError) as exc:
            # idle timeout, reset, half-open detected by the idle timer, or a TLS failure
            if self._pending is not None and not isinstance(exc, TimeoutError):
                self._pending.abandon(conn)  # a reset or failed peer cannot be written to: end its waiting entries now
        finally:
            reply_wait = self.cfg.reply.timeout_s if self.cfg.reply is not None else 0.0
            try:
                await asyncio.wait_for(q.put(None), self.cfg.write_timeout_s)
                await asyncio.wait_for(writer_task, self.cfg.ack_timeout_s + reply_wait + self.cfg.write_timeout_s)
            except (TimeoutError, asyncio.CancelledError):
                writer_task.cancel()
                await self._discard_queue(q, conn)
            if self._pending is not None:
                self._pending.abandon(conn)  # the sender is gone: its waiting entries end, none outlive the handler
            self.metrics.connections_open -= 1
            self._conns.pop(w, None)
            w.close()

    async def start(self) -> asyncio.base_events.Server:
        """Start listening. Known bound, not counted: a TLS handshake in progress is not yet a connection
        (asyncio calls the handler after the handshake), so it does not count against `max_conns`. Half-open
        handshakes are bounded by `tls_handshake_timeout_s` and the listen backlog; a plaintext-to-TLS flood
        is the operator's edge-firewall concern. Counting them needs a custom protocol factory, not done."""
        if self.cfg.reply is not None:
            if self._reply_feed is None:
                raise ConfigError(
                    f"channel {self.cfg.channel!r}: source.reply needs a reply consumer, none was provided"
                )
            loop = asyncio.get_running_loop()
            self._pending = PendingMap(
                self.cfg.reply, feed=self._reply_feed, loop=loop, budget=self.budget, metrics=self.metrics
            )
            self._reply_feed.attach(self._pending)
        try:
            self._listener = await asyncio.start_server(
                self.handle,
                self.cfg.bind,
                self.cfg.port,
                ssl=self._ssl_context,
                backlog=1024,
                ssl_handshake_timeout=self.cfg.tls_handshake_timeout_s if self._ssl_context else None,
            )
        except BaseException:
            if self._pending is not None:
                self._pending.retire()  # a failed bind leaves no sink behind on the shared feed
            raise
        return self._listener


def emit_lite_profile_warning(*, profile: str) -> None:
    """`contracts/topics/v0` README 3.8 / `topic-defaults.json` `profiles.lite`: RF=1
    (`min.insync.replicas: 1`) cannot guarantee RPO=0 for an acknowledged message. Every data-plane
    component must emit this, unsuppressed, at startup when running against a lite deployment —
    `deploy/compose/topic-bootstrap/bootstrap_topics.py` emits its own copy for the topic-creation
    job; this is the receiver's (`tributary-loaders.http.main` carries the loader's)."""
    if profile != "lite":
        return
    print(
        json.dumps(
            {
                "event": "lite_profile_active",
                "detail": "RF=1, min.insync.replicas=1: no RPO=0 guarantee for an acknowledged message "
                "(contracts/topics/v0/topic-defaults.json profiles.lite)",
            }
        ),
        flush=True,
    )


def main() -> None:
    """`python -m tributary.receivers.mllp --channel <name> --env <env>`.

    The channel spec is read from the compacted `_tb.<env>.channels` topic over the engine Kafka link
    (invariant 3); there is no file input. The listener opens only after a valid spec for `--channel` has
    been read. Exit codes: 0 after SIGTERM/SIGINT or after a spec change that needs a restart (the
    supervisor restarts on 0), 2 for a refused startup configuration."""
    from tributary.runtime.config import load_topic_defaults
    from tributary.runtime.errors import RuntimeConfigurationError

    from ..cli import QuietArgumentParser, run_until_stopped, spec_feed_opener
    from ..startup import refuse, refuse_retired_channel_spec_flag, spec_reader_group_or_exit
    from .runner import MllpRunner, RunnerOptions

    argv = sys.argv[1:]
    log = JsonLogger("receiver-mllp")
    # Before parse_args: argparse echoes an unknown flag's value to stderr, so a retired
    # --bootstrap / --engine-security must reach the ENGINE_KAFKA_RETIRED_INPUT refusal first.
    kafka = load_engine_kafka_or_exit(argv=argv, log=log)
    refuse_retired_channel_spec_flag(argv, log)
    p = QuietArgumentParser(log, description=__doc__)
    p.add_argument("--channel", required=True, help="the deployed channel name (spec.metadata.name)")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--instance", default="mllp-0", help="this process's identity, for tb-source")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
    p.add_argument("--metrics-file")
    p.add_argument(
        "--profile",
        default=os.environ.get("TB_PROFILE", "lite"),
        choices=["lite", "production"],
        help="topic-defaults.json profile in use; 'lite' emits the mandatory RF=1 startup warning",
    )
    a = p.parse_args(argv)
    emit_lite_profile_warning(profile=a.profile)
    # Operator input is validated (and refused without being echoed) before it becomes a log field.
    spec_group = spec_reader_group_or_exit(a.env, a.channel, a.instance, log)
    log = JsonLogger("receiver-mllp", base={"channel": a.channel, "instance": a.instance})
    try:
        defaults = load_topic_defaults(a.contracts_dir)
    except RuntimeConfigurationError as exc:
        refuse(log, error=type(exc).__name__, code=exc.code)  # a bad --contracts-dir: exit 2, no traceback
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else None
    topic = raw_topic_name(a.env, a.channel)

    open_feed = spec_feed_opener(
        kafka=kafka, defaults=defaults, env=a.env, channel=a.channel, group=spec_group, log=log
    )

    def make_publisher(cfg: MllpSettings) -> RawPublisher:
        publisher = RawPublisher(
            kafka,
            topic,
            linger_ms=cfg.linger_ms,
            queue_max_messages=cfg.max_inflight * 2,
            topic_defaults=defaults,
            ordering="per-key" if cfg.key_expression is not None else "none",
        )
        publisher.start(asyncio.get_running_loop())
        return publisher

    def make_server(cfg: MllpSettings, publisher: RawPublisher, metrics: Metrics) -> MllpServer:
        return MllpServer(cfg, publisher, metrics=metrics, secret_resolver=resolver)

    def make_reply_feed(metrics: Metrics) -> ReplyFeed:
        return open_reply_feed(
            kafka,
            defaults,
            env=a.env,
            channel=a.channel,
            instance=a.instance,
            pid=os.getpid(),
            log=log,
            metrics=metrics,
        )

    runner = MllpRunner(
        open_feed=open_feed,
        make_publisher=make_publisher,
        make_server=make_server,
        make_reply_feed=make_reply_feed,
        options=RunnerOptions(env=a.env, instance=a.instance, topic=topic),
        log=log,
        metrics_file=a.metrics_file,
    )

    run_until_stopped(runner)


if __name__ == "__main__":
    main()
