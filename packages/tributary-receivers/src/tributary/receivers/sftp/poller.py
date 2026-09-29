"""The SFTP poller: claim a file by renaming it, publish its bytes to `<env>.<channel>.raw`, and delete
or move it only after the record is committed (invariant 1; README 3.2.2's `afterCommit` row).

**Crash safety.** A file is only ever deleted or moved to the archive after the delivery report for
its record says success. Until then it exists under a claim name, `.tb-claim.<instance>.<name>`, in
the source directory:

* crash before commit: the claim is still there. On its next cycle this instance finds its own claims
  (by its instance id, stable across restarts like a static-membership id) and re-processes them
  first, so no file is ever lost. If the record *was* committed before the crash, this ingests the
  file again under a new `tb-msg-id`: the inherent at-least-once duplicate of README 3.2.2, bounded by
  one file per instance.
* Kafka unavailable: the claim is renamed back and the cycle stops (README: "leave the file; retry
  next poll"). If the rename back fails, the claim is recovered next cycle as above.
* claim atomicity: SFTP `rename` fails rather than overwrites, and each claim name is unique to this
  instance, so of two pollers racing for one file exactly one rename succeeds; the loser skips it.
  Pollers on the same directory must have distinct instance ids. Claims left by an instance that
  never comes back are not touched (they are reported as `foreign_claims`): stealing a live poller's
  claim would risk two ingests, so that stays an operator decision.

**Refusals** (oversize, empty, key unresolved, malformed JSON when `parse-error` is declared) move the
claimed file to `rejectDirectory`, never delete it (the NACK column of README 3.2.2), and never publish.

**Bounds.** One file at a time, so at most `maxMessageBytes + 1` bytes are buffered; the remote listing
is capped; every network read has a timeout (`session.connect`); a poll cycle publishes at most
`maxFilesPerPoll` files.

**PHI.** Remote file names are PHI (README 3.2.4). They are used only as opaque strings passed to the
server and never reach a log line, metric, header, exception message or event; logs carry the channel,
`tb-msg-id`, byte counts and outcome codes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fnmatch
import json
import logging
import posixpath
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any, Protocol

from tributary.sdk import Message
from tributary.sdk import headers as h

from ..engine_kafka import engine_security_properties
from ..envelope import build_raw_envelope
from ..errors import ConfigError
from ..keys import resolve_key
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher, topic_name
from ..payload import well_formed
from ..secrets import DirSecretResolver, SecretResolver
from .config import SftpSettings, sftp_settings_from_channel_spec
from .session import RemoteEntry, RemoteExistsError, RemoteNotFoundError, SessionError, SftpSession, connect

__all__ = ["SftpPoller", "serve_from_channel_spec"]

CLAIM_PREFIX = ".tb-claim."
_MAX_NAME = 200  # leaves room for the claim prefix inside a 255-byte file name limit
_log = logging.getLogger("tributary.receivers.sftp")


class _Publisher(Protocol):  # the slice of RawPublisher this module uses
    async def publish(self, message: Message, *, timeout_s: float) -> bool: ...


def _safe_name(name: str) -> bool:
    """A listing entry is untrusted input: refuse anything that could escape the directory."""
    return bool(name) and len(name) <= _MAX_NAME and "/" not in name and "\x00" not in name and name not in (".", "..")


def _event(event: str, **fields: object) -> None:
    _log.info(json.dumps({"event": event, **fields}))


class SftpPoller:
    def __init__(
        self,
        cfg: SftpSettings,
        publisher: _Publisher,
        session_factory: Callable[[], SftpSession],
        *,
        metrics: Metrics | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cfg = cfg
        self.publisher = publisher
        self._connect = session_factory
        self.metrics = metrics if metrics is not None else Metrics()
        self._clock = clock
        self.foreign_claims = 0
        self._claim_prefix = f"{CLAIM_PREFIX}{cfg.instance}."

    # ------------------------------------------------------------------ one cycle

    async def poll_once(self) -> int:
        """One connect-list-process cycle. Returns the number of files committed. Never raises for a
        remote or Kafka failure (those end the cycle and are counted); a programming error propagates."""
        self.metrics.connections_total += 1
        try:
            session = await asyncio.to_thread(self._connect)
        except SessionError as exc:
            self.metrics.connections_refused += 1  # a failed connect, whatever the reason
            self.metrics.poll_errors += 1
            _event("sftp_connect_failed", channel=self.cfg.channel, error=type(exc).__name__)
            return 0
        self.metrics.connections_open += 1
        try:
            return await self._cycle(session)
        except SessionError as exc:
            self.metrics.poll_errors += 1
            _event("sftp_poll_failed", channel=self.cfg.channel, error=type(exc).__name__)
            return 0
        finally:
            self.metrics.connections_open -= 1
            await asyncio.to_thread(session.close)

    async def _cycle(self, session: SftpSession) -> int:
        cfg = self.cfg
        skipped_before = session.undecodable_names
        entries = await asyncio.to_thread(session.list_dir, cfg.directory, cfg.max_listing)
        skipped = session.undecodable_names - skipped_before
        if skipped:  # names that are not valid UTF-8 are skipped, never fatal (and never logged)
            self.metrics.files_skipped += skipped
            _event("sftp_names_skipped", channel=self.cfg.channel, count=skipped)
        own_claims: list[str] = []
        candidates: list[RemoteEntry] = []
        now = self._clock()
        for entry in entries:
            if not entry.is_regular or not _safe_name(entry.name):
                continue
            if entry.name.startswith(self._claim_prefix):
                own_claims.append(entry.name)
            elif entry.name.startswith(CLAIM_PREFIX):
                self.foreign_claims += 1
            elif fnmatch.fnmatchcase(entry.name, cfg.file_pattern) and now - entry.mtime >= cfg.min_file_age_s:
                candidates.append(entry)
        candidates.sort(key=lambda e: (e.mtime, e.name))  # oldest first: the order senders wrote them

        committed = 0
        # Our own leftover claims first: they are files a previous run took and never finished.
        for claim_name in own_claims[: cfg.max_files_per_poll]:
            outcome = await self._process(session, claim_name[len(self._claim_prefix) :], claim_name)
            if outcome is None:
                return committed
            committed += outcome
        budget = cfg.max_files_per_poll - len(own_claims)
        for entry in candidates[: max(budget, 0)]:
            claim_name = f"{self._claim_prefix}{entry.name}"
            src = posixpath.join(cfg.directory, entry.name)
            try:
                await asyncio.to_thread(session.rename, src, posixpath.join(cfg.directory, claim_name))
            except (RemoteNotFoundError, RemoteExistsError):
                continue  # another poller claimed it first (or the sender removed it)
            outcome = await self._process(session, entry.name, claim_name)
            if outcome is None:
                return committed  # Kafka refused: stop hammering, retry next poll
            committed += outcome
        return committed

    # ------------------------------------------------------------------ one file

    async def _process(self, session: SftpSession, original: str, claim_name: str) -> int | None:
        """`1` committed, `0` refused (rejected), `None` Kafka unavailable (file left for next poll)."""
        cfg = self.cfg
        claim = posixpath.join(cfg.directory, claim_name)
        self.metrics.messages_received += 1
        try:
            # lstat, not stat: a claim swapped for a symlink must not be followed (it would read a file the
            # sender never meant to send). A non-regular claim is refused like any other bad input.
            regular = await asyncio.to_thread(session.is_regular_file, claim)
            payload = await asyncio.to_thread(session.read_bounded, claim, cfg.max_message_bytes) if regular else b""
        except RemoteNotFoundError:
            return 0  # vanished under us: nothing to commit and nothing to lose
        if not regular:
            self.metrics.nacked_parse_error += 1
            await self._reject(session, claim, original, "not-regular-file")
            return 0
        if payload is None:
            self.metrics.nacked_oversize += 1
            await self._reject(session, claim, original, "oversize")
            return 0
        if not payload:
            self.metrics.nacked_parse_error += 1
            await self._reject(session, claim, original, "empty")
            return 0
        if "parse-error" in cfg.nack_on and not well_formed(cfg.content_type, payload):
            self.metrics.nacked_parse_error += 1
            await self._reject(session, claim, original, "parse-error")
            return 0
        outcome = resolve_key(
            cfg.key_expression, payload=payload, content_type=cfg.content_type, headers=None, header_source="kafka"
        )
        if outcome.reason is not None:
            self.metrics.nacked_parse_error += 1
            await self._reject(session, claim, original, "key-unresolved")
            return 0

        message = build_raw_envelope(
            channel=cfg.channel,
            channel_version=cfg.channel_version,
            source=f"sftp:{cfg.instance}@{cfg.host}",
            payload=payload,
            key=outcome.key,
            content_type=cfg.content_type,
        )
        msg_id = message.require(h.TB_MSG_ID)
        t0 = time.monotonic()
        try:
            ok = await self.publisher.publish(message, timeout_s=cfg.ack_timeout_s)
        except Exception:  # any failure is "not committed"; its text is never surfaced
            ok = False
        self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
        if not ok:
            self.metrics.publish_failures += 1
            self.metrics.nacked_unavailable += 1
            await self._unclaim(session, claim, posixpath.join(cfg.directory, original))
            _event("sftp_publish_refused", channel=cfg.channel, tb_msg_id=msg_id, bytes=len(payload))
            return None

        # Committed. Only now does the file leave the source directory.
        self.metrics.acked += 1
        try:
            await self._finish(session, claim, original)
        except SessionError as exc:
            # The record is committed but the file is still under our claim: it is re-ingested (a
            # duplicate, never a loss) when the claim is recovered next cycle.
            self.metrics.poll_errors += 1
            _event("sftp_after_commit_failed", channel=cfg.channel, tb_msg_id=msg_id, error=type(exc).__name__)
            return 1
        _event("sftp_committed", channel=cfg.channel, tb_msg_id=msg_id, bytes=len(payload), after=cfg.after_commit)
        return 1

    async def _finish(self, session: SftpSession, claim: str, original: str) -> None:
        if self.cfg.after_commit == "delete":
            await asyncio.to_thread(session.remove, claim)
        else:
            archive = self.cfg.archive_directory
            if archive is None:  # unreachable: SftpSettings refuses `move` without one
                raise ConfigError("afterCommit 'move' requires archiveDirectory")
            await self._move(session, claim, archive, original)

    async def _reject(self, session: SftpSession, claim: str, original: str, reason: str) -> None:
        try:
            await self._move(session, claim, self.cfg.reject_directory, original)
        except SessionError as exc:
            # Never delete a refused file. If it cannot be moved it stays under our claim and is retried.
            self.metrics.poll_errors += 1
            _event("sftp_reject_failed", channel=self.cfg.channel, reason=reason, error=type(exc).__name__)
            return
        _event("sftp_rejected", channel=self.cfg.channel, reason=reason)

    async def _move(self, session: SftpSession, claim: str, directory: str, original: str) -> None:
        """Move under the original name (kept: it is on the sender's server, README 3.2.4), or under a
        collision-free variant if that name is taken. Never overwrites."""
        target = posixpath.join(directory, original)
        if await asyncio.to_thread(session.exists, target):
            target = f"{target}.{uuid.uuid4().hex[:12]}"
        await asyncio.to_thread(session.rename, claim, target)

    async def _unclaim(self, session: SftpSession, claim: str, original_path: str) -> None:
        with contextlib.suppress(SessionError):  # still ours under the claim name: recovered next cycle
            await asyncio.to_thread(session.rename, claim, original_path)

    # ------------------------------------------------------------------ loop

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.poll_once()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self.cfg.poll_interval_s)


async def serve_from_channel_spec(
    spec: dict[str, Any],
    *,
    env: str,
    bootstrap_servers: str,
    instance: str = "sftp-0",
    secret_resolver: SecretResolver | None = None,
    metrics_file: str | None = None,
    engine_security_file: str | None = None,
) -> None:
    cfg = sftp_settings_from_channel_spec(spec, env=env, bootstrap_servers=bootstrap_servers, instance=instance)
    resolver = secret_resolver or DirSecretResolver()
    engine_security = engine_security_properties(cfg.bootstrap_servers, engine_security_file, resolver)
    topic = topic_name(cfg.env, cfg.channel)
    publisher = RawPublisher(
        cfg.bootstrap_servers,
        topic,
        security=engine_security,
        ordering="per-key" if cfg.key_expression is not None else "none",
        client_id_prefix="sftp-receiver",
    )
    publisher.start(asyncio.get_running_loop())
    metrics = Metrics()
    poller = SftpPoller(cfg, publisher, lambda: connect(cfg, resolver), metrics=metrics)
    stop = asyncio.Event()
    print(
        json.dumps({"event": "listening", "channel": cfg.channel, "topic": topic, "port": 0, "tls": False}), flush=True
    )
    try:
        task = asyncio.create_task(poller.run(stop))
        while not task.done():
            await asyncio.sleep(1)
            if metrics_file:
                with open(metrics_file, "w") as f:
                    json.dump({**metrics.snapshot(), "foreign_claims": poller.foreign_claims}, f)
        await task
    finally:
        stop.set()
        publisher.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel-spec", required=True)
    p.add_argument("--env", required=True)
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--instance", default="sftp-0")
    p.add_argument("--secrets-dir")
    p.add_argument("--metrics-file")
    p.add_argument(
        "--engine-security",
        help="JSON file (channel-spec $defs/kafkaSecurity shape) for the engine Kafka link; absent means "
        "plaintext, which is refused unless every --bootstrap host is loopback (PROVISIONAL, engine_kafka.py)",
    )
    a = p.parse_args()
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    with open(a.channel_spec, encoding="utf-8") as f:
        spec = json.load(f)
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else None
    coro = serve_from_channel_spec(
        spec,
        env=a.env,
        bootstrap_servers=a.bootstrap,
        instance=a.instance,
        secret_resolver=resolver,
        metrics_file=a.metrics_file,
        engine_security_file=a.engine_security,
    )
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        asyncio.run(coro)
    else:
        uvloop.run(coro)


if __name__ == "__main__":
    main()
