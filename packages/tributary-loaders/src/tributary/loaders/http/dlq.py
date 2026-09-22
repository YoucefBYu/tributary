"""`onExhaust: dlq-and-continue`: after `retry.maxAttempts`, publish to `<env>.<channel>.dlq` and
commit past the message rather than halting the partition.

The DLQ record keeps the original message's `tb-msg-id` (and so its key, per the envelope
contract's per-class key identity rule: `dlq` is keyed by `tb-msg-id`) and headers, adding
`tb-stage: dlq` and `tb-error-code`, and dropping `tb-retry-not-before` (forbidden for this class;
blocking retry never sets it in this wave, but a hand-built message might). The DLQ write is
awaited and must succeed before the caller commits the original offset — a crash between the two
must re-deliver from `out.<dest>`, not lose the record (the same durability discipline as invariant
1, applied to the loader's own retry-exhaustion path rather than the receiver's ACK).
"""

from __future__ import annotations

import asyncio
import threading

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.projection import validate_for_produce


def build_dlq_envelope(original: Message, *, error_code: str, final_attempt: int) -> Message:
    """`error_code` is always a short fixed code this package chooses (`HTTP_5XX`, `HTTP_TIMEOUT`,
    ...), never response body text or an exception message — the envelope contract's `tb-error-code`
    pattern (`^[A-Z][A-Z0-9_]{0,63}$`) enforces the shape; PHI-safety is why the value is fixed."""
    message = (
        original.with_attempt(final_attempt)
        .with_header(h.TB_STAGE, "dlq")
        .with_header(h.TB_ERROR_CODE, error_code)
        .without_header(h.TB_RETRY_NOT_BEFORE)
        .with_key(original.msg_id.encode("utf-8"))
    )
    validate_for_produce(message, topic_class="dlq")
    return message


class DlqPublisher:
    """Awaitable produce, `acks=all` idempotent, resolves `True` only on a successful delivery
    report — the same shape as `tributary-receivers.mllp.kafka_publisher.RawPublisher`, reimplemented
    here rather than shared because there is no common home for it yet (see `secrets.py`'s note)."""

    def __init__(
        self, bootstrap_servers: str, topic: str, *, linger_ms: int = 2, delivery_timeout_ms: int = 5000
    ) -> None:
        from confluent_kafka import Producer  # lazy: unit tests need no broker client

        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": linger_ms,
                "message.timeout.ms": delivery_timeout_ms,
            }
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll, daemon=True, name="dlq-publisher-poll")

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._poller.start()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.005)

    def close(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._producer.flush(timeout_s)

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        loop = self._loop
        if loop is None:
            raise RuntimeError("DlqPublisher.start() must run before publish() is awaited")
        future: asyncio.Future[bool] = loop.create_future()

        def on_delivery(err: object, _msg: object) -> None:
            def set_result() -> None:
                if not future.done():
                    future.set_result(err is None)

            loop.call_soon_threadsafe(set_result)

        kafka_headers: list[tuple[str, str | bytes | None]] = [
            (name, value.encode("utf-8")) for name, value in message.headers.items()
        ]
        try:
            self._producer.produce(
                self.topic, value=message.payload, key=message.key, headers=kafka_headers, on_delivery=on_delivery
            )
        except BufferError:
            return False
        try:
            return await asyncio.wait_for(future, timeout_s)
        except TimeoutError:
            return False
