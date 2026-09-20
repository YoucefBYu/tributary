"""Server-level ACK tests for S2 (framing tests live in test_mllp.py). No Kafka, no PHI (synthetic ids)."""

from __future__ import annotations

import asyncio

from mllp import EB_CR, SB
from server import Server, Settings

MSG = b"MSH|^~\\&|TRIBUTARY-SYNTH|X|||20260101||ADT^A01|CID1|P|2.5\rPID|1\r"


def framed(m: bytes = MSG) -> bytes:
    return SB + m + EB_CR


def run(*a, **k):  # noqa: ANN
    return asyncio.run(_roundtrip(*a, **k))


class Stub:
    def __init__(self, ok: bool = True, delay: float = 0.0) -> None:
        self.ok, self.delay, self.calls = ok, delay, 0

    async def __call__(self, payload: bytes, cid: str) -> bool:
        self.calls += 1
        await asyncio.sleep(self.delay)
        return self.ok


async def _roundtrip(pub: Stub, data: bytes, cfg: Settings | None = None, n_acks: int = 1) -> list[bytes]:
    cfg = cfg or Settings(port=0, host="127.0.0.1")
    srv = Server(cfg, pub)
    s = await srv.start()
    port = s.sockets[0].getsockname()[1]
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(data)
    buf, acks = b"", []
    try:
        while len(acks) < n_acks:
            buf += await asyncio.wait_for(r.read(4096), 5)
            while (e := buf.find(EB_CR)) != -1:
                acks.append(buf[1:e])
                buf = buf[e + 2 :]
    finally:
        w.close()
        s.close()
    return acks


def test_aa_only_after_commit_verdict() -> None:
    acks = run(Stub(ok=True), framed())
    assert b"MSA|AA|CID1" in acks[0]


def test_failed_commit_is_ar_never_aa() -> None:
    acks = run(Stub(ok=False), framed())
    assert b"MSA|AR|CID1" in acks[0] and b"MSA|AA" not in acks[0]


def test_publish_timeout_is_ar() -> None:
    cfg = Settings(port=0, host="127.0.0.1", ack_timeout_s=0.1)
    acks = run(Stub(ok=True, delay=1.0), framed(), cfg)
    assert b"MSA|AR|CID1" in acks[0]


def test_oversize_gets_ar_and_is_not_published() -> None:
    pub = Stub()
    cfg = Settings(port=0, host="127.0.0.1", max_msg=10)
    acks = run(pub, framed(), cfg)
    assert b"MSA|AR|CID1" in acks[0] and pub.calls == 0


def test_acks_in_arrival_order_when_commits_finish_out_of_order() -> None:
    class Varied(Stub):
        async def __call__(self, payload: bytes, cid: str) -> bool:
            await asyncio.sleep(0.2 if cid == "A" else 0.0)
            return True

    def m(c: str) -> bytes:
        return framed(MSG.replace(b"CID1", c.encode()))

    acks = run(Varied(), m("A") + m("B"), n_acks=2)
    assert b"MSA|AA|A|" in acks[0] and b"MSA|AA|B|" in acks[1]
