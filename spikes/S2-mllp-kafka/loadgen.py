"""S2 load generator (throwaway). Open-loop: sends at a fixed aggregate rate over N
connections, records send->ACK latency and every AA'd control id (for the loss check).

Messages are synthetic; MSH-3 = TRIBUTARY-SYNTH. When qa-engineer's corpus lands
(benchmarks.gen) pass --corpus <file of messages separated by blank lines>; until then
a tiny built-in synthetic ADT is used.

Usage: python loadgen.py --rate 1000 --conns 10 --seconds 30 [--tls] --out run.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import time
import uuid

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def synth(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|TB|SPIKE|20260101000000||ADT^A01|{cid}|P|2.5\r"
        "EVN|A01|20260101000000\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
        "PV1|1|I|WARD^1^1\r"
    ).encode()


def pct(v: list[float], q: float) -> float:
    return v[min(len(v) - 1, int(len(v) * q))] if v else float("nan")


async def conn_worker(
    a: argparse.Namespace, idx: int, interval: float, res: dict, ctx: ssl.SSLContext | None
) -> None:
    r, w = await asyncio.open_connection(a.host, a.port, ssl=ctx)
    sent: dict[str, float] = {}
    done = asyncio.Event()

    async def reader() -> None:
        buf = bytearray()
        while True:
            c = await r.read(65536)
            if not c:
                return
            buf += c
            while (e := buf.find(EB + CR)) != -1:
                frame = bytes(buf[1:e])
                del buf[: e + 2]
                now = time.perf_counter()
                msa = frame.split(b"\rMSA|")[1].split(b"|") if b"\rMSA|" in frame else [b"?", b""]
                code, cid = msa[0].decode(), msa[1].decode()
                t0 = sent.pop(cid, None)
                if code == "AA" and t0 is not None:
                    res["lat"].append((now - t0) * 1000)
                    res["acked"].append(cid)
                else:
                    res["nack"] += 1
                if done.is_set() and not sent:
                    return

    rt = asyncio.create_task(reader())
    start = time.perf_counter()
    n = 0
    end = start + a.seconds
    while (now := time.perf_counter()) < end:
        cid = f"{a.run_id}-{idx}-{n}"
        sent[cid] = time.perf_counter()
        w.write(SB + synth(cid) + EB + CR)
        res["sent"] += 1
        n += 1
        nxt = start + n * interval
        if nxt > now:
            await asyncio.sleep(nxt - now)
        else:
            await asyncio.sleep(0)
    done.set()
    try:
        await asyncio.wait_for(rt, a.drain_s)
    except TimeoutError:
        res["unanswered"] += len(sent)
    w.close()


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=2575)
    p.add_argument("--rate", type=float, default=1000)
    p.add_argument("--conns", type=int, default=10)
    p.add_argument("--seconds", type=float, default=30)
    p.add_argument("--drain-s", type=float, default=10)
    p.add_argument("--tls", action="store_true")
    p.add_argument("--insecure-selfsigned", action="store_true", help="spike only: skip verify of self-signed cert")
    p.add_argument("--run-id", default=uuid.uuid4().hex[:8])
    p.add_argument("--out", default="run.json")
    a = p.parse_args()
    ctx = None
    if a.tls:
        ctx = ssl.create_default_context()
        if a.insecure_selfsigned:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
    res = {"sent": 0, "nack": 0, "unanswered": 0, "lat": [], "acked": []}
    interval = a.conns / a.rate
    await asyncio.gather(*(conn_worker(a, i, interval, res, ctx) for i in range(a.conns)))
    lat = sorted(res["lat"])
    summary = {
        "rate": a.rate, "conns": a.conns, "tls": a.tls, "sent": res["sent"],
        "acked": len(res["acked"]), "nack": res["nack"], "unanswered": res["unanswered"],
        "p50_ms": pct(lat, .5), "p99_ms": pct(lat, .99), "p999_ms": pct(lat, .999),
        "max_ms": lat[-1] if lat else None,
    }
    print(json.dumps(summary))
    with open(a.out, "w") as f:
        json.dump({**summary, "acked_ids": res["acked"]}, f)


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
