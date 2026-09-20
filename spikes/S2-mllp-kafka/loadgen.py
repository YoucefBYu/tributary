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


def load_corpus(path: str) -> list[bytes]:
    """Payloads from an MLLP-framed corpus file (benchmarks.gen output)."""
    data = open(path, "rb").read()
    return [f[1:] for f in data.split(EB + CR) if len(f) > 1]


def with_cid(msg: bytes, cid: str) -> bytes:
    """Replace MSH-10 so every send is uniquely identifiable for the loss check."""
    end = msg.find(bytes([13]))
    f = msg[:end].split(b"|")
    f[9] = cid.encode()
    return b"|".join(f) + msg[end:]


def pct(v: list[float], q: float) -> float:
    return v[min(len(v) - 1, int(len(v) * q))] if v else float("nan")


async def conn_worker(
    a: argparse.Namespace, idx: int, interval: float, res: dict, ctx: ssl.SSLContext | None
) -> None:
    r, w = await asyncio.open_connection(a.host, a.port, ssl=ctx, server_hostname=a.server_hostname if ctx else None)
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
                    res["tl"].append((int(t0 - res["t_start"]), (now - t0) * 1000))
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
        body = with_cid(res["corpus"][(idx * 7919 + n) % len(res["corpus"])], cid) if res["corpus"] else synth(cid)
        w.write(SB + body + EB + CR)
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
    p.add_argument("--corpus", help="MLLP-framed corpus file; default built-in tiny ADT")
    p.add_argument("--warmup-s", type=int, default=3, help="excluded from 'steady' stats only")
    p.add_argument("--tls", action="store_true")
    p.add_argument("--cafile", help="trust this CA/self-signed cert (verification stays ON)")
    p.add_argument("--server-hostname", default=None)
    p.add_argument("--insecure-selfsigned", action="store_true", help="spike only: skip verify of self-signed cert")
    p.add_argument("--run-id", default=uuid.uuid4().hex[:8])
    p.add_argument("--out", default="run.json")
    a = p.parse_args()
    ctx = None
    if a.tls:
        ctx = ssl.create_default_context(cafile=a.cafile)
        if a.insecure_selfsigned:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
    res = {"corpus": load_corpus(a.corpus) if a.corpus else [], "sent": 0, "nack": 0, "unanswered": 0, "lat": [], "tl": [], "t_start": time.perf_counter(), "acked": []}
    interval = a.conns / a.rate
    await asyncio.gather(*(conn_worker(a, i, interval, res, ctx) for i in range(a.conns)))
    lat = sorted(res["lat"])
    summary = {
        "rate": a.rate, "conns": a.conns, "tls": a.tls, "sent": res["sent"],
        "acked": len(res["acked"]), "nack": res["nack"], "unanswered": res["unanswered"],
        "p50_ms": pct(lat, .5), "p99_ms": pct(lat, .99), "p999_ms": pct(lat, .999),
        "max_ms": lat[-1] if lat else None,
    }
    secs: dict[int, list[float]] = {}
    for t, l in res["tl"]:
        secs.setdefault(t, []).append(l)
    summary["per_second"] = [
        {"t": t, "n": len(v), "p50": round(pct(sorted(v), .5), 1), "p99": round(pct(sorted(v), .99), 1)}
        for t, v in sorted(secs.items())
    ]
    tail = sorted(l for t, l in res["tl"] if t >= a.warmup_s)
    summary["steady"] = {"warmup_s": a.warmup_s, "n": len(tail), "p50_ms": pct(tail, .5),
                         "p99_ms": pct(tail, .99), "p999_ms": pct(tail, .999)}
    print(json.dumps(summary))
    with open(a.out, "w") as f:
        json.dump({**summary, "corpus": a.corpus, "acked_ids": res["acked"]}, f)


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
