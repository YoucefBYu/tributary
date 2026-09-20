"""S2 attribution probe (throwaway): commit latency of a bare idempotent acks=all producer
at a fixed rate, no MLLP. Separates broker/VM latency from server overhead."""
import json, sys, time
from confluent_kafka import Producer

rate, secs, linger, topic = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
p = Producer({"bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092", "enable.idempotence": True,
              "acks": "all", "linger.ms": linger, "socket.keepalive.enable": True})
lat: list[tuple[float, float]] = []
t0 = time.perf_counter()
def mk(ts):
    def cb(err, _m):
        if err is None:
            lat.append((ts - t0, (time.perf_counter() - ts) * 1000))
    return cb
n, msg = 0, b"x" * 465
while (now := time.perf_counter()) < t0 + secs:
    p.produce(topic, msg, key=str(n).encode(), on_delivery=mk(now)); n += 1
    p.poll(0)
    nxt = t0 + n / rate
    while time.perf_counter() < nxt:
        p.poll(0.0005)
p.flush(20)
v = sorted(l for t, l in lat if t >= 3)
q = lambda x: v[int(len(v) * x)]
print(json.dumps({"rate": rate, "linger": linger, "n": len(v), "p50": round(q(.5), 1), "p99": round(q(.99), 1), "p999": round(q(.999), 1)}))
