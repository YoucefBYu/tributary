"""Minimal synthetic payload builder.

PLACEHOLDER. `qa-engineer` owns the real generator (P0-1,
`benchmarks.gen`). This exists only so P0-2's smoke test and the S5 harness
can run before P0-1 lands; swap it out once the corpus exists.

Synthetic data only. MSH-3 is always TRIBUTARY-SYNTH. Names and identifiers
are invented and obviously fake.
"""

from __future__ import annotations

import random

SENDING_APPLICATION = "TRIBUTARY-SYNTH"

_FAMILY = ("TESTPATIENT", "SYNTHCASE", "SAMPLEONE", "DEMOFILE", "FIXTUREX")
_GIVEN = ("ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO")
_EVENTS = ("A01", "A03", "A08")


def patient_key(index: int) -> str:
    """Deterministic fake MRN, used as the Kafka message key (ordering key)."""
    return f"SYNTH-MRN-{index:06d}"


def adt(key: str, control_id: str, *, event: str = "A01", rng: random.Random | None = None) -> bytes:
    """A small, well-formed-enough ER7 ADT message. Segments use \\r, per HL7."""
    r = rng or random.Random(hash(key) & 0xFFFF_FFFF)
    family = r.choice(_FAMILY)
    given = r.choice(_GIVEN)
    segments = [
        f"MSH|^~\\&|{SENDING_APPLICATION}|SYNTH-FACILITY|TB-SPIKE|TB-LAB|"
        f"20260101120000||ADT^{event}|{control_id}|P|2.5.1",
        f"EVN|{event}|20260101120000",
        f"PID|1||{key}^^^SYNTH^MR||{family}^{given}||19700101|M|||"
        f"1 SYNTH STREET^^SYNTHVILLE^ZZ^00000",
        f"PV1|1|I|SYNTHWARD^01^01||||SYNTHDOC^DOCTOR^SYNTH|||MED||||||||V{control_id}",
    ]
    return "\r".join(segments).encode("ascii")


def stream(count: int, keys: int, *, seed: int = 1) -> list[tuple[str, int, bytes]]:
    """``count`` messages spread over ``keys`` ordering keys.

    Returns ``(key, seq, payload)`` where ``seq`` is monotonic **per key**.
    That per-key sequence is what the ordering property tests assert on.
    """
    rng = random.Random(seed)
    per_key: dict[str, int] = {}
    out: list[tuple[str, int, bytes]] = []
    for i in range(count):
        key = patient_key(rng.randrange(keys))
        seq = per_key.get(key, 0)
        per_key[key] = seq + 1
        event = _EVENTS[seq % len(_EVENTS)]
        out.append((key, seq, adt(key, f"{i:08d}", event=event, rng=rng)))
    return out
