"""Deterministic synthetic HL7 v2 generator.

All data is invented. MSH-3 is always ``TRIBUTARY-SYNTH``. Stdlib only.

    python -m benchmarks.gen --count 10000 --seed 1 [--out PATH] [--format mllp|lines]

Output ``mllp`` (default): each message framed 0x0B <hl7 with \r segments> 0x1C 0x0D.
Output ``lines``: segments joined with newlines, messages separated by a blank line.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

SENDING_APP = "TRIBUTARY-SYNTH"
MESSAGE_TYPES = ("ADT^A01", "ADT^A03", "ADT^A08", "ORU^R01", "ORM^O01")
DEFAULT_MIX = (0.30, 0.20, 0.20, 0.20, 0.10)
EPOCH = datetime(2026, 1, 1, 0, 0, 0)

_FAMILY = ["Testerson", "Syntheton", "Fakeman", "Placeholder", "Mockwood", "Dummyfield", "Sampleby", "Nulliston"]
_GIVEN = ["Alex", "Blair", "Casey", "Drew", "Emery", "Finley", "Gray", "Harper", "Indigo", "Jules"]
_STREETS = ["1 Example Way", "22 Test Lane", "300 Synthetic Ave", "4 Sandbox Rd"]
_CITIES = [("Faketown", "ZZ", "00000"), ("Mockville", "ZZ", "00001"), ("Testburg", "ZZ", "00002")]
_UNITS = ["ICU", "MED", "SURG", "ED", "PEDS"]
_LABS = [
    ("718-7", "Hemoglobin", "g/dL", (8.0, 18.0)),
    ("2345-7", "Glucose", "mg/dL", (60.0, 300.0)),
    ("2951-2", "Sodium", "mmol/L", (125.0, 150.0)),
    ("6690-2", "Leukocytes", "10*3/uL", (2.0, 20.0)),
    ("2160-0", "Creatinine", "mg/dL", (0.4, 3.0)),
]
_DX = [("Z00.00", "Synthetic exam"), ("R50.9", "Synthetic fever"), ("J06.9", "Synthetic URI")]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _msh(mtype: str, ctrl: str, now: datetime) -> str:
    return (
        f"MSH|^~\&|{SENDING_APP}|SYNTH-FAC|TRIBUTARY-DST|DST-FAC|{_ts(now)}||{mtype}|{ctrl}|P|2.5.1"
    )


def _patient(rng: random.Random, n: int) -> dict[str, str]:
    dob = datetime(1930, 1, 1) + timedelta(days=rng.randrange(0, 30000))
    city, st, zipc = rng.choice(_CITIES)
    return {
        "mrn": f"SYN{n:08d}",
        "family": rng.choice(_FAMILY),
        "given": rng.choice(_GIVEN),
        "dob": dob.strftime("%Y%m%d"),
        "sex": rng.choice("MFU"),
        "street": rng.choice(_STREETS),
        "city": city,
        "state": st,
        "zip": zipc,
        "phone": f"555-01{rng.randrange(0, 100):02d}",
    }


def _pid(p: dict[str, str]) -> str:
    return (
        f"PID|1||{p['mrn']}^^^SYNTHMRN^MR||{p['family']}^{p['given']}||{p['dob']}|{p['sex']}"
        f"|||{p['street']}^^{p['city']}^{p['state']}^{p['zip']}||{p['phone']}"
    )


def _pv1(rng: random.Random, visit: int, now: datetime, discharge: bool) -> str:
    unit = rng.choice(_UNITS)
    out = _ts(now) if discharge else ""
    return (
        f"PV1|1|I|{unit}^{rng.randrange(1, 40):03d}^{rng.choice('AB')}||||"
        f"D{rng.randrange(100, 999)}^Doctor^Synthetic|||MED||||||||V{visit:08d}|||||||||||||||||||||||||{_ts(now)}|{out}"
    )


def generate_message(rng: random.Random, index: int, mtype: str) -> str:
    now = EPOCH + timedelta(seconds=index * 7 + rng.randrange(0, 7))
    ctrl = f"SYN{index:09d}"
    p = _patient(rng, rng.randrange(1, 5000))
    visit = rng.randrange(1, 100000)
    segs = [_msh(mtype, ctrl, now)]
    if mtype.startswith("ADT"):
        segs.append(f"EVN|{mtype.split('^')[1]}|{_ts(now)}")
        segs.append(_pid(p))
        segs.append(_pv1(rng, visit, now, mtype == "ADT^A03"))
        if mtype != "ADT^A03":
            for i in range(1, rng.randrange(1, 4)):
                code, text = rng.choice(_DX)
                segs.append(f"DG1|{i}||{code}^{text}^I10||{_ts(now)}|F")
            segs.append(f"NK1|1|{rng.choice(_FAMILY)}^{rng.choice(_GIVEN)}|SPO|{p['street']}^^{p['city']}^{p['state']}^{p['zip']}|555-0199")
    elif mtype == "ORU^R01":
        segs.append(_pid(p))
        segs.append(_pv1(rng, visit, now, False))
        segs.append(f"OBR|1|ORD{index:08d}|FIL{index:08d}|24323-8^Synthetic panel^LN|||{_ts(now)}")
        for i in range(1, rng.randrange(2, 9)):
            code, name, unit, (lo, hi) = rng.choice(_LABS)
            val = round(rng.uniform(lo, hi), 1)
            flag = "N" if lo * 1.1 < val < hi * 0.9 else "A"
            segs.append(f"OBX|{i}|NM|{code}^{name}^LN||{val}|{unit}|{lo}-{hi}|{flag}|||F")
        if rng.random() < 0.2:
            segs.append(f"NTE|1||Synthetic comment {index}")
    else:  # ORM^O01
        segs.append(_pid(p))
        segs.append(_pv1(rng, visit, now, False))
        segs.append(f"ORC|NW|ORD{index:08d}||GRP{index % 1000:04d}|SC")
        segs.append(f"OBR|1|ORD{index:08d}||24323-8^Synthetic panel^LN|||{_ts(now)}")
    return "\r".join(segs) + "\r"


def generate(count: int, seed: int, mix: tuple[float, ...] = DEFAULT_MIX) -> Iterator[str]:
    rng = random.Random(seed)
    for i in range(count):
        mtype = rng.choices(MESSAGE_TYPES, weights=mix)[0]
        yield generate_message(rng, i, mtype)


def frame_mllp(msg: str) -> bytes:
    return b"\x0b" + msg.encode("ascii") + b"\x1c\x0d"


def load_corpus(path: Path) -> list[bytes]:
    """Read an MLLP-framed corpus back into raw HL7 payloads (\r-separated segments)."""
    data = path.read_bytes()
    return [f[1:] for f in data.split(b"\x1c\x0d") if f]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--count", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", type=Path, default=Path("benchmarks/results/corpus-{seed}-{count}.mllp"))
    ap.add_argument("--format", choices=("mllp", "lines"), default="mllp")
    args = ap.parse_args(argv)
    out = Path(str(args.out).format(seed=args.seed, count=args.count))
    out.parent.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256()
    with out.open("wb") as f:
        for msg in generate(args.count, args.seed):
            if args.format == "mllp":
                b = frame_mllp(msg)
            else:
                b = (msg.replace("\r", "\n") + "\n").encode("ascii")
            h.update(b)
            f.write(b)
    print(f"wrote {args.count} messages to {out} sha256={h.hexdigest()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
