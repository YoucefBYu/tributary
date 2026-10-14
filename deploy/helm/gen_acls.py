"""Generate deploy/helm/tributary/files/acls.json from contracts/topics/v0/topic-defaults.json.

The chart's KafkaUser ACLs are never hand-written (the set 1.4 lesson): this script reuses the parser
the Compose lite stack uses (`deploy/compose/topic-bootstrap/bootstrap_topics.py parse_grants`) with
sentinel placeholders, and the chart substitutes env, channel and destination at render time.

    python deploy/helm/gen_acls.py           # rewrite the file
    python deploy/helm/gen_acls.py --check   # CI drift check: exit 1 if the file is stale
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULTS = ROOT / "contracts/topics/v0/topic-defaults.json"
OUT = ROOT / "deploy/helm/tributary/files/acls.json"
SYNC_PREFIX = "Set 1.2, sync-reply channels only: "
SYNC_PREFIX = "Set 1.2, sync-reply channels only: "
ENV, CH, DEST = "@ENV@", "@CHANNEL@", "@DEST@"
# chart role -> security.acls key
ROLES = {"receiver": "receiver", "worker": "pipeline_worker", "loader": "loader", "archiver": "archiver", "api": "api"}
# Placeholder until the architect adds `lag_reader` to the contract (W3-29): DESCRIBE only, topics
# PREFIXED `<env>.` and LITERAL `_tb.<env>.events`, groups PREFIXED `tb.<env>.`. No READ, no WRITE.
LAG_READER_PLACEHOLDER = "DESCRIBE on <env>.*, _tb.<env>.events; DESCRIBE on group tb.<env>.*"


def _parser():  # type: ignore[no-untyped-def]
    path = ROOT / "deploy/compose/topic-bootstrap/bootstrap_topics.py"
    spec = importlib.util.spec_from_file_location("bootstrap_topics", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load bootstrap_topics.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bootstrap_topics"] = mod  # dataclass(slots=True) needs the module registered
    spec.loader.exec_module(mod)
    return mod


def _row(g) -> dict[str, object]:  # type: ignore[no-untyped-def]
    return {"resourceType": g.resource_type, "name": g.name, "patternType": g.pattern, "operation": g.operation}


def build() -> dict[str, object]:
    raw = DEFAULTS.read_bytes()
    acls = json.loads(raw)["security"]["acls"]
    bt = _parser()
    roles: dict[str, list[dict[str, object]]] = {}
    sources = {r: (k, acls[k]) for r, k in ROLES.items()}
    placeholder = "lag_reader" not in acls
    sources["lag_reader"] = ("lag_reader", LAG_READER_PLACEHOLDER if placeholder else acls["lag_reader"])
    for role, (_key, text) in sources.items():
        grants = bt.parse_grants(text, env=ENV, channel=CH, destinations=(DEST,))
        rows: list[dict[str, object]] = [_row(g) for g in grants]
        # Set 1.2 sync-reply additions: parse_grants skips that sentence, so parse its body here. Flagged
        # syncReplyOnly: the chart grants them only on channels with `syncReply: true`.
        for sentence in re.split(r"\.\s+", text.strip().rstrip(".")):
            if sentence.startswith(SYNC_PREFIX):
                body = sentence[len(SYNC_PREFIX) :].replace(
                    " and READ on ", "; READ on "
                )  # the sentence joins clauses with "and"
                rows += [
                    {**_row(g), "syncReplyOnly": True}
                    for g in bt.parse_grants(body, env=ENV, channel=CH, destinations=(DEST,))
                ]
        roles[role] = sorted(rows, key=lambda g: tuple(str(v) for v in g.values()))
    defaults = json.loads(raw)
    topics = {  # `_tb.<env>.<kind>`: events (3 d) and audit (30 d buffer, ADR-004 R7); every value comes from the contract
        kind: {
            "partitions": defaults["classes"][kind]["partitions"],
            "replicas": defaults["profiles"]["production"]["replication_factor"],
            "config": {k: str(v) for k, v in defaults["classes"][kind]["configs"].items()},
        }
        for kind in ("events", "audit")
    }
    return {
        "topics": topics,
        "generatedFrom": "contracts/topics/v0/topic-defaults.json",
        "sourceSha256": hashlib.sha256(raw).hexdigest(),
        "lagReaderIsPlaceholder": placeholder,
        "placeholders": {"env": ENV, "channel": CH, "destination": DEST},
        "roles": roles,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    text = json.dumps(build(), indent=2) + "\n"
    if ap.parse_args().check:
        if not OUT.exists() or OUT.read_text() != text:
            print("FAIL: deploy/helm/tributary/files/acls.json is stale; run python deploy/helm/gen_acls.py")
            return 1
        print("gen_acls: up to date")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
