"""S6 throwaway corpus analyzer. Usage: python analyze.py <dir-with-channel-xml>

Emits aggregate counts only. Never prints property values, hostnames or script text,
so output is safe to paste into a report. Feature detection is regex-heuristic.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET

FEATURES = {
    "e4x": re.compile(r"\bnew\s+XML\s*\(|\bXML\s*\(|<\w+>.*</\w+>|\.\.\w+|@\w+|\bnamespace\s*\(", re.S),
    "java_call": re.compile(r"\b(Packages\.|java\.|javax\.|org\.apache|new\s+Packages|Java\.type|\.getClass\()"),
    "db_lookup": re.compile(r"DatabaseConnectionFactory|createDatabaseConnection|executeCachedQuery|\.executeUpdate\(|\bDriverManager\b"),
    "global_map": re.compile(r"\bglobalMap\b|\bglobalChannelMap\b|\bconfigurationMap\b|\$g\(|\$gc\(|\$cfg\("),
    "channel_map": re.compile(r"\bchannelMap\b|\bconnectorMap\b|\bsourceMap\b|\bresponseMap\b|\$c\(|\$co\(|\$s\(|\$r\("),
    "hl7_access": re.compile(r"\bmsg\b\s*[\[.]|\btmp\b\s*[\[.]"),
    "http_call": re.compile(r"HttpUtil|HTTPUtil|XMLHttpRequest|\bhttpClient\b"),
    "file_io": re.compile(r"FileUtil|java\.io\.File"),
    "router": re.compile(r"router\.routeMessage|VMRouter"),
    "attachments": re.compile(r"AttachmentUtil|addAttachment|getAttachments"),
    "logger": re.compile(r"\blogger\.(info|warn|error|debug)"),
}


def loc(text: str) -> int:
    return sum(1 for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("//"))


def short(tag_or_cls: str | None) -> str:
    return (tag_or_cls or "?").rsplit(".", 1)[-1]


def scripts(chan: ET.Element) -> list[str]:
    """Every element text that looks like a script body (script/ pre/post/deploy fields)."""
    out: list[str] = []
    for el in chan.iter():
        if el.tag in {"script", "preprocessingScript", "postprocessingScript",
                      "deployScript", "undeployScript"} and (el.text or "").strip():
            out.append(el.text or "")
    return out


def analyze(root: Path) -> dict[str, object]:
    conn_src: Counter[str] = Counter()
    conn_dst: Counter[str] = Counter()
    feats: Counter[str] = Counter()
    locs: list[int] = []
    clean = 0
    skipped: Counter[str] = Counter()
    n = 0
    for p in sorted(root.rglob("*.xml")):
        try:
            tree = ET.parse(p)
        except ET.ParseError:
            skipped["skipped_unparseable"] += 1
            continue
        chan = tree.getroot()
        if chan.tag != "channel":
            skipped["skipped_non_channel"] += 1
            continue
        n += 1
        src = chan.find("sourceConnector/properties")
        if src is not None:
            conn_src[short(src.get("class"))] += 1
        for d in chan.findall("destinationConnectors/connector"):
            dp = d.find("properties")
            conn_dst[short(dp.get("class") if dp is not None else None)] += 1
        bodies = [b for b in scripts(chan) if not b.strip().startswith("return;")]
        total = sum(loc(b) for b in bodies)
        locs.append(total)
        seen = {name for name, rx in FEATURES.items() for b in bodies if rx.search(b)}
        feats.update(seen)
        # "no manual work" = no non-trivial script body and only connector types the importer supports
        # aligned with importer.py: only the TCP receiver is supported, any destination needs work
        supported = {"TcpReceiverProperties"}
        kinds = set(conn_src_of(chan)) | set(conn_dst_of(chan))
        if total == 0 and kinds <= supported:
            clean += 1
    locs.sort()
    pct = lambda q: locs[int(q * (len(locs) - 1))] if locs else 0  # noqa: E731
    return {
        "channels": n,
        "source_connectors": dict(conn_src),
        "destination_connectors": dict(conn_dst),
        "script_loc": {"p50": pct(0.5), "p90": pct(0.9), "max": locs[-1] if locs else 0,
                       "total": sum(locs)},
        "feature_channels": dict(feats),
        "no_manual_work": clean,
        "skipped_unparseable": skipped["skipped_unparseable"],
        "skipped_non_channel": skipped["skipped_non_channel"],
    }


def conn_src_of(chan: ET.Element) -> list[str]:
    p = chan.find("sourceConnector/properties")
    return [short(p.get("class"))] if p is not None else []


def conn_dst_of(chan: ET.Element) -> list[str]:
    out = []
    for d in chan.findall("destinationConnectors/connector"):
        p = d.find("properties")
        out.append(short(p.get("class") if p is not None else None))
    return out


if __name__ == "__main__":
    print(json.dumps(analyze(Path(sys.argv[1])), indent=2, sort_keys=True))
