"""A runnable copy of the walking-skeleton channel-spec example for tests.

`contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json` hardcodes an MLLP port
(6661), an HTTP destination (`http://127.0.0.1:8080/ingest`) and a step ref
(`adt_flow.steps.passthrough`) that only make sense for the devops-owned compose stack. Tests need
their own free ports (parallel runs collide otherwise) and a step ref that actually resolves — see
`tests/support/demo_steps.py` for why the contract example's own ref cannot be used yet.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
WALKING_SKELETON_EXAMPLE = (
    CONTRACTS_DIR / "channel-spec" / "v0" / "examples" / "valid" / "walking-skeleton-loopback.json"
)

STEP_REF = "tests.support.demo_steps.passthrough"


def walking_skeleton_document(
    *,
    mllp_port: int,
    http_url: str,
    channel: str = "adt-passthrough",
    destination: str = "http-stub",
    version: int = 1,
) -> dict[str, Any]:
    """The contract example, retargeted at test-owned ports and a resolvable step.

    `metadata.version` is stamped here rather than left absent: `tributary.runtime.spec.ChannelSpec`
    requires it even though the schema makes it optional for an authored file (only the *published*
    form carries it) — see that module's docstring, section 6, rule 1.
    """
    document: dict[str, Any] = copy.deepcopy(json.loads(WALKING_SKELETON_EXAMPLE.read_text(encoding="utf-8")))
    document["metadata"]["name"] = channel
    document["metadata"]["version"] = version
    document["spec"]["source"]["port"] = mllp_port
    document["spec"]["pipeline"]["steps"] = [{"name": "passthrough", "ref": STEP_REF}]
    document["spec"]["destinations"][0]["name"] = destination
    document["spec"]["destinations"][0]["with"]["url"] = http_url
    return document


def write_channel_spec_file(path: Path, document: Mapping[str, Any]) -> Path:
    path.write_text(json.dumps(document), encoding="utf-8")
    return path
