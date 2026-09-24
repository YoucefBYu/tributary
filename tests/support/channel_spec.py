"""A runnable copy of the walking-skeleton channel-spec example for tests.

`contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json` hardcodes an MLLP port
(6661) and an HTTP destination (`http://127.0.0.1:8080/ingest`) that only make sense for the
devops-owned compose stack; tests need their own free ports so parallel runs never collide. The
example's `pipeline.steps` is left untouched: it resolves `uses: tributary.core.passthrough`, a real
`tributary.steps` entry point registered by `tributary-runtime`
(architect/platform-engineer, 2026-09-23, replacing an earlier `ref` at a bundle module nothing
published) — the same step name `deploy/compose/docker-compose.yml`'s worker resolves, so a test using
this document exercises the real step-resolution path, not a stand-in for it.
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


def walking_skeleton_document(
    *,
    mllp_port: int,
    http_url: str,
    channel: str = "adt-passthrough",
    destination: str = "http-stub",
    version: int = 1,
) -> dict[str, Any]:
    """The contract example, retargeted at test-owned ports. `pipeline.steps` is not touched — see
    the module docstring.

    `metadata.version` is stamped here rather than left absent: `tributary.runtime.spec.ChannelSpec`
    requires it even though the schema makes it optional for an authored file (only the *published*
    form carries it) — see that module's docstring, section 6, rule 1.
    """
    document: dict[str, Any] = copy.deepcopy(json.loads(WALKING_SKELETON_EXAMPLE.read_text(encoding="utf-8")))
    document["metadata"]["name"] = channel
    document["metadata"]["version"] = version
    document["spec"]["source"]["port"] = mllp_port
    document["spec"]["destinations"][0]["name"] = destination
    document["spec"]["destinations"][0]["with"]["url"] = http_url
    return document


def ordered_document(
    *,
    mllp_port: int,
    http_url: str,
    channel: str = "adt-passthrough-ordered",
    destination: str = "http-stub",
    version: int = 1,
    partition_key: str = "hl7:PID.3.1",
) -> dict[str, Any]:
    """The same walking-skeleton shape, but `ordering: per-key`.

    The MLLP receiver refuses `per-key` channels in this wave (no `keyExpression` evaluator yet --
    `tributary.receivers.mllp.config`, W2-4) -- so a test using this document must publish directly to
    `raw` (`tests.support.kafka_helpers.publish_messages`) with its own record key rather than run a
    receiver against it. The worker and loader do not care where the key came from; they only need
    `spec.ordering` and `spec.partitionKey` to be set so `check_partition_counts` and
    `validate_for_produce` apply the per-key rules W2-9's chaos suite is testing.
    """
    document = walking_skeleton_document(
        mllp_port=mllp_port, http_url=http_url, channel=channel, destination=destination, version=version
    )
    document["spec"]["ordering"] = "per-key"
    document["spec"]["partitionKey"] = partition_key
    return document


def write_channel_spec_file(path: Path, document: Mapping[str, Any]) -> Path:
    path.write_text(json.dumps(document), encoding="utf-8")
    return path
