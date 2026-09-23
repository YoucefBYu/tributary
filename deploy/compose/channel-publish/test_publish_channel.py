"""`published()` is pure and unit-tested without a broker; `main` is the only part that touches Kafka."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from publish_channel import published

WALKING_SKELETON = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "channel-spec"
    / "v0"
    / "examples"
    / "valid"
    / "walking-skeleton-loopback.json"
)


def test_published_stamps_metadata_version_without_mutating_the_input() -> None:
    import json

    document = json.loads(WALKING_SKELETON.read_text(encoding="utf-8"))
    assert "version" not in document["metadata"]  # the authored form has none

    result = published(document, version=1)

    assert result["metadata"]["version"] == 1
    assert result["metadata"]["name"] == "adt-passthrough"
    assert "version" not in document["metadata"]  # the caller's dict is untouched


def test_published_preserves_the_rest_of_the_document() -> None:
    import json

    document = json.loads(WALKING_SKELETON.read_text(encoding="utf-8"))
    result = published(document, version=3)
    assert result["spec"] == document["spec"]
    assert result["apiVersion"] == document["apiVersion"]
