"""Unit tests for S3 reply framing (no Kafka). Run from spikes/S2-mllp-kafka:
uv run pytest ../S3-sync-reply/test_sync_reply.py"""

import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "S2-mllp-kafka"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

from server import new_msg_id  # noqa: E402
from sync_server import frame_reply  # noqa: E402


def test_frame_reply_wraps_clean_reply() -> None:
    assert frame_reply(b"MSH|x\rMSA|AA|1") == b"\x0bMSH|x\rMSA|AA|1\x1c\x0d"


@pytest.mark.parametrize("bad", [b"MSA|AA\x1c\x0bMSH|forged", b"a\x0bb", b"a\x1cb"])
def test_frame_reply_rejects_embedded_framing_bytes(bad: bytes) -> None:
    assert frame_reply(bad) is None


def test_msg_id_is_canonical_uuid7_text() -> None:
    a, b = new_msg_id(), new_msg_id()
    u = uuid.UUID(a.decode("utf-8"))
    assert str(u) == a.decode() and u.version == 7 and u.variant == uuid.RFC_4122
    assert a != b


def test_render_turns_forged_reply_into_ar_and_counts_it() -> None:
    from server import Settings
    from sync_server import SyncServer

    srv = SyncServer(Settings(), None, 1.0, 10, "AE", None)  # type: ignore[arg-type]
    good, ok = srv.render("C1", b"MSH|x\rMSA|AA|C1")
    assert ok and good.startswith(b"\x0b")
    bad, ok = srv.render("C1", b"MSA|AA|C1\x1c\x0d\x0bMSH|forged")
    assert not ok and b"forged" not in bad and b"MSA|AR|C1" in bad
    assert srv.sstats.bad_replies == 1
