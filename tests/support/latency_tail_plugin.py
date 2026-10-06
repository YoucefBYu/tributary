"""pytest plugin that records every sender-observed exchange time of the MLLP sync-reply latency test, so
the tail (p90, share over 100 ms and 200 ms) can be reported without editing the connector-engineer's
test. Load it with `-p tests.support.latency_tail_plugin` and `PYTHONPATH=<repo root>`.

It wraps `Sender.exchange` from `sync_reply_rig` with the same `time.perf_counter()` bracket the test
itself uses around that call, and writes the post-warm-up samples (the first 10 exchanges are the test's
own warm-up) to `$TB_LATENCY_SAMPLES_FILE` as one JSON list of milliseconds. Control ids only: nothing
else is recorded (invariant 6).
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import pytest

WARMUP_EXCHANGES = 10


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Any:
    rig = next((m for name, m in list(sys.modules.items()) if name.endswith("sync_reply_rig")), None)
    path = os.environ.get("TB_LATENCY_SAMPLES_FILE")
    if rig is None or not path or "latency_is_reported" not in item.name:
        return (yield)
    sender_class = rig.Sender
    original = sender_class.exchange
    samples: list[float] = []

    def timed(self: Any, control_id: str, timeout_s: float = 60.0) -> Any:
        started = time.perf_counter()
        ack = original(self, control_id, timeout_s)
        samples.append((time.perf_counter() - started) * 1000)
        return ack

    sender_class.exchange = timed
    try:
        return (yield)
    finally:
        sender_class.exchange = original
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(samples[WARMUP_EXCHANGES:], handle)
