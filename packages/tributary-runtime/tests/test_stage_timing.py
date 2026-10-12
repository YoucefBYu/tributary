"""W3-24 stage timestamps: off by default, one id-only line per answered record when on."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from itertools import count
from types import SimpleNamespace
from typing import Any

import pytest
from tributary.runtime.logs import JsonLogger
from tributary.runtime.stage_timing import StageTimer, enabled_from_env
from tributary.sdk import Message, step

from .fakes import FakeConsumer, FakeRecord
from .log_line_schema import assert_lines_conform
from .reply_harness import (
    CONTRACTS,
    CONTROL_ID_MARKER,
    KEY_MARKER,
    RAW,
    REQUEST_PAYLOAD,
    make_spec,
    rig,
    sync_request,
    to_raw_headers,
)

RAW_TS_MS = 1_760_000_000_123


@step
def passthrough(message: Message) -> None:
    return None


@dataclass(frozen=True)
class StampedRecord(FakeRecord):
    """A fake record that also answers `timestamp()` like `confluent_kafka.Message`."""

    _timestamp: tuple[int, int] = (1, RAW_TS_MS)

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp


def ticking_clock(start: float = 1_760_000_000.0, step_s: float = 0.001) -> Any:
    ticks = count()
    return lambda: start + next(ticks) * step_s


def lines(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    out = []
    for raw in stream.getvalue().splitlines():
        record = json.loads(raw)
        if record.get("event") == event:
            out.append(record)
    return out


def run_one(message: Message, *, timer: StageTimer | None, ts: tuple[int, int] = (1, RAW_TS_MS)) -> Any:
    r = rig(steps=(passthrough,), stage_timer=timer)
    consumer = FakeConsumer()
    consumer.append(StampedRecord(RAW, 0, 0, message.key, message.payload, to_raw_headers(message), _timestamp=ts))
    r.loop(consumer).run(max_iterations=1)
    return r


def test_the_flag_is_off_unless_set() -> None:
    assert not enabled_from_env({})
    assert not enabled_from_env({"TB_STAGE_TIMING": "0"})
    assert enabled_from_env({"TB_STAGE_TIMING": "1"})
    assert enabled_from_env({"TB_STAGE_TIMING": " True "})


def test_off_writes_no_stage_line() -> None:
    r = run_one(sync_request(), timer=None)
    assert r.producer.replies()
    assert lines(r.log, "stage_timing") == []


def test_a_sync_record_writes_one_ordered_id_only_line() -> None:
    stream = io.StringIO()
    timer = StageTimer(JsonLogger("timing", stream=stream), clock=ticking_clock())
    request = sync_request()
    run_one(request, timer=timer)

    (line,) = lines(stream, "stage_timing")
    assert line["message_id"] == request.msg_id
    assert line["reply_status"] == "accepted"
    assert line["raw_ts_ms"] == RAW_TS_MS and line["raw_ts_type"] == "create"
    assert line["polled_ms"] < line["handle_ms"] < line["flushed_ms"] < line["reply_ms"]
    assert line["poll_wait_ms"] >= 0
    # Invariant 6: ids and timestamps only; nothing from the payload, the key or the sender's control id.
    text = stream.getvalue()
    for marker in (CONTROL_ID_MARKER, KEY_MARKER.decode()):
        assert marker not in text
    assert REQUEST_PAYLOAD.decode("utf-8", "replace")[:20] not in text
    assert_lines_conform(text)
    assert timer.written == 1


def test_log_append_time_is_reported_as_such() -> None:
    stream = io.StringIO()
    timer = StageTimer(JsonLogger("timing", stream=stream), clock=ticking_clock())
    run_one(sync_request(), timer=timer, ts=(2, RAW_TS_MS))
    (line,) = lines(stream, "stage_timing")
    assert line["raw_ts_type"] == "log_append"


def test_an_async_record_writes_nothing_and_does_not_leak_into_the_next() -> None:
    stream = io.StringIO()
    timer = StageTimer(JsonLogger("timing", stream=stream), clock=ticking_clock())
    r = rig(steps=(passthrough,), stage_timer=timer)
    consumer = FakeConsumer()
    first, second = sync_request(partition=None), sync_request()
    for offset, message in enumerate((first, second)):
        consumer.append(StampedRecord(RAW, 0, offset, message.key, message.payload, to_raw_headers(message)))
    r.loop(consumer).run(max_iterations=2)
    (line,) = lines(stream, "stage_timing")
    assert line["message_id"] == second.msg_id


def test_a_record_without_a_timestamp_still_gets_the_worker_stamps() -> None:
    stream = io.StringIO()
    timer = StageTimer(JsonLogger("timing", stream=stream), clock=ticking_clock())
    r = rig(steps=(passthrough,), stage_timer=timer)
    r.run(sync_request())  # plain FakeRecord: no timestamp()
    (line,) = lines(stream, "stage_timing")
    assert "raw_ts_ms" not in line and "reply_ms" in line


# -- the real PipelineWorker.build wiring (review LOW, W3-24) ------------------------------------------


class _Client:
    """A stand-in `confluent_kafka` client that records its config and answers anything with a no-op."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def __getattr__(self, name: str) -> Any:
        return lambda *args, **kwargs: None


def _build(monkeypatch: pytest.MonkeyPatch, *, stage_timing: bool) -> tuple[Any, list[dict[str, Any]]]:
    import confluent_kafka
    from tributary.runtime import worker as worker_module
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.worker import PipelineWorker, WorkerSettings

    producers: list[dict[str, Any]] = []

    def producer(config: dict[str, Any]) -> _Client:
        producers.append(config)
        return _Client(config)

    spec = make_spec(reply={})
    monkeypatch.setattr(confluent_kafka, "Consumer", _Client)
    monkeypatch.setattr(confluent_kafka, "Producer", producer)
    monkeypatch.setattr(worker_module, "open_spec_source", lambda **_kw: SimpleNamespace(current=lambda: spec))
    monkeypatch.setattr(worker_module, "check_partition_counts", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        worker_module, "ReplyPartitionCount", lambda *_a, **_kw: SimpleNamespace(prime=lambda: 3, __call__=None)
    )
    built = PipelineWorker(
        WorkerSettings(
            env="dev",
            channel=spec.name,
            kafka=KafkaSettings(bootstrap_servers="localhost:9092"),
            instance_id="pipeline-1",
            stage_timing=stage_timing,
        ),
        log=JsonLogger("worker", stream=io.StringIO()),
    )
    built.build()
    return built, producers


@pytest.mark.parametrize("flag", [True, False])
def test_build_wires_the_setting_into_one_timer_shared_by_loop_and_handler(
    monkeypatch: pytest.MonkeyPatch, flag: bool
) -> None:
    built, _ = _build(monkeypatch, stage_timing=flag)
    loop_timer, handler_timer = built.loop._stage_timer, built.handler._stage_timer
    if flag:
        assert isinstance(loop_timer, StageTimer) and loop_timer is handler_timer
    else:
        assert loop_timer is None and handler_timer is None


def test_build_ignores_the_environment_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    # Review MED: the flag is parsed once, in `main`; `build()` reads settings only.
    monkeypatch.setenv("TB_STAGE_TIMING", "1")
    built, _ = _build(monkeypatch, stage_timing=False)
    assert built.loop._stage_timer is None


def test_the_non_transactional_data_producer_is_the_worker_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """3.7 for `clients.worker_producer` on the real build path: linger 0 reaches the data producer."""
    _, producers = _build(monkeypatch, stage_timing=False)
    data = [config for config in producers if str(config.get("client.id", "")).startswith("tb-dev-")]
    (config,) = data
    contract = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text(encoding="utf-8"))
    block = contract["clients"]["worker_producer"]
    for key in ("acks", "enable.idempotence", "max.in.flight.requests.per.connection", "delivery.timeout.ms"):
        assert config[key] == block[key], key
    assert config["linger.ms"] == block["linger.ms"] == 0
    assert config["compression.type"] == block["compression.type"]
    assert "transactional.id" not in config


def test_main_parses_the_flag_into_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from tributary.runtime import worker as worker_module

    seen: list[bool] = []

    class _Stop(Exception):
        pass

    def capture(settings: Any, **_kw: Any) -> Any:
        seen.append(settings.stage_timing)
        raise _Stop

    monkeypatch.setattr(worker_module, "load_engine_kafka_or_exit", lambda **_kw: object())
    monkeypatch.setattr(worker_module, "WorkerSettings", lambda **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(worker_module, "PipelineWorker", capture)
    monkeypatch.setenv("TB_CHANNEL", "adt-passthrough")
    for value, expected in (("1", True), ("", False)):
        monkeypatch.setenv("TB_STAGE_TIMING", value)
        with pytest.raises(_Stop):
            worker_module.main(["--instance-id", "pipeline-1"])
        assert seen[-1] is expected
