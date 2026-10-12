"""`/metrics` of a receiver: every series is a name from `observability/v0` and mirrors the plain counters."""

from __future__ import annotations

import asyncio

from tributary.receivers import readiness as rd
from tributary.receivers.exposition import ReceiverMetrics
from tributary.receivers.metrics import Metrics
from tributary.runtime.ops import OpsServer, OpsSettings

from .readiness.support import raw_request


def _report() -> rd.Report:
    checks = (rd.Check("spec", None), rd.Check("component", None), rd.Check("raw_producer", rd.NOT_WARMED))
    return rd.Report("lab-in", "mllp", 3, False, checks, (rd.SPEC_SOURCE_UNPROVEN,))


def _build(m: Metrics) -> ReceiverMetrics:
    return ReceiverMetrics.create(m, _report, channel="lab-in", receiver_type="mllp", strict=True)


def test_counters_gauges_and_histogram_are_mirrored_with_closed_labels() -> None:
    m = Metrics()
    exp = _build(m)
    m.connections_total = 2
    m.connections_open = 1
    m.messages_received = 5
    m.acked = 3
    m.nacked_oversize = 1
    m.rejected_auth = 1
    m.refuse_connection("max_conns")
    m.record_sync_answer("AE", "REPLY_TIMEOUT")
    m.record_ack_latency_ms(7.0)
    text = exp.render().decode()
    scope = 'channel="lab-in",receiver_type="mllp"'
    assert f"tb_receiver_connections_total{{{scope}}} 2" in text
    assert f"tb_receiver_connections_open{{{scope}}} 1" in text
    assert f'tb_receiver_responses_total{{{scope},outcome="acked",reason="committed"}} 3' in text
    assert f'tb_receiver_responses_total{{{scope},outcome="nacked",reason="oversize"}} 1' in text
    assert f'tb_receiver_connections_refused_total{{{scope},reason="max_conns"}} 1' in text
    assert f'tb_receiver_sync_answers_total{{{scope},answer="AE",code="REPLY_TIMEOUT"}} 1' in text
    assert f"tb_ack_latency_seconds_count{{{scope}}} 1" in text
    assert 'tb_ready{component="receiver",channel="lab-in"} 0' in text
    assert 'tb_ready_check{component="receiver",channel="lab-in",check="raw_producer"} 0' in text
    assert 'tb_spec_version{component="receiver",channel="lab-in"} 3' in text
    assert 'tb_spec_source_unproven{component="receiver",channel="lab-in"} 1' in text


def test_a_scrape_serves_only_registered_names_and_no_default_process_metrics() -> None:
    names: set[str] = set()

    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), _report, component="receiver", metrics=_build(Metrics()).render)
        port = await server.start()
        try:
            status, headers, body = await raw_request(port, b"GET /metrics HTTP/1.1\r\n\r\n")
            assert status == 200 and headers["content-type"].startswith("text/plain")
            for line in body.decode().splitlines():
                if line and not line.startswith("#"):
                    names.add(line.split("{")[0].split(" ")[0])
        finally:
            await server.close()

    asyncio.run(go())
    assert names and all(n.startswith("tb_") for n in names)
    assert not any(n.startswith(("process_", "python_")) for n in names)


def test_an_unregistered_or_content_derived_sync_code_is_exported_as_unknown() -> None:
    m = Metrics()
    exp = _build(m)
    m.record_sync_answer("AE", "SYNTH-FORGED-CODE-1")
    m.record_sync_answer("AE", "REPLY_TIMEOUT")
    m.record_sync_answer("AE", None)
    text = exp.render().decode()
    assert "FORGED" not in text
    assert 'answer="AE",code="UNKNOWN"} 1' in text
    assert 'answer="AE",code="REPLY_TIMEOUT"} 1' in text
    assert 'answer="AE",code="NONE"} 1' in text
    assert len(m.sync_answers) == 3
