"""`/metrics` content for the archiver against `contracts/observability/v0/metrics.json`."""

from __future__ import annotations

from tributary.archiver.exposition import ArchiverExposition
from tributary.archiver.metrics import ArchiverMetrics


def test_registers_every_archiver_metric_in_the_contract_and_scrapes_counts() -> None:
    metrics = ArchiverMetrics()
    exposition = ArchiverExposition(metrics, version="1.2.3", strict=True)
    metrics.rows_written, metrics.events_rejected = 12, 2
    metrics.attrs_dropped, metrics.attrs_dropped_hmac = 5, 3
    metrics.batches_written, metrics.insert_failures = 4, 1
    metrics.commits_ok, metrics.commit_failures = 4, 1
    assert metrics.observe_insert is not None
    metrics.observe_insert(0.03)
    text = exposition.render().decode()
    assert 'tb_build_info{component="archiver",version="1.2.3"} 1' in text
    assert 'tb_archiver_events_total{outcome="written"} 12' in text
    assert 'tb_archiver_events_total{outcome="invalid"} 2' in text
    assert 'tb_archiver_attrs_dropped_total{reason="hmac"} 3' in text
    assert 'tb_archiver_attrs_dropped_total{reason="denied_name"} 2' in text
    assert 'tb_archiver_batches_total{outcome="ok"} 4' in text
    assert 'tb_archiver_batches_total{outcome="failed"} 1' in text
    assert 'tb_commits_total{component="archiver",channel="tb-archiver",outcome="ok"} 4' in text
    assert 'tb_commits_total{component="archiver",channel="tb-archiver",outcome="failed"} 1' in text
    assert "tb_archiver_insert_duration_seconds_count 1" in text


def test_scrape_series_carry_only_contract_labels() -> None:
    metrics = ArchiverMetrics()
    metrics.rows_written = 1
    text = ArchiverExposition(metrics, strict=True).render().decode()
    series = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert series and all(line.startswith("tb_") for line in series)
    assert not [line for line in series if "message_id" in line or "attrs=" in line or "topic=" in line]
