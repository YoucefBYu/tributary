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


def _contract_archiver_names() -> set[str]:
    from tributary.runtime.metrics import MetricsContract

    return {name for name, d in MetricsContract.load().metrics.items() if "archiver" in d.emitters}


def test_the_events_archiver_registers_exactly_the_contracts_archiver_metrics() -> None:
    """Two-way: nothing outside the contract, and no contract metric left unregistered."""
    assert set(ArchiverExposition(ArchiverMetrics(), strict=True).registry.names) == _contract_archiver_names()


def test_the_content_archiver_registers_a_strict_subset_of_the_contract_and_scrapes() -> None:
    """`content=True` omits the events and attrs families (they describe events, not content). Everything it does
    register is a contract name, so the registry accepts it; no content-specific family exists in the contract."""
    metrics = ArchiverMetrics()
    exposition = ArchiverExposition(metrics, strict=True, content=True)
    names = set(exposition.registry.names)
    assert names < _contract_archiver_names()
    assert _contract_archiver_names() - names == {
        "tb_archiver_events_total",
        "tb_archiver_attrs_dropped_total",
        "tb_archiver_audit_records_total",
    }
    metrics.batches_written, metrics.commits_ok = 3, 3
    text = exposition.render().decode()
    assert 'tb_archiver_batches_total{outcome="ok"} 3' in text and "tb_archiver_events_total" not in text


def test_audit_records_counter_exposes_each_outcome_and_events_written_excludes_audit_rows() -> None:
    metrics = ArchiverMetrics()
    exposition = ArchiverExposition(metrics, strict=True)
    metrics.rows_written, metrics.audit_written = 10, 4
    metrics.audit_invalid, metrics.audit_unsupported_version = 3, 2
    text = exposition.render().decode()
    assert 'tb_archiver_audit_records_total{outcome="written"} 4' in text
    assert 'tb_archiver_audit_records_total{outcome="invalid"} 3' in text
    assert 'tb_archiver_audit_records_total{outcome="unsupported_version"} 2' in text
    assert 'tb_archiver_events_total{outcome="written"} 6' in text, "audit rows are not events"


def test_the_audit_counter_is_zero_filled_before_the_first_record() -> None:
    text = ArchiverExposition(ArchiverMetrics(), strict=True).render().decode()
    for outcome in ("written", "invalid", "unsupported_version"):
        assert f'tb_archiver_audit_records_total{{outcome="{outcome}"}} 0' in text
