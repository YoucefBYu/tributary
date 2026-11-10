"""`tributary.runtime.metrics`: registration is bound to `observability/v0/metrics.json`, label values come from
the closed lists, and the text exposition parses as Prometheus 0.0.4."""

from __future__ import annotations

import copy
import io
import json
import threading
from typing import Any

import pytest
from tributary.runtime.logs import JsonLogger
from tributary.runtime.metrics import (
    MetricLabelError,
    MetricRegistrationError,
    MetricsContract,
    MetricsContractError,
    MetricsRegistry,
    build_version,
)

from .log_line_schema import assert_lines_conform
from .ops_support import METRICS_JSON, parse_exposition

DOCUMENT: dict[str, Any] = json.loads(METRICS_JSON.read_text(encoding="utf-8"))


def contract(document: dict[str, Any] | None = None) -> MetricsContract:
    return MetricsContract.from_document(copy.deepcopy(document or DOCUMENT))


def registry(component: str = "worker", **kwargs: Any) -> MetricsRegistry:
    return MetricsRegistry(contract(), component=component, **kwargs)


def test_the_contract_loads_from_the_checkout_with_every_metric() -> None:
    loaded = MetricsContract.load()
    assert list(loaded.metrics) == [m["name"] for m in DOCUMENT["metrics"]]
    assert loaded.metrics["tb_step_duration_seconds"].buckets == tuple(DOCUMENT["buckets"]["latency"])
    assert loaded.metrics["tb_stage_duration_seconds"].buckets == tuple(DOCUMENT["buckets"]["delivery"])


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["metrics"][0].update(labels=["component", "no_such_label"]),
        lambda d: d["metrics"][0].update(type="summary"),
        lambda d: (
            d["labels"]["outcome"].update(per_metric=True)
            or next(m for m in d["metrics"] if m["name"] == "tb_worker_records_total").pop("label_values")
        ),
        lambda d: d["buckets"].update(latency=[1, 0.5]),
        lambda d: d.pop("metrics"),
    ],
)
def test_a_malformed_contract_is_refused(mutate: Any) -> None:
    document = copy.deepcopy(DOCUMENT)
    mutate(document)
    with pytest.raises(MetricsContractError):
        MetricsContract.from_document(document)


def test_registration_refuses_unregistered_names_wrong_types_wrong_emitters_and_duplicates() -> None:
    reg = registry()
    with pytest.raises(MetricRegistrationError):
        reg.counter("tb_not_in_the_contract_total")
    with pytest.raises(MetricRegistrationError):
        reg.gauge("tb_messages_total")  # a counter
    with pytest.raises(MetricRegistrationError):
        reg.counter("tb_deliveries_total")  # loader only
    with pytest.raises(MetricRegistrationError):
        reg.counter("process_cpu_seconds_total")  # what a client library's default registry would add
    reg.counter("tb_messages_total")
    with pytest.raises(MetricRegistrationError):
        reg.counter("tb_messages_total")


def test_a_series_must_carry_exactly_the_registered_label_names() -> None:
    gauge = registry(strict=False).gauge("tb_partitions_halted")
    with pytest.raises(MetricLabelError):
        gauge.set(1, component="worker", channel="lab-in")  # destination missing
    with pytest.raises(MetricLabelError):
        gauge.set(1, component="worker", channel="lab-in", destination="", partition="3")  # never a label


@pytest.mark.parametrize(
    "labels",
    [
        {"component": "worker", "channel": "lab-in", "stage": "SYNTH", "status": "ok", "destination": ""},
        {"component": "worker", "channel": "SYNTH^DOE", "stage": "routed", "status": "ok", "destination": ""},
        {"component": "worker", "channel": "lab-in", "stage": "routed", "status": "ok", "destination": "MRN 12345"},
        {"component": "nurse", "channel": "lab-in", "stage": "routed", "status": "ok", "destination": ""},
    ],
)
def test_a_value_outside_the_closed_list_raises_in_strict_mode(labels: dict[str, str]) -> None:
    counter = registry(strict=True).counter("tb_messages_total")
    with pytest.raises(MetricLabelError) as caught:
        counter.inc(**labels)
    assert "SYNTH" not in str(caught.value) and "12345" not in str(caught.value)


def test_a_per_metric_outcome_is_checked_against_its_own_list() -> None:
    counter = registry(strict=True).counter("tb_worker_records_total")
    counter.inc(channel="lab-in", outcome="handled")
    with pytest.raises(MetricLabelError):
        counter.inc(channel="lab-in", outcome="success")  # a loader outcome, not a worker-records one


def test_non_strict_mode_drops_the_observation_and_logs_the_label_name_once_never_the_value() -> None:
    stream = io.StringIO()
    reg = registry(strict=False, log=JsonLogger("worker", stream=stream))
    counter = reg.counter("tb_messages_total")
    for _ in range(3):
        counter.inc(component="worker", channel="SYNTH^DOE", stage="routed", status="ok", destination="")
    exposition = parse_exposition(reg.render().decode())
    assert exposition.series("tb_messages_total") == []
    lines = assert_lines_conform(stream.getvalue())
    assert [(line["event"], line["metric"], line["label"]) for line in lines] == [
        ("metric_label_refused", "tb_messages_total", "channel")
    ]
    assert "SYNTH" not in stream.getvalue()


def test_series_per_metric_are_capped() -> None:
    reg = registry(strict=False, max_series=2)
    gauge = reg.gauge("tb_spec_version")
    for name in ("a-one", "a-two", "a-three"):
        gauge.set(1, component="worker", channel=name)
    assert len(parse_exposition(reg.render().decode()).series("tb_spec_version")) == 2


def test_counters_only_go_up() -> None:
    counter = registry().counter("tb_commits_total")
    with pytest.raises(ValueError):
        counter.inc(-1, component="worker", channel="lab-in", outcome="ok")
    counter.set_total(5, component="worker", channel="lab-in", outcome="ok")
    counter.set_total(3, component="worker", channel="lab-in", outcome="ok")
    assert counter.value(component="worker", channel="lab-in", outcome="ok") == 5


def test_the_exposition_parses_with_help_type_and_cumulative_histogram_buckets() -> None:
    reg = registry(strict=True)
    info = reg.gauge("tb_build_info")
    info.set(1, component="worker", version="1.2.3+synth")
    steps = reg.histogram("tb_step_duration_seconds")
    labels = {"channel": "lab-in", "scope": "-", "step": "parse", "outcome": "ok"}
    for seconds in (0.0005, 0.003, 0.003, 0.2, 99.0):
        steps.observe(seconds, **labels)
    text = reg.render().decode()
    exposition = parse_exposition(text)
    assert exposition.types == {"tb_build_info": "gauge", "tb_step_duration_seconds": "histogram"}
    assert exposition.helps["tb_build_info"].startswith("Always 1.")
    assert exposition.value("tb_build_info", component="worker", version="1.2.3+synth") == 1
    buckets = {s.labels["le"]: s.value for s in exposition.series("tb_step_duration_seconds_bucket")}
    assert list(buckets) == [repr(float(b)) for b in DOCUMENT["buckets"]["latency"]] + ["+Inf"]
    assert buckets["0.001"] == 1 and buckets["0.005"] == 3 and buckets["0.25"] == 4 and buckets["10.0"] == 4
    assert buckets["+Inf"] == 5
    assert exposition.value("tb_step_duration_seconds_count", **labels) == 5
    assert exposition.value("tb_step_duration_seconds_sum", **labels) == pytest.approx(99.2065)


def test_label_values_and_help_are_escaped() -> None:
    document = copy.deepcopy(DOCUMENT)
    document["labels"]["version"]["pattern"] = "^[\\s\\S]{1,64}$"
    document["metrics"][0]["help"] = "line one\nback\\slash"
    reg = MetricsRegistry(MetricsContract.from_document(document), component="worker", strict=True)
    reg.gauge("tb_build_info").set(1, component="worker", version='a"b\\c\nd')
    text = reg.render().decode()
    assert 'version="a\\"b\\\\c\\nd"' in text
    assert "# HELP tb_build_info line one\\nback\\\\slash" in text
    assert parse_exposition(text).value("tb_build_info", component="worker", version='a"b\\c\nd') == 1


def test_a_failing_collector_leaves_the_scrape_working_and_logs_its_class_once() -> None:
    stream = io.StringIO()
    reg = registry(log=JsonLogger("worker", stream=stream))
    reg.gauge("tb_spec_version").set(4, component="worker", channel="lab-in")

    def boom() -> None:
        raise RuntimeError("SYNTH-secret")

    reg.collector(boom)
    for _ in range(2):
        assert (
            parse_exposition(reg.render().decode()).value("tb_spec_version", component="worker", channel="lab-in") == 4
        )
    lines = assert_lines_conform(stream.getvalue())
    assert [(line["event"], line["error"]) for line in lines] == [("metrics_collector_failed", "RuntimeError")]
    assert "SYNTH" not in stream.getvalue()


def test_observations_and_renders_from_two_threads_do_not_corrupt_the_exposition() -> None:
    reg = registry(strict=True)
    counter = reg.counter("tb_messages_total")
    labels = {"component": "worker", "channel": "lab-in", "stage": "routed", "status": "ok", "destination": ""}
    stop = threading.Event()

    def scrape() -> None:
        while not stop.is_set():
            parse_exposition(reg.render().decode())

    scraper = threading.Thread(target=scrape)
    scraper.start()
    try:
        for _ in range(20_000):
            counter.inc(**labels)
    finally:
        stop.set()
        scraper.join()
    assert parse_exposition(reg.render().decode()).value("tb_messages_total", **labels) == 20_000


def test_build_version_is_pattern_safe() -> None:
    assert build_version() and build_version("tributary-no-such-distribution") == "0.0.0+unknown"
