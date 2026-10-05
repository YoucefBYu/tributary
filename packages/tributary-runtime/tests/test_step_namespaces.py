"""Which distribution may answer a `uses` name: channel-spec README 3.3.5, rules N1-N6 (set 1.6, ADR-007 D23).

The regression this file exists for (security re-review Medium, pre-existing): `steps.py` loaded a `uses`
name from the **first** installed `tributary.steps` entry point carrying it. A look-alike package, or a
transitive dependency registering `tributary.core.passthrough`, could win. Every refusal below is
checked with a `load()` that raises if it is reached: "refused" is not enough, the look-alike must also
never be imported.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import io
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Any, cast

import orjson
import pytest
from tributary.runtime.capabilities import normalise_distribution
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import RecordProducer
from tributary.runtime.provenance import canonical_distribution, provenance_problem, ships_module
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import ChannelSpec, StepRef
from tributary.runtime.step_namespaces import (
    DISTRIBUTION_MAX_LENGTH,
    DISTRIBUTION_PATTERN,
    ENTRY_POINT_GROUP,
    NAMESPACE_MAX_LENGTH,
    NAMESPACE_PATTERN,
    RESERVED_FIRST_SEGMENT,
    STEP_NAMESPACES,
    namespace_of,
)
from tributary.runtime.steps import StepResolver, build_plan, load_step
from tributary.runtime.worker import PipelineHandler
from tributary.sdk import Message, Step, step
from tributary.sdk.testing import make_message

from .fakes import FakeProducer

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "contracts" / "channel-spec" / "v0"
REGISTRY = json.loads((CONTRACT / "step-namespaces.json").read_text(encoding="utf-8"))
SCHEMA = json.loads((CONTRACT / "channel.schema.json").read_text(encoding="utf-8"))
EXAMPLES = CONTRACT / "examples" / "valid"
PHI_MARKER = "SYNTHPATIENT^ALPHA"


# -- the runtime copy of the registry, against the contract both ways ------------------------------


def test_every_contract_namespace_is_in_the_runtime_with_its_distribution() -> None:
    contract = {name: entry["distribution"] for name, entry in REGISTRY["namespaces"].items()}
    assert {name: normalise_distribution(dist) for name, dist in contract.items()} == dict(STEP_NAMESPACES)


def test_every_runtime_namespace_is_in_the_contract_and_is_reserved() -> None:
    assert set(STEP_NAMESPACES) <= set(REGISTRY["namespaces"])
    assert all(name.split(".")[0] == RESERVED_FIRST_SEGMENT for name in STEP_NAMESPACES)
    assert all(NAMESPACE_PATTERN.fullmatch(name) for name in STEP_NAMESPACES)
    assert all(normalise_distribution(dist) == dist for dist in STEP_NAMESPACES.values())


def test_the_group_and_reserved_segment_match_the_contract() -> None:
    assert REGISTRY["entry_point_group"] == ENTRY_POINT_GROUP
    assert REGISTRY["reserved_first_segment"] == RESERVED_FIRST_SEGMENT


def test_the_parser_grammar_is_the_schema_s() -> None:
    """The parser refuses what the schema refuses; the two patterns and caps are the same text."""
    block = SCHEMA["$defs"]["pipeline"]["properties"]["stepNamespaces"]
    key, reserved = block["propertyNames"]["allOf"]
    assert key["pattern"] == f"^{NAMESPACE_PATTERN.pattern}$"
    assert key["maxLength"] == NAMESPACE_MAX_LENGTH
    assert reserved == {"not": {"pattern": f"^{RESERVED_FIRST_SEGMENT}\\."}}
    assert block["additionalProperties"]["pattern"] == f"^{DISTRIBUTION_PATTERN.pattern}$"
    assert block["additionalProperties"]["maxLength"] == DISTRIBUTION_MAX_LENGTH


def test_every_installed_engine_entry_point_comes_from_its_registered_distribution() -> None:
    """Real metadata: the image this test runs in has no look-alike, and every engine step resolves."""
    engine = [point for point in entry_points(group=ENTRY_POINT_GROUP) if point.name.startswith("tributary.")]
    assert engine, "no engine steps installed: the check would pass vacuously"
    for point in engine:
        namespace = namespace_of(point.name)
        assert namespace in STEP_NAMESPACES, point.name
        assert point.dist is not None and normalise_distribution(point.dist.name) == STEP_NAMESPACES[namespace]


@pytest.mark.parametrize(
    ("uses", "namespace"),
    [("tributary.core.passthrough", "tributary.core"), ("acme.lab.a.b", "acme.lab"), ("acme.lab", None)],
)
def test_the_namespace_is_the_first_two_of_at_least_three_segments(uses: str, namespace: str | None) -> None:
    assert namespace_of(uses) == namespace


# -- strict parsing of pipeline.stepNamespaces ------------------------------------------------------


def channel(
    *,
    pipeline: list[dict[str, Any]] | None = None,
    step_namespaces: Any = None,
    destination_steps: list[dict[str, Any]] | None = None,
    version: int = 4,
) -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": version}
    if pipeline is not None:
        document["spec"]["pipeline"]["steps"] = pipeline
    if step_namespaces is not None:
        document["spec"]["pipeline"]["stepNamespaces"] = step_namespaces
    if destination_steps is not None:
        document["spec"]["destinations"][0]["steps"] = destination_steps
    return ChannelSpec.from_document(document)


def test_absent_step_namespaces_is_empty_and_a_binding_is_parsed() -> None:
    assert dict(channel().pipeline.step_namespaces) == {}
    bound = channel(step_namespaces={"acme.lab": "acme-lab-steps"})
    assert dict(bound.pipeline.step_namespaces) == {"acme.lab": "acme-lab-steps"}


def test_the_third_party_example_parses_with_its_binding() -> None:
    document = json.loads((EXAMPLES / "third-party-step-namespace.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": 1}
    bindings = ChannelSpec.from_document(document).pipeline.step_namespaces
    assert bindings and all(not name.startswith("tributary.") for name in bindings)


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ({}, "non-empty object"),
        (["acme.lab"], "non-empty object"),
        ("acme.lab=acme-lab-steps", "non-empty object"),
        ({"acme": "acme-lab-steps"}, "two-segment namespaces"),
        ({"acme.lab.steps": "acme-lab-steps"}, "two-segment namespaces"),
        ({"Acme.lab": "acme-lab-steps"}, "two-segment namespaces"),
        ({"a" * 61 + ".lab": "acme-lab-steps"}, "two-segment namespaces"),
        ({"tributary.core": "tributary-runtime"}, "bound only by the engine registry"),
        ({"tributary.acme": "acme-lab-steps"}, "bound only by the engine registry"),
        ({"acme.lab": "Acme_Lab_Steps"}, "PEP 503 normalised"),
        ({"acme.lab": "acme--lab"}, "PEP 503 normalised"),
        ({"acme.lab": 7}, "PEP 503 normalised"),
        ({"acme.lab": "a" * 101}, "PEP 503 normalised"),
    ],
)
def test_a_malformed_or_reserved_binding_refuses_the_spec(value: Any, match: str) -> None:
    """Not lenient (CHANGELOG 1.6): dropping it would refuse every customer step later, less clearly,
    and a `tributary.*` key is a spec trying to rebind an engine step (N2)."""
    with pytest.raises(ChannelSpecError, match=match):
        channel(step_namespaces=value)


# -- resolution (N1-N4), through the test seam (N6) -------------------------------------------------


@dataclass
class FakeDist:
    """Duck-types an installed distribution's metadata: `name`, `version`, its install folder (`_path`,
    as `PathDistribution` has) and its `RECORD` (`files`), which lists the fake entry points' module."""

    name: str
    version: str = "1.0.0"
    files: tuple[str, ...] = ("synth_module.py",)
    folder: str | None = None  # the install folder's name part; defaults to the claimed name

    @property
    def _path(self) -> Path:
        return Path("/site-packages") / f"{self.folder or self.name.replace('-', '_')}-{self.version}.dist-info"


@step
def synthetic_step(message: Message) -> Message:
    return message


@dataclass
class FakePoint:
    """Duck-types `importlib.metadata.EntryPoint`: `name`, `value`, `dist`, and a `load()` that counts."""

    name: str
    dist: FakeDist | None
    target: Callable[[], Any] = lambda: synthetic_step
    value: str = "synth_module:step"
    loads: int = 0

    def load(self) -> Any:
        self.loads += 1
        return self.target()


def never_imported() -> Any:
    raise AssertionError(f"imported a look-alike ({PHI_MARKER} must never reach an error either)")


def lookalike(name: str, dist: str | None = "tributary-runtlme") -> FakePoint:
    return FakePoint(name, FakeDist(dist) if dist is not None else None, target=never_imported)


def resolver(*installed: FakePoint, stream: io.StringIO | None = None) -> StepResolver:
    return StepResolver(
        log=JsonLogger("worker", stream=stream if stream is not None else io.StringIO(), level="debug"),
        points=lambda: cast("list[EntryPoint]", list(installed)),
    )


def lines(stream: io.StringIO) -> list[dict[str, Any]]:
    return [orjson.loads(line) for line in stream.getvalue().splitlines()]


def events(stream: io.StringIO, event: str) -> list[tuple[str, str, str | None]]:
    return [
        (line["step"], line["distribution"], line.get("reason")) for line in lines(stream) if line["event"] == event
    ]


def uses(name: str) -> StepRef:
    return StepRef(name="s", uses=name)


def test_the_bound_distribution_resolves_and_is_logged_once_per_name_per_version() -> None:
    stream = io.StringIO()
    real = FakePoint("tributary.core.passthrough", FakeDist("tributary_runtime", "0.4.0"))
    spec = channel(
        pipeline=[{"name": "a", "uses": "tributary.core.passthrough"}],
        destination_steps=[{"name": "b", "uses": "tributary.core.passthrough"}],
    )
    build_plan(spec, resolve=resolver(real, stream=stream).bind(spec))
    assert real.loads == 2  # two steps, one import: Python caches the module; the log line is per name
    loaded = [line for line in lines(stream) if line["event"] == "step_entry_point_loaded"]
    assert loaded == [
        {
            **loaded[0],
            "step": "tributary.core.passthrough",
            "distribution": "tributary_runtime",
            "version": "0.4.0",
            "channel": spec.name,
            "channel_version": 4,
            "level": "info",
        }
    ]
    assert "reason" not in loaded[0]


def test_a_duplicate_is_refused_and_neither_entry_point_is_imported() -> None:
    """A look-alike registering `tributary.core.passthrough` beside the real `tributary-runtime`."""
    stream = io.StringIO()
    real = FakePoint("tributary.core.passthrough", FakeDist("tributary-runtime"), target=never_imported)
    squat = lookalike("tributary.core.passthrough")
    spec = channel()
    with pytest.raises(ChannelSpecError, match="more than one entry point; none is loaded"):
        build_plan(spec, resolve=resolver(real, squat, stream=stream).bind(spec))
    assert real.loads == 0 and squat.loads == 0
    assert sorted(events(stream, "step_entry_point_skipped")) == [
        ("tributary.core.passthrough", "tributary-runtime", "duplicate"),
        ("tributary.core.passthrough", "tributary-runtlme", "distribution-mismatch"),  # image-wide (N5)
        ("tributary.core.passthrough", "tributary-runtlme", "duplicate"),
    ]
    assert all(line["level"] == "warning" for line in lines(stream))


def test_a_distribution_mismatch_is_refused_without_importing() -> None:
    """A look-alike registering a `tributary.*` name while the real package is absent."""
    squat = lookalike("tributary.hl7v2.parse", dist="tributary-hl7-v2")
    with pytest.raises(
        ChannelSpecError,
        match="comes from another distribution: distribution tributary-hl7-v2, bound to tributary-hl7v2",
    ):
        resolver(squat).load(uses("tributary.hl7v2.parse"))
    assert squat.loads == 0


def test_an_entry_point_without_distribution_metadata_never_matches() -> None:
    orphan = lookalike("tributary.core.passthrough", dist=None)
    with pytest.raises(ChannelSpecError, match="has no distribution metadata"):
        resolver(orphan).load(uses("tributary.core.passthrough"))
    assert orphan.loads == 0


@pytest.mark.parametrize("name", ["tributary.passthrough", "acme.normalise", "t.broken"])
def test_a_two_segment_name_has_no_namespace_and_is_refused(name: str) -> None:
    installed = lookalike(name, dist="acme")
    with pytest.raises(ChannelSpecError, match="at least three segments"):
        resolver(installed).load(uses(name), namespaces={"acme.normalise": "acme"})
    assert installed.loads == 0


def test_an_unregistered_engine_namespace_is_refused() -> None:
    stream = io.StringIO()
    installed = lookalike("tributary.dicom.parse", dist="tributary-dicom")
    spec = channel(pipeline=[{"name": "p", "uses": "tributary.dicom.parse"}])
    with pytest.raises(ChannelSpecError, match=r"namespace tributary.dicom is not bound by the engine registry"):
        build_plan(spec, resolve=resolver(installed, stream=stream).bind(spec))
    assert installed.loads == 0
    assert events(stream, "step_entry_point_skipped") == [
        ("tributary.dicom.parse", "tributary-dicom", "unregistered-namespace")
    ]


def test_a_customer_namespace_resolves_only_with_its_binding() -> None:
    stream = io.StringIO()
    customer = FakePoint("acme.lab.normalise_units", FakeDist("Acme_Lab_Steps"))
    bare = channel(pipeline=[{"name": "n", "uses": "acme.lab.normalise_units"}])
    with pytest.raises(ChannelSpecError, match=r"namespace acme.lab is not bound by spec.pipeline.stepNamespaces"):
        build_plan(bare, resolve=resolver(customer, stream=stream).bind(bare))
    assert customer.loads == 0

    bound = channel(
        pipeline=[{"name": "n", "uses": "acme.lab.normalise_units"}], step_namespaces={"acme.lab": "acme-lab-steps"}
    )
    build_plan(bound, resolve=resolver(customer, stream=stream).bind(bound))
    assert customer.loads == 1
    assert events(stream, "step_entry_point_loaded") == [("acme.lab.normalise_units", "Acme_Lab_Steps", None)]


def test_a_customer_look_alike_beside_the_bound_package_is_a_duplicate() -> None:
    real = FakePoint("acme.lab.normalise_units", FakeDist("acme-lab-steps"), target=never_imported)
    squat = lookalike("acme.lab.normalise_units", dist="acme-labs-steps")
    with pytest.raises(ChannelSpecError, match="more than one entry point"):
        resolver(real, squat).load(uses("acme.lab.normalise_units"), namespaces={"acme.lab": "acme-lab-steps"})
    assert real.loads == 0 and squat.loads == 0


def test_a_caller_binding_cannot_rebind_an_engine_namespace() -> None:
    """The schema and the parser refuse a `tributary.*` key; the resolver ignores one regardless (N2)."""
    squat = lookalike("tributary.core.passthrough", dist="evil-steps")
    with pytest.raises(ChannelSpecError, match="bound to tributary-runtime"):
        resolver(squat).load(uses("tributary.core.passthrough"), namespaces={"tributary.core": "evil-steps"})
    assert squat.loads == 0


def test_an_unnamed_look_alike_in_an_engine_namespace_is_logged_image_wide_and_never_imported() -> None:
    """N5: the signal for a look-alike package, whether or not a spec names it. It stops nothing."""
    stream = io.StringIO()
    real = FakePoint("tributary.core.passthrough", FakeDist("tributary-runtime"))
    rogue = lookalike("tributary.core.telemetry", dist="tributary-telemetry")
    unregistered = lookalike("tributary.metrics.push", dist="tributary-metrics")
    customer = lookalike("acme.lab.other", dist="acme-lab-steps")
    spec = channel()
    build_plan(spec, resolve=resolver(real, rogue, unregistered, customer, stream=stream).bind(spec))
    assert (rogue.loads, unregistered.loads, customer.loads) == (0, 0, 0)
    assert sorted(events(stream, "step_entry_point_skipped")) == [
        ("tributary.core.telemetry", "tributary-telemetry", "distribution-mismatch"),
        ("tributary.metrics.push", "tributary-metrics", "unregistered-namespace"),
    ]


def test_load_step_defaults_to_engine_namespaces_only() -> None:
    """The unlogged form `resolve_custom` and the corpus test use: an unbound namespace is refused."""
    assert isinstance(load_step(uses("tributary.core.passthrough")), Step)
    with pytest.raises(ChannelSpecError, match=r"not bound by spec.pipeline.stepNamespaces"):
        load_step(uses("acme.lab.normalise_units"))


def test_no_refusal_repeats_exception_text() -> None:
    boom = FakePoint("tributary.core.boom", FakeDist("tributary-runtime"), target=lambda: never_imported())
    with pytest.raises(ChannelSpecError) as refused:
        resolver(boom).load(uses("tributary.core.boom"))
    assert "AssertionError" in str(refused.value) and PHI_MARKER not in str(refused.value)


# -- real importlib.metadata: what "exactly one" counts (N4) ----------------------------------------


def _distribution(
    root: Path,
    dist: str,
    *,
    entry: str,
    module: str,
    target: str | None = None,
    folder: str | None = None,
    version: str = "1.0",
) -> None:
    """A minimal installed distribution on a path of its own: `.dist-info` metadata, a `RECORD`, and one
    module. `target` overrides the entry point's value; `folder` the install folder's name part."""
    info = root / f"{folder or dist.replace('-', '_')}-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {dist}\nVersion: {version}\n", encoding="utf-8")
    (info / "entry_points.txt").write_text(
        f"[{ENTRY_POINT_GROUP}]\n{entry} = {target or f'{module}:step'}\n", encoding="utf-8"
    )
    (root / f"{module}.py").write_text(
        "from tributary.sdk import Message, step\n\n\n@step\ndef step(message: Message) -> Message:\n"
        "    return message\n",
        encoding="utf-8",
    )
    record = [f"{module}.py,,", f"{info.name}/METADATA,,", f"{info.name}/entry_points.txt,,", f"{info.name}/RECORD,,"]
    (info / "RECORD").write_text("\n".join(record) + "\n", encoding="utf-8")


@pytest.fixture
def site(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[[str], Path]:
    """Directories prepended to `sys.path`, as a second site-packages would be."""

    def add(name: str) -> Path:
        root = tmp_path / name
        root.mkdir()
        monkeypatch.syspath_prepend(str(root))
        return root

    return add


def _carriers(name: str) -> list[EntryPoint]:
    importlib.invalidate_caches()
    return [point for point in entry_points(group=ENTRY_POINT_GROUP) if point.name == name]


def test_importlib_metadata_deduplicates_one_distribution_seen_twice(
    site: Callable[[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pins the N4 claim the architect did not verify: CPython 3.12 `entry_points()` keeps the first
    distribution per normalised name, so one package visible on two `sys.path` entries is not a
    duplicate, and it resolves and loads for real."""
    for root in (site("first"), site("second")):
        _distribution(root, "acme-synth-steps", entry="acme.synth.upper", module="acme_synth_dedup")
    monkeypatch.delitem(sys.modules, "acme_synth_dedup", raising=False)
    carriers = _carriers("acme.synth.upper")
    assert len(carriers) == 1
    node = StepResolver().load(uses("acme.synth.upper"), namespaces={"acme.synth": "acme-synth-steps"})
    assert isinstance(node, Step)
    message = make_message(b"synthetic")
    assert node(message) is message


def test_two_real_distributions_carrying_one_name_are_both_counted_and_refused(
    site: Callable[[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    _distribution(site("real"), "acme-synth-steps", entry="acme.synth.lower", module="acme_synth_real")
    _distribution(site("squat"), "acme-synth-stepz", entry="acme.synth.lower", module="acme_synth_squat")
    for module in ("acme_synth_real", "acme_synth_squat"):
        monkeypatch.delitem(sys.modules, module, raising=False)
    assert sorted(str(point.dist.name) for point in _carriers("acme.synth.lower") if point.dist) == [
        "acme-synth-steps",
        "acme-synth-stepz",
    ]
    with pytest.raises(ChannelSpecError, match="more than one entry point"):
        StepResolver().load(uses("acme.synth.lower"), namespaces={"acme.synth": "acme-synth-steps"})
    assert "acme_synth_real" not in sys.modules and "acme_synth_squat" not in sys.modules


# -- the worker: startup refuses, a hot reload keeps the previous version ---------------------------


def test_the_worker_refuses_a_look_alike_at_startup_and_keeps_the_previous_version_on_reload() -> None:
    stream = io.StringIO()
    real = FakePoint("tributary.core.passthrough", FakeDist("tributary-runtime"))
    squat = lookalike("tributary.core.passthrough")
    step_resolver = resolver(real, stream=stream)
    specs = [channel(version=4)]
    producer, emitter = FakeProducer(), RecordingEmitter()
    subject = PipelineHandler(
        env="dev",
        spec_provider=lambda: specs[-1],
        producer=RecordProducer(producer, log=JsonLogger("test", stream=stream)),
        emitter=emitter,
        log=JsonLogger("worker", stream=stream),
        instance="worker:test@localhost",
        step_resolver=step_resolver,
    )
    subject.prepare(specs[0])
    specs.append(channel(version=5))
    subject._step_resolver = resolver(real, squat, stream=stream)  # a look-alike appears in the image
    message = make_message(b"synthetic", key=b"SYNTH-KEY-1", channel=specs[0].name)
    subject.handle(ConsumedRecord(topic="dev.adt-passthrough.raw", partition=0, offset=1, message=message))
    assert squat.loads == 0
    assert {event.channel_version for event in emitter.events if hasattr(event, "channel_version")} == {4}
    refused = [line for line in lines(stream) if line["event"] == "channel_spec_refused"]
    assert [(line["channel_version"], line["running_version"]) for line in refused] == [(5, 4)]

    fresh = PipelineHandler(
        env="dev",
        spec_provider=lambda: specs[-1],
        producer=RecordProducer(FakeProducer(), log=JsonLogger("test", stream=io.StringIO())),
        emitter=RecordingEmitter(),
        log=JsonLogger("worker", stream=io.StringIO()),
        instance="worker:test@localhost",
        step_resolver=resolver(real, squat),
    )
    with pytest.raises(ChannelSpecError, match="more than one entry point"):
        fresh.prepare(specs[-1])


# -- provenance: name, install folder, target module (security review of a1219af, Lows 1-3) -------------


KELVIN = "\N{KELVIN SIGN}"  # KELVIN SIGN: `str.lower()` turns it into ASCII "k"


def test_a_non_ascii_look_alike_name_never_normalises_onto_a_binding() -> None:
    """Low 1: a name spelled with U+212A KELVIN SIGN used to normalise to `acme-kit` through `str.lower()`."""
    assert f"acme-{KELVIN}it".lower() == "acme-kit", "the Unicode fold this guards against"
    assert normalise_distribution(f"ACME-{KELVIN}IT") != "acme-kit"
    assert canonical_distribution(f"acme-{KELVIN}it") is None
    assert canonical_distribution("Acme_Kit") == "acme-kit"
    squat = lookalike("acme.kit.run", dist=f"acme-{KELVIN}it")
    with pytest.raises(ChannelSpecError, match="outside PEP 508 ASCII"):
        resolver(squat).load(uses("acme.kit.run"), namespaces={"acme.kit": "acme-kit"})
    assert squat.loads == 0


def test_metadata_claiming_another_name_than_its_install_folder_is_refused() -> None:
    """Low 1: `METADATA` says `Name: tributary-runtime` from a folder named `evil_steps-1.0.dist-info`,
    and registers an engine step name the real runtime does not have."""
    stream = io.StringIO()
    impostor = FakePoint(
        "tributary.core.only_here", FakeDist("tributary-runtime", folder="evil_steps"), target=never_imported
    )
    with pytest.raises(ChannelSpecError, match="Name does not match its install folder"):
        resolver(impostor, stream=stream).load(uses("tributary.core.only_here"))
    assert impostor.loads == 0


def test_a_real_impostor_install_folder_is_refused(
    site: Callable[[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    _distribution(
        site("impostor"),
        "tributary-runtime",
        entry="tributary.core.only_here",
        module="synth_impostor",
        folder="evil_steps",
    )
    monkeypatch.delitem(sys.modules, "synth_impostor", raising=False)
    importlib.invalidate_caches()
    with pytest.raises(ChannelSpecError, match="Name does not match its install folder"):
        StepResolver().load(uses("tributary.core.only_here"))
    assert "synth_impostor" not in sys.modules


def test_a_same_named_install_earlier_on_sys_path_is_reported_as_a_duplicate(
    site: Callable[[str], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Low 2: `entry_points()` keeps the first distribution per name, so this install hides the real
    `tributary-runtime` without a trace. The scan reads every distribution and logs each hidden engine
    entry point as a `duplicate` warning. Whether to refuse is a contract question (README 3.3.5 N4
    counts after de-duplication, and its threat table lists same-name replacement as undetected)."""
    _distribution(
        site("shadow"), "tributary-runtime", entry="tributary.core.shadow_only", module="synth_shadow", version="9.9"
    )
    monkeypatch.delitem(sys.modules, "synth_shadow", raising=False)
    importlib.invalidate_caches()
    stream = io.StringIO()
    StepResolver(log=JsonLogger("worker", stream=stream)).bind(channel())
    hidden = {line["step"] for line in lines(stream) if line.get("reason") == "duplicate"}
    real = {point.name for point in entry_points(group=ENTRY_POINT_GROUP) if point.name.startswith("tributary.core.")}
    assert "tributary.core.passthrough" in hidden, "the real runtime's steps are the hidden ones"
    assert real == {"tributary.core.shadow_only"}, "what importlib.metadata alone shows: only the impostor"
    assert "synth_shadow" not in sys.modules


@pytest.mark.parametrize("target", ["subprocess:getoutput", "os:system", "synth_elsewhere:step", "a..b:c"])
def test_a_target_outside_the_bound_distribution_is_refused_before_import(target: str) -> None:
    """Low 3: the bound distribution's own `entry_points.txt` could name any importable callable."""
    stray = FakePoint(
        "acme.lab.run", FakeDist("acme-lab-steps", files=("acme_lab/steps.py",)), target=never_imported, value=target
    )
    with pytest.raises(ChannelSpecError, match="targets a module its distribution does not ship"):
        resolver(stray).load(uses("acme.lab.run"), namespaces={"acme.lab": "acme-lab-steps"})
    assert stray.loads == 0


def test_a_real_stdlib_target_is_refused(site: Callable[[str], Path]) -> None:
    _distribution(
        site("stray"), "acme-synth-steps", entry="acme.synth.run", module="synth_own", target="subprocess:getoutput"
    )
    importlib.invalidate_caches()
    with pytest.raises(ChannelSpecError, match="targets a module its distribution does not ship"):
        StepResolver().load(uses("acme.synth.run"), namespaces={"acme.synth": "acme-synth-steps"})


def test_an_editable_install_ships_what_its_pth_directory_holds(tmp_path: Path) -> None:
    """The uv workspace installs editable: `RECORD` lists a `.pth`, and the module lives in `src/`."""
    site_dir, src = tmp_path / "site-packages", tmp_path / "src"
    (src / "acme" / "lab").mkdir(parents=True)
    (src / "acme" / "lab" / "steps.py").write_text("", encoding="utf-8")
    info = site_dir / "acme_lab_steps-1.0.dist-info"
    info.mkdir(parents=True)
    (site_dir / "_editable_acme_lab_steps.pth").write_text(f"import os\n{src}\n", encoding="utf-8")
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: acme-lab-steps\nVersion: 1.0\n", encoding="utf-8")
    (info / "RECORD").write_text(
        "_editable_acme_lab_steps.pth,,\nacme_lab_steps-1.0.dist-info/METADATA,,\n", encoding="utf-8"
    )
    dist = importlib.metadata.PathDistribution(info)
    assert ships_module(dist, "acme.lab.steps")
    assert not ships_module(dist, "acme.lab.other")
    assert not ships_module(dist, "os")
    assert provenance_problem(dist, "acme.lab.steps:normalise", "acme-lab-steps") is None


def test_every_installed_engine_and_context_entry_point_passes_provenance() -> None:
    """Real metadata: every engine step and every registered capability in this image passes all three
    checks, so the rule refuses nothing the repo ships (editable installs included)."""
    from tributary.runtime.capabilities import CAPABILITIES
    from tributary.runtime.capabilities import ENTRY_POINT_GROUP as CONTEXT_GROUP

    checked = 0
    for point in entry_points(group=ENTRY_POINT_GROUP):
        namespace = namespace_of(point.name)
        if namespace in STEP_NAMESPACES:
            assert provenance_problem(point.dist, point.value, STEP_NAMESPACES[namespace]) is None, point.name
            checked += 1
    for point in entry_points(group=CONTEXT_GROUP):
        if point.name in CAPABILITIES:
            assert provenance_problem(point.dist, point.value, CAPABILITIES[point.name]) is None, point.name
            checked += 1
    assert checked >= 9, "eight engine steps and the fhir capability"


def test_every_test_file_a_runtime_docstring_names_exists() -> None:
    """Low 4: `step_namespaces.py` named `tests/test_step_namespaces_registry.py`, which never existed."""
    source = ROOT / "packages" / "tributary-runtime" / "src"
    named = {
        (path.name, ref)
        for path in source.rglob("*.py")
        for ref in re.findall(r"`(tests/[A-Za-z0-9_/]+\.py)", path.read_text(encoding="utf-8"))
    }
    assert named, "no references found: the check would pass vacuously"
    package_tests = ROOT / "packages" / "tributary-runtime"
    missing = sorted(
        (name, ref) for name, ref in named if not (package_tests / ref).is_file() and not (ROOT / ref).is_file()
    )
    assert not missing, missing
