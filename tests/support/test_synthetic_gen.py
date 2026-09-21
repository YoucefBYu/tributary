import re

from benchmarks.gen import MESSAGE_TYPES, generate


def test_deterministic_and_synthetic() -> None:
    a = list(generate(300, 1))
    assert a == list(generate(300, 1))
    assert a != list(generate(300, 2))
    for m in a:
        assert m.startswith(r"MSH|^~\&|TRIBUTARY-SYNTH|")
        assert m.split("\r")[0].split("|")[8] in MESSAGE_TYPES
        assert not re.search(r"\n", m)
