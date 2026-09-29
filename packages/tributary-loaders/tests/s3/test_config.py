"""`s3_destination_config_from_destination` refuses what this loader cannot honour."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.s3.config import S3DestinationConfig, s3_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def make_destination(params: dict[str, object] | None = None, technique: str = "deterministic-name") -> Destination:
    base: dict[str, object] = {
        "bucket": "tributary-archive",
        "region": "eu-west-1",
        "serverSideEncryption": "AES256",
        "auth": {"type": "ambient"},
        "timeoutMs": 5000,
    }
    base.update(params or {})
    return Destination(
        name="s3-out",
        type="s3",
        params=base,
        idempotency=Idempotency(technique=technique),
        retry=RetryPolicy(policy="blocking", max_attempts=20, on_exhaust="halt-partition"),
    )


def build(params: dict[str, object] | None = None, technique: str = "deterministic-name") -> S3DestinationConfig:
    return s3_destination_config_from_destination("c", make_destination(params, technique))


def test_builds_with_defaults() -> None:
    cfg = build()
    assert (cfg.bucket, cfg.region, cfg.prefix, cfg.endpoint, cfg.file_extension) == (
        "tributary-archive",
        "eu-west-1",
        "",
        None,
        "",
    )
    assert cfg.auth_type == "ambient" and cfg.auth_secret_ref is None
    assert cfg.timeout_s == 5.0


def test_a_published_example_with_an_s3_destination_builds() -> None:
    for path in sorted(EXAMPLES.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        spec["metadata"].setdefault("version", 0)
        for destination in spec.get("spec", {}).get("destinations", []):
            if destination.get("type") == "s3":
                parsed = ChannelSpec.from_document(spec).destination(destination["name"])
                assert s3_destination_config_from_destination("c", parsed).bucket


def test_empty_circuit_breaker_block_means_configured_with_defaults() -> None:
    cfg = build({"circuitBreaker": {}})
    assert (cfg.circuit_breaker.failure_threshold, cfg.circuit_breaker.open_seconds) == (5, 30.0)


def test_access_key_needs_a_secret_ref_and_ambient_must_not_have_one() -> None:
    ref = "s3-creds"
    assert build({"auth": {"type": "access-key", "secretRef": ref}}).auth_secret_ref == ref
    with pytest.raises(ConfigError, match="secretRef"):
        build({"auth": {"type": "access-key"}})
    with pytest.raises(ConfigError, match="ambient"):
        build({"auth": {"type": "ambient", "secretRef": "s3-creds"}})
    with pytest.raises(ConfigError, match="auth"):
        build({"auth": {"type": "anonymous"}})


@pytest.mark.parametrize("host", ["minio.example.org", "10.0.0.5", "127.0.0.1.evil.example"])
def test_plaintext_endpoint_to_a_non_loopback_host_is_refused_at_runtime(host: str) -> None:
    with pytest.raises(ConfigError, match="plaintext"):
        build({"endpoint": f"http://{host}:9000"})


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "[::1]"])
def test_plaintext_endpoint_to_loopback_is_allowed(host: str) -> None:
    assert build({"endpoint": f"http://{host}:9000"}).endpoint == f"http://{host}:9000"


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:pw@s3.example.org",
        "https://s3.example.org/bucket",
        "https://s3.example.org?x=1",
        "https://s3.example.org#f",
        "ftp://s3.example.org",
        "https://",
    ],
)
def test_bad_endpoints_are_refused_without_echoing_them(endpoint: str) -> None:
    with pytest.raises(ConfigError) as exc:
        build({"endpoint": endpoint})
    assert "pw" not in str(exc.value)


def test_encryption_is_mandatory_and_kms_key_only_with_kms() -> None:
    destination = make_destination()
    del destination.params["serverSideEncryption"]
    with pytest.raises(ConfigError, match=r"serverSideEncryption|missing"):
        s3_destination_config_from_destination("c", destination)
    with pytest.raises(ConfigError, match="serverSideEncryption"):
        build({"serverSideEncryption": "none"})
    with pytest.raises(ConfigError, match="kmsKeyId"):
        build({"serverSideEncryption": "AES256", "kmsKeyId": "alias/x"})
    cfg = build({"serverSideEncryption": "aws:kms", "kmsKeyId": "arn:aws:kms:eu-west-1:1:key/abc"})
    assert cfg.kms_key_id == "arn:aws:kms:eu-west-1:1:key/abc"


@pytest.mark.parametrize("prefix", ["Upper", "a/../b", "a//b", "/lead", "trail/", ".", "a b", "x" * 70])
def test_unsafe_prefixes_are_refused(prefix: str) -> None:
    with pytest.raises(ConfigError, match="prefix"):
        build({"prefix": prefix})


def test_valid_prefix_is_kept() -> None:
    assert build({"prefix": "adt/inbound"}).prefix == "adt/inbound"


@pytest.mark.parametrize("bucket", ["UPPER", "a", "has..dots", "-lead", "trail-", "with space", "x" * 64])
def test_bad_buckets_are_refused(bucket: str) -> None:
    with pytest.raises(ConfigError, match="bucket"):
        build({"bucket": bucket})


@pytest.mark.parametrize("technique", ["sink-native", "none", "conditional-update", "idempotency-key-header"])
def test_only_deterministic_name_is_declarable(technique: str) -> None:
    with pytest.raises(ConfigError, match="not permitted"):
        build(technique=technique)


def test_bad_region_extension_and_timeout_are_refused() -> None:
    with pytest.raises(ConfigError, match="region"):
        build({"region": "EU WEST"})
    with pytest.raises(ConfigError, match="fileExtension"):
        build({"fileExtension": "hl7"})
    with pytest.raises(ConfigError, match="timeoutMs"):
        build({"timeoutMs": 0})
