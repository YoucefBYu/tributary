"""`S3Loader` against a real MinIO server (`testcontainers`), proving invariant 4 and README 3.4.5 for real:
a message delivered twice leaves exactly one object with the right bytes and encryption, the second delivery
reported as a duplicate by the store's own `412`, and a key holding another message is refused.

MinIO is AGPL-3.0 and is used here only as a throwaway test container image (`cgr.dev/chainguard/minio`;
the Docker Hub `minio/minio` image is no longer published); it is not a dependency of any package.

The genuine-redelivery half (a real `ConsumerLoop` replay from an uncommitted offset) is
`tests/integration/test_s3_loader_duplicate_delivery_uncommitted_redelivery.py`.

`uv run pytest -m integration packages/tributary-loaders/tests/s3`.
"""

from __future__ import annotations

import base64
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
from tributary.loaders.s3.client import build_client
from tributary.loaders.s3.config import S3DestinationConfig
from tributary.loaders.s3.loader import S3Loader, object_key

from .test_loader import make_config, make_message

pytestmark = pytest.mark.integration

IMAGE = "cgr.dev/chainguard/minio:latest"
ROOT_USER = "synthminio"
ROOT_LOGIN = uuid.uuid4().hex  # a fresh throwaway login for a container that lives for this module
BUCKET = "tributary-test"
CREDS_REF = "minio-creds"  # a secretRef name


class _Resolver:
    def resolve_text(self, ref: str, key: str) -> str:
        values = {"access_key_id": ROOT_USER, "secret_access_key": ROOT_LOGIN}
        if key not in values:
            raise FileNotFoundError(key)  # an optional key (session_token) is simply absent
        return values[key]

    def resolve_file(self, ref: str, key: str) -> Path:  # pragma: no cover
        raise FileNotFoundError(key)


class Store:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    def config(self, **overrides: object) -> S3DestinationConfig:
        base: dict[str, object] = {
            "bucket": BUCKET,
            "endpoint": self.endpoint,
            "auth_type": "access-key",
            "auth_secret_ref": CREDS_REF,
            "prefix": "adt",
            "timeout_s": 10.0,
        }
        base.update(overrides)
        return make_config(**base)

    def loader(self, **overrides: object) -> S3Loader:
        config = self.config(**overrides)
        return S3Loader(config, build_client(config, _Resolver()))

    def admin(self) -> Any:
        return build_client(self.config(), _Resolver())

    def keys(self, prefix: str) -> list[str]:
        listing = self.admin().list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        return [o["Key"] for o in listing.get("Contents", [])]


@pytest.fixture(scope="module")
def minio() -> Iterator[Store]:
    kms_key = "test-key:" + base64.b64encode(os.urandom(32)).decode()  # SSE-S3 needs a KMS key configured
    container = (
        DockerContainer(IMAGE)
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", ROOT_USER)
        .with_env("MINIO_ROOT_PASSWORD", ROOT_LOGIN)
        .with_env("MINIO_KMS_SECRET_KEY", kms_key)
        .with_command("server /data --address :9000")
        .waiting_for(HttpWaitStrategy(9000, "/minio/health/live").with_startup_timeout(60))
    )
    with container:
        store = Store(f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}")
        store.admin().create_bucket(Bucket=BUCKET)
        yield store


def test_the_same_message_delivered_twice_leaves_one_object_with_the_right_bytes(minio: Store) -> None:
    loader = minio.loader()
    message = make_message(b"MSH|^~\\&|TRIBUTARY-SYNTH|x\rPID|1\r")
    key = object_key(minio.config(), message)

    first = loader.deliver(message)
    second = loader.deliver(message)  # a redelivery of the identical record

    assert (first.success, first.reason) == (True, "success")
    assert (second.success, second.reason) == (True, "duplicate")
    assert loader.conditional_put_supported is True  # the store's own 412 made it a duplicate
    assert minio.keys(f"adt/2026/03/09/{message.msg_id}") == [key]
    stored = minio.admin().get_object(Bucket=BUCKET, Key=key)
    assert stored["Body"].read() == message.payload
    assert stored["ServerSideEncryption"] == "AES256"
    assert stored["Metadata"]["tb-msg-id"] == message.msg_id
    loader.close()


def test_two_different_messages_are_two_objects(minio: Store) -> None:
    loader = minio.loader(prefix="two")
    a, b = make_message(b"one\r"), make_message(b"two\r")
    assert loader.deliver(a).success and loader.deliver(b).success
    assert len(minio.keys("two/")) == 2
    loader.close()


def test_a_reprocessed_copy_with_a_new_msg_id_is_a_second_object_by_design(minio: Store) -> None:
    """README 5: reprocess mints a new `tb-msg-id`. Also the control showing the one-object assertions are
    not vacuous: the key really is derived from the id."""
    loader = minio.loader(prefix="reprocess")
    assert loader.deliver(make_message(b"same bytes\r")).success
    assert loader.deliver(make_message(b"same bytes\r")).success
    assert len(minio.keys("reprocess/")) == 2
    loader.close()


def test_a_key_holding_another_message_is_refused_not_treated_as_a_duplicate(minio: Store) -> None:
    loader = minio.loader(prefix="conflict")
    message = make_message(b"the real payload\r")
    minio.admin().put_object(Bucket=BUCKET, Key=object_key(minio.config(prefix="conflict"), message), Body=b"other")
    outcome = loader.deliver(message)
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "S3_KEY_CONFLICT", False)
    loader.close()


def test_a_missing_bucket_is_a_non_retryable_store_error(minio: Store) -> None:
    outcome = minio.loader(bucket="no-such-bucket").deliver(make_message(b"x\r"))
    assert (outcome.reason, outcome.retryable) == ("S3_NO_SUCH_BUCKET", False)


def test_wrong_credentials_are_a_non_retryable_store_error(minio: Store) -> None:
    class Wrong(_Resolver):
        def resolve_text(self, ref: str, key: str) -> str:
            return "wrong-value-0000"

    config = minio.config()
    outcome = S3Loader(config, build_client(config, Wrong())).deliver(make_message(b"x\r"))
    assert outcome.success is False and outcome.retryable is False
    assert outcome.reason in ("S3_INVALID_ACCESS_KEY_ID", "S3_SIGNATURE_DOES_NOT_MATCH", "S3_ACCESS_DENIED")


def test_a_refused_connection_is_retryable_connect_failed() -> None:
    config = make_config(
        endpoint="http://127.0.0.1:1", auth_type="access-key", auth_secret_ref=CREDS_REF, timeout_s=2.0
    )
    outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message(b"x\r"))
    assert (outcome.reason, outcome.retryable) == ("CONNECT_FAILED", True)
