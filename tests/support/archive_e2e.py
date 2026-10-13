"""The W3-9b topology: every process of the archive path, real, on one host.

Kafka (the caller's), ClickHouse, a TLS PostgreSQL, the events archiver and the content archiver (each as its own
INSERT/SELECT-only ClickHouse user under the pinned settings profile), the API (a SELECT-only ClickHouse user,
static-JWKS auth under profile lite), a real MLLP receiver and a real pipeline worker, started from the spec the
API deployed. `tests/e2e/test_archive_search_reprocess_replay_e2e.py` drives it and `benchmarks/` reuses it.

Synthetic data only (MSH-3 `TRIBUTARY-SYNTH`); no payload text is ever put in an assertion message or printed.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from tributary.api.db.engine import make_engine, make_session_factory
from tributary.api.db.migrate import upgrade
from tributary.api.db.repository import EnvironmentRepository

from tests.support.channel_spec import ordered_document
from tests.support.control_plane_stack import ClickHouse, TlsPostgres
from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import free_port
from tests.support.processes import ManagedProcess, spawn_mllp_receiver_from_topic, spawn_pipeline_worker

DATABASE = "test"
AUDIENCE = "tb-api"
ISSUER = "https://idp.invalid"


class Tokens:
    """A throwaway signing key and the static JWKS file the API trusts (profile lite, ADR-009 D9 b)."""

    def __init__(self, workdir: Path) -> None:
        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(self._key.public_key()))
        jwk.update({"kid": "w9b", "use": "sig", "alg": "RS256"})
        self.jwks = workdir / "jwks.json"
        self.jwks.write_text(json.dumps({"keys": [jwk]}))
        self._pem = self._key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )

    def bearer(self, role: str = "tb-admin", *, lifetime_s: int = 900) -> dict[str, str]:
        now = int(time.time())
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "synth-operator-w9b",
            "iat": now,
            "exp": now + lifetime_s,
            "roles": [role],
        }
        token = jwt.encode(claims, self._pem, algorithm="RS256", headers={"kid": "w9b", "typ": "at+jwt"})
        return {"Authorization": f"Bearer {token}"}


class Sse:
    """A server-sent event reader on its own thread: `events` fills with `(event, data)` as they arrive."""

    def __init__(self, base: str, path: str, headers: dict[str, str]) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self._stop = threading.Event()
        self._ready = threading.Event()
        self.status: int | None = None
        self._thread = threading.Thread(target=self._run, args=(base, path, headers), daemon=True)

    def _run(self, base: str, path: str, headers: dict[str, str]) -> None:
        with (
            httpx.Client(base_url=base, timeout=httpx.Timeout(10.0, read=None)) as http,
            http.stream("GET", path, headers=headers) as response,
        ):
            self.status = response.status_code
            self._ready.set()
            event, data = "message", ""
            for line in response.iter_lines():
                if self._stop.is_set():
                    return
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data += line[5:].strip()
                elif line == "":
                    if data:
                        with suppress(ValueError):
                            self.events.append((event, json.loads(data)))
                    event, data = "message", ""

    def __enter__(self) -> Sse:
        self._thread.start()
        assert self._ready.wait(15), "the event stream did not open"
        assert self.status == 200, f"event stream answered {self.status}"
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()

    def message_ids(self, *, stage: str | None = None) -> set[str]:
        return {
            str(d["message_id"])
            for e, d in list(self.events)
            if e == "message" and (stage is None or d.get("stage") == stage)
        }


def wait_until(predicate: Callable[[], bool], what: str, *, timeout_s: float = 60.0, every_s: float = 0.25) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(every_s)


class ArchiveStack:
    def __init__(self, workdir: Path, kafka: str, *, env: str, channel: str, run: str, content: bool = True) -> None:
        self.workdir, self.kafka, self.env, self.channel, self.run, self.content = (
            workdir,
            kafka,
            env,
            channel,
            run,
            content,
        )
        self.secrets_dir = workdir / "secrets"
        self.api_port = free_port()
        self.clickhouse: ClickHouse | None = None
        self.postgres: TlsPostgres | None = None
        self.tokens = Tokens(workdir)
        self.procs: dict[str, ManagedProcess] = {}
        self.mllp_port = 0

    # -- plumbing ------------------------------------------------------------------------------------------
    def _secret(self, ref: str, key: str, value: str) -> None:
        directory = self.secrets_dir / ref
        directory.mkdir(parents=True, exist_ok=True)
        (directory / key).write_text(value)
        (directory / key).chmod(0o600)

    def _archiver_env(self, ref: str, **extra: str) -> dict[str, str]:
        assert self.clickhouse is not None
        return engine_kafka_env(
            self.kafka,
            {
                "PATH": "/usr/bin:/bin",
                "TB_SECRETS_DIR": str(self.secrets_dir),
                "TB_OPS_PORT": "0",
                "TB_ARCHIVE_CLICKHOUSE_HOST": "127.0.0.1",
                "TB_ARCHIVE_CLICKHOUSE_PORT": str(self.clickhouse.port),
                "TB_ARCHIVE_CLICKHOUSE_DATABASE": DATABASE,
                "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
                "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": ref,
                "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "chadmin",
                "TB_ARCHIVE_CONTENT": "true" if self.content else "false",
                "TB_ARCHIVE_CONTENT_CHANNELS": self.channel,
                **extra,
            },
        )

    # -- start ---------------------------------------------------------------------------------------------
    def start_stores(self) -> None:
        self.clickhouse = ClickHouse(name=f"tb-qa-w39b-{self.run}-ch")
        self.postgres = TlsPostgres(self.workdir, name=f"tb-qa-w39b-{self.run}-pg")
        ch = self.clickhouse
        password_arch, password_api = secrets.token_urlsafe(16), secrets.token_urlsafe(16)
        self._secret("chadmin", "user", ch.user)
        self._secret("chadmin", "password", ch.password)
        self._secret("archcred", "user", "tbarch")
        self._secret("archcred", "password", password_arch)
        self._secret("apicred", "password", password_api)
        self._secret("pgcred", "password", self.postgres.password)
        self._secret("pgca", "ca.crt", self.postgres.ca_pem.decode())
        ch.query(f"CREATE USER IF NOT EXISTS tbarch IDENTIFIED BY '{password_arch}'")
        ch.query(f"GRANT INSERT, SELECT ON {DATABASE}.* TO tbarch")
        ch.query(
            "CREATE SETTINGS PROFILE IF NOT EXISTS tb_archiver_profile SETTINGS insert_allow_materialized_columns = 0 CONST"
        )
        ch.query("ALTER USER tbarch SETTINGS PROFILE 'tb_archiver_profile'")
        ch.query(f"CREATE USER IF NOT EXISTS tbapi IDENTIFIED BY '{password_api}'")
        ch.query(f"GRANT SELECT ON {DATABASE}.* TO tbapi")
        done = subprocess.run(  # noqa: S603 - fixed argv
            [sys.executable, "-m", "tributary.archiver", "--env", self.env, "--migrate-only"],
            env=self._archiver_env("archcred", TB_ARCHIVE_CONTENT_TTL_DAYS="30"),
            capture_output=True,
            timeout=180,
            check=False,
        )
        assert done.returncode == 0, f"archiver --migrate-only failed: {done.stdout[-500:]!r}"

    def start_api(self) -> None:
        assert self.postgres is not None and self.clickhouse is not None
        upgrade(self.postgres.plain_url)

        async def seed() -> None:
            engine = make_engine(self.postgres.plain_url)  # type: ignore[union-attr]
            try:
                async with make_session_factory(engine)() as s, s.begin():
                    await EnvironmentRepository(s).create(self.env, "ui-managed")
            finally:
                await engine.dispose()

        asyncio.run(seed())
        env = engine_kafka_env(
            self.kafka,
            {
                "PATH": "/usr/bin:/bin",
                "TB_DATABASE_HOST": "127.0.0.1",
                "TB_DATABASE_PORT": str(self.postgres.port),
                "TB_DATABASE_NAME": self.postgres.db,
                "TB_DATABASE_USER": self.postgres.user,
                "TB_DATABASE_SECRET_REF": "pgcred",
                "TB_DATABASE_CA_SECRET_REF": "pgca",
                "TB_SECRETS_DIR": str(self.secrets_dir),
                "TB_OIDC_ISSUER": ISSUER,
                "TB_OIDC_AUDIENCE": AUDIENCE,
                "TB_OIDC_ROLE_MAP": json.dumps({"tb-admin": "admin", "tb-viewer": "viewer", "tb-operator": "operator"}),
                "TB_OIDC_JWKS_FILE": str(self.tokens.jwks),
                "TB_PROFILE": "lite",
                "TB_API_BIND": "127.0.0.1",
                "TB_API_PORT": str(self.api_port),
                "TB_OPS_PORT": "0",
                "TB_CLICKHOUSE_URL": f"http://127.0.0.1:{self.clickhouse.port}",
                "TB_CLICKHOUSE_USER": "tbapi",
                "TB_CLICKHOUSE_SECRET_REF": "apicred",
                "TB_AUDIT_TAMPER_TARGETS": json.dumps({self.env: DATABASE}),
            },
        )
        api = ManagedProcess([sys.executable, "-m", "tributary.api"], env=env)
        self.procs["api"] = api
        api.wait_for_event("api_started", timeout_s=90)
        wait_until(lambda: self.http_get("/readyz").status_code == 200, "the API /readyz", timeout_s=30)

    def http_get(self, path: str) -> httpx.Response:
        return httpx.get(f"{self.base}{path}", timeout=5)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    def client(self, role: str = "tb-admin") -> httpx.Client:
        return httpx.Client(base_url=self.base, headers=self.tokens.bearer(role), timeout=60)

    def deploy_channel(self) -> None:
        document = ordered_document(
            mllp_port=free_port(),
            http_url="http://127.0.0.1:18080/never-dialled",
            channel=self.channel,
            destination="stub",
        )
        document["spec"]["source"]["bind"] = "127.0.0.1"
        del document["metadata"]["version"]  # the server assigns it
        with self.client() as http:
            created = http.post("/v1/channels", json={"document": document, "origin": {"kind": "api"}})
            assert created.status_code == 201, (
                created.status_code,
                created.json().get("code"),
                created.json().get("detail"),
            )
            deployed = http.post(f"/v1/environments/{self.env}/channels/{self.channel}/deploy", json={"version": 1})
            assert deployed.status_code == 202, deployed.status_code

    def start_pipeline(self) -> None:
        self.procs["receiver"] = spawn_mllp_receiver_from_topic(
            channel=self.channel, env=self.env, bootstrap=self.kafka, instance=f"{self.run}-rx", timeout_s=60
        )
        for line in self.procs["receiver"].lines:
            if line.get("event") == "listening":
                self.mllp_port = int(line["port"])
        assert self.mllp_port
        self.procs["worker"] = spawn_pipeline_worker(
            env=self.env, channel=self.channel, bootstrap=self.kafka, instance_id=f"{self.run}-wk", timeout_s=60
        )

    def start_archivers(self) -> None:
        events = ManagedProcess(
            [
                sys.executable,
                "-m",
                "tributary.archiver",
                "--env",
                self.env,
                "--instance-id",
                f"{self.run}-ev",
                "--skip-ddl",
            ],
            env=self._archiver_env("archcred"),
        )
        self.procs["archiver"] = events
        events.wait_for_event("archiver_started", timeout_s=90)
        if self.content:
            content = ManagedProcess(
                [
                    sys.executable,
                    "-m",
                    "tributary.archiver.content_archiver",
                    "--env",
                    self.env,
                    "--instance-id",
                    f"{self.run}-ct",
                ],
                env=self._archiver_env("archcred"),
            )
            self.procs["content_archiver"] = content
            content.wait_for_event("content_archiver_started", timeout_s=90)

    def start_all(self) -> None:
        self.start_stores()
        self.start_api()
        self.deploy_channel()
        self.start_archivers()
        self.start_pipeline()

    def teardown(self) -> None:
        for proc in self.procs.values():
            with suppress(Exception):
                proc.terminate_and_wait()
        for service in (self.postgres, self.clickhouse):
            if service is not None:
                with suppress(Exception):
                    service.stop()
