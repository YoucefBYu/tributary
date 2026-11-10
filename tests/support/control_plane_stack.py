"""The control-plane and archive side of the W3-9a topology: ClickHouse, PostgreSQL (TLS), the real archiver
process and the real API process, each startable and stoppable on its own. Everything here is test plumbing
for `tests/chaos/test_outage_control_plane.py`; no secret is committed (credentials are generated per run and
written to a temporary `secretRef` directory).

Containers get unique names (`tb-qa-w39a-<run>-<service>`) because several agents share one Docker daemon, and
teardown stops only the containers this object started.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import ipaddress
import json
import secrets
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer

from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import free_port
from tests.support.processes import ManagedProcess

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.8"
POSTGRES_IMAGE = "postgres:16-alpine"
DATABASE = "test"  # the testcontainers ClickHouse default database
EVENTS_TABLE = "message_events"  # `tributary.archiver.schema.TABLE`


def port_refuses(host: str, port: int, *, timeout_s: float = 2.0) -> bool:
    """True when a TCP connect to host:port fails (nothing is listening or it is unreachable)."""
    try:
        socket.create_connection((host, port), timeout=timeout_s).close()
    except OSError:
        return True
    return False


def _pem(key: ec.EllipticCurvePrivateKey | rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


class TlsPostgres:
    """PostgreSQL with TLS on (throwaway self-signed certificate with a 127.0.0.1 IP SAN, generated at start).
    The API connects with `verify-full` (ADR-004 C1/C2), so a plain container would be refused."""

    def __init__(self, workdir: Path, *, name: str) -> None:
        key = ec.generate_private_key(ec.SECP256R1())
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tb-qa-w39a-pg")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))
            .not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(hours=2))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
            .sign(key, hashes.SHA256())
        )
        certs = workdir / "pgcerts"
        certs.mkdir()
        self.ca_pem = cert.public_bytes(serialization.Encoding.PEM)
        (certs / "server.crt").write_bytes(self.ca_pem)
        (certs / "server.key").write_bytes(_pem(key))
        certs.chmod(0o755)
        for entry in certs.iterdir():
            entry.chmod(0o644)
        self.user, self.db, self.password = "tbapi", "tb", secrets.token_urlsafe(16)
        script = (
            "cp /certs/server.crt /certs/server.key /tmp/ && chown postgres /tmp/server.crt /tmp/server.key "
            "&& chmod 600 /tmp/server.key && exec docker-entrypoint.sh postgres -c ssl=on "
            "-c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key"
        )
        self.container = (
            PostgresContainer(POSTGRES_IMAGE, username=self.user, password=self.password, dbname=self.db, driver=None)
            .with_volume_mapping(str(certs), "/certs", "ro")
            .with_command(["sh", "-c", script])
            .with_name(name)
        )
        self.container.start()
        try:
            self.port = int(self.container.get_exposed_port(5432))
        except BaseException:
            self.container.stop()
            raise

    @property
    def plain_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@127.0.0.1:{self.port}/{self.db}"

    def stop(self) -> None:
        self.container.stop()


class ClickHouse:
    """A real ClickHouse server. `query` talks to the HTTP interface with the stdlib, so the test needs no
    client library."""

    def __init__(self, *, name: str) -> None:
        self.container = (
            ClickHouseContainer(CLICKHOUSE_IMAGE).with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1").with_name(name)
        )
        self.container.start()
        try:
            self.port = int(self.container.get_exposed_port(8123))
            self.user: str = self.container.username
            self.password: str = self.container.password
        except BaseException:
            self.container.stop()
            raise

    def query(self, sql: str, *, timeout_s: float = 10.0, as_user: tuple[str, str] | None = None) -> str:
        user, password = as_user or (self.user, self.password)
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/?database={DATABASE}",
            data=sql.encode(),
            headers={"Authorization": f"Basic {token}"},
        )
        with urllib.request.urlopen(request, timeout=timeout_s) as response:  # noqa: S310
            return str(response.read().decode().strip())

    def archived_rows(self) -> int:
        """Rows in the archive's event table (0 while the archiver has not created or filled it)."""
        try:
            return int(self.query(f"SELECT count() FROM {DATABASE}.{EVENTS_TABLE}"))  # noqa: S608 - module constants
        except (urllib.error.URLError, OSError, ValueError):
            return 0

    def stop(self) -> None:
        self.container.stop()


class ControlPlaneStack:
    """One run's ClickHouse + PostgreSQL + archiver + API. Start order: containers, migrations, archiver (it
    creates its schema and needs ClickHouse at startup), API. `stop_all_services` is the W3-9a fault."""

    def __init__(self, workdir: Path, kafka: str, *, run: str, env: str) -> None:
        self.workdir, self.kafka, self.run, self.env = workdir, kafka, run, env
        self.clickhouse: ClickHouse | None = None
        self.postgres: TlsPostgres | None = None
        self.archiver: ManagedProcess | None = None
        self.api: ManagedProcess | None = None
        self.api_port = free_port()
        self.secrets_dir = workdir / "secrets"

    # -- start -------------------------------------------------------------------------------------------
    def start_containers(self) -> None:
        self.clickhouse = ClickHouse(name=f"tb-qa-w39a-{self.run}-{uuid.uuid4().hex[:8]}-ch")
        self.postgres = TlsPostgres(self.workdir, name=f"tb-qa-w39a-{self.run}-{uuid.uuid4().hex[:8]}-pg")
        for ref, key, data in (
            ("chcred", "user", self.clickhouse.user.encode()),
            ("chcred", "password", self.clickhouse.password.encode()),
            ("pgcred", "password", self.postgres.password.encode()),
            ("pgca", "ca.crt", self.postgres.ca_pem),
        ):
            directory = self.secrets_dir / ref
            directory.mkdir(parents=True, exist_ok=True)
            (directory / key).write_bytes(data)
            (directory / key).chmod(0o600)

    def _migrate_and_create_runtime_user(self) -> None:
        """The archiver refuses a ClickHouse user holding more than INSERT and SELECT on `audit_log` (ADR-004
        A5, R7), so: migrate with the admin credential, then run the archiver as an INSERT/SELECT-only user."""
        assert self.clickhouse is not None
        env = engine_kafka_env(
            self.kafka,
            {
                "PATH": "/usr/bin:/bin",
                "TB_SECRETS_DIR": str(self.secrets_dir),
                "TB_ARCHIVE_CLICKHOUSE_HOST": "127.0.0.1",
                "TB_ARCHIVE_CLICKHOUSE_PORT": str(self.clickhouse.port),
                "TB_ARCHIVE_CLICKHOUSE_DATABASE": DATABASE,
                "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
                "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "chcred",
            },
        )
        done = subprocess.run(  # noqa: S603 - fixed argv
            [sys.executable, "-m", "tributary.archiver", "--env", self.env, "--migrate-only"],
            env=env,
            capture_output=True,
            timeout=120,
            check=False,
        )
        assert done.returncode == 0, f"archiver --migrate-only failed: {done.stdout[-600:]!r} {done.stderr[-600:]!r}"
        password = secrets.token_urlsafe(16)
        self.clickhouse.query(f"CREATE USER IF NOT EXISTS tbarch IDENTIFIED BY '{password}'")
        self.clickhouse.query(f"GRANT INSERT, SELECT ON {DATABASE}.* TO tbarch")
        self.clickhouse.query(
            "CREATE SETTINGS PROFILE IF NOT EXISTS tb_archiver_profile SETTINGS insert_allow_materialized_columns = 0 CONST"
        )
        self.clickhouse.query("ALTER USER tbarch SETTINGS PROFILE 'tb_archiver_profile'")
        seen = self.clickhouse.query(
            "SELECT value, readonly FROM system.settings WHERE name = 'insert_allow_materialized_columns' FORMAT TSV",
            as_user=("tbarch", password),
        )
        assert seen.split() == ["0", "1"], (
            "the archiver user must see insert_allow_materialized_columns 0 and readonly 1"
        )
        directory = self.secrets_dir / "archcred"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "user").write_bytes(b"tbarch")
        (directory / "password").write_bytes(password.encode())

    def start_archiver(self, *, timeout_s: float = 60.0) -> ManagedProcess:
        assert self.clickhouse is not None
        self._migrate_and_create_runtime_user()
        env = engine_kafka_env(
            self.kafka,
            {
                "PATH": "/usr/bin:/bin",
                "TB_OPS_PORT": "0",
                "TB_SECRETS_DIR": str(self.secrets_dir),
                "TB_ARCHIVE_CLICKHOUSE_HOST": "127.0.0.1",
                "TB_ARCHIVE_CLICKHOUSE_PORT": str(self.clickhouse.port),
                "TB_ARCHIVE_CLICKHOUSE_DATABASE": DATABASE,
                "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
                "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "archcred",
            },
        )
        self.archiver = ManagedProcess(
            [
                sys.executable,
                "-m",
                "tributary.archiver",
                "--env",
                self.env,
                "--instance-id",
                f"w39a-{self.run}",
                "--skip-ddl",
            ],
            env=env,
        )
        self.archiver.wait_for_event("archiver_started", timeout_s=timeout_s)
        return self.archiver

    def start_api(self, *, timeout_s: float = 90.0) -> ManagedProcess:
        assert self.postgres is not None
        from tributary.api.db.migrate import upgrade

        upgrade(self.postgres.plain_url)
        jwks = self.workdir / "jwks.json"
        public = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()
        import jwt

        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(public))
        jwk.update({"kid": "w39a", "use": "sig", "alg": "RS256"})
        jwks.write_text(json.dumps({"keys": [jwk]}))
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
                "TB_OIDC_ISSUER": "https://idp.invalid",
                "TB_OIDC_AUDIENCE": "tb-api",
                "TB_OIDC_ROLE_MAP": json.dumps({"tb-admin": "admin"}),
                "TB_OIDC_JWKS_FILE": str(jwks),
                "TB_OPS_PORT": "0",
                "TB_PROFILE": "lite",
                "TB_API_BIND": "127.0.0.1",
                "TB_API_PORT": str(self.api_port),
            },
        )
        self.api = ManagedProcess([sys.executable, "-m", "tributary.api"], env=env)
        self.api.wait_for_event("api_started", timeout_s=timeout_s)
        return self.api

    # -- probes -------------------------------------------------------------------------------------------
    def api_ready(self) -> bool:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.api_port}/readyz", timeout=3) as response:
                return bool(response.status == 200)
        except (urllib.error.URLError, OSError):
            return False

    def wait_api_ready(self, *, timeout_s: float = 30.0) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.api_ready():
                return
            time.sleep(0.2)
        raise AssertionError("the API never reported /readyz 200 (so its PostgreSQL link is not proven)")

    # -- the fault ----------------------------------------------------------------------------------------
    def stop_all_services(self) -> None:
        """Stop the API process, PostgreSQL and ClickHouse. The archiver is NOT stopped: it stays in the
        topology so a stopped ClickHouse is a real fault for a live consumer (invariant 9)."""
        assert self.api is not None and self.postgres is not None and self.clickhouse is not None
        self.api.terminate_and_wait()
        self.postgres.stop()
        self.clickhouse.stop()

    def teardown(self) -> None:
        for proc in (self.api, self.archiver):
            if proc is not None:
                with contextlib.suppress(Exception):
                    proc.terminate_and_wait()
        for service in (self.postgres, self.clickhouse):
            if service is not None:
                with contextlib.suppress(Exception):
                    service.stop()
