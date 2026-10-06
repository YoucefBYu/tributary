"""Shared kit for the API tests: a local HTTPS identity provider with synthetic keys (generated at test start,
never committed, ADR-009 D9), a token minter, a fake channels publisher and an OpenAPI response checker."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import ssl
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
from jsonschema import Draft202012Validator, FormatChecker
from tributary.api.auth.jwks import KeySet
from tributary.api.config import OidcSettings
from tributary.api.publisher import PublishError

REPO = Path(__file__).resolve().parents[4]
OPENAPI = REPO / "contracts" / "openapi" / "v0" / "openapi.json"
AUDIENCE = "tb-api"
ROLE_MAP = {
    "tb-viewer": "viewer",
    "tb-developer": "developer",
    "tb-operator": "operator",
    "tb-auditor": "auditor",
    "tb-admin": "admin",
}


def public_jwk(key: Any, kid: str, alg: str | None = None) -> dict[str, Any]:
    jwk = (
        json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
        if isinstance(key, rsa.RSAPrivateKey)
        else (json.loads(jwt.algorithms.ECAlgorithm.to_jwk(key.public_key())))
    )
    jwk.update({"kid": kid, "use": "sig"})
    if alg:
        jwk["alg"] = alg
    return jwk


def pem(key: Any) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


class Idp:
    """An HTTPS server on 127.0.0.1 serving discovery and a JWKS, recording every request path."""

    def __init__(self, tmp: Path) -> None:
        self.rsa = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.rsa2 = rsa.generate_private_key(public_exponent=65537, key_size=2048)  # an attacker's key
        self.ec = ec.generate_private_key(ec.SECP256R1())
        self.requests: list[str] = []
        self.fail = False
        self.delay_s = 0.0
        self.issuer_override: str | None = None
        self.jwks_uri_override: str | None = None
        self.keys: list[dict[str, Any]] = [public_jwk(self.rsa, "rsa-1"), public_jwk(self.ec, "ec-1")]
        cert_key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tb-test-idp")])
        import ipaddress

        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(cert_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))
            .not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
            .sign(cert_key, hashes.SHA256())
        )
        self.cert_file = tmp / "idp.pem"
        self.cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_file = self.key_file = tmp / "idp.key"
        key_file.write_bytes(pem(cert_key))
        idp = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:  # silence
                pass

            def do_GET(self) -> None:
                idp.requests.append(self.path)
                time.sleep(idp.delay_s)
                if idp.fail:
                    self.send_response(500)
                    self.end_headers()
                    return
                if self.path == "/.well-known/openid-configuration":
                    body = {
                        "issuer": idp.issuer_override or idp.issuer,
                        "jwks_uri": idp.jwks_uri_override or f"{idp.issuer}/jwks",
                    }
                elif self.path == "/jwks":
                    body = {"keys": idp.keys}
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert_file, key_file)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_address[1]
        self.issuer = f"https://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def client_context(self) -> ssl.SSLContext:
        return ssl.create_default_context(cafile=str(self.cert_file))

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def jwks_file(self, path: Path) -> Path:
        path.write_text(json.dumps({"keys": self.keys}))
        return path

    def mint(
        self,
        *,
        key: Any = None,
        kid: str | None = "rsa-1",
        alg: str = "RS256",
        typ: str | None = "at+jwt",
        claims: dict[str, Any] | None = None,
        drop: tuple[str, ...] = (),
        headers: dict[str, Any] | None = None,
        roles: list[str] | str | None = None,
    ) -> str:
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": self.issuer,
            "aud": AUDIENCE,
            "sub": "user-1",
            "exp": now + 300,
            "iat": now,
            "roles": ["tb-developer"] if roles is None else roles,
        }
        payload.update(claims or {})
        for name in drop:
            payload.pop(name, None)
        hdr: dict[str, Any] = {"kid": kid, "typ": typ, **(headers or {})}
        hdr = {k: v for k, v in hdr.items() if v is not None or k == "typ"}  # typ=None omits it
        signing: Any = key if key is not None else (self.ec if alg == "ES256" else self.rsa)
        return jwt.encode(payload, None if alg == "none" else pem(signing), algorithm=alg, headers=hdr)


class FakePublisher:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, bytes | None]] = []
        self.fail_with: type[PublishError] | None = None
        self.gate: asyncio.Event | None = None
        self._degraded = False

    @property
    def degraded(self) -> bool:
        return self._degraded

    async def publish(self, env: str, channel: str, value: bytes | None) -> None:
        if self.gate is not None:
            await self.gate.wait()
        if self.fail_with is not None:
            self._degraded = True
            raise self.fail_with
        self._degraded = False
        self.records.append((env, channel, value))


class Contract:
    """Checks a response against `openapi.json`: declared status, content type and schema."""

    def __init__(self) -> None:
        self.doc = json.loads(OPENAPI.read_text())

    def _resolve(self, node: dict[str, Any]) -> dict[str, Any]:
        while "$ref" in node:
            target: Any = self.doc
            for part in node["$ref"].removeprefix("#/").split("/"):
                target = target[part]
            node = target
        return node

    def check(self, method: str, template: str, response: httpx.Response) -> None:
        op = self.doc["paths"][template][method.lower()]
        declared = op["responses"].get(str(response.status_code))
        if declared is None:
            assert response.status_code >= 500, f"{response.status_code} is not declared for {method} {template}"
            declared = {"content": {"application/problem+json": {"schema": {"$ref": "#/components/schemas/Problem"}}}}
        declared = self._resolve(declared)
        content = declared.get("content", {})
        media = response.headers["content-type"].split(";")[0]
        assert media in content, f"{media} not declared for {response.status_code} {method} {template}"
        schema = {"$ref": content[media]["schema"]["$ref"], "components": self.doc["components"]}
        errors = list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(response.json()))
        assert not errors, [e.message for e in errors][:3]


def oidc_settings(idp: Idp, **overrides: Any) -> OidcSettings:
    values: dict[str, Any] = {"issuer": idp.issuer, "audience": AUDIENCE, "role_map": ROLE_MAP}
    values.update(overrides)
    return OidcSettings(**values)


class Harness:
    def __init__(self, app: FastAPI, keys: KeySet, publisher: FakePublisher) -> None:
        self.app, self.keys, self.publisher = app, keys, publisher

    @asynccontextmanager
    async def running(self) -> AsyncIterator[httpx.AsyncClient]:
        async with self.app.router.lifespan_context(self.app):
            await self.keys.refresh()
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                yield client


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def run[T](coro: Any) -> T:
    return asyncio.run(coro)  # type: ignore[no-any-return]


class TlsPg:
    """A PostgreSQL container with TLS on (a throwaway self-signed certificate with an IP SAN for 127.0.0.1,
    generated at start, never committed). `plain_url` is for seeding and migrations only; the API under test
    connects with `verify-full` using `ca_file`."""

    def __init__(self, tmp: Path) -> None:
        import ipaddress
        import secrets

        from testcontainers.community.postgres import PostgresContainer

        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tb-test-pg")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))
            .not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(hours=2))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
            .sign(key, hashes.SHA256())
        )
        certs = tmp / "pgcerts"
        certs.mkdir()
        self.ca_file = certs / "ca.crt"
        self.ca_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (certs / "server.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (certs / "server.key").write_bytes(pem(key))
        certs.chmod(0o755)
        for f in certs.iterdir():
            f.chmod(0o644)
        self.user, self.db, self.password = "tbapi", "tb", secrets.token_urlsafe(16)
        script = (
            "cp /certs/server.crt /certs/server.key /tmp/ && chown postgres /tmp/server.crt /tmp/server.key "
            "&& chmod 600 /tmp/server.key && exec docker-entrypoint.sh postgres -c ssl=on "
            "-c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key"
        )
        self.container = (
            PostgresContainer(
                "postgres:16-alpine", username=self.user, password=self.password, dbname=self.db, driver=None
            )
            .with_volume_mapping(str(certs), "/certs", "ro")
            .with_command(["sh", "-c", script])
        )
        self.container.start()
        self.host = "127.0.0.1"
        self.port = int(self.container.get_exposed_port(5432))

    @property
    def plain_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    def secrets_dir(self, tmp: Path) -> Path:
        """`<dir>/pgcred/password` and `<dir>/pgca/ca.crt`, the secretRef layout the API reads."""
        root = tmp / "secrets"
        for ref, key, data in (
            ("pgcred", "password", self.password.encode()),
            ("pgca", "ca.crt", self.ca_file.read_bytes()),
        ):
            d = root / ref
            d.mkdir(parents=True, exist_ok=True)
            (d / key).write_bytes(data)
            (d / key).chmod(0o600)
        return root

    def env(self, secrets_dir: Path) -> dict[str, str]:
        return {
            "TB_DATABASE_HOST": self.host,
            "TB_DATABASE_PORT": str(self.port),
            "TB_DATABASE_NAME": self.db,
            "TB_DATABASE_USER": self.user,
            "TB_DATABASE_SECRET_REF": "pgcred",
            "TB_DATABASE_CA_SECRET_REF": "pgca",
            "TB_SECRETS_DIR": str(secrets_dir),
        }

    def stop(self) -> None:
        self.container.stop()
