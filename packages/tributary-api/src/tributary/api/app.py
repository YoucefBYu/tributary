"""The FastAPI application (`openapi/v0` part 1). Authentication and the single authorization layer are
`auth.request`; this module wires routes from the operation table and keeps every error a fixed problem."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from tributary.sdk.context import Logger

from . import schemas
from .audit import Actor, AuditHook
from .auth.jwks import KeySet
from .auth.request import authorize
from .auth.validator import TokenValidator
from .config import Settings
from .errors import ConfigurationError
from .health import LIVE, Readiness
from .operations import OPERATIONS, check_routes
from .problems import ApiProblem, render
from .publisher import ChannelsPublisher
from .reconciler import Reconciler, TopicReader
from .retention import RetentionReader
from .service import _ABSENT, INFLIGHT_WAIT_S, MAX_INFLIGHT_DEPLOYS, ControlPlane
from .tamper import TamperChecker
from .validation import control_plane_issues, validate_document

MAX_BODY_BYTES = 262144
# The path grammar deliberately still admits `tb-*` (ADR-010 section 3 reserves it for creation only): a read of
# such a name is a harmless 404, and the header, topic and metric-label grammars stay unchanged.
CHANNEL = r"^[a-z][a-z0-9-]{0,63}$"
ENVIRONMENT = r"^[a-z][a-z0-9-]{0,15}$"
CURSOR = r"^[A-Za-z0-9_-]{1,256}$"
UUID_ANY = r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$"
UUID_V7 = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
ChannelPath = Annotated[str, Path(pattern=CHANNEL)]
EnvPath = Annotated[str, Path(pattern=ENVIRONMENT)]
VersionPath = Annotated[int, Path(ge=1, le=2147483647)]
LimitQuery = Annotated[int, Query(ge=1, le=200)]
CursorQuery = Annotated[str | None, Query(pattern=CURSOR)]
MessageIdPath = Annotated[str, Path(pattern=UUID_ANY)]
ReplayIdPath = Annotated[str, Path(pattern=UUID_V7)]


def assert_access_log_off() -> None:
    """ADR-009 D1: the server access log is off by rule; a handler on `uvicorn.access` fails the process."""
    if logging.getLogger("uvicorn.access").handlers:
        raise ConfigurationError("ACCESS_LOG_ENABLED", "the uvicorn access log must be off (ADR-009 D1)")


def _reject_constant(_name: str) -> Any:
    raise ValueError("non-finite number")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise ValueError("duplicate key")
        out[k] = v
    return out


async def json_body(request: Request) -> Any:
    """Content type, size and syntax, in that order, each a fixed problem."""
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise ApiProblem("UNSUPPORTED_MEDIA_TYPE")
    declared = request.headers.get("content-length")
    if declared is not None and (not declared.isdigit() or int(declared) > MAX_BODY_BYTES):
        raise ApiProblem("PAYLOAD_TOO_LARGE" if declared.isdigit() else "REQUEST_INVALID")
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_BODY_BYTES:
            raise ApiProblem("PAYLOAD_TOO_LARGE")
    try:
        return json.loads(bytes(body), object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise ApiProblem("REQUEST_INVALID") from None


def _parse[M: schemas._Strict](model: type[M], raw: Any) -> M:
    try:
        return model.model_validate(raw)
    except ValidationError:
        raise ApiProblem("REQUEST_INVALID") from None  # the pydantic error echoes input: never forwarded


Body = Annotated[Any, Depends(json_body)]


def _actor(request: Request) -> Actor:
    """Who is asking, for the audit record: `sub`, the mapped Tributary roles and the request id. Nothing else
    from the token or the request."""
    principal = request.state.principal
    return Actor(str(principal.subject), frozenset(principal.roles), getattr(request.state, "request_id", None))


class _RequestLog:
    """Pure ASGI: request id, and one log line per request with method, route TEMPLATE, status, duration and
    request id only (ADR-009 D1). Never the raw path, query string or any header."""

    def __init__(self, app: ASGIApp, log: Logger) -> None:
        self.app = app
        self.log = log

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = str(uuid.uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        started = time.monotonic()
        status = 500

        async def capture(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, capture)
        finally:
            route = scope.get("route")
            self.log.info(
                "http_request",
                method=str(scope["method"])[:16],
                route=getattr(route, "path", "unmatched"),
                status=status,
                duration_ms=int((time.monotonic() - started) * 1000),
                request_id=request_id,
            )


class _FixedServerError:
    """The outermost user layer: any exception that reaches it becomes the fixed 500 problem and is NOT re-raised.

    Starlette's own `ServerErrorMiddleware` sends a 500 and then re-raises, and uvicorn logs that traceback
    through `uvicorn.error`, which has no PHI guard and carries exception text (invariant 6). Here the only
    thing logged is the exception CLASS name and the request id, through the guarded logger."""

    def __init__(self, app: ASGIApp, log: Logger) -> None:
        self.app = app
        self.log = log

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = False

        async def tracking(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, receive, tracking)
        except Exception as exc:
            request_id = scope.get("state", {}).get("request_id")
            self.log.error("unhandled_error", error=type(exc).__name__, request_id=request_id or "none")
            if started:
                return  # too late for a body; the connection ends without a re-raise
            response = render(ApiProblem("INTERNAL"), request_id)
            await response(scope, receive, send)


def create_app(
    *,
    settings: Settings,
    sessions: async_sessionmaker[AsyncSession],
    publisher: ChannelsPublisher,
    keys: KeySet,
    log: Logger,
    on_shutdown: list[Callable[[], Awaitable[None]]] | None = None,
    max_inflight_deploys: int = MAX_INFLIGHT_DEPLOYS,
    inflight_wait_s: float = INFLIGHT_WAIT_S,
    topic_reader: TopicReader | None = None,
    tamper: TamperChecker | None = None,
    audit: AuditHook,
    retention: RetentionReader | None = None,
) -> FastAPI:
    service = ControlPlane(
        sessions,
        publisher,
        log,
        max_inflight=max_inflight_deploys,
        inflight_wait_s=inflight_wait_s,
        audit=audit,
        audit_home=settings.audit_home_env,
    )
    readiness = Readiness(service.ping_database, keys, publisher)
    reconciler = (
        None
        if topic_reader is None
        else Reconciler(sessions, publisher, topic_reader, log, pending_grace_s=settings.reconcile_pending_grace_s)
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            assert_access_log_off()
            check_routes(list(app.routes))
            await service.resolve_audit_home(settings.audit_home_env)  # several environments and none set: refuse
        except ConfigurationError as exc:
            app.state.startup_error = exc  # `main` turns this into `startup_refused` and exit 2
            raise
        if settings.oidc.jwks_file is not None:
            log.warning("static_jwks_file_in_use", profile=settings.profile)
        keys.start()
        tamper_task: asyncio.Task[None] | None = None
        if tamper is not None:  # a failed check never stops the API: `check_once` does not raise
            tamper_task = asyncio.create_task(tamper.run_forever())
        loop_task: asyncio.Task[None] | None = None
        if reconciler is not None and settings.reconcile_interval_s > 0:
            loop_task = asyncio.create_task(reconciler.run_forever(settings.reconcile_interval_s))
        log.info("api_started", profile=settings.profile)
        try:
            yield
        finally:
            for task in (loop_task, tamper_task):
                if task is not None:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as exc:  # a dead background task must not skip `keys.stop()` and the hooks
                        log.error("background_task_failed", error_type=type(exc).__name__)
            await keys.stop()
            for hook in on_shutdown or []:
                await hook()

    app = FastAPI(
        title="Tributary control plane",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
        dependencies=[Depends(authorize)],
        lifespan=lifespan,
    )
    app.router.redirect_slashes = False  # a redirect would echo the path in Location
    app.state.operations = {(o.method, o.path): o for o in OPERATIONS}
    app.state.validator = TokenValidator(settings.oidc, keys)
    app.state.keys = keys
    app.state.reconciler = reconciler

    def request_id(request: Request) -> str | None:
        return getattr(request.state, "request_id", None)

    @app.exception_handler(ApiProblem)
    async def _problem(request: Request, exc: ApiProblem) -> JSONResponse:
        return render(exc, request_id(request))

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(exc.status_code, "INTERNAL")
        problem = ApiProblem(code, status=exc.status_code if code != "INTERNAL" else 500)
        if exc.status_code == 405 and exc.headers and "allow" in {k.lower() for k in exc.headers}:
            problem.headers = {k: v for k, v in exc.headers.items() if k.lower() == "allow"}
        return render(problem, request_id(request))

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        # FastAPI's default body echoes input. A malformed path parameter names something that cannot exist
        # (the contract declares 404, not 400, for operations whose only input is a path); query and body
        # problems are 400.
        in_path = any(e.get("loc", ("",))[0] == "path" for e in exc.errors())
        return render(ApiProblem("NOT_FOUND" if in_path else "REQUEST_INVALID"), request_id(request))

    # -- handlers (thin: the service and the repository do the work) ------------------------------

    async def livez() -> JSONResponse:
        return JSONResponse(LIVE)

    async def readyz() -> JSONResponse:
        status, body = await readiness.body()
        return JSONResponse(body, status_code=status)

    async def list_environments() -> JSONResponse:
        return JSONResponse(await service.environments())

    async def list_channels(limit: LimitQuery = 50, cursor: CursorQuery = None) -> JSONResponse:
        return JSONResponse(await service.list_channels(limit, cursor))

    async def create_channel(request: Request, body: Body) -> JSONResponse:
        req = _parse(schemas.ChannelVersionCreate, body)
        name = _checked_document(req.document)
        return JSONResponse(
            await service.create_channel(name, req.document, req.origin.stored(), _actor(request)),
            status_code=201,
        )

    async def get_channel(channel: ChannelPath) -> JSONResponse:
        return JSONResponse(await service.get_channel(channel))

    async def list_versions(channel: ChannelPath, limit: LimitQuery = 50, cursor: CursorQuery = None) -> JSONResponse:
        return JSONResponse(await service.list_versions(channel, limit, cursor))

    async def create_version(request: Request, channel: ChannelPath, body: Body) -> JSONResponse:
        req = _parse(schemas.ChannelVersionCreate, body)
        name = _checked_document(req.document)
        if name != channel:
            raise ApiProblem("CHANNEL_NAME_MISMATCH")
        return JSONResponse(
            await service.add_version(channel, req.document, req.origin.stored(), _actor(request)),
            status_code=201,
        )

    async def get_version(channel: ChannelPath, version: VersionPath) -> JSONResponse:
        return JSONResponse(await service.get_version(channel, version))

    async def validate(body: Body) -> JSONResponse:
        if not isinstance(body, dict):
            raise ApiProblem("REQUEST_INVALID")
        errors = (control_plane_issues(body) + validate_document(body))[:100]
        return JSONResponse({"valid": not errors, "errors": errors, "warnings": []})

    async def current_deployments(env: EnvPath, limit: LimitQuery = 50, cursor: CursorQuery = None) -> JSONResponse:
        return JSONResponse(await service.current_deployments(env, limit, cursor))

    async def channel_deployments(
        env: EnvPath, channel: ChannelPath, limit: LimitQuery = 50, cursor: CursorQuery = None
    ) -> JSONResponse:
        return JSONResponse(await service.channel_deployments(env, channel, limit, cursor))

    async def deploy(request: Request, env: EnvPath, channel: ChannelPath, body: Body) -> JSONResponse:
        req = _parse(schemas.DeployRequest, body)
        expected: Any = req.expected_current_version if "expected_current_version" in req.model_fields_set else _ABSENT
        result = await service.deploy(env, channel, req.version, expected, _actor(request))
        return JSONResponse(result, status_code=202)

    async def rollback(request: Request, env: EnvPath, channel: ChannelPath, body: Body) -> JSONResponse:
        req = _parse(schemas.RollbackRequest, body)
        result = await service.rollback(env, channel, req.expected_current_version, req.to_version, _actor(request))
        return JSONResponse(result, status_code=202)

    async def undeploy(request: Request, env: EnvPath, channel: ChannelPath, body: Body) -> JSONResponse:
        req = _parse(schemas.UndeployRequest, body)
        result = await service.undeploy(env, channel, req.expected_current_version, _actor(request))
        return JSONResponse(result, status_code=202)

    # -- part 2 stubs (W3-6 replaces them). Authn and RBAC ran in the global dependency, so only a permitted
    # caller reaches these. They read no body, query or header and log nothing: the content operations
    # (x-tributary-phi-content) therefore cannot leak request data, and no content is ever returned. The
    # answer is the failure the contract declares for the operation.

    async def archive_unavailable() -> JSONResponse:
        raise ApiProblem("ARCHIVE_UNAVAILABLE")

    async def search_messages(env: EnvPath, channel: ChannelPath) -> JSONResponse:
        return await archive_unavailable()

    async def get_message(env: EnvPath, channel: ChannelPath, message_id: MessageIdPath) -> JSONResponse:
        return await archive_unavailable()

    async def get_message_content(env: EnvPath, channel: ChannelPath, message_id: MessageIdPath) -> JSONResponse:
        return await archive_unavailable()

    async def reprocess_message(env: EnvPath, channel: ChannelPath, message_id: MessageIdPath) -> JSONResponse:
        return await archive_unavailable()

    async def list_replays(env: EnvPath, channel: ChannelPath) -> JSONResponse:
        raise ApiProblem("NOT_FOUND")  # the contract declares no 503 here; no replay can exist yet

    async def start_replay(env: EnvPath, channel: ChannelPath) -> JSONResponse:
        return await archive_unavailable()

    async def get_replay(env: EnvPath, channel: ChannelPath, replay_id: ReplayIdPath) -> JSONResponse:
        raise ApiProblem("NOT_FOUND")

    async def channel_stats(env: EnvPath, channel: ChannelPath) -> JSONResponse:
        return await archive_unavailable()

    async def stream_events(env: EnvPath) -> JSONResponse:
        return await archive_unavailable()

    async def get_retention(env: EnvPath) -> JSONResponse:
        if env not in {e["name"] for e in (await service.environments())["items"]}:
            raise ApiProblem("NOT_FOUND")
        if retention is None:
            raise ApiProblem("ARCHIVE_UNAVAILABLE")
        return JSONResponse(await retention.read(env))

    endpoints: dict[str, Callable[..., Any]] = {
        "getLiveness": livez,
        "getReadiness": readyz,
        "listEnvironments": list_environments,
        "listChannels": list_channels,
        "createChannel": create_channel,
        "getChannel": get_channel,
        "listChannelVersions": list_versions,
        "createChannelVersion": create_version,
        "getChannelVersion": get_version,
        "validateChannelDocument": validate,
        "listCurrentDeployments": current_deployments,
        "listChannelDeployments": channel_deployments,
        "deployChannelVersion": deploy,
        "rollbackChannel": rollback,
        "undeployChannel": undeploy,
        "searchMessages": search_messages,
        "getMessage": get_message,
        "getMessageContent": get_message_content,
        "reprocessMessage": reprocess_message,
        "listReplays": list_replays,
        "startReplay": start_replay,
        "getReplay": get_replay,
        "getChannelStats": channel_stats,
        "streamEvents": stream_events,
        "getRetention": get_retention,
    }
    for op in OPERATIONS:
        app.add_api_route(
            op.path, endpoints[op.operation_id], methods=[op.method], operation_id=op.operation_id, response_model=None
        )

    app.add_middleware(_RequestLog, log=log)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type"],
            allow_credentials=False,
        )
    app.add_middleware(_FixedServerError, log=log)  # last added = outermost of ours
    return app


def _checked_document(document: dict[str, Any]) -> str:
    """The create-time rules: `metadata.version` is the server's (422), then the channel schema (422 with
    `errors`). Returns `metadata.name`."""
    if control_plane_issues(document):
        raise ApiProblem("VERSION_SET_BY_CLIENT")
    if errors := validate_document(document):
        raise ApiProblem("SPEC_INVALID", errors=errors)
    return str(document["metadata"]["name"])
