"""The operation table: one entry per route, with the roles from `openapi/v0` `x-tributary-roles`.

Routes are registered from this table, a startup check refuses a route that is not in it, and a contract
test compares it with `openapi.json`. Roles are global in v0 (ADR-009 D6). Message content operations
(D14) are limited to operator and admin. The part 2 (archive) operations are answered by stubs until W3-6
(`app.py`): authn and RBAC are real, the answer is the contract's declared failure.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import ConfigurationError

ALL_READ = ("viewer", "developer", "operator", "auditor", "admin")


@dataclass(frozen=True, slots=True)
class Operation:
    method: str
    path: str
    operation_id: str
    roles: tuple[str, ...]
    public: bool = False  # `security: []` in the contract (health)


_CH = "/v1/environments/{env}/channels/{channel}"

OPERATIONS: tuple[Operation, ...] = (
    Operation("GET", "/livez", "getLiveness", (), public=True),
    Operation("GET", "/readyz", "getReadiness", (), public=True),
    Operation("GET", "/v1/environments", "listEnvironments", ALL_READ),
    Operation("GET", "/v1/channels", "listChannels", ALL_READ),
    Operation("POST", "/v1/channels", "createChannel", ("developer", "admin")),
    Operation("GET", "/v1/channels/{channel}", "getChannel", ALL_READ),
    Operation("GET", "/v1/channels/{channel}/versions", "listChannelVersions", ALL_READ),
    Operation("POST", "/v1/channels/{channel}/versions", "createChannelVersion", ("developer", "admin")),
    Operation(
        "GET",
        "/v1/channels/{channel}/versions/{version}",
        "getChannelVersion",
        ("developer", "operator", "auditor", "admin"),
    ),
    Operation("POST", "/v1/validate", "validateChannelDocument", ("developer", "operator", "admin")),
    Operation("GET", "/v1/environments/{env}/deployments", "listCurrentDeployments", ALL_READ),
    Operation("GET", "/v1/environments/{env}/channels/{channel}/deployments", "listChannelDeployments", ALL_READ),
    Operation(
        "POST", "/v1/environments/{env}/channels/{channel}/deploy", "deployChannelVersion", ("operator", "admin")
    ),
    Operation("POST", "/v1/environments/{env}/channels/{channel}/rollback", "rollbackChannel", ("operator", "admin")),
    Operation("POST", "/v1/environments/{env}/channels/{channel}/undeploy", "undeployChannel", ("operator", "admin")),
    # -- part 2 (W3-6): archive operations --------------------------------------------------------
    Operation("POST", f"{_CH}/messages/search", "searchMessages", ALL_READ),
    Operation("GET", f"{_CH}/messages/{{message_id}}", "getMessage", ALL_READ),
    Operation("GET", f"{_CH}/messages/{{message_id}}/content", "getMessageContent", ("operator", "admin")),
    Operation("POST", f"{_CH}/messages/{{message_id}}/reprocess", "reprocessMessage", ("operator", "admin")),
    Operation("GET", f"{_CH}/replays", "listReplays", ALL_READ),
    Operation("POST", f"{_CH}/replays", "startReplay", ("operator", "admin")),
    Operation("GET", f"{_CH}/replays/{{replay_id}}", "getReplay", ALL_READ),
    Operation("GET", f"{_CH}/stats", "getChannelStats", ALL_READ),
    Operation("GET", "/v1/environments/{env}/events", "streamEvents", ALL_READ),
    Operation("GET", "/v1/environments/{env}/retention", "getRetention", ALL_READ),
)

TABLE: dict[tuple[str, str], Operation] = {(o.method, o.path): o for o in OPERATIONS}


def check_routes(routes: list[object]) -> None:
    """Startup check (D6): every registered route and method has an entry. Refuses to start otherwise."""
    for route in routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if path is None or not methods:
            continue
        for method in methods:
            if (method, path) not in TABLE:
                raise ConfigurationError("ROUTE_WITHOUT_ROLES", "a registered route has no x-tributary-roles entry")
