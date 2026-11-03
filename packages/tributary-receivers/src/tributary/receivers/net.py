"""The bind-address rule, shared by every receiver type (gate requirement 8, SR-8/CR-10).

Phase 0's S2 spike defaulted its MLLP listener to `0.0.0.0` with no peer authentication (gate finding
SR-8). The channel-spec contract now encodes the fix as data (`contracts/channel-spec/v0` `source`
`allOf`: a non-loopback `bind` requires `tls`), and this module is the runtime-side mirror of that
rule: **there is no default bind address**, and starting a plaintext listener on anything but loopback
is refused rather than logged and allowed. A spec that already violates this was rejected by the
producer-side JSON Schema before it could reach here; this check exists because invariant 3 says the
runtime must not trust the control plane to have run, and because a hand-built `Settings` (tests, a
future non-channel-spec caller) has no schema in front of it at all.
"""

from __future__ import annotations

import ipaddress

from tributary.runtime.ops import is_loopback as is_loopback

from .errors import BindError


def require_safe_bind(bind: str, *, tls_configured: bool) -> None:
    """Raise `BindError` unless the listener is loopback-only or TLS is configured.

    Wildcard addresses (`0.0.0.0`, `::`) are not loopback and so require TLS, matching the channel
    spec's `allOf` rule exactly. This does not by itself provide mTLS (client certificate
    authentication) — TLS presence and `clientAuth` are checked separately, in `tls.py` — it only
    stops a plaintext listener from being reachable off-box at all.
    """
    if not is_loopback(bind) and not tls_configured:
        raise BindError(
            f"refusing to start a plaintext MLLP listener on non-loopback bind {bind!r}: "
            "either bind to loopback (127.0.0.1, ::1, localhost) or configure tls "
            "(gate requirement 8; contracts/channel-spec/v0 source.allOf)"
        )


def is_loopback_host_port(entry: str) -> bool:
    """True for `localhost:<port>`, `<IPv4 literal in 127.0.0.0/8>:<port>` or `[::1]:<port>`, port 1-65535.
    Literals are parsed with `ipaddress` (a regex accepted `127.1.1.300`); names other than `localhost`
    are never resolved and never loopback."""
    host, sep, port = entry.rpartition(":")
    if not sep or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
        return False
    if host.startswith("[") and host.endswith("]"):
        try:
            return ipaddress.IPv6Address(host[1:-1]).is_loopback
        except ValueError:
            return False
    if host == "localhost":
        return True
    try:
        return ipaddress.IPv4Address(host).is_loopback
    except ValueError:
        return False
