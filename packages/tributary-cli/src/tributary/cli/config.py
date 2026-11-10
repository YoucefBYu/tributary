"""Client configuration. The API token comes from `TRIBUTARY_TOKEN` or from the file named by
`TRIBUTARY_TOKEN_FILE`, never from a command line argument (arguments show up in `ps` and shell history).

* Exactly one of the two sources; both set is refused so a stale variable cannot silently win.
* A token file must be a regular file readable by its owner only (mode 0600 or tighter), like an SSH key.
* The token is sent only over https, or over http to a loopback address (the lite stack). Redirects are not
  followed (see `client.py`), so the header cannot be forwarded to another host.
* `repr` of the config never contains the token.
"""

from __future__ import annotations

import ipaddress
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .errors import ConfigError

DEFAULT_URL = "http://127.0.0.1:8080"
MAX_TOKEN_BYTES = 16 * 1024


@dataclass(frozen=True, slots=True)
class ClientConfig:
    base_url: str
    token: str = field(repr=False)
    timeout_s: float = 30.0


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def check_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError("TRIBUTARY_API_URL must be an http(s) URL")
    if parts.username or parts.password:
        raise ConfigError("TRIBUTARY_API_URL must not carry credentials")
    if parts.scheme == "http" and not _is_loopback(parts.hostname):
        raise ConfigError("refusing to send a token over plain http to a non-loopback host; use https")
    return url.rstrip("/")


def read_token_file(path: str) -> str:
    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        raise ConfigError("TRIBUTARY_TOKEN_FILE cannot be read") from None
    if not stat.S_ISREG(st.st_mode):
        raise ConfigError("TRIBUTARY_TOKEN_FILE is not a regular file")
    if st.st_mode & 0o077:
        raise ConfigError("TRIBUTARY_TOKEN_FILE is readable by group or others; chmod 600 it")
    if st.st_size > MAX_TOKEN_BYTES:
        raise ConfigError("TRIBUTARY_TOKEN_FILE is too large to be a token")
    try:
        token = p.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        raise ConfigError("TRIBUTARY_TOKEN_FILE cannot be read as a token") from None
    return token


def load(environ: Mapping[str, str] | None = None) -> ClientConfig:
    env = os.environ if environ is None else environ
    direct, from_file = env.get("TRIBUTARY_TOKEN", ""), env.get("TRIBUTARY_TOKEN_FILE", "")
    if direct and from_file:
        raise ConfigError("set TRIBUTARY_TOKEN or TRIBUTARY_TOKEN_FILE, not both")
    token = direct.strip() if direct else read_token_file(from_file) if from_file else ""
    if not token:
        raise ConfigError("no token: set TRIBUTARY_TOKEN or TRIBUTARY_TOKEN_FILE (an OIDC access token)")
    if any(c.isspace() for c in token):
        raise ConfigError("the token must be a single line")
    return ClientConfig(base_url=check_url(env.get("TRIBUTARY_API_URL", DEFAULT_URL)), token=token)
