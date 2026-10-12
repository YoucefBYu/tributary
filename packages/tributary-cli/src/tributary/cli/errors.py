"""Typed failures and the exit codes they map to. Messages are fixed text: never a token, a document
or a response body (invariant 6)."""

from __future__ import annotations

EXIT_OK = 0
EXIT_FAILED = 1  # the operation ran and the answer is "no": invalid document, deployment failed
EXIT_USAGE = 2  # bad arguments, bad configuration, bad input file (click also uses 2)
EXIT_AUTH = 3  # 401 or 403
EXIT_API = 4  # any other 4xx or 5xx
EXIT_UNREACHABLE = 5
EXIT_UNAVAILABLE = 69  # the API has no such operation yet (sysexits EX_UNAVAILABLE)


class CliError(Exception):
    exit_code = EXIT_USAGE

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ConfigError(CliError):
    exit_code = EXIT_USAGE


class InputError(CliError):
    exit_code = EXIT_USAGE


class UnreachableError(CliError):
    exit_code = EXIT_UNREACHABLE


class ApiError(CliError):
    """A problem response (`openapi/v0` Problem). `code` is the contract's code, `title` its fixed text."""

    def __init__(self, status: int, code: str, title: str) -> None:
        super().__init__(f"{status} {code}: {title}")
        self.status = status
        self.code = code
        self.title = title
        self.exit_code = EXIT_AUTH if status in (401, 403) else EXIT_API


class UnavailableError(CliError):
    exit_code = EXIT_UNAVAILABLE
