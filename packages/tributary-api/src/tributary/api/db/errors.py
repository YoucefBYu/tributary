"""Typed repository errors. Messages carry names and ids only."""

from __future__ import annotations


class RepositoryError(Exception):
    pass


class NotFoundError(RepositoryError):
    pass


class AlreadyExistsError(RepositoryError):
    """Unique-constraint or concurrent-write conflict."""


class NothingToRollBackError(RepositoryError):
    pass


class InvalidValueError(RepositoryError):
    """A CHECK constraint or Python-side validation rejected a value."""


class InUseError(RepositoryError):
    """A foreign key (RESTRICT) refuses the change because other rows reference the row."""


class TransientError(RepositoryError):
    """Serialization failure, deadlock or connection-class failure: the caller may retry the transaction."""
