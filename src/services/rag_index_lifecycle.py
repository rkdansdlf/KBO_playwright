"""Lifecycle contracts for canonical and vector RAG index rows."""

from __future__ import annotations

from enum import StrEnum


class IndexLifecycleStatus(StrEnum):
    """Allowed lifecycle states for a canonical chunk and its vector row."""

    PENDING = "PENDING"
    INDEXED = "INDEXED"
    ACTIVE = "ACTIVE"
    STALE = "STALE"
    REINDEX_REQUIRED = "REINDEX_REQUIRED"
    DELETE_PENDING = "DELETE_PENDING"
    TOMBSTONED = "TOMBSTONED"
    DELETED = "DELETED"
    PURGED = "PURGED"


_TRANSITIONS: dict[IndexLifecycleStatus, frozenset[IndexLifecycleStatus]] = {
    IndexLifecycleStatus.PENDING: frozenset({IndexLifecycleStatus.INDEXED, IndexLifecycleStatus.DELETE_PENDING}),
    IndexLifecycleStatus.INDEXED: frozenset({IndexLifecycleStatus.ACTIVE, IndexLifecycleStatus.STALE}),
    IndexLifecycleStatus.ACTIVE: frozenset(
        {
            IndexLifecycleStatus.STALE,
            IndexLifecycleStatus.DELETE_PENDING,
        }
    ),
    IndexLifecycleStatus.STALE: frozenset({IndexLifecycleStatus.REINDEX_REQUIRED, IndexLifecycleStatus.DELETE_PENDING}),
    IndexLifecycleStatus.REINDEX_REQUIRED: frozenset(
        {IndexLifecycleStatus.INDEXED, IndexLifecycleStatus.DELETE_PENDING}
    ),
    IndexLifecycleStatus.DELETE_PENDING: frozenset({IndexLifecycleStatus.TOMBSTONED, IndexLifecycleStatus.DELETED}),
    IndexLifecycleStatus.TOMBSTONED: frozenset({IndexLifecycleStatus.PURGED}),
    IndexLifecycleStatus.DELETED: frozenset({IndexLifecycleStatus.PURGED}),
    IndexLifecycleStatus.PURGED: frozenset(),
}


def can_transition(current: str, target: str) -> bool:
    """Return whether a lifecycle state transition is allowed."""
    try:
        current_status = IndexLifecycleStatus(current)
        target_status = IndexLifecycleStatus(target)
    except ValueError:
        return False
    return target_status in _TRANSITIONS[current_status]


def transition_status(current: str, target: str) -> str:
    """Validate and return a lifecycle transition target."""
    if current == target:
        return target
    if not can_transition(current, target):
        message = f"Invalid RAG index lifecycle transition: {current} -> {target}"
        raise ValueError(message)
    return target
