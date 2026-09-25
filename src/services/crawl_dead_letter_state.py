"""Dead letter queue state machine and transition invariants.

Transitions are enforced by the service layer, not just by documentation, so an
invalid lifecycle change raises :class:`InvalidDlqTransitionError` before any
row is mutated.
"""

from __future__ import annotations

from src.models.crawl_dead_letter import DlqStatus

ALLOWED_TRANSITIONS: dict[DlqStatus, frozenset[DlqStatus]] = {
    DlqStatus.PENDING: frozenset({DlqStatus.RETRYING, DlqStatus.IGNORED}),
    DlqStatus.RETRYING: frozenset({DlqStatus.RESOLVED, DlqStatus.PENDING, DlqStatus.EXHAUSTED}),
    DlqStatus.EXHAUSTED: frozenset({DlqStatus.IGNORED}),
    DlqStatus.RESOLVED: frozenset(),
    DlqStatus.IGNORED: frozenset(),
}

RETRYABLE_STATUSES = frozenset({DlqStatus.PENDING})

#: Operator ``requeue()`` is an explicit forced-retry override. It is only
#: allowed from terminal states that still need intervention, never from a
#: resolved incident; ``retry_count`` is preserved so the audit trail keeps
#: counting automatic plus forced attempts.
REQUEUEABLE_STATUSES = frozenset({DlqStatus.IGNORED, DlqStatus.EXHAUSTED})


class InvalidDlqTransitionError(ValueError):
    """Raised when a dead letter lifecycle transition is not permitted."""

    def __init__(self, current: DlqStatus | str, target: DlqStatus | str) -> None:
        """Initialize with the rejected source and target statuses."""
        self.current = str(current)
        self.target = str(target)
        super().__init__(f"Invalid dead letter transition: {self.current} -> {self.target}")


def ensure_transition(current: DlqStatus | str, target: DlqStatus | str) -> None:
    """Raise if ``current -> target`` is not an allowed transition."""
    current_status = DlqStatus(current)
    target_status = DlqStatus(target)
    if target_status not in ALLOWED_TRANSITIONS.get(current_status, frozenset()):
        raise InvalidDlqTransitionError(current_status, target_status)


def can_retry(status: DlqStatus | str) -> bool:
    """Return whether a dead letter is eligible to start a retry."""
    return DlqStatus(status) in RETRYABLE_STATUSES


def requeue_target() -> DlqStatus:
    """Return the status an operator ``requeue()`` moves a letter to."""
    return DlqStatus.PENDING


def can_requeue(status: DlqStatus | str) -> bool:
    """Return whether an operator may force a letter back to pending."""
    return DlqStatus(status) in REQUEUEABLE_STATUSES


def ensure_requeue(status: DlqStatus | str) -> None:
    """Raise unless ``status`` is eligible for an operator requeue."""
    status_value = DlqStatus(status)
    if status_value not in REQUEUEABLE_STATUSES:
        raise InvalidDlqTransitionError(status_value, DlqStatus.PENDING)


def next_status_after_retry(
    *,
    retry_count: int,
    max_retries: int,
    success: bool,
) -> DlqStatus:
    """Return the status after a replay attempt.

    ``retry_count`` is the count *including* the attempt that just ran.
    """
    if success:
        return DlqStatus.RESOLVED
    if retry_count >= max_retries:
        return DlqStatus.EXHAUSTED
    return DlqStatus.PENDING
