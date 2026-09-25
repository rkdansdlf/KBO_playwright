"""State-machine tests for the crawler dead letter queue."""

from __future__ import annotations

import pytest

from src.models.crawl_dead_letter import DlqStatus
from src.services.crawl_dead_letter_state import (
    InvalidDlqTransitionError,
    can_requeue,
    can_retry,
    ensure_requeue,
    ensure_transition,
    next_status_after_retry,
    requeue_target,
)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (DlqStatus.PENDING, DlqStatus.RETRYING),
        (DlqStatus.PENDING, DlqStatus.IGNORED),
        (DlqStatus.RETRYING, DlqStatus.RESOLVED),
        (DlqStatus.RETRYING, DlqStatus.PENDING),
        (DlqStatus.RETRYING, DlqStatus.EXHAUSTED),
        (DlqStatus.EXHAUSTED, DlqStatus.IGNORED),
    ],
)
def test_allowed_transitions(current: DlqStatus, target: DlqStatus) -> None:
    ensure_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (DlqStatus.RESOLVED, DlqStatus.PENDING),
        (DlqStatus.RESOLVED, DlqStatus.RETRYING),
        (DlqStatus.IGNORED, DlqStatus.RETRYING),
        (DlqStatus.IGNORED, DlqStatus.PENDING),
        (DlqStatus.EXHAUSTED, DlqStatus.RETRYING),
        (DlqStatus.PENDING, DlqStatus.RESOLVED),
        (DlqStatus.PENDING, DlqStatus.EXHAUSTED),
    ],
)
def test_forbidden_transitions_raise(current: DlqStatus, target: DlqStatus) -> None:
    with pytest.raises(InvalidDlqTransitionError):
        ensure_transition(current, target)


def test_invalid_transition_error_exposes_endpoints() -> None:
    with pytest.raises(InvalidDlqTransitionError) as excinfo:
        ensure_transition(DlqStatus.RESOLVED, DlqStatus.RETRYING)
    assert excinfo.value.current == "resolved"
    assert excinfo.value.target == "retrying"


def test_can_retry_only_from_pending() -> None:
    assert can_retry(DlqStatus.PENDING) is True
    for status in (DlqStatus.RETRYING, DlqStatus.RESOLVED, DlqStatus.EXHAUSTED, DlqStatus.IGNORED):
        assert can_retry(status) is False


def test_requeue_target_is_pending() -> None:
    assert requeue_target() is DlqStatus.PENDING


@pytest.mark.parametrize("status", [DlqStatus.IGNORED, DlqStatus.EXHAUSTED])
def test_requeue_allows_unresolved_terminal(status: DlqStatus) -> None:
    assert can_requeue(status) is True
    ensure_requeue(status)


@pytest.mark.parametrize(
    "status",
    [DlqStatus.PENDING, DlqStatus.RETRYING, DlqStatus.RESOLVED],
)
def test_requeue_rejects_other_states(status: DlqStatus) -> None:
    assert can_requeue(status) is False
    with pytest.raises(InvalidDlqTransitionError):
        ensure_requeue(status)


def test_requeue_rejects_resolved_target_pending() -> None:
    with pytest.raises(InvalidDlqTransitionError) as excinfo:
        ensure_requeue(DlqStatus.RESOLVED)
    assert excinfo.value.current == "resolved"
    assert excinfo.value.target == "pending"


def test_next_status_after_retry_success() -> None:
    assert next_status_after_retry(retry_count=1, max_retries=5, success=True) is DlqStatus.RESOLVED


@pytest.mark.parametrize("retry_count", [1, 2, 3, 4])
def test_next_status_after_retry_failure_returns_pending(retry_count: int) -> None:
    assert next_status_after_retry(retry_count=retry_count, max_retries=5, success=False) is DlqStatus.PENDING


@pytest.mark.parametrize("retry_count", [5, 6])
def test_next_status_after_retry_exhausts(retry_count: int) -> None:
    assert next_status_after_retry(retry_count=retry_count, max_retries=5, success=False) is DlqStatus.EXHAUSTED
