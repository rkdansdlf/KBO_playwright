"""Retry policy and backoff-index contract tests."""

from __future__ import annotations

import pytest

from src.services.crawl_retry_policy import (
    DEFAULT_MAX_RETRIES,
    RETRY_SCHEDULE,
    decide,
)


def test_schedule_contract_matches_default_max_retries() -> None:
    assert DEFAULT_MAX_RETRIES == 5
    assert RETRY_SCHEDULE == (60, 300, 900, 3600)
    assert len(RETRY_SCHEDULE) == DEFAULT_MAX_RETRIES - 1


@pytest.mark.parametrize(
    "error_code",
    ["FETCH_TIMEOUT", "FETCH_HTTP_ERROR", "FETCH_RATE_LIMITED", "PERSIST_CONNECTION", "SOURCE_PARTIAL"],
)
def test_retryable_codes(error_code: str) -> None:
    decision = decide(error_code, retry_count=0)
    assert decision.retryable is True
    assert decision.reason == "retryable"


@pytest.mark.parametrize(
    "error_code",
    ["FETCH_BLOCKED", "PARSE_SELECTOR_MISSING", "VALIDATION_SCHEMA", "PERSIST_CONSTRAINT"],
)
def test_non_retryable_codes(error_code: str) -> None:
    decision = decide(error_code, retry_count=0)
    assert decision.retryable is False
    assert decision.delay_seconds is None
    assert decision.reason == f"non-retryable:{error_code}"


def test_unknown_code_is_non_retryable() -> None:
    assert decide("NOT_A_REAL_CODE", retry_count=0).retryable is False


def test_enqueue_has_no_delay() -> None:
    """retry_count == 0 means the letter is eligible immediately."""
    assert decide("FETCH_TIMEOUT", retry_count=0).delay_seconds is None


@pytest.mark.parametrize(
    ("retry_count", "expected_delay"),
    [
        (1, 60),
        (2, 300),
        (3, 900),
        (4, 3600),
    ],
)
def test_backoff_index_contract(retry_count: int, expected_delay: int) -> None:
    decision = decide("FETCH_TIMEOUT", retry_count=retry_count)
    assert decision.retryable is True
    assert decision.delay_seconds == expected_delay
    assert decision.delay_seconds == RETRY_SCHEDULE[retry_count - 1]


def test_exhausted_after_max_retries() -> None:
    decision = decide("FETCH_TIMEOUT", retry_count=DEFAULT_MAX_RETRIES)
    assert decision.retryable is False
    assert decision.delay_seconds is None
    assert decision.reason == "max retries exhausted"


def test_per_letter_budget_extends_retries() -> None:
    """A letter budget above the default keeps retrying past DEFAULT_MAX_RETRIES."""
    decision = decide("FETCH_TIMEOUT", retry_count=DEFAULT_MAX_RETRIES, max_retries=8)
    assert decision.retryable is True

    exhausted = decide("FETCH_TIMEOUT", retry_count=8, max_retries=8)
    assert exhausted.retryable is False
    assert exhausted.reason == "max retries exhausted"


def test_per_letter_budget_truncates_retries() -> None:
    decision = decide("FETCH_TIMEOUT", retry_count=3, max_retries=3)
    assert decision.retryable is False
    assert decision.reason == "max retries exhausted"


@pytest.mark.parametrize(
    ("retry_count", "expected_delay"),
    [
        (5, 3600),
        (6, 3600),
        (7, 3600),
    ],
)
def test_backoff_index_is_clamped_to_last_delay(retry_count: int, expected_delay: int) -> None:
    """Beyond the schedule length the last delay repeats instead of raising."""
    decision = decide("FETCH_TIMEOUT", retry_count=retry_count, max_retries=8)
    assert decision.retryable is True
    assert decision.delay_seconds == expected_delay
