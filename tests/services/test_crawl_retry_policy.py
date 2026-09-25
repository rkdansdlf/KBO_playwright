"""Retry policy and backoff-index contract tests."""

from __future__ import annotations

import pytest

from src.services.crawl_retry_policy import (
    MAX_RETRIES,
    RETRY_SCHEDULE,
    decide,
)


def test_schedule_contract_matches_max_retries() -> None:
    assert MAX_RETRIES == 5
    assert RETRY_SCHEDULE == (60, 300, 900, 3600)
    assert len(RETRY_SCHEDULE) == MAX_RETRIES - 1


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
    decision = decide("FETCH_TIMEOUT", retry_count=MAX_RETRIES)
    assert decision.retryable is False
    assert decision.delay_seconds is None
    assert decision.reason == "max retries exhausted"
