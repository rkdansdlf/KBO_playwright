"""Tests for the crawler failure taxonomy."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    FailureStage,
    classify_failure,
    classify_persist_failure,
    stage_for_code,
)


def _db_error(exc_type: type[Exception], message: str) -> Exception:
    return exc_type("INSERT INTO awards", {}, Exception(message))


def test_integrity_error_is_constraint() -> None:
    assert classify_persist_failure(_db_error(IntegrityError, "unique")) == (
        FailureStage.PERSIST,
        FailureCode.PERSIST_CONSTRAINT,
    )


@pytest.mark.parametrize("message", ["connection timed out", "socket timeout"])
def test_operational_timeout_is_persist_timeout(message: str) -> None:
    assert classify_persist_failure(_db_error(OperationalError, message)) == (
        FailureStage.PERSIST,
        FailureCode.PERSIST_TIMEOUT,
    )


def test_operational_error_is_connection() -> None:
    assert classify_persist_failure(_db_error(OperationalError, "connection refused")) == (
        FailureStage.PERSIST,
        FailureCode.PERSIST_CONNECTION,
    )


def test_bare_timeout_is_persist_timeout_not_fetch() -> None:
    assert classify_persist_failure(TimeoutError("slow")) == (FailureStage.PERSIST, FailureCode.PERSIST_TIMEOUT)
    # The generic classifier still treats it as a fetch timeout.
    assert classify_failure(TimeoutError("slow")) == (FailureStage.FETCH, FailureCode.FETCH_TIMEOUT)


def test_unknown_persist_exception_defaults_to_connection() -> None:
    assert classify_persist_failure(RuntimeError("boom")) == (
        FailureStage.PERSIST,
        FailureCode.PERSIST_CONNECTION,
    )


def test_crawl_persist_error_exposes_taxonomy() -> None:
    error = CrawlPersistError(
        "write failed",
        error_code=FailureCode.PERSIST_CONSTRAINT,
    )
    assert error.error_code == "PERSIST_CONSTRAINT"
    assert error.failure_stage == "persist"
    assert str(error) == "write failed"


def test_stage_for_persist_code() -> None:
    assert stage_for_code(FailureCode.PERSIST_TIMEOUT) is FailureStage.PERSIST
    assert stage_for_code("FETCH_TIMEOUT") is FailureStage.FETCH
