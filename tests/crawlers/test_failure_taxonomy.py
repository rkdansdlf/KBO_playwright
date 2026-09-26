"""Tests for the crawler failure taxonomy."""

from __future__ import annotations

import httpx
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    FailureStage,
    classify_failure,
    classify_persist_failure,
    failure_code_for_status,
    stage_for_code,
)


class TestStatusCodeMapping:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (408, FailureCode.FETCH_TIMEOUT),
            (429, FailureCode.FETCH_RATE_LIMITED),
            (401, FailureCode.FETCH_BLOCKED),
            (403, FailureCode.FETCH_BLOCKED),
            (500, FailureCode.FETCH_HTTP_ERROR),
            (502, FailureCode.FETCH_HTTP_ERROR),
            (503, FailureCode.FETCH_HTTP_ERROR),
            (504, FailureCode.FETCH_HTTP_ERROR),
            (404, FailureCode.FETCH_HTTP_ERROR),
            (400, FailureCode.FETCH_HTTP_ERROR),
        ],
    )
    def test_status_maps_to_a_code(self, status: int, expected: FailureCode) -> None:
        assert failure_code_for_status(status) is expected

    @pytest.mark.parametrize("status", [408, 429, 401, 403, 500, 503])
    def test_exception_path_agrees_with_the_status_path(self, status: int) -> None:
        """The same cause must not get two names depending on whether it arrived
        as a status or as a raised exception.
        """
        request = httpx.Request("GET", "https://example.test")
        exc = httpx.HTTPStatusError("boom", request=request, response=httpx.Response(status))

        assert classify_failure(exc) == (FailureStage.FETCH, failure_code_for_status(status))

    def test_thrown_timeout_is_a_timeout(self) -> None:
        assert classify_failure(httpx.ReadTimeout("slow")) == (FailureStage.FETCH, FailureCode.FETCH_TIMEOUT)

    def test_transport_error_is_a_fetch_error(self) -> None:
        assert classify_failure(httpx.ConnectError("refused")) == (
            FailureStage.FETCH,
            FailureCode.FETCH_HTTP_ERROR,
        )

    def test_bare_timeout_during_fetch(self) -> None:
        assert classify_failure(TimeoutError("slow")) == (FailureStage.FETCH, FailureCode.FETCH_TIMEOUT)

    def test_unrecognised_exception_is_unknown(self) -> None:
        assert classify_failure(RuntimeError("?")) == (FailureStage.UNKNOWN, FailureCode.UNKNOWN)


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
