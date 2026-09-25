"""Failure taxonomy shared by crawler run ledgers and the dead letter queue.

``error_code`` is a coarse, queryable classification; ``failure_stage`` locates
where in the pipeline the failure happened. Both are kept independent of any
free-form exception message so operators can aggregate failures reliably.
"""

from __future__ import annotations

from enum import StrEnum

import httpx

_HTTP_RATE_LIMIT = 429
_HTTP_FORBIDDEN = (401, 403)


class FailureStage(StrEnum):
    """Pipeline stage where a crawl failure occurred."""

    FETCH = "fetch"
    PARSE = "parse"
    VALIDATE = "validate"
    PERSIST = "persist"
    RECONCILE = "reconcile"
    UNKNOWN = "unknown"


class FailureCode(StrEnum):
    """Stable, queryable failure classification."""

    FETCH_TIMEOUT = "FETCH_TIMEOUT"
    FETCH_HTTP_ERROR = "FETCH_HTTP_ERROR"
    FETCH_BLOCKED = "FETCH_BLOCKED"
    FETCH_RATE_LIMITED = "FETCH_RATE_LIMITED"

    PARSE_SELECTOR_MISSING = "PARSE_SELECTOR_MISSING"
    PARSE_INVALID_FORMAT = "PARSE_INVALID_FORMAT"
    PARSE_EMPTY = "PARSE_EMPTY"

    VALIDATION_SCHEMA = "VALIDATION_SCHEMA"
    VALIDATION_QUALITY = "VALIDATION_QUALITY"

    PERSIST_CONSTRAINT = "PERSIST_CONSTRAINT"
    PERSIST_CONNECTION = "PERSIST_CONNECTION"
    PERSIST_TIMEOUT = "PERSIST_TIMEOUT"

    SOURCE_PARTIAL = "SOURCE_PARTIAL"
    UNKNOWN = "UNKNOWN"


_FETCH_CODES = frozenset(
    {
        FailureCode.FETCH_TIMEOUT,
        FailureCode.FETCH_HTTP_ERROR,
        FailureCode.FETCH_BLOCKED,
        FailureCode.FETCH_RATE_LIMITED,
    },
)
_PARSE_CODES = frozenset(
    {
        FailureCode.PARSE_SELECTOR_MISSING,
        FailureCode.PARSE_INVALID_FORMAT,
        FailureCode.PARSE_EMPTY,
    },
)
_VALIDATE_CODES = frozenset({FailureCode.VALIDATION_SCHEMA, FailureCode.VALIDATION_QUALITY})
_PERSIST_CODES = frozenset(
    {
        FailureCode.PERSIST_CONSTRAINT,
        FailureCode.PERSIST_CONNECTION,
        FailureCode.PERSIST_TIMEOUT,
    },
)


def stage_for_code(code: FailureCode | str) -> FailureStage:
    """Return the pipeline stage that owns an error code."""
    value = FailureCode(code)
    if value in _FETCH_CODES:
        return FailureStage.FETCH
    if value in _PARSE_CODES:
        return FailureStage.PARSE
    if value in _VALIDATE_CODES:
        return FailureStage.VALIDATE
    if value in _PERSIST_CODES:
        return FailureStage.PERSIST
    return FailureStage.UNKNOWN


_ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]
_CLASSIFICATIONS: tuple[tuple[_ExceptionTypes, FailureStage, FailureCode], ...] = (
    (httpx.TimeoutException, FailureStage.FETCH, FailureCode.FETCH_TIMEOUT),
    (httpx.TransportError, FailureStage.FETCH, FailureCode.FETCH_HTTP_ERROR),
    (TimeoutError, FailureStage.FETCH, FailureCode.FETCH_TIMEOUT),
    (OSError, FailureStage.FETCH, FailureCode.FETCH_HTTP_ERROR),
    ((ValueError, TypeError, KeyError), FailureStage.PARSE, FailureCode.PARSE_INVALID_FORMAT),
)


def _http_status_code(status_code: int) -> FailureCode:
    """Classify an HTTP status code as a fetch failure code."""
    if status_code == _HTTP_RATE_LIMIT:
        return FailureCode.FETCH_RATE_LIMITED
    if status_code in _HTTP_FORBIDDEN:
        return FailureCode.FETCH_BLOCKED
    return FailureCode.FETCH_HTTP_ERROR


def classify_failure(exc: BaseException) -> tuple[FailureStage, FailureCode]:
    """Map an exception to a ``(failure_stage, error_code)`` pair."""
    if isinstance(exc, httpx.HTTPStatusError):
        return FailureStage.FETCH, _http_status_code(exc.response.status_code)
    for exc_types, stage, code in _CLASSIFICATIONS:
        if isinstance(exc, exc_types):
            return stage, code
    return FailureStage.UNKNOWN, FailureCode.UNKNOWN
