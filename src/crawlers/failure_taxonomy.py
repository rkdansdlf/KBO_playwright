"""Failure taxonomy shared by crawler run ledgers and the dead letter queue.

``error_code`` is a coarse, queryable classification; ``failure_stage`` locates
where in the pipeline the failure happened. Both are kept independent of any
free-form exception message so operators can aggregate failures reliably.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

import httpx
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, SQLAlchemyError

from src.crawlers.result import CrawlOutcome

if TYPE_CHECKING:
    from src.crawlers.result import CrawlResult

_HTTP_RATE_LIMIT = 429
_HTTP_TIMEOUT = 408
_HTTP_FORBIDDEN = (401, 403)
_HTTP_SERVER_ERROR_FLOOR = 400


class FailureStage(StrEnum):
    """Pipeline stage where a crawl failure occurred."""

    FETCH = "fetch"
    PARSE = "parse"
    VALIDATE = "validate"
    PERSIST = "persist"
    RECONCILE = "reconcile"
    ORCHESTRATE = "orchestrate"
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

    REPLAY_INTERRUPTED = "REPLAY_INTERRUPTED"
    REPLAY_RUN_MISSING = "REPLAY_RUN_MISSING"

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
_ORCHESTRATE_CODES = frozenset(
    {
        FailureCode.REPLAY_INTERRUPTED,
        FailureCode.REPLAY_RUN_MISSING,
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
    if value in _ORCHESTRATE_CODES:
        return FailureStage.ORCHESTRATE
    return FailureStage.UNKNOWN


_ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]
_CLASSIFICATIONS: tuple[tuple[_ExceptionTypes, FailureStage, FailureCode], ...] = (
    (httpx.TimeoutException, FailureStage.FETCH, FailureCode.FETCH_TIMEOUT),
    (httpx.TransportError, FailureStage.FETCH, FailureCode.FETCH_HTTP_ERROR),
    (TimeoutError, FailureStage.FETCH, FailureCode.FETCH_TIMEOUT),
    (OSError, FailureStage.FETCH, FailureCode.FETCH_HTTP_ERROR),
    ((ValueError, TypeError, KeyError), FailureStage.PARSE, FailureCode.PARSE_INVALID_FORMAT),
)


def failure_code_for_status(status_code: int) -> FailureCode:
    """Classify an HTTP status code as a fetch failure code.

    `503` is a throttle signal for the adaptive limiter, but it is still a
    server-side failure here: transport control policy and failure semantics are
    kept apart so a code in the ledger means one thing only.
    """
    if status_code == _HTTP_RATE_LIMIT:
        return FailureCode.FETCH_RATE_LIMITED
    if status_code == _HTTP_TIMEOUT:
        return FailureCode.FETCH_TIMEOUT
    if status_code in _HTTP_FORBIDDEN:
        return FailureCode.FETCH_BLOCKED
    return FailureCode.FETCH_HTTP_ERROR


def classify_failure(exc: BaseException) -> tuple[FailureStage, FailureCode]:
    """Map an exception to a ``(failure_stage, error_code)`` pair."""
    if isinstance(exc, httpx.HTTPStatusError):
        return FailureStage.FETCH, failure_code_for_status(exc.response.status_code)
    for exc_types, stage, code in _CLASSIFICATIONS:
        if isinstance(exc, exc_types):
            return stage, code
    return FailureStage.UNKNOWN, FailureCode.UNKNOWN


def classification_for_result(result: CrawlResult[object]) -> tuple[FailureStage, FailureCode] | None:
    """Return the taxonomy pair for a crawler result, or ``None`` if it succeeded.

    `failure_stage` is never read from the result. It is always derived from
    `error_code` so a stage can never drift away from the code it describes.

    The `error_code` policy is deliberately strict:

    * ``None`` -- the producer predates the taxonomy, so fall back to what the
      result can prove on its own (a failing status, or a known outcome).
    * A known :class:`FailureCode` -- keep it as the canonical classification.
    * An unknown string -- raise ``ValueError``. Quietly downgrading a typo such
      as ``FETCH_TIMOUT`` to ``UNKNOWN`` would let the ledger, the dead letter
      queue, and the metrics drift back into disagreeing about the same failure,
      which is exactly what this function exists to prevent. Operational
      best-effort defensiveness belongs to the metrics layer, not to the domain
      taxonomy.

    Returns:
        ``(stage, code)`` for a failure, or ``None`` for ``SUCCESS``/``EMPTY``.

    """
    if result.outcome in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY}:
        return None

    if result.error_code is not None:
        code = FailureCode(result.error_code)
        return stage_for_code(code), code

    # Legacy fallback: only a failing status can be read as a fetch failure. A
    # 2xx status alongside a failure outcome means the response arrived and was
    # the wrong shape, so the status says nothing about the cause.
    status = result.http_status
    if status is not None and status >= _HTTP_SERVER_ERROR_FLOOR:
        return FailureStage.FETCH, failure_code_for_status(status)
    if result.outcome is CrawlOutcome.SCHEMA_CHANGED:
        return FailureStage.PARSE, FailureCode.PARSE_INVALID_FORMAT
    return FailureStage.UNKNOWN, FailureCode.UNKNOWN


def classify_persist_failure(exc: BaseException) -> tuple[FailureStage, FailureCode]:
    """Map a persistence exception to the ``persist`` stage and its code.

    Kept separate from :func:`classify_failure` because a bare ``TimeoutError``
    during a DB write is a persistence timeout, not a fetch timeout.
    """
    if isinstance(exc, IntegrityError):
        return FailureStage.PERSIST, FailureCode.PERSIST_CONSTRAINT
    if isinstance(exc, TimeoutError):
        return FailureStage.PERSIST, FailureCode.PERSIST_TIMEOUT
    if isinstance(exc, OperationalError):
        message = str(exc).lower()
        if "timeout" in message or "timed out" in message:
            return FailureStage.PERSIST, FailureCode.PERSIST_TIMEOUT
        return FailureStage.PERSIST, FailureCode.PERSIST_CONNECTION
    if isinstance(exc, (DBAPIError, SQLAlchemyError, ConnectionError, OSError)):
        return FailureStage.PERSIST, FailureCode.PERSIST_CONNECTION
    return FailureStage.PERSIST, FailureCode.PERSIST_CONNECTION


class CrawlPersistError(Exception):
    """Raised when a persistence write fails and the run must be marked failed."""

    def __init__(
        self,
        message: str,
        *,
        error_code: FailureCode,
        failure_stage: FailureStage = FailureStage.PERSIST,
    ) -> None:
        """Initialize with a taxonomy classification for the failed write."""
        self.error_code = error_code.value
        self.failure_stage = failure_stage.value
        super().__init__(message)


__all__ = [
    "CrawlPersistError",
    "FailureCode",
    "FailureStage",
    "classification_for_result",
    "classify_failure",
    "classify_persist_failure",
    "failure_code_for_status",
    "stage_for_code",
]
