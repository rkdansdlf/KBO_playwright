"""Typed outcomes and the tracked runner shared by the all-series crawlers.

The batting and pitching crawlers are twins: both read the KBO Record pages for
one season and series, both fall back to DB aggregation when the page cannot be
read, and both swallow a failure into an empty list. They also drive a browser
synchronously, so they cannot use ``CrawlResult`` -- that models an HTTP request
this crawl does not make.

What the shared runner adds is the answer the empty list could not give. A series
with no rows is the normal state for a season that has not started, and it is
also what a page that lost its table produces; a fallback that answered from the
database is a different thing again. Recording which of those happened is what
lets the ledger, the queue and the metric agree.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.models.crawl_execution import RUN_STATUS_FAILED, RUN_STATUS_PARTIAL
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.crawl_run_service import track_crawl_run

if TYPE_CHECKING:
    from collections.abc import Callable

    from src.models.crawl_execution import CrawlExecutionRun

logger = logging.getLogger(__name__)


class SeriesStatus(StrEnum):
    """How one season-and-series read ended."""

    SUCCESS = "success"
    """The page was readable and carried players."""

    EMPTY = "empty"
    """The page was readable and the series has no rows.

    The normal state for a season that has not started, so it is not a failure.
    """

    FALLBACK = "fallback"
    """The page could not be read and the database aggregation answered instead.

    Recorded as ``partial`` rather than ``failed``: data was produced, so a
    reader is served, but it is the previously stored rows rather than this
    crawl's. The dead letter queue is deliberately not used here, because the
    fallback is an accepted resolution and the fallback monitor already raises
    the incident -- queueing as well would make one failure into two.
    """

    BLOCKED = "blocked"
    """The source was not consulted: robots.txt disallows it.

    A deliberate policy skip is not a data outcome, so it is neither ``EMPTY``
    nor a failure. The source was never asked, and retrying would not ask it.
    """

    FAILED = "failed"
    """The read did not produce data and nothing answered in its place.

    Retryable when the cause is the moment rather than the site, which is why
    the taxonomy code is derived from the reason below.
    """


#: Reason keys mapped to their taxonomy code and terminality.
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool]] = {
    "compliance_blocked": (
        FailureCode.FETCH_BLOCKED,
        "the record page is disallowed by robots.txt",
        True,
    ),
    "season_series_selection_failed": (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the season or series controls are no longer the ones the crawl operates",
        True,
    ),
    "page_setup_failed": (
        FailureCode.FETCH_HTTP_ERROR,
        "the record page could not be prepared for reading",
        False,
    ),
    "crawl_error": (
        FailureCode.FETCH_HTTP_ERROR,
        "the series crawl raised while reading the record page",
        False,
    ),
}


@dataclass(frozen=True)
class SeriesRead:
    """One season-and-series outcome together with what the crawl kept."""

    status: SeriesStatus
    reason: str | None = None
    rows: int = 0

    @property
    def is_terminal(self) -> bool:
        """Return whether this outcome can still change on its own."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return bool(entry and entry[2])

    @property
    def error_code(self) -> str | None:
        """Return the taxonomy code for a failure, or ``None`` when readable."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return entry[0].value if entry else None

    @property
    def explanation(self) -> str | None:
        """Return the human-readable detail for the reason."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return entry[1] if entry else None


@dataclass
class SeriesReadRecorder:
    """Collects the outcome a crawl observed, for the runner to act on.

    The crawl reports only what a caller cannot infer: a blocked source, a
    fallback, an exception. The runner infers ``SUCCESS`` and ``EMPTY`` from the
    rows, so a crawl that says nothing and returns rows is unambiguous rather
    than unclassified.

    An instance is created per call rather than held at module level, because
    two series crawls can run at once and a module-level flag would let one
    crawl's failure be recorded against the other's run.
    """

    read: SeriesRead | None = None

    def __call__(self, read: SeriesRead) -> None:
        """Record one observed outcome."""
        self.read = read


def run_series_crawl(  # noqa: PLR0913 - the coordinates are the crawler's identity and its unit
    *,
    crawl: Callable[[SeriesReadRecorder], list[Any]],
    crawler: str,
    target_type: str,
    year: int,
    series_key: str,
    source_url: str,
    exceptions: tuple[type[BaseException], ...],
    parser_version: str | None = None,
    run_spec: CrawlRunSpec | None = None,
    record_dead_letters: bool = True,
) -> list[Any]:
    """Run one series crawl under a tracked run and act on its outcome.

    Args:
        crawl: The crawl, handed the recorder it reports its outcome to.
        crawler: Crawler name for the ledger and the queue.
        target_type: Unit type for the ledger and the queue.
        year: Season year.
        series_key: Series key, the other half of the replay unit.
        source_url: The page the crawl reads.
        exceptions: Faults to record rather than propagate. A configuration
            error such as an unknown series key is deliberately not in this
            tuple: it is the caller's mistake and raising it is the point.
        parser_version: Parser version recorded on the run.
        run_spec: Optional pre-built ledger spec (replay supplies one).
        record_dead_letters: Whether a failure enqueues a DLQ entry.

    Returns:
        The rows the crawl produced, from the page or from its fallback.

    """
    spec = run_spec or CrawlRunSpec(
        crawler=crawler,
        target_type=target_type,
        target_id=f"{year}:{series_key}",
        season=year,
        source_url=source_url,
        parser_version=parser_version,
    )
    recorder = SeriesReadRecorder()

    with track_crawl_run(spec) as run:
        try:
            rows = crawl(recorder)
        except exceptions:
            # The crawl raised instead of reporting. Classifying at the boundary
            # is what keeps a raised read in the ledger and, when asked for, in
            # the queue -- a crawler that only reports its failures when it
            # returns quietly leaves the loud ones unrecorded.
            logger.exception("[SERIES] %s %s raised", crawler, spec.target_id)
            read = SeriesRead(status=SeriesStatus.FAILED, reason="crawl_error", rows=0)
            _mark_failed(run, read)
            if record_dead_letters:
                _enqueue_dead_letter(spec, run.run_id, crawler, target_type, year, source_url, read)
            return []

        read = recorder.read or _read_from_rows(rows)

        run.records_read = len(rows)
        run.checkpoint = {
            "year": year,
            "series_key": series_key,
            "status": str(read.status),
            "reason": read.reason,
        }

        if read.status is SeriesStatus.BLOCKED:
            # Not a failure: the source was never consulted, so a retry has
            # nothing to fix. The checkpoint is what tells the two apart later.
            run.checkpoint["outcome"] = "source_limited"
            logger.info("[SERIES] %s %s skipped: %s", crawler, spec.target_id, read.reason)
            return rows

        if read.status is SeriesStatus.FALLBACK:
            run.status = RUN_STATUS_PARTIAL
            logger.warning("[SERIES] %s %s served from the fallback: %s", crawler, spec.target_id, read.reason)
            return rows

        if read.status is SeriesStatus.FAILED:
            _mark_failed(run, read)
            if record_dead_letters:
                _enqueue_dead_letter(spec, run.run_id, crawler, target_type, year, source_url, read)
            return rows

        return rows


def _read_from_rows(rows: list[Any]) -> SeriesRead:
    """Return the outcome for a crawl that reported none of its own."""
    if rows:
        return SeriesRead(status=SeriesStatus.SUCCESS, rows=len(rows))
    return SeriesRead(status=SeriesStatus.EMPTY, rows=0)


def _mark_failed(run: CrawlExecutionRun, read: SeriesRead) -> None:
    """Copy a classified failure onto its tracked run."""
    run.status = RUN_STATUS_FAILED
    run.error_code = read.error_code or FailureCode.UNKNOWN.value
    run.error_message = read.explanation or read.reason
    run.records_failed = 1


def _enqueue_dead_letter(  # noqa: PLR0913 - mirrors the run coordinates the letter must carry
    spec: CrawlRunSpec,
    original_run_id: str,
    crawler: str,
    target_type: str,
    year: int,
    source_url: str,
    read: SeriesRead,
) -> None:
    """Queue one unreadable season-and-series for operator or scheduled replay."""
    error_code = read.error_code or FailureCode.UNKNOWN.value
    try:
        enqueue_failure(
            DeadLetterSpec(
                original_run_id=original_run_id,
                crawler=crawler,
                target_type=target_type,
                # The season and series together are the replay unit.
                target_id=spec.target_id,
                season=year,
                source_url=source_url,
                # Derived from the code, never supplied beside it.
                failure_stage=stage_for_code(error_code).value,
                error_code=error_code,
                error_message=read.explanation or read.reason,
            ),
        )
    except Exception:
        logger.exception("Failed to enqueue series dead letter for %s", spec.target_id)


__all__ = [
    "SeriesRead",
    "SeriesReadRecorder",
    "SeriesStatus",
    "run_series_crawl",
]
