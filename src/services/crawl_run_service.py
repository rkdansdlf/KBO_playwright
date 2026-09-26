"""Service layer for the generic crawl execution run ledger.

The service owns the transaction boundary. Crawlers use :func:`track_crawl_run`
to record a run without threading session handling through their own code.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_RUNNING, CrawlExecutionRun
from src.monitoring.crawler_metrics import record_crawl_run
from src.repositories.crawl_execution_repository import (
    CrawlExecutionRepository,
    CrawlRunSpec,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _measure(run: CrawlExecutionRun) -> None:
    """Project a terminal run onto the Prometheus metrics.

    Metrics are a projection of the ledger, so every terminal transition routes
    through here. A measurement failure must not fail the crawl it describes,
    hence the guard: an unmeasurable run is still a recorded run.
    """
    try:
        record_crawl_run(run)
    except Exception:  # measurement is best-effort; the run is still recorded
        logger.exception("Failed to record crawl metrics for run %s", getattr(run, "run_id", "?"))


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


class CrawlRunService:
    """Own ledger transaction boundaries for a single session."""

    def __init__(self, session: Session) -> None:
        """Initialize the service with a caller-managed session."""
        self.session = session
        self.repository = CrawlExecutionRepository(session)

    def start(self, spec: CrawlRunSpec) -> CrawlExecutionRun:
        """Create a run row in the ``running`` state."""
        return self.repository.start_run(spec)

    def success(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        checkpoint: dict | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as successful."""
        result = self.repository.mark_success(
            run,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            checkpoint=checkpoint,
            finished_at=finished_at,
        )
        _measure(result)
        return result

    def partial(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        checkpoint: dict | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as partially successful."""
        result = self.repository.mark_partial(
            run,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            checkpoint=checkpoint,
            finished_at=finished_at,
        )
        _measure(result)
        return result

    def failed(  # noqa: PLR0913
        self,
        run: CrawlExecutionRun,
        *,
        error_code: str,
        error_message: str,
        records_read: int | None = None,
        records_written: int | None = None,
        records_failed: int | None = None,
        finished_at: datetime | None = None,
    ) -> CrawlExecutionRun:
        """Finalize a run as failed with an error classification."""
        result = self.repository.mark_failed(
            run,
            error_code=error_code,
            error_message=error_message,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
            finished_at=finished_at,
        )
        _measure(result)
        return result

    def record_counts(
        self,
        run: CrawlExecutionRun,
        *,
        records_read: int = 0,
        records_written: int = 0,
        records_failed: int = 0,
    ) -> CrawlExecutionRun:
        """Increment the run counters by the supplied deltas."""
        return self.repository.increment_counts(
            run,
            records_read=records_read,
            records_written=records_written,
            records_failed=records_failed,
        )


def _persist(session: Session, *, owns_session: bool) -> None:
    """Commit when this service owns the session, otherwise flush."""
    if owns_session:
        session.commit()
    else:
        session.flush()


@contextmanager
def track_crawl_run(
    spec: CrawlRunSpec,
    *,
    session: Session | None = None,
) -> Iterator[CrawlExecutionRun]:
    """Record a crawl execution, finalizing it based on the block outcome.

    When ``session`` is omitted the service opens and commits its own session,
    so the ledger entry survives even if the surrounding code later fails. When
    a session is supplied the caller owns the transaction and the ledger row is
    only flushed.

    Args:
        spec: Description of the execution to record.
        session: Optional caller-managed session.

    Yields:
        The live :class:`CrawlExecutionRun` row, which callers may mutate to set
        counters or a ``partial`` status before the block exits.

    """
    owns_session = session is None
    active = session if session is not None else SessionLocal()
    service = CrawlRunService(active)
    run = service.start(spec)
    _persist(active, owns_session=owns_session)

    try:
        yield run
    except BaseException as exc:
        try:
            error_code = getattr(exc, "error_code", None) or type(exc).__name__
            service.failed(run, error_code=str(error_code), error_message=str(exc))
            _persist(active, owns_session=owns_session)
        except Exception:
            logger.exception("Failed to persist crawl run failure for crawler=%s", spec.crawler)
        raise
    else:
        if run.status == RUN_STATUS_RUNNING:
            service.success(run)
        elif run.finished_at is None:
            run.finished_at = _utcnow()
        _persist(active, owns_session=owns_session)
    finally:
        if owns_session:
            active.close()
