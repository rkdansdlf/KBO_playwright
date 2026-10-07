"""Independent replay of a past crawl execution run.

Unlike ``kbo dlq retry`` (which advances an existing incident lifecycle), this
creates a brand new execution run linked to the original and never mutates the
original run or the dead letter queue.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from src.crawlers.award_crawler import AWARD_CRAWLER_NAME, AwardCrawler
from src.crawlers.run_origin import CrawlRunOrigin
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_RUNNING, RUN_STATUS_SUCCESS
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.utils.async_bridge import run_coro_blocking

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class CrawlRunNotFoundError(LookupError):
    """Raised when the original run id does not exist."""

    def __init__(self, run_id: str) -> None:
        """Initialize with the missing run id."""
        self.run_id = run_id
        super().__init__(f"Crawl run not found: {run_id}")


class CrawlRunNotReplayableError(ValueError):
    """Raised when the original run is not in a terminal state."""

    def __init__(self, run_id: str, status: str) -> None:
        """Initialize with the offending run id and status."""
        self.run_id = run_id
        self.status = status
        super().__init__(f"Crawl run {run_id} is not replayable (status={status})")


class UnsupportedReplayCrawlerError(ValueError):
    """Raised when no replay executor is registered for the run's crawler."""

    def __init__(self, crawler: str) -> None:
        """Initialize with the unsupported crawler name."""
        self.crawler = crawler
        super().__init__(f"No replay executor registered for crawler '{crawler}'")


@dataclass(frozen=True)
class CrawlReplayResult:
    """Outcome of an independent crawl replay."""

    original_run_id: str
    replay_run_id: str
    status: str
    success: bool


@dataclass(frozen=True)
class _RunSnapshot:
    """Plain view of the original run, safe to use after session close."""

    run_id: str
    crawler: str
    target_type: str
    target_id: str | None
    season: int | None
    game_id: str | None
    source_url: str | None


ReplayExecutor = Callable[["_RunSnapshot", str], None]


async def _run_award_replay(spec: CrawlRunSpec) -> None:
    crawler = AwardCrawler()
    try:
        await crawler.run(run_spec=spec, save=True, record_dead_letters=False)
    finally:
        await crawler.close()


def _execute_awards(snapshot: _RunSnapshot, replay_run_id: str) -> None:
    """Re-run the full award crawl into a new linked execution run."""
    spec = CrawlRunSpec(
        crawler=snapshot.crawler,
        target_type=snapshot.target_type,
        target_id=snapshot.target_id,
        season=snapshot.season,
        game_id=snapshot.game_id,
        source_url=snapshot.source_url,
        parent_run_id=snapshot.run_id,
        replay_of_run_id=snapshot.run_id,
        run_id=replay_run_id,
        origin=CrawlRunOrigin.REPLAY,
    )
    run_coro_blocking(_run_award_replay(spec))


def build_default_executors() -> dict[str, ReplayExecutor]:
    """Return the replay executors registered for the canary crawlers."""
    return {AWARD_CRAWLER_NAME: _execute_awards}


def _load_snapshot(session: Session, run_id: str) -> _RunSnapshot:
    original = CrawlExecutionRepository(session).get_by_run_id(run_id)
    if original is None:
        raise CrawlRunNotFoundError(run_id)
    if original.status == RUN_STATUS_RUNNING:
        raise CrawlRunNotReplayableError(run_id, original.status)
    return _RunSnapshot(
        run_id=original.run_id,
        crawler=original.crawler,
        target_type=original.target_type,
        target_id=original.target_id,
        season=original.season,
        game_id=original.game_id,
        source_url=original.source_url,
    )


def _read_status(session_factory: Callable[[], Session], replay_run_id: str) -> str | None:
    with session_factory() as session:
        run = CrawlExecutionRepository(session).get_by_run_id(replay_run_id)
        return run.status if run is not None else None


def replay_crawl_run(
    original_run_id: str,
    *,
    executors: dict[str, ReplayExecutor] | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> CrawlReplayResult:
    """Replay a terminal crawl run as a new linked execution run.

    The original run and the dead letter queue are never modified.
    """
    factory: Callable[[], Session] = session_factory or SessionLocal
    active_executors = executors if executors is not None else build_default_executors()

    with factory() as session:
        snapshot = _load_snapshot(session, original_run_id)

    executor = active_executors.get(snapshot.crawler)
    if executor is None:
        raise UnsupportedReplayCrawlerError(snapshot.crawler)

    replay_run_id = uuid4().hex
    try:
        executor(snapshot, replay_run_id)
    except Exception:
        logger.exception("Crawl replay failed for run %s", original_run_id)
        status = _read_status(factory, replay_run_id) or "failed"
        return CrawlReplayResult(
            original_run_id=original_run_id,
            replay_run_id=replay_run_id,
            status=status,
            success=False,
        )

    status = _read_status(factory, replay_run_id)
    if status is None:
        return CrawlReplayResult(
            original_run_id=original_run_id,
            replay_run_id=replay_run_id,
            status="missing",
            success=False,
        )
    return CrawlReplayResult(
        original_run_id=original_run_id,
        replay_run_id=replay_run_id,
        status=status,
        success=status == RUN_STATUS_SUCCESS,
    )
