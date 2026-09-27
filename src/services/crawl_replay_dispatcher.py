"""Replay dispatcher for dead letter retries.

The dispatcher maps a ``crawler`` name to a handler that re-runs the failing
unit of work. Handlers are registered explicitly so Phase B can onboard one
canary at a time instead of assuming every crawler shares a replay interface.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.crawlers.award_crawler import (
    AWARD_CRAWLER_NAME,
    AWARD_TARGET_TYPE,
    AwardCrawler,
)
from src.crawlers.roster_transaction_crawler import (
    ROSTER_CRAWLER_NAME,
    ROSTER_TARGET_TYPE,
    RosterTransactionCrawler,
)
from src.crawlers.schedule_crawler import (
    SCHEDULE_CRAWLER_NAME,
    SCHEDULE_TARGET_TYPE,
    ScheduleCrawler,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_SUCCESS
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.utils.async_bridge import run_coro_blocking

if TYPE_CHECKING:
    from src.models.crawl_dead_letter import CrawlDeadLetter

logger = logging.getLogger(__name__)

#: A calendar month, used to sanity-check a replay target before crawling it.
MAX_SCHEDULE_MONTH = 12


@dataclass(frozen=True)
class ReplayOutcome:
    """Outcome of one replay execution."""

    success: bool
    replay_run_id: str
    status: str
    error_message: str | None = None
    error_code: str | None = None
    failure_stage: str | None = None


ReplayHandler = Callable[["CrawlDeadLetter", str], ReplayOutcome]


class ReplayDispatcher:
    """Dispatch dead letter replays to registered per-crawler handlers."""

    def __init__(self) -> None:
        """Initialize an empty handler registry."""
        self._handlers: dict[str, ReplayHandler] = {}

    def register(self, crawler: str, handler: ReplayHandler) -> None:
        """Register the replay handler for a crawler name."""
        self._handlers[crawler] = handler

    def registered_crawlers(self) -> set[str]:
        """Return the crawler names with a registered handler."""
        return set(self._handlers)

    def replay(self, dead_letter: CrawlDeadLetter, *, replay_run_id: str) -> ReplayOutcome:
        """Replay a dead letter with its registered crawler handler."""
        handler = self._handlers.get(dead_letter.crawler)
        if handler is None:
            msg = f"No replay handler registered for crawler '{dead_letter.crawler}'"
            raise KeyError(msg)
        return handler(dead_letter, replay_run_id)


async def _execute_award_replay(
    crawler: AwardCrawler,
    spec: CrawlRunSpec,
    source_key: str | None,
) -> None:
    try:
        await crawler.run(
            run_spec=spec,
            source_key=source_key,
            save=True,
            record_dead_letters=False,
            raise_on_persist_error=True,
        )
    finally:
        await crawler.close()


async def _execute_roster_replay(
    crawler: RosterTransactionCrawler,
    spec: CrawlRunSpec,
    target_date: str | None,
) -> None:
    await crawler.run(
        run_spec=spec,
        target_date=target_date,
        save=True,
        record_dead_letters=False,
        raise_on_persist_error=True,
    )


def _replay_awards(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay only the failed award source and report the replay run status."""
    spec = CrawlRunSpec(
        crawler=AWARD_CRAWLER_NAME,
        target_type=dead_letter.target_type or AWARD_TARGET_TYPE,
        target_id=dead_letter.target_id,
        season=dead_letter.season,
        game_id=dead_letter.game_id,
        source_url=dead_letter.source_url,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(_execute_award_replay(AwardCrawler(), spec, dead_letter.target_id))

    with SessionLocal() as session:
        run = CrawlExecutionRepository(session).get_by_run_id(replay_run_id)
    if run is None:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="missing",
            error_message="replay run was not recorded",
        )
    success = run.status == RUN_STATUS_SUCCESS
    return ReplayOutcome(
        success=success,
        replay_run_id=replay_run_id,
        status=run.status,
        error_message=None if success else run.error_message,
        error_code=None if success else run.error_code,
    )


def _replay_roster_transactions(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay one roster date and report the replay run status.

    The dead letter's `target_id` is the date, which is also the whole replay
    unit, so the replay is exact rather than a superset.
    """
    spec = CrawlRunSpec(
        crawler=ROSTER_CRAWLER_NAME,
        target_type=dead_letter.target_type or ROSTER_TARGET_TYPE,
        target_id=dead_letter.target_id,
        season=dead_letter.season,
        game_id=dead_letter.game_id,
        source_url=dead_letter.source_url,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(
        _execute_roster_replay(RosterTransactionCrawler(), spec, dead_letter.target_id),
    )

    with SessionLocal() as session:
        run = CrawlExecutionRepository(session).get_by_run_id(replay_run_id)
    if run is None:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="missing",
            error_message="replay run was not recorded",
        )
    success = run.status == RUN_STATUS_SUCCESS
    return ReplayOutcome(
        success=success,
        replay_run_id=replay_run_id,
        status=run.status,
        error_message=None if success else run.error_message,
        error_code=None if success else run.error_code,
    )


def _replay_schedule(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay one schedule month and report the replay run status.

    The dead letter's `target_id` is the `YYYY-MM` month, which is the whole
    replay unit. An empty month is a legitimate result, so a replay that confirms
    an off-season month succeeds rather than failing again.
    """
    year, month = _month_of(dead_letter.target_id)
    spec = CrawlRunSpec(
        crawler=SCHEDULE_CRAWLER_NAME,
        target_type=dead_letter.target_type or SCHEDULE_TARGET_TYPE,
        target_id=dead_letter.target_id,
        season=dead_letter.season or year,
        source_url=dead_letter.source_url,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(_execute_schedule_replay(ScheduleCrawler(), spec, year, month))

    with SessionLocal() as session:
        run = CrawlExecutionRepository(session).get_by_run_id(replay_run_id)
    if run is None:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="missing",
            error_message="replay run was not recorded",
        )
    success = run.status == RUN_STATUS_SUCCESS
    return ReplayOutcome(
        success=success,
        replay_run_id=replay_run_id,
        status=run.status,
        error_message=None if success else run.error_message,
        error_code=None if success else run.error_code,
    )


def _month_of(target_id: str | None) -> tuple[int | None, int]:
    """Parse a ``YYYY-MM`` replay target into a year and month.

    An unparseable target is a data problem, not a reason to crash the dispatcher,
    so the year is left unset and the crawl falls back to its default date.
    """
    if not target_id or "-" not in target_id:
        return None, 0
    year_text, _, month_text = target_id.partition("-")
    if not (year_text.isdigit() and month_text[:2].isdigit()):
        return None, 0
    return int(year_text), int(month_text[:2])


async def _execute_schedule_replay(
    crawler: ScheduleCrawler,
    spec: CrawlRunSpec,
    year: int | None,
    month: int,
) -> None:
    """Re-crawl one month, recording the replay run against the letter's unit."""
    if year is None or not 1 <= month <= MAX_SCHEDULE_MONTH:
        return
    await crawler.crawl_schedule(year, month, run_spec=spec, record_dead_letters=False)


def build_default_dispatcher() -> ReplayDispatcher:
    """Build a dispatcher with the Phase B canary handlers registered."""
    dispatcher = ReplayDispatcher()
    dispatcher.register(AWARD_CRAWLER_NAME, _replay_awards)
    dispatcher.register(ROSTER_CRAWLER_NAME, _replay_roster_transactions)
    dispatcher.register(SCHEDULE_CRAWLER_NAME, _replay_schedule)
    return dispatcher
