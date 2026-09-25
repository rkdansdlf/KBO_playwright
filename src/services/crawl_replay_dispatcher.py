"""Replay dispatcher for dead letter retries.

The dispatcher maps a ``crawler`` name to a handler that re-runs the failing
unit of work. Handlers are registered explicitly so Phase B can onboard one
canary at a time instead of assuming every crawler shares a replay interface.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.crawlers.award_crawler import (
    AWARD_CRAWLER_NAME,
    AWARD_TARGET_TYPE,
    AwardCrawler,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_SUCCESS
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec

if TYPE_CHECKING:
    from src.models.crawl_dead_letter import CrawlDeadLetter

logger = logging.getLogger(__name__)


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


def _run_coro(coro: Any) -> Any:  # noqa: ANN401
    """Run a coroutine from sync code, even inside a running event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _target() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=_target, name="crawl-replay")
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


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
    _run_coro(_execute_award_replay(AwardCrawler(), spec, dead_letter.target_id))

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


def build_default_dispatcher() -> ReplayDispatcher:
    """Build a dispatcher with the Phase B canary handlers registered."""
    dispatcher = ReplayDispatcher()
    dispatcher.register(AWARD_CRAWLER_NAME, _replay_awards)
    return dispatcher
