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
from src.crawlers.food_crawler import FOOD_CRAWLER_NAME, FOOD_TARGET_TYPE, FoodCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.kbo_event_crawler import (
    KBO_EVENT_CRAWLER_NAME,
    KBO_EVENT_TARGET_TYPE,
    KboEventCrawler,
)
from src.crawlers.parking_crawler import (
    PARKING_CRAWLER_NAME,
    PARKING_TARGET_TYPE,
    ParkingCrawler,
)
from src.crawlers.player_movement_crawler import (
    PLAYER_MOVEMENT_CRAWLER_NAME,
    PLAYER_MOVEMENT_TARGET_TYPE,
    PlayerMovementCrawler,
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
from src.crawlers.team_history_crawler import (
    TEAM_HISTORY_CRAWLER_NAME,
    TEAM_HISTORY_TARGET_ID,
    TEAM_HISTORY_TARGET_TYPE,
    TeamHistoryCrawler,
)
from src.db.engine import SessionLocal
from src.models.crawl_execution import RUN_STATUS_SUCCESS
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.repositories.player_repository import PlayerRepository
from src.services.game_collection_service import (
    GameCollectionConfig,
    replay_single_game_detail,
    replay_single_relay,
)
from src.services.game_detail_runs import (
    GAME_DETAIL_CRAWLER_NAME,
    GAME_DETAIL_TARGET_TYPE,
    season_of,
)
from src.services.relay_runs import RELAY_CRAWLER_NAME, RELAY_TARGET_TYPE
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


async def _execute_game_detail_replay(
    game_id: str,
    spec: CrawlRunSpec,
) -> None:
    """Run the single-game full detail collection for one dead letter."""
    await replay_single_game_detail(
        game_id,
        spec,
        detail_crawler=GameDetailCrawler(),
        config=GameCollectionConfig(),
    )


def _outcome_from_persisted_run(replay_run_id: str) -> ReplayOutcome:
    """Read the verdict off the stored replay run.

    Every handler ends here, and the reason is the same for all of them: the run
    ledger writes on its own session, so a crash between finishing the work and
    returning from the handler must not be reported as a successful retry. What
    the database holds is the claim; what the crawler returned is not.

    `success` is only ever `success`. A partial replay stored some rows and left
    the same incompleteness in place, and closing that would retire an incident
    that is still true. An empty or unchanged result is a success, because a
    replay that confirms the source has nothing has answered the question.
    """
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


def _spec_for(dead_letter: CrawlDeadLetter, replay_run_id: str, **overrides: object) -> CrawlRunSpec:
    """Build a replay run spec that points back at the letter's original run.

    The lineage fields are not optional bookkeeping: without `replay_of_run_id`
    the retry policy has nothing to count attempts against, and the ledger
    cannot show which failure a given retry was addressing.
    """
    base: dict[str, object] = {
        "crawler": dead_letter.crawler,
        "target_type": dead_letter.target_type,
        "target_id": dead_letter.target_id,
        "season": dead_letter.season,
        "game_id": dead_letter.game_id,
        "source_url": dead_letter.source_url,
        "parent_run_id": dead_letter.original_run_id,
        "replay_of_run_id": dead_letter.original_run_id,
        "run_id": replay_run_id,
    }
    base.update(overrides)
    return CrawlRunSpec(**base)  # type: ignore[arg-type]


def _replay_game_detail(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Re-crawl one game and report the persisted replay run, not our own verdict."""
    game_id = dead_letter.target_id or dead_letter.game_id
    if not game_id:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_message="dead letter carries no game id",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=GAME_DETAIL_CRAWLER_NAME,
        target_type=dead_letter.target_type or GAME_DETAIL_TARGET_TYPE,
        target_id=game_id,
        season=dead_letter.season or season_of(game_id),
        game_id=game_id,
    )
    run_coro_blocking(_execute_game_detail_replay(game_id, spec))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_relay_replay(game_id: str, spec: CrawlRunSpec) -> None:
    """Run the single-game relay collection for one dead letter."""
    from src.crawlers.relay_crawler import RelayCrawler

    await replay_single_relay(
        game_id,
        spec,
        relay_crawler=RelayCrawler(),
        config=GameCollectionConfig(),
    )


def _replay_relay(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Re-crawl one game's relay and report the persisted replay run.

    The authority is the stored RUN-B, not whatever this call returned: the run
    ledger writes on its own session, so a crash between the work and this return
    must not be reported as a successful retry.

    `success` is only ever `success`. A partial replay stored some innings and is
    still missing others, and closing that as resolved would retire an incident
    that is still true. An empty or unchanged result counts as success -- the
    replay confirmed there is nothing to fetch or that nothing changed.
    """
    game_id = dead_letter.target_id or dead_letter.game_id
    if not game_id:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_message="dead letter carries no game id",
        )
    spec = CrawlRunSpec(
        crawler=RELAY_CRAWLER_NAME,
        target_type=dead_letter.target_type or RELAY_TARGET_TYPE,
        target_id=game_id,
        season=dead_letter.season or season_of(game_id),
        game_id=game_id,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(_execute_relay_replay(game_id, spec))

    return _outcome_from_persisted_run(replay_run_id)


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

    return _outcome_from_persisted_run(replay_run_id)


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

    return _outcome_from_persisted_run(replay_run_id)


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

    return _outcome_from_persisted_run(replay_run_id)


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


# ── Phase I: the five crawlers that record a ledger entry and a dead letter ──
#
# Every handler below obeys three rules, and they are the reason the chain can be
# trusted:
#
#   * `record_dead_letters=False`. The retry policy owns the attempt count. A
#     replay that enqueued its own letter would make one incident into two on
#     every attempt, and the operator would be counting letters instead of
#     failures.
#   * The crawl arguments are pinned explicitly. `save` defaults to False on most
#     of these crawlers, so inheriting the default would mean a replay that
#     fetched the page and stored nothing, and reported success.
#   * The verdict comes from `_outcome_from_persisted_run`, never from what the
#     crawl returned.


def _replay_team_page(
    crawler: FoodCrawler | ParkingCrawler,
    dead_letter: CrawlDeadLetter,
    replay_run_id: str,
    *,
    crawler_name: str,
    target_type: str,
) -> ReplayOutcome:
    """Replay one team's stadium page, which is the unit the letter names.

    A stadium sweep fails one team at a time and the letter is keyed by that
    team, so `team_filter` makes the replay exact: fixing one stadium does not
    re-fetch the other nine.
    """
    team_code = dead_letter.target_id
    if not team_code:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_message="dead letter carries no team code",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=crawler_name,
        target_type=dead_letter.target_type or target_type,
        target_id=team_code,
        season=dead_letter.season,
    )
    run_coro_blocking(
        _execute_team_page_replay(crawler, spec, team_code),
    )

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_team_page_replay(
    crawler: FoodCrawler | ParkingCrawler,
    spec: CrawlRunSpec,
    team_code: str,
) -> None:
    """Re-crawl one team's stadium page, writing the results this time."""
    await crawler.run(save=True, team_filter=team_code, run_spec=spec, record_dead_letters=False)


def _replay_food(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the one stadium's food page the letter names."""
    return _replay_team_page(
        FoodCrawler(),
        dead_letter,
        replay_run_id,
        crawler_name=FOOD_CRAWLER_NAME,
        target_type=FOOD_TARGET_TYPE,
    )


def _replay_parking(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the one stadium's parking page the letter names."""
    return _replay_team_page(
        ParkingCrawler(),
        dead_letter,
        replay_run_id,
        crawler_name=PARKING_CRAWLER_NAME,
        target_type=PARKING_TARGET_TYPE,
    )


def _replay_kbo_event(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the one event page the letter names.

    The letter's `source_url` is the failing page, and handing it back as the
    crawler's `base_url` narrows `self.urls` to that page alone -- so the replay
    is one page, not the seven-page sweep the original run was. Without a usable
    URL there is nothing to aim at, and re-running the sweep would be a guess.
    """
    url = dead_letter.source_url
    if not url:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_message="dead letter carries no page url",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=KBO_EVENT_CRAWLER_NAME,
        target_type=dead_letter.target_type or KBO_EVENT_TARGET_TYPE,
        source_url=url,
    )
    run_coro_blocking(_execute_kbo_event_replay(KboEventCrawler(base_url=url), spec))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_kbo_event_replay(crawler: KboEventCrawler, spec: CrawlRunSpec) -> None:
    """Re-crawl a single event page, persisting anything it yields."""
    await crawler.run(save=True, run_spec=spec, record_dead_letters=False)


def _year_range_of(target_id: str | None) -> tuple[int, int] | None:
    """Parse a player movement replay target into an inclusive year range.

    The crawl records one year as ``"2026"`` and a sweep as ``"2023-2024"``,
    because that is what it crawled and the letter has to name it exactly. An
    unparseable target is left for the caller to reject rather than guessed at,
    since crawling the wrong years would report a resolution for work that was
    never done.
    """
    if not target_id:
        return None
    first, separator, last = target_id.partition("-")
    if not first.isdigit():
        return None
    if not separator:
        return int(first), int(first)
    if not last.isdigit():
        return None
    return int(first), int(last)


def _replay_player_movement(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the year or year range the letter names, and store what it found.

    This is the one handler that writes domain rows itself. Every other crawler
    persists from inside `run`, but `PlayerMovementCrawler` deliberately does
    not: its caller owns the write, so the daily pipeline can decide the order.
    A replay has no such caller, so without a write here the incident would be
    resolved over rows nobody had refreshed -- re-fetched evidence that no
    reader will ever look at, which is the quietest possible way to lose data.
    """
    years = _year_range_of(dead_letter.target_id)
    if years is None:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_message=f"dead letter carries an unreadable year target: {dead_letter.target_id}",
        )
    start_year, end_year = years
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=PLAYER_MOVEMENT_CRAWLER_NAME,
        target_type=dead_letter.target_type or PLAYER_MOVEMENT_TARGET_TYPE,
        season=dead_letter.season or start_year,
    )
    run_coro_blocking(_execute_player_movement_replay(PlayerMovementCrawler(), spec, start_year, end_year))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_player_movement_replay(
    crawler: PlayerMovementCrawler,
    spec: CrawlRunSpec,
    start_year: int,
    end_year: int,
) -> None:
    """Re-crawl one year range and persist the movements it yields."""
    movements = await crawler.run(
        start_year,
        end_year,
        save_snapshots=True,
        run_spec=spec,
        record_dead_letters=False,
    )
    if not movements:
        return
    with SessionLocal() as session:
        saved = PlayerRepository(session).save_player_movements(movements)
        # Raised rather than swallowed: our own write failing has to reach the
        # retry policy, which is the only thing that can decide to try again.
        session.commit()
    logger.info("Replay stored %s player movement rows for %s-%s", saved, start_year, end_year)


def _replay_team_history(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the team history page, which is a single page for all seasons.

    There is one unit here rather than many -- the page carries every season --
    so the letter's target is the whole replay.
    """
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=TEAM_HISTORY_CRAWLER_NAME,
        target_type=dead_letter.target_type or TEAM_HISTORY_TARGET_TYPE,
        target_id=dead_letter.target_id or TEAM_HISTORY_TARGET_ID,
    )
    run_coro_blocking(_execute_team_history_replay(TeamHistoryCrawler(), spec))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_team_history_replay(crawler: TeamHistoryCrawler, spec: CrawlRunSpec) -> None:
    """Re-crawl the team history page and persist it."""
    await crawler.run(save=True, run_spec=spec, record_dead_letters=False)


def build_default_dispatcher() -> ReplayDispatcher:
    """Build a dispatcher with every adopted crawler registered."""
    dispatcher = ReplayDispatcher()
    dispatcher.register(AWARD_CRAWLER_NAME, _replay_awards)
    dispatcher.register(ROSTER_CRAWLER_NAME, _replay_roster_transactions)
    dispatcher.register(SCHEDULE_CRAWLER_NAME, _replay_schedule)
    dispatcher.register(GAME_DETAIL_CRAWLER_NAME, _replay_game_detail)
    dispatcher.register(RELAY_CRAWLER_NAME, _replay_relay)
    dispatcher.register(FOOD_CRAWLER_NAME, _replay_food)
    dispatcher.register(PARKING_CRAWLER_NAME, _replay_parking)
    dispatcher.register(KBO_EVENT_CRAWLER_NAME, _replay_kbo_event)
    dispatcher.register(PLAYER_MOVEMENT_CRAWLER_NAME, _replay_player_movement)
    dispatcher.register(TEAM_HISTORY_CRAWLER_NAME, _replay_team_history)
    return dispatcher
