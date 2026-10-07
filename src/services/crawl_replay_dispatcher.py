"""Replay dispatcher for dead letter retries.

The dispatcher maps a ``crawler`` name to a handler that re-runs the failing
unit of work. Handlers are registered explicitly so Phase B can onboard one
canary at a time instead of assuming every crawler shares a replay interface.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import DATE_STR_LEN, KST
from src.crawlers.award_crawler import (
    AWARD_CRAWLER_NAME,
    AWARD_TARGET_TYPE,
    WIKI_SOURCE_KEY,
    YAGOONARA_SOURCE_KEY,
    AwardCrawler,
)
from src.crawlers.food_crawler import FOOD_CRAWLER_NAME, FOOD_TARGET_TYPE, TEAM_FOOD_SOURCES, FoodCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.kbo_event_crawler import (
    KBO_EVENT_CRAWLER_NAME,
    KBO_EVENT_SOURCE_KEY,
    KBO_EVENT_TARGET_TYPE,
    KboEventCrawler,
    _page_key,
)
from src.crawlers.parking_crawler import (
    PARKING_CRAWLER_NAME,
    PARKING_TARGET_TYPE,
    TEAM_PARKING_SOURCES,
    ParkingCrawler,
)
from src.crawlers.pbp_crawler import (
    PBP_CRAWLER_NAME,
    PBP_TARGET_TYPE,
    PBPCrawler,
)
from src.crawlers.player_batting_all_series_crawler import (
    BATTING_SERIES_CRAWLER_NAME,
    BATTING_SERIES_TARGET_TYPE,
    BattingSeriesCrawlRequest,
    run_batting_series,
)
from src.crawlers.player_movement_crawler import (
    PLAYER_MOVEMENT_CRAWLER_NAME,
    PLAYER_MOVEMENT_TARGET_TYPE,
    PlayerMovementCrawler,
)
from src.crawlers.player_pitching_all_series_crawler import (
    PITCHING_SERIES_CRAWLER_NAME,
    PITCHING_SERIES_TARGET_TYPE,
    PitchingSeriesCrawlRequest,
    run_pitching_series,
)
from src.crawlers.preview_crawler import (
    PREVIEW_CRAWLER_NAME,
    PREVIEW_TARGET_TYPE,
    PreviewCrawler,
)
from src.crawlers.realtime_issue_crawler import (
    MLBPARK_BULLPEN_TARGET_ID,
    NAVER_NEWS_TARGET_ID,
    REALTIME_ISSUE_CRAWLER_NAME,
    REALTIME_ISSUE_TARGET_TYPE,
    RealtimeIssueCrawler,
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
from src.repositories.game_relay import save_relay_data
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
from src.services.pregame_context_writer import save_preview_contexts
from src.services.relay_runs import RELAY_CRAWLER_NAME, RELAY_TARGET_TYPE
from src.utils.async_bridge import run_coro_blocking

if TYPE_CHECKING:
    from src.models.crawl_dead_letter import CrawlDeadLetter

logger = logging.getLogger(__name__)

#: A calendar month, used to sanity-check a replay target before crawling it.
MAX_SCHEDULE_MONTH = 12

#: Team codes each stadium-sweep crawler can serve, keyed by crawler name.
#: A letter naming anything else selects no source at all, so the shared
#: handler refuses it instead of closing the letter over an empty sweep.
_TEAM_SOURCES: dict[str, Collection[str]] = {
    FOOD_CRAWLER_NAME: TEAM_FOOD_SOURCES,
    PARKING_CRAWLER_NAME: TEAM_PARKING_SOURCES,
}


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
            error_code="VALIDATION_SCHEMA",
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
            error_code="VALIDATION_SCHEMA",
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
    """Replay only the failed award source and report the replay run status.

    The letter names one source, and only two exist. An unknown key matches
    neither: the crawl would fetch nothing and still record a success, closing
    the letter over a source it never read.
    """
    source_key = dead_letter.target_id
    if source_key is not None and source_key not in {WIKI_SOURCE_KEY, YAGOONARA_SOURCE_KEY}:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unknown award source: {source_key}",
        )
    spec = CrawlRunSpec(
        crawler=AWARD_CRAWLER_NAME,
        target_type=dead_letter.target_type or AWARD_TARGET_TYPE,
        target_id=source_key,
        season=dead_letter.season,
        game_id=dead_letter.game_id,
        source_url=dead_letter.source_url,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(_execute_award_replay(AwardCrawler(), spec, source_key))

    return _outcome_from_persisted_run(replay_run_id)


def _replay_roster_transactions(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay one roster date and report the replay run status.

    The dead letter's `target_id` is the date, which is also the whole replay
    unit, so the replay is exact rather than a superset.

    A missing or unreadable date is refused instead of passed through: the
    crawler reads a missing `target_date` as *today*, so the letter would be
    resolved by a crawl of a day it never named.
    """
    target_date = dead_letter.target_id
    if not target_date or not _is_iso_date(target_date):
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unreadable roster date: {target_date}",
        )
    spec = CrawlRunSpec(
        crawler=ROSTER_CRAWLER_NAME,
        target_type=dead_letter.target_type or ROSTER_TARGET_TYPE,
        target_id=target_date,
        season=dead_letter.season,
        game_id=dead_letter.game_id,
        source_url=dead_letter.source_url,
        parent_run_id=dead_letter.original_run_id,
        replay_of_run_id=dead_letter.original_run_id,
        run_id=replay_run_id,
    )
    run_coro_blocking(
        _execute_roster_replay(RosterTransactionCrawler(), spec, target_date),
    )

    return _outcome_from_persisted_run(replay_run_id)


def _replay_schedule(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay one schedule month and report the replay run status.

    The dead letter's `target_id` is the `YYYY-MM` month, which is the whole
    replay unit. An empty month is a legitimate result, so a replay that confirms
    an off-season month succeeds rather than failing again.
    """
    year, month = _month_of(dead_letter.target_id)
    if year is None or not 1 <= month <= MAX_SCHEDULE_MONTH:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="missing",
            error_message=f"dead letter carries an unreadable schedule month: {dead_letter.target_id}",
            error_code="VALIDATION_SCHEMA",
        )
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


def _is_iso_date(value: str) -> bool:
    """Return whether the value is the date form the roster producer writes.

    The check mirrors `RosterTransactionCrawler.run`, which parses ``target_date``
    with the same format: a date it would raise on is a malformed letter, not a
    reason to spend a retry attempt.
    """
    try:
        datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=KST)
    except ValueError:
        return False
    return True


def _is_compact_date(value: str) -> bool:
    """Return whether the value is the ``YYYYMMDD`` the preview crawler fetches.

    The length check is load-bearing: `strptime` parses ``"2026107"`` happily,
    but the Naver preview endpoint takes exactly eight digits, so a shorter
    string would confirm an empty date against the wrong day.
    """
    if len(value) != DATE_STR_LEN or not value.isdigit():
        return False
    try:
        datetime.strptime(value, "%Y%m%d").replace(tzinfo=KST)
    except ValueError:
        return False
    return True


async def _execute_schedule_replay(
    crawler: ScheduleCrawler,
    spec: CrawlRunSpec,
    year: int | None,
    month: int,
) -> None:
    """Re-crawl one month, recording the replay run against the letter's unit.

    `save=True` is load-bearing. `crawl_schedule` defaults it to False, and a
    replay that fetched the month without writing it would land as `success`
    with `records_written=0` -- which `_outcome_from_persisted_run` reads as a
    completed retry and uses to resolve the letter. The failed month would be
    closed as recovered having been neither refreshed nor stored. Although the
    attempt is counted, success prevents any further retry from being scheduled.
    """
    if year is None or not 1 <= month <= MAX_SCHEDULE_MONTH:
        return
    await crawler.crawl_schedule(year, month, save=True, run_spec=spec, record_dead_letters=False)


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

    A code the crawler does not know is refused before anything runs. Passing it
    through would select no sources at all, and the empty sweep would land as a
    success that resolves the letter -- a wrong target closing as repaired.
    """
    team_code = dead_letter.target_id
    if not team_code:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message="dead letter carries no team code",
        )
    if team_code not in _TEAM_SOURCES[crawler_name]:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unknown team code: {team_code}",
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
    """Re-crawl one team's stadium page, writing the results this time.

    ``raise_on_persist_error`` matches the other Phase I handlers. Without it a
    write that stored nothing is still visible -- the run lands as ``failed``
    and ``_outcome_from_persisted_run`` refuses to call it resolved -- but the
    exception carries the taxonomy code straight to ``retry_dead_letter``, which
    is where the attempt is decided. One team means ``partial`` and ``failed``
    describe the same outcome anyway.
    """
    await crawler.run(
        save=True,
        team_filter=team_code,
        run_spec=spec,
        record_dead_letters=False,
        raise_on_persist_error=True,
    )


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
            error_code="VALIDATION_SCHEMA",
            error_message="dead letter carries no page url",
        )
    if dead_letter.target_id not in (None, "", KBO_EVENT_SOURCE_KEY, _page_key(url)):
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter target does not match its source url: {dead_letter.target_id}",
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
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unreadable year target: {dead_letter.target_id}",
        )
    start_year, end_year = years
    if start_year > end_year:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries a reversed year range: {dead_letter.target_id}",
        )
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


def _replay_realtime_issue(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay exactly the source unit named by the realtime issue letter."""
    target_id = dead_letter.target_id
    if target_id not in {NAVER_NEWS_TARGET_ID, MLBPARK_BULLPEN_TARGET_ID}:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unsupported realtime issue target: {target_id}",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=REALTIME_ISSUE_CRAWLER_NAME,
        target_type=dead_letter.target_type or REALTIME_ISSUE_TARGET_TYPE,
        target_id=target_id,
    )
    run_coro_blocking(_execute_realtime_issue_replay(RealtimeIssueCrawler(), spec, target_id))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_realtime_issue_replay(
    crawler: RealtimeIssueCrawler,
    spec: CrawlRunSpec,
    target_id: str,
) -> None:
    """Re-fetch one source and archive its response without creating a new DLQ."""
    await crawler.run(
        target_id=target_id,
        save=True,
        run_spec=spec,
        record_dead_letters=False,
        raise_on_persist_error=True,
    )


def _replay_preview(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay one date's pregame data and store whatever it now finds.

    This crawler does not persist: the preview batch owns the write because it
    also writes the manifest and filters which previews are storable. A replay
    has no such caller, so without storing here the incident would close over a
    refreshed fetch that nothing ever read.
    """
    game_date = dead_letter.target_id or dead_letter.game_id
    if not game_date:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message="dead letter carries no preview date",
        )
    if not _is_compact_date(game_date):
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message=f"dead letter carries an unreadable preview date: {game_date}",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=PREVIEW_CRAWLER_NAME,
        target_type=dead_letter.target_type or PREVIEW_TARGET_TYPE,
        target_id=game_date,
        game_id=game_date,
    )
    run_coro_blocking(_execute_preview_replay(PreviewCrawler(request_delay=1.0), spec, game_date))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_preview_replay(crawler: PreviewCrawler, spec: CrawlRunSpec, game_date: str) -> None:
    """Re-fetch one date's pregame data and store it through the batch's writer."""
    previews = await crawler.run(game_date, run_spec=spec, record_dead_letters=False)
    if not previews:
        return
    # Raised rather than swallowed: our own write failing has to reach the retry
    # policy, which is the only thing that can decide to try again.
    saved_ids = await asyncio.to_thread(save_preview_contexts, previews, game_date)
    logger.info("Replay stored %s pregame rows for %s", len(saved_ids), game_date)


def _replay_pbp(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Replay the one game the letter names and store the plays it returns.

    The crawl does not persist: its callers own the write because they decide
    whether the rows are a relay refresh or a repair of a game the relay source
    could not verify. A replay has no such caller, so without storing here the
    incident would close over a refreshed read that nothing ever wrote.
    """
    game_id = dead_letter.target_id or dead_letter.game_id
    if not game_id:
        return ReplayOutcome(
            success=False,
            replay_run_id=replay_run_id,
            status="unaddressable",
            error_code="VALIDATION_SCHEMA",
            error_message="dead letter carries no game id",
        )
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=PBP_CRAWLER_NAME,
        target_type=dead_letter.target_type or PBP_TARGET_TYPE,
        target_id=game_id,
        game_id=game_id,
    )
    run_coro_blocking(_execute_pbp_replay(PBPCrawler(), spec, game_id))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_pbp_replay(crawler: PBPCrawler, spec: CrawlRunSpec, game_id: str) -> None:
    """Re-read one game's play-by-play and persist what it found."""
    events = await crawler.run(game_id, run_spec=spec, record_dead_letters=False)
    if not events:
        return
    with SessionLocal() as session:
        saved = save_relay_data(
            game_id,
            events=events,
            source_name="kbo_pbp_replay",
            notes="Re-crawled from the dead letter queue after an unreadable play-by-play page.",
            session=session,
            # Raised rather than swallowed: our own write failing has to reach
            # the retry policy, which is the only thing that can decide to try
            # again. A silent zero would close the incident over nothing.
            raise_on_error=True,
        )
        session.commit()
    logger.info("Replay stored %s play-by-play rows for %s", saved, game_id)


def _series_replay_spec(
    dead_letter: CrawlDeadLetter,
    replay_run_id: str,
    *,
    crawler: str,
    target_type: str,
) -> tuple[CrawlRunSpec, int, str] | None:
    """Build the replay spec for one season-and-series letter.

    Returns ``None`` when the letter does not name a readable unit, so the
    handler reports it as unaddressable rather than crawling a guessed year.
    """
    target_id = dead_letter.target_id
    if not target_id or ":" not in target_id:
        return None
    year_text, _, series_key = target_id.partition(":")
    year = dead_letter.season or (int(year_text) if year_text.isdigit() else None)
    if year is None or not series_key:
        return None
    spec = _spec_for(
        dead_letter,
        replay_run_id,
        crawler=crawler,
        target_type=dead_letter.target_type or target_type,
        target_id=target_id,
        season=year,
    )
    return spec, year, series_key


def _unaddressable_series(replay_run_id: str, target_id: str | None) -> ReplayOutcome:
    return ReplayOutcome(
        success=False,
        replay_run_id=replay_run_id,
        status="unaddressable",
        error_message=f"dead letter carries an unreadable series target: {target_id}",
    )


def _replay_batting_series(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Re-crawl the season and series the letter names, and store what it finds."""
    parsed = _series_replay_spec(
        dead_letter,
        replay_run_id,
        crawler=BATTING_SERIES_CRAWLER_NAME,
        target_type=BATTING_SERIES_TARGET_TYPE,
    )
    if parsed is None:
        return _unaddressable_series(replay_run_id, dead_letter.target_id)
    spec, year, series_key = parsed
    run_coro_blocking(_execute_batting_series_replay(spec, year, series_key))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_batting_series_replay(spec: CrawlRunSpec, year: int, series_key: str) -> None:
    """Re-run one batting series through the tracked path, writing this time.

    ``sync_playwright`` refuses to run inside an event loop, which is why the
    crawl is dispatched to a worker thread rather than awaited directly.
    """
    await asyncio.to_thread(
        run_batting_series,
        BattingSeriesCrawlRequest(year=year, series_key=series_key, save_to_db=True),
        run_spec=spec,
        record_dead_letters=False,
    )


def _replay_pitching_series(dead_letter: CrawlDeadLetter, replay_run_id: str) -> ReplayOutcome:
    """Re-crawl the season and series the letter names, and store what it finds."""
    parsed = _series_replay_spec(
        dead_letter,
        replay_run_id,
        crawler=PITCHING_SERIES_CRAWLER_NAME,
        target_type=PITCHING_SERIES_TARGET_TYPE,
    )
    if parsed is None:
        return _unaddressable_series(replay_run_id, dead_letter.target_id)
    spec, year, series_key = parsed
    run_coro_blocking(_execute_pitching_series_replay(spec, year, series_key))

    return _outcome_from_persisted_run(replay_run_id)


async def _execute_pitching_series_replay(spec: CrawlRunSpec, year: int, series_key: str) -> None:
    """Re-run one pitching series through the tracked path, writing this time."""
    await asyncio.to_thread(
        run_pitching_series,
        PitchingSeriesCrawlRequest(year=year, series_key=series_key, save_to_db=True),
        run_spec=spec,
        record_dead_letters=False,
    )


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
    dispatcher.register(REALTIME_ISSUE_CRAWLER_NAME, _replay_realtime_issue)
    dispatcher.register(PREVIEW_CRAWLER_NAME, _replay_preview)
    dispatcher.register(PBP_CRAWLER_NAME, _replay_pbp)
    dispatcher.register(BATTING_SERIES_CRAWLER_NAME, _replay_batting_series)
    dispatcher.register(PITCHING_SERIES_CRAWLER_NAME, _replay_pitching_series)
    return dispatcher
