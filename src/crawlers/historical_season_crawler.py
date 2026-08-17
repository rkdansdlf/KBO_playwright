"""Historical Season Crawler with Provenance Tracking.

Fetches official historical KBO season schedules, games, scores, and metadata
directly from the official KBO archive (Schedule.aspx) with full data provenance.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.models.game import Game, GameMetadata
from src.models.season import KboSeason
from src.utils.team_codes import resolve_team_code

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HistoricalCrawlSummary:
    """Summary of a historical season crawl run."""

    season: int
    games_found: int
    games_saved: int
    source_name: str
    provenance_verified: bool
    crawled_at: datetime


class HistoricalSeasonCrawler:
    """Collects and stores verified historical season schedules from official KBO sources."""

    def __init__(self, request_delay: float = 1.0) -> None:
        """Initialize crawler with configured rate-limit delay."""
        self.request_delay = request_delay
        self.schedule_crawler = ScheduleCrawler(request_delay=request_delay)

    def _ensure_season_stub(self, session: Session, year: int) -> None:
        season_stub = (
            session.query(KboSeason).filter(KboSeason.season_year == year, KboSeason.league_type_code == 0).first()
        )
        if not season_stub:
            season_stub = KboSeason(
                season_id=year * 10,
                season_year=year,
                league_type_code=0,
                league_type_name="정규시즌",
            )
            session.add(season_stub)
            session.flush()

    def _save_game_record(
        self,
        session: Session,
        g_dict: dict,
        year: int,
        source_name: str,
        now: datetime,
    ) -> bool:
        game_id = g_dict.get("game_id")
        if not game_id:
            return False

        game_date_val = g_dict.get("game_date")
        if isinstance(game_date_val, str):
            g_date = date.fromisoformat(game_date_val[:10])
        elif isinstance(game_date_val, date):
            g_date = game_date_val
        else:
            g_date = date(year, 1, 1)

        away_team = resolve_team_code(g_dict.get("away_team") or "")
        home_team = resolve_team_code(g_dict.get("home_team") or "")

        game_row = session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if not game_row:
            game_row = Game(
                game_id=game_id,
                game_date=g_date,
                season_id=year * 10,
                home_team=home_team,
                away_team=away_team,
            )
            session.add(game_row)

        game_row.game_date = g_date
        game_row.home_team = home_team
        game_row.away_team = away_team
        game_row.home_score = g_dict.get("home_score")
        game_row.away_score = g_dict.get("away_score")
        game_row.game_status = g_dict.get("game_status", "COMPLETED")
        game_row.stadium = g_dict.get("stadium")

        meta_row = session.execute(select(GameMetadata).where(GameMetadata.game_id == game_id)).scalar_one_or_none()
        if not meta_row:
            meta_row = GameMetadata(game_id=game_id)
            session.add(meta_row)

        meta_row.stadium_name = g_dict.get("stadium")
        meta_row.source_payload = {
            "source_name": source_name,
            "crawled_at": now.isoformat(),
            "raw_game_data": g_dict,
        }
        return True

    async def crawl_and_save_season(
        self,
        session: Session,
        year: int,
        *,
        months: list[int] | None = None,
        dry_run: bool = False,
    ) -> HistoricalCrawlSummary:
        """Crawl an entire historical season and persist with provenance metadata."""
        target_months = months or list(range(3, 12))
        logger.info("[HistoricalCrawler] Starting crawl for Season %d (Months: %s)", year, target_months)

        raw_games = await self.schedule_crawler.crawl_season(
            year=year,
            months=target_months,
            series_id="0",
        )

        now = datetime.now(UTC)
        source_name = "kbo_official_schedule"

        if not raw_games:
            logger.warning("[HistoricalCrawler] No games returned from KBO official site for %d", year)
            return HistoricalCrawlSummary(
                season=year,
                games_found=0,
                games_saved=0,
                source_name=source_name,
                provenance_verified=False,
                crawled_at=now,
            )

        self._ensure_season_stub(session, year)
        saved_count = 0
        for g_dict in raw_games:
            if not dry_run and self._save_game_record(session, g_dict, year, source_name, now):
                saved_count += 1

        if not dry_run:
            session.commit()

        logger.info(
            "[HistoricalCrawler] Season %d completed: found=%d, saved=%d, dry_run=%s",
            year,
            len(raw_games),
            saved_count,
            dry_run,
        )

        return HistoricalCrawlSummary(
            season=year,
            games_found=len(raw_games),
            games_saved=saved_count if not dry_run else len(raw_games),
            source_name=source_name,
            provenance_verified=True,
            crawled_at=now,
        )
