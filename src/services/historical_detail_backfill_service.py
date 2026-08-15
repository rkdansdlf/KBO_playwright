"""Historical (2001-2009) KBO Game Detail and Boxscore Backfill Service."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from sqlalchemy import select

from src.cli.historical_boxscore_import import validate_boxscore_payload
from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.repositories.game_repository import save_game_detail
from src.services.player_id_resolver import PlayerIdResolver

if TYPE_CHECKING:
    from playwright.sync_api import Page, Route
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

BLOCKED_RESOURCE_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".mp4",
    ".mp3",
)
CRAWLER_PROCESS_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    OSError,
)


@dataclass
class BackfillResult:
    """Summary metrics of a historical backfill execution."""

    year: int
    total_missing: int
    attempted: int
    saved: int
    skipped_validation: int
    failed: int
    dry_run: bool
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "year": self.year,
            "total_missing": self.total_missing,
            "attempted": self.attempted,
            "saved": self.saved,
            "skipped_validation": self.skipped_validation,
            "failed": self.failed,
            "dry_run": self.dry_run,
        }


class HistoricalDetailBackfillService:
    """Service to automatically identify missing 2001-2009 games, crawl, validate, and save."""

    def __init__(self, session: Session, request_delay: float = 1.5) -> None:
        """Initialize backfill service."""
        self.session = session
        self.request_delay = request_delay
        self.resolver = PlayerIdResolver(session)
        self.crawler = LegacyGameDetailCrawler(request_delay=request_delay, resolver=self.resolver)

    def get_missing_games(self, year: int, limit: int | None = None) -> list[dict[str, Any]]:
        """Query terminal games in a season that lack batting or pitching details.

        Args:
            year: Season year (e.g. 2009).
            limit: Optional maximum number of games to return.

        Returns:
            List of dictionaries containing game_id and game_date.

        """
        year_prefix = f"{year}%"
        stmt = (
            select(Game.game_id, Game.game_date, Game.home_team, Game.away_team)
            .where((Game.game_id.like(year_prefix)) | (Game.season_id == year))
            .where(Game.game_status.in_(["COMPLETED", "FINAL", "DRAW", "TERMINAL", "종료", "콜드게임"]))
            .where(
                ~Game.game_id.in_(
                    select(GameBattingStat.game_id).where(GameBattingStat.game_id.like(year_prefix)).distinct()
                )
                | ~Game.game_id.in_(
                    select(GamePitchingStat.game_id).where(GamePitchingStat.game_id.like(year_prefix)).distinct()
                )
            )
            .order_by(Game.game_date.asc(), Game.game_id.asc())
        )
        if limit:
            stmt = stmt.limit(limit)

        rows = list(self.session.execute(stmt).all())
        results: list[dict[str, Any]] = []
        for r in rows:
            clean_date = str(r.game_date).replace("-", "") if r.game_date else r.game_id[:8]
            results.append(
                {
                    "game_id": r.game_id,
                    "game_date": clean_date,
                    "home_team": r.home_team,
                    "away_team": r.away_team,
                }
            )
        return results

    @staticmethod
    def _block_unnecessary_resources(route: Route) -> None:
        """Route handler to abort images, fonts, and media downloads."""
        url_lower = route.request.url.lower()
        if any(url_lower.endswith(ext) or ext in url_lower for ext in BLOCKED_RESOURCE_EXTENSIONS):
            route.abort()
        else:
            route.continue_()

    def process_game(
        self,
        page: Page,
        game_info: dict[str, Any],
        *,
        dry_run: bool = True,
    ) -> tuple[bool, str]:
        """Crawl, extract, validate, and save a single historical game.

        Args:
            page: Playwright Page object.
            game_info: Dict with game_id and game_date.
            dry_run: If True, do not commit to database.

        Returns:
            Tuple of (success_boolean, status_message).

        """
        game_id = game_info["game_id"]
        game_date = game_info["game_date"]
        review_url = (
            f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
            f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
        )

        try:
            logger.info("📡 Navigating to Review Page for %s: %s", game_id, review_url)
            page.goto(review_url, wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(500)

            data = self.crawler.extract_game_details(
                page_or_html=page,
                game_id=game_id,
                game_date=game_date,
                db_session=self.session,
            )

            is_valid, error_msg = validate_boxscore_payload(data, strict=True)
            if not is_valid:
                logger.warning("⚠️ Validation failed for %s: %s", game_id, error_msg)
                return False, f"validation_failed: {error_msg}"

            if not dry_run:
                saved = save_game_detail(data, db_session=self.session)
                if saved:
                    logger.info("✅ Saved game detail for %s into DB", game_id)
                    return True, "saved"
                return False, "save_failed"

        except CRAWLER_PROCESS_EXCEPTIONS as e:
            logger.warning("❌ Failed to process game %s: %s", game_id, e)
            return False, f"error: {e}"
        else:
            logger.info("🔍 [DRY-RUN] Game %s validated successfully (not saved)", game_id)
            return True, "validated_dry_run"

    def run_backfill(
        self,
        start_year: int,
        end_year: int,
        limit_per_season: int | None = None,
        *,
        dry_run: bool = True,
        headless: bool = True,
    ) -> list[BackfillResult]:
        """Execute backfill loop across seasons."""
        results: list[BackfillResult] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            page.route("**/*", self._block_unnecessary_resources)

            for year in range(start_year, end_year + 1):
                logger.info("🚀 Starting historical backfill for Season %s (dry_run=%s)", year, dry_run)
                self.resolver.preload_season_index(year)

                missing_games = self.get_missing_games(year, limit=limit_per_season)
                total_missing = len(missing_games)
                logger.info("📊 Found %s missing games for %s", total_missing, year)

                metric = BackfillResult(
                    year=year,
                    total_missing=total_missing,
                    attempted=0,
                    saved=0,
                    skipped_validation=0,
                    failed=0,
                    dry_run=dry_run,
                )

                for game_info in missing_games:
                    metric.attempted += 1
                    ok, msg = self.process_game(page, game_info, dry_run=dry_run)

                    metric.details.append({"game_id": game_info["game_id"], "status": msg})
                    if ok:
                        metric.saved += 1
                    elif "validation_failed" in msg:
                        metric.skipped_validation += 1
                    else:
                        metric.failed += 1

                    if self.request_delay > 0:
                        time.sleep(self.request_delay)

                results.append(metric)
                logger.info("🏁 Finished Season %s: attempted=%s, saved=%s", year, metric.attempted, metric.saved)

            browser.close()

        return results
