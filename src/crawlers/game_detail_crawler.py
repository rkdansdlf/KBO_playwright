"""GameCenter box score crawler with structured outputs."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from src.constants import KST

logger = logging.getLogger(__name__)

import contextlib
from datetime import datetime

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.crawlers.selectors import GAME_DETAIL
from src.db.engine import SessionLocal
from src.urls import GAME_CENTER
from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT, SEL_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment
from src.utils.type_helpers import parse_innings_to_outs, safe_int_or_none

HITTER_HEADER_MAP = {
    "타석": "plate_appearances",
    "타수": "at_bats",
    "득점": "runs",
    "안타": "hits",
    "2루타": "doubles",
    "3루타": "triples",
    "홈런": "home_runs",
    "타점": "rbi",
    "볼넷": "walks",
    "고의4구": "intentional_walks",
    "사구": "hbp",
    "삼진": "strikeouts",
    "도루": "stolen_bases",
    "도실": "caught_stealing",
    "희타": "sacrifice_hits",
    "희비": "sacrifice_flies",
    "병살": "gdp",
    "타율": "avg",
    "출루율": "obp",
    "장타율": "slg",
    "OPS": "ops",
    "ISO": "iso",
    "BABIP": "babip",
}


PITCHER_HEADER_MAP = {
    "이닝": "innings",
    "타자": "batters_faced",
    "투구수": "pitches",
    "피안타": "hits_allowed",
    "실점": "runs_allowed",
    "자책": "earned_runs",
    "피홈런": "home_runs_allowed",
    "볼넷": "walks_allowed",
    "삼진": "strikeouts",
    "사구": "hit_batters",
    "폭투": "wild_pitches",
    "보크": "balks",
    "승": "wins",
    "패": "losses",
    "세": "saves",
    "홀드": "holds",
    "ERA": "era",
    "WHIP": "whip",
    "K/9": "k_per_nine",
    "BB/9": "bb_per_nine",
    "K/BB": "kbb",
}


HITTER_FLOAT_KEYS = {"avg", "obp", "slg", "ops", "iso", "babip"}
PITCHER_FLOAT_KEYS = {"era", "whip", "fip", "k_per_nine", "bb_per_nine", "kbb"}
DETAIL_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    OSError,
)

TEAM_CODE_TO_NAME: dict[str, str] = {
    "KIA": "KIA",
    "HT": "KIA",
    "OB": "두산",
    "DB": "두산",
    "SS": "삼성",
    "LT": "롯데",
    "LG": "LG",
    "KT": "KT",
    "NC": "NC",
    "KH": "키움",
    "WO": "키움",
    "HH": "한화",
    "SSG": "SSG",
    "SK": "SSG",
}


def _team_code_to_name(code: str) -> str:
    return TEAM_CODE_TO_NAME.get(code.upper(), code)


class PlayerIdResolver(Protocol):
    """PlayerIdResolver class."""

    def resolve_id(
        self,
        player_name: str,
        team_code: str,
        season_year: int,
        *,
        uniform_no: str | None = None,
        is_pitcher: bool = False,
    ) -> int | None:
        """Resolves player ID from name and team information."""
        ...


@dataclass
class BoxscoreCrawlContext:
    """BoxscoreCrawlContext class."""

    page: Page
    game_id: str | None = None
    game_date: str | None = None
    team_info: dict[str, dict[str, Any]] | None = None
    season_year: int | None = None
    roster_map: dict[str, list[dict[str, Any]]] | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class HitterPayloadContext:
    """HitterPayloadContext class."""

    row: dict[str, Any]
    idx: int
    player_name: str
    p_id: int | None
    uniform_no: str | None
    team_code: str | None
    team_side: str
    stats: dict[str, Any]
    extras: dict[str, Any]


@dataclass
class PitcherResolutionContext:
    """PitcherResolutionContext class."""

    row: dict[str, Any]
    rows: list[dict[str, Any]]
    idx: int
    player_name: str
    team_code: str | None
    season_year: int | None
    uniform_no: str | None


@dataclass
class PitcherPayloadContext:
    """PitcherPayloadContext class."""

    row: dict[str, Any]
    idx: int
    player_name: str
    p_id: int | None
    uniform_no: str | None
    team_code: str | None
    team_side: str


class GameDetailCrawler:
    """Crawl KBO GameCenter review pages and return structured box score data."""

    def __init__(
        self,
        request_delay: float | None = None,
        resolver: PlayerIdResolver | None = None,
        pool: AsyncPlaywrightPool | None = None,
    ) -> None:
        """Initializes a new instance."""
        self.base_url = GAME_CENTER
        self.policy = RequestPolicy.with_delay(request_delay)
        self.resolver = resolver
        self.pool = pool
        self._last_failure_reason: dict[str, str] = {}

    def get_last_failure_reason(self, game_id: str) -> str | None:
        """
        Gets last failure reason.

        Args:
            game_id: Game ID.

        Returns:
            The result of the operation.

        """
        return self._last_failure_reason.get(game_id)

    async def close(self) -> None:
        """Handles the close operation."""
        if self.pool:
            await self.pool.stop()
            self.pool = None

    def _section_url(self, game_id: str, game_date: str, section: str) -> str:
        """
        Handles the section url operation.

        Args:
            game_id: Game ID.
            game_date: Game Date.
            section: Section.

        Returns:
            String result.

        """
        return f"{self.base_url}?gameDate={game_date}&gameId={game_id}&section={section}"

    @staticmethod
    def _empty_metadata() -> dict[str, Any]:
        """
        Handles the empty metadata operation.

        Returns:
            Dictionary mapping.

        """
        return {
            "stadium": None,
            "attendance": None,
            "start_time": None,
            "end_time": None,
            "game_time": None,
            "duration_minutes": None,
        }

    @staticmethod
    def _parse_name_and_uniform(player_name: str, cells: dict[str, Any]) -> tuple[str, str | None]:
        """
        Parses name and uniform.

        Args:
            player_name: Player Name.
            cells: Cells.

        Returns:
            Tuple result.

        """
        uniform_no = cells.get("등번호")
        match = re.search(r"\(([^)]+)\)", player_name)
        if not match:
            return player_name, uniform_no

        suffix = match.group(1).strip()
        clean_name = re.sub(r"\s*\([^)]*\)\s*$", "", player_name).strip()
        if suffix.isdigit():
            uniform_no = suffix
        return clean_name, uniform_no

    @staticmethod
    def _resolve_from_roster_map(
        roster_map: dict[str, list[dict[str, Any]]] | None,
        player_name: str,
        uniform_no: str | None,
    ) -> tuple[int | None, str | None]:
        """
        Resolves from roster.

        Args:
            roster_map: Roster Map.
            player_name: Player Name.
            uniform_no: Uniform No.

        Returns:
            Tuple result.

        """
        if not roster_map or player_name not in roster_map:
            return None, uniform_no
        candidates = roster_map[player_name]
        if len(candidates) == 1:
            return candidates[0]["id"], uniform_no or candidates[0]["uniform"]
        if uniform_no:
            for candidate in candidates:
                if candidate["uniform"] == str(uniform_no):
                    return candidate["id"], uniform_no
        return None, uniform_no

    async def _navigate_section(
        self,
        ctx: BoxscoreCrawlContext,
        section: str,
        *,
        required_selector: str | None = None,
        selector_timeout: int = 15000,
    ) -> tuple[bool, str, str]:
        """
        Handles the navigate section operation.

        Args:
            ctx: Ctx.
            section: Section.

        Returns:
            Tuple result.

        """
        if ctx.game_id is None or ctx.game_date is None:
            msg = "ctx.game_id and ctx.game_date required"
            raise ValueError(msg)
        url = self._section_url(ctx.game_id, ctx.game_date, section)
        if not await compliance.is_allowed(url):
            logger.error("❌ BLOCKED by compliance policy: %s", url)
            return False, "blocked", url

        async def _navigate() -> None:
            """Handles the navigate operation."""
            await self.policy.delay_async(host="www.koreabaseball.com")
            await ctx.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            if required_selector:
                await ctx.page.wait_for_selector(required_selector, timeout=selector_timeout)

        try:
            await self.policy.run_with_retry_async(_navigate)
        except DETAIL_CRAWLER_EXCEPTIONS:
            logger.exception("❌ Failed to navigate %s for %s", section, ctx.game_id)
            return False, "navigation_error", url

        return True, "ok", url

    async def crawl_game(self, game_id: str, game_date: str, *, lightweight: bool = False) -> dict[str, Any] | None:
        """
        Crawls game.

        Args:
            game_id: Game ID.
            game_date: Game Date.

        Returns:
            The result of the operation.

        """
        game_id = normalize_kbo_game_id(game_id)
        self._last_failure_reason.pop(game_id, None)

        # Ensure resolver is available if not provided in __init__
        close_session = False
        if not self.resolver:
            from src.services.player_id_resolver import PlayerIdResolver

            session = SessionLocal()
            self.resolver = PlayerIdResolver(
                session,
                strict_game_resolution=True,
                allow_auto_register=False,
            )
            close_session = True

        try:
            result = await self.crawl_games([{"game_id": game_id, "game_date": game_date}], lightweight=lightweight)
            return result[0] if result else None
        finally:
            if close_session and hasattr(self.resolver, "session"):
                self.resolver.session.close()
                self.resolver = None

    async def crawl_games(
        self,
        games: list[dict[str, str]],
        concurrency: int | None = None,
        *,
        lightweight: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Crawls games.

        Args:
            games: Games.
            concurrency: Maximum number of concurrent requests.

        Returns:
            List of results.

        """
        if not games:
            return []

        max_concurrency = concurrency or int(os.getenv("KBO_GAME_DETAIL_CONCURRENCY", "3"))
        max_concurrency = max(1, min(max_concurrency, len(games)))

        pool = self.pool or AsyncPlaywrightPool(max_pages=max_concurrency)
        owns_pool = self.pool is None
        if self.pool:
            max_concurrency = min(max_concurrency, self.pool.max_pages)

        results: list[dict[str, Any] | None] = [None] * len(games)
        await pool.start()
        try:
            queue: asyncio.Queue[tuple[int, dict[str, str]] | None] = asyncio.Queue()
            for idx, entry in enumerate(games):
                normalized_entry = dict(entry)
                normalized_entry["game_id"] = normalize_kbo_game_id(entry["game_id"])
                queue.put_nowait((idx, normalized_entry))
            for _ in range(max_concurrency):
                queue.put_nowait(None)

            async def worker() -> None:
                """Handles the worker operation."""
                page = await pool.acquire()
                try:
                    while True:
                        item = await queue.get()
                        if item is None:
                            queue.task_done()
                            break
                        idx, entry = item
                        game_id = entry["game_id"]
                        game_date = entry["game_date"]
                        try:
                            payload = await self._crawl_single(page, game_id, game_date, lightweight=lightweight)
                            results[idx] = payload
                        except DETAIL_CRAWLER_EXCEPTIONS:  # pragma: no cover - resilience path
                            self._last_failure_reason[game_id] = "exception"
                            logger.exception("❌ Error crawling %s", game_id)
                        finally:
                            queue.task_done()
                finally:
                    await pool.release(page)

            workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
            await queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            if owns_pool:
                await pool.close()

        return [payload for payload in results if payload]

    async def _crawl_single(
        self,
        page: Page,
        game_id: str,
        game_date: str,
        *,
        lightweight: bool = False,
    ) -> dict[str, Any] | None:
        """
        Crawls single.

        Args:
            page: Playwright page object.
            game_id: Game ID.
            game_date: Game Date.

        Returns:
            The result of the operation.

        """
        review_url = self._section_url(game_id, game_date, "REVIEW")
        logger.info("📡 Navigating to REVIEW: %s", review_url)

        nav_ctx = BoxscoreCrawlContext(page=page, game_id=game_id, game_date=game_date)
        ok, reason, _ = await self._navigate_section(nav_ctx, "REVIEW")
        if not ok:
            self._last_failure_reason[game_id] = reason
            return None

        is_ready, failure_reason = await self._wait_for_boxscore(page, game_id=game_id, lightweight=lightweight)
        if not is_ready:
            if lightweight:
                logger.warning(
                    "⚠️ Boxscore presence check failed in lightweight mode for %s: %s. "
                    "Proceeding with partial extraction...",
                    game_id,
                    failure_reason,
                )
            else:
                self._last_failure_reason[game_id] = failure_reason
                return None

        roster_map = await self._load_roster_map_from_lineup(
            page, game_id, game_date, review_url, lightweight=lightweight
        )
        season_year = self._parse_season_year(game_date)
        team_info = await self._extract_team_info(page, game_id, season_year)
        metadata = await self._extract_metadata(page)

        # New: Extract Game Summary
        game_summary = await self._extract_game_summary(page)

        if lightweight:
            hitters = {"away": [], "home": []}
            pitchers = {"away": [], "home": []}
        else:
            detail_ctx = BoxscoreCrawlContext(
                page=page,
                game_id=game_id,
                game_date=game_date,
                team_info=team_info,
                metadata=metadata,
                season_year=season_year,
                roster_map=roster_map,
            )
            detailed_stats = await self._extract_detailed_stats(detail_ctx)
            if detailed_stats is None:
                return None
            hitters, pitchers = detailed_stats

        game_data = {
            "game_id": game_id,
            "game_date": game_date,
            "metadata": metadata,
            "summary": game_summary,  # Add to payload
            "teams": team_info,
            "home_team_code": team_info["home"]["code"],
            "away_team_code": team_info["away"]["code"],
            "hitters": hitters,
            "pitchers": pitchers,
        }

        self._last_failure_reason.pop(game_id, None)
        if not lightweight:
            self._log_unresolved_player_ids(game_id, hitters, pitchers)
        return game_data

    async def _click_review_tab_if_present(self, page: Page) -> None:
        """
        Handles the click review tab if present operation.

        Args:
            page: Playwright page object.

        """
        try:
            review_tab = await page.query_selector(GAME_DETAIL.review_tab)
            if review_tab:
                await review_tab.click()
                await asyncio.sleep(0.5)
        except PlaywrightError:
            logger.debug("Review tab not clickable for game")

    async def _extract_hitter_pair(
        self,
        page: Page,
        team_info: dict[str, dict[str, Any]],
        season_year: int | None,
        roster_map: dict[str, list[dict[str, Any]]] | None,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
        """
        Extracts hitter pair.

        Args:
            page: Playwright page object.
            team_info: Team Info.
            season_year: Season Year.
            roster_map: Roster Map.

        Returns:
            Tuple result.

        """
        ctx = BoxscoreCrawlContext(
            page=page,
            team_info=team_info,
            season_year=season_year,
            roster_map=roster_map,
        )
        away_hitters, away_total = await self._extract_hitters(ctx, "away", team_info["away"]["code"])
        home_hitters, home_total = await self._extract_hitters(ctx, "home", team_info["home"]["code"])
        return {"away": away_hitters, "home": home_hitters}, {"away": away_total, "home": home_total}

    async def _extract_pitcher_pair(
        self,
        page: Page,
        team_info: dict[str, dict[str, Any]],
        season_year: int | None,
        roster_map: dict[str, list[dict[str, Any]]] | None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Extracts pitcher pair.

        Args:
            page: Playwright page object.
            team_info: Team Info.
            season_year: Season Year.
            roster_map: Roster Map.

        Returns:
            Dictionary mapping.

        """
        ctx = BoxscoreCrawlContext(
            page=page,
            team_info=team_info,
            season_year=season_year,
            roster_map=roster_map,
        )
        return {
            "away": await self._extract_pitchers(ctx, "away", team_info["away"]["code"]),
            "home": await self._extract_pitchers(ctx, "home", team_info["home"]["code"]),
        }

    @staticmethod
    def _stats_complete(hitters: dict[str, list[dict[str, Any]]], pitchers: dict[str, list[dict[str, Any]]]) -> bool:
        """
        Handles the stats complete operation.

        Args:
            hitters: Hitters.
            pitchers: Pitchers.

        Returns:
            True if the condition is met, False otherwise.

        """
        return bool(hitters["away"]) and bool(hitters["home"]) and bool(pitchers["away"]) and bool(pitchers["home"])

    async def _retry_missing_boxscore_sections(
        self,
        ctx: BoxscoreCrawlContext,
        hitters: dict[str, list[dict[str, Any]]],
        hitter_totals: dict[str, dict[str, Any]],
        pitchers: dict[str, list[dict[str, Any]]],
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        """
        Handles the retry missing boxscore sections operation.

        Args:
            ctx: Ctx.
            hitters: Hitters.
            hitter_totals: Hitter Totals.
            pitchers: Pitchers.

        Returns:
            Tuple result.

        """
        game_id = ctx.game_id
        max_attempts = max(1, int(os.getenv("GAMEDETAIL_SECTION_FALLBACK_ATTEMPTS", "2")))
        for attempt in range(1, max_attempts + 1):
            if self._stats_complete(hitters, pitchers) or attempt >= max_attempts:
                break
            logger.warning(
                "⚠️  Incomplete stats on REVIEW for %s. Trying HITTER/PITCHER tabs (attempt %s/%s)...",
                game_id,
                attempt,
                max_attempts,
            )
            hitters, hitter_totals = await self._recover_hitter_section_if_missing(ctx, hitters, hitter_totals)
            pitchers = await self._recover_pitcher_section_if_missing(ctx, pitchers)
            await asyncio.sleep(0.4)
        return hitters, hitter_totals, pitchers

    async def _recover_hitter_section_if_missing(
        self,
        ctx: BoxscoreCrawlContext,
        hitters: dict[str, list[dict[str, Any]]],
        hitter_totals: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
        """
        Handles the recover hitter section if missing operation.

        Args:
            ctx: Ctx.
            hitters: Hitters.
            hitter_totals: Hitter Totals.

        Returns:
            Tuple result.

        """
        if bool(hitters["away"]) and bool(hitters["home"]):
            return hitters, hitter_totals
        ok, reason, _ = await self._navigate_section(
            ctx,
            "HITTER",
            required_selector=GAME_DETAIL.hitter_fallback,
            selector_timeout=SEL_TIMEOUT,
        )
        if ok:
            return await self._extract_hitter_pair(
                ctx.page,
                ctx.team_info,
                ctx.season_year,
                ctx.roster_map,
            )
        logger.warning("⚠️ HITTER section navigation failed for %s: %s", ctx.game_id, reason)
        return hitters, hitter_totals

    async def _recover_pitcher_section_if_missing(
        self,
        ctx: BoxscoreCrawlContext,
        pitchers: dict[str, list[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Handles the recover pitcher section if missing operation.

        Args:
            ctx: Ctx.
            pitchers: Pitchers.

        Returns:
            Dictionary mapping.

        """
        if bool(pitchers["away"]) and bool(pitchers["home"]):
            return pitchers
        ok, reason, _ = await self._navigate_section(
            ctx,
            "PITCHER",
            required_selector=GAME_DETAIL.pitcher_fallback,
            selector_timeout=SEL_TIMEOUT,
        )
        if ok:
            return await self._extract_pitcher_pair(
                ctx.page,
                ctx.team_info,
                ctx.season_year,
                ctx.roster_map,
            )
        logger.warning("⚠️ PITCHER section navigation failed for %s: %s", ctx.game_id, reason)
        return pitchers

    @staticmethod
    def _has_partial_recovery_anchor(team_info: dict[str, dict[str, Any]], metadata: dict[str, Any]) -> bool:
        """
        Handles the has partial recovery anchor operation.

        Args:
            team_info: Team Info.
            metadata: Metadata.

        Returns:
            True if the condition is met, False otherwise.

        """
        return bool(
            team_info.get("away", {}).get("line_score")
            or team_info.get("home", {}).get("line_score")
            or metadata.get("stadium")
            or metadata.get("attendance"),
        )

    async def _validate_hitter_totals(
        self,
        page: Page,
        game_id: str,
        hitters: dict[str, list[dict[str, Any]]],
        hitter_totals: dict[str, dict[str, Any]],
    ) -> None:
        """
        Validates hitter totals.

        Args:
            page: Playwright page object.
            game_id: Game ID.
            hitters: Hitters.
            hitter_totals: Hitter Totals.

        """
        for side, player_list in hitters.items():
            total_row = hitter_totals.get(side)
            if not total_row:
                continue
            sum_hits = sum(player["stats"].get("hits", 0) for player in player_list)
            sum_ab = sum(player["stats"].get("at_bats", 0) for player in player_list)
            if sum_hits == total_row.get("hits") and sum_ab == total_row.get("at_bats"):
                continue
            logger.warning(
                "⚠️ Integrity check FAILED for %s (%s): Players Sum(%sH, %sAB) != Team Total(%sH, %sAB)",
                game_id,
                side,
                sum_hits,
                sum_ab,
                total_row.get("hits"),
                total_row.get("at_bats"),
            )
            await page.screenshot(path=f"data/integrity_warning_{game_id}_{side}.png")

    async def _extract_detailed_stats(
        self,
        ctx: BoxscoreCrawlContext,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]] | None:
        """
        Extracts detailed stats.

        Args:
            ctx: Ctx.

        Returns:
            The result of the operation.

        """
        await self._click_review_tab_if_present(ctx.page)
        team_info = ctx.team_info
        hitters, hitter_totals = await self._extract_hitter_pair(ctx.page, team_info, ctx.season_year, ctx.roster_map)
        pitchers = await self._extract_pitcher_pair(ctx.page, team_info, ctx.season_year, ctx.roster_map)
        hitters, hitter_totals, pitchers = await self._retry_missing_boxscore_sections(
            ctx,
            hitters,
            hitter_totals,
            pitchers,
        )
        if not any((hitters["away"], hitters["home"], pitchers["away"], pitchers["home"])):
            if not self._has_partial_recovery_anchor(team_info, ctx.metadata):
                self._last_failure_reason[ctx.game_id] = "incomplete_detail"
                return None
            logger.info(
                "ℹ️  No box scores found for %s, but scoreboard/metadata available. Proceeding with partial recovery.",
                ctx.game_id,
            )
        await self._validate_hitter_totals(ctx.page, ctx.game_id, hitters, hitter_totals)
        return hitters, pitchers

    async def _is_cancelled_boxscore_page(self, page: Page) -> bool:
        """
        Handles the is cancelled boxscore page operation.

        Args:
            page: Playwright page object.

        Returns:
            True if the condition is met, False otherwise.

        """
        for selector in GAME_DETAIL.status_selectors:
            status_el = await page.query_selector(selector)
            if not status_el:
                continue
            try:
                txt = (await status_el.text_content() or "").strip()
            except PlaywrightError:
                continue
            if any(cancel_word in txt for cancel_word in ["경기취소", "취소", "우천취소"]):
                logger.info("ℹ️ Game %s is marked as CANCELLED in UI: '%s'", page.url, txt)
                return True
        try:
            content_area = await page.query_selector(GAME_DETAIL.content_boxscore_area)
            if content_area:
                txt = (await content_area.text_content() or "").strip()
                return "취소" in txt
        except PlaywrightError:
            logger.debug("Cancellation check after timeout failed")
        return False

    @staticmethod
    def _boxscore_timeout_debug_path(game_id: str, *, lightweight: bool) -> str:
        """
        Handles the boxscore timeout debug path operation.

        Args:
            game_id: Game ID.

        Returns:
            String result.

        """
        prefix = "lightweight_timeout" if lightweight else "timeout"
        return f"data/{prefix}_{game_id}_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}.png"

    async def _save_boxscore_timeout_screenshot(self, page: Page, debug_path: str) -> None:
        """
        Saves boxscore timeout screenshot.

        Args:
            page: Playwright page object.
            debug_path: Debug file path.

        """
        Path("data").mkdir(parents=True, exist_ok=True)
        try:
            await page.screenshot(path=debug_path)
            logger.warning("📸 Boxscore timeout debug screenshot saved to: %s", debug_path)
        except PlaywrightError:
            logger.exception("⚠️ Failed to save timeout debug screenshot for %s", page.url)

    async def _wait_for_boxscore(self, page: Page, *, game_id: str, lightweight: bool = False) -> tuple[bool, str]:
        """Wait for box score elements to be visible with fast-fail for cancelled games."""
        if await self._is_cancelled_boxscore_page(page):
            return False, "cancelled"

        selectors = list(GAME_DETAIL.boxscore_presence_selectors)
        if lightweight:
            selectors.extend(
                [
                    "#tblScordboard1",
                    "#tblScoreboard1",
                    "#tblScordboard2",
                    "#tblScoreboard2",
                ]
            )

        try:
            await page.wait_for_selector(", ".join(selectors), timeout=SEL_TIMEOUT)
        except PlaywrightError:
            logger.warning("⚠️ Timeout waiting for boxscore selectors. Page URL: %s", page.url)

            if await self._is_cancelled_boxscore_page(page):
                return False, "cancelled"

            debug_path = self._boxscore_timeout_debug_path(game_id, lightweight=lightweight)
            await self._save_boxscore_timeout_screenshot(page, debug_path)
            return False, "timeout"
        else:
            return True, "ok"

    @staticmethod
    def _parse_metadata_info_text(metadata: dict[str, Any], text: str) -> None:
        """
        Parses metadata info text.

        Args:
            metadata: Metadata.
            text: Input text content.

        """
        stadium_match = re.search(r"구장\s*[:：]\s*([^\s]+)", text)
        if stadium_match:
            metadata["stadium"] = stadium_match.group(1).strip()

        attendance_match = re.search(r"관중\s*[:：]\s*([\d,]+)", text)
        if attendance_match:
            with contextlib.suppress(ValueError):
                metadata["attendance"] = int(attendance_match.group(1).replace(",", "").strip())

        for key, pattern in (
            ("start_time", r"개시\s*[:：]\s*([\d:]+)"),
            ("end_time", r"종료\s*[:：]\s*([\d:]+)"),
            ("game_time", r"경기시간\s*[:：]\s*([\d:]+)"),
        ):
            match = re.search(pattern, text)
            if match:
                metadata[key] = match.group(1).strip()

    async def _extract_metadata(self, page: Page) -> dict[str, Any]:
        """
        Extracts metadata.

        Args:
            page: Playwright page object.

        Returns:
            Dictionary mapping.

        """
        metadata = self._empty_metadata()

        try:
            # 1. Try explicit ID selectors (common in older years)
            stadium_el = await page.query_selector(GAME_DETAIL.stadium)
            if stadium_el:
                metadata["stadium"] = (await stadium_el.text_content()).replace("구장 :", "").strip()

            crowd_el = await page.query_selector(GAME_DETAIL.crowd)
            if crowd_el:
                try:
                    val = (await crowd_el.text_content()).replace("관중 :", "").replace(",", "").strip()
                    metadata["attendance"] = int(val)
                except (ValueError, TypeError):
                    logger.debug("Failed to parse attendance value from %s", GAME_DETAIL.crowd)

            # 2. Try generic area search
            info_area = await page.query_selector(GAME_DETAIL.info_area)
            if not info_area:
                return metadata

            text = (await info_area.text_content()).replace("\n", " ")

            self._parse_metadata_info_text(metadata, text)
            if metadata["game_time"]:
                metadata["duration_minutes"] = self._parse_duration_minutes(metadata["game_time"])

        except DETAIL_CRAWLER_EXCEPTIONS:  # pragma: no cover - resilience path
            logger.exception("⚠️  Error extracting metadata")

        return metadata

    async def _extract_live_scores(self, page: Page) -> dict[str, dict[str, Any]] | None:
        try:
            data = await page.evaluate("""
            (function() {
                var selected = document.querySelector('li.game-cont.on');
                if (!selected) return null;
                var info = selected.querySelector('.info');
                if (!info) return null;
                var teams = info.querySelectorAll('[class*="team"]');
                if (teams.length < 2) return null;
                var away = teams[0];
                var home = teams[1];
                var awayScore = away.querySelector('.score');
                var homeScore = home.querySelector('.score');
                var awayName = away.querySelector('img');
                var homeName = home.querySelector('img');
                return {
                    away: {
                        code: awayName ? (awayName.alt || '').toUpperCase() : '',
                        name: awayName ? (awayName.alt || '') : '',
                        score: awayScore ? parseInt(awayScore.innerText.trim(), 10) : null,
                    },
                    home: {
                        code: homeName ? (homeName.alt || '').toUpperCase() : '',
                        name: homeName ? (homeName.alt || '') : '',
                        score: homeScore ? parseInt(homeScore.innerText.trim(), 10) : null,
                    }
                };
            })()
            """)
            if not data or not data.get("away") or not data.get("home"):
                return None
            if data["away"]["score"] is None and data["home"]["score"] is None:
                return None
            return {
                "away": {
                    "code": data["away"]["code"],
                    "name": data["away"]["name"],
                    "score": data["away"]["score"],
                    "hits": None,
                    "errors": None,
                    "line_score": [],
                },
                "home": {
                    "code": data["home"]["code"],
                    "name": data["home"]["name"],
                    "score": data["home"]["score"],
                    "hits": None,
                    "errors": None,
                    "line_score": [],
                },
            }
        except (PlaywrightError, ValueError, TypeError):
            return None

    async def _fetch_scoreboard_inning_scores(
        self,
        page: Page,
        game_id: str,
        away_code: str,
        home_code: str,
    ) -> dict[str, dict[str, Any]] | None:
        scoreboard_url = f"{GAME_CENTER}?gameDate={game_id[:8]}".replace(
            "/Schedule/GameCenter/Main.aspx", "/Schedule/ScoreBoard.aspx"
        )
        review_url = f"{GAME_CENTER}?gameDate={game_id[:8]}&gameId={game_id}&section=REVIEW"

        try:
            await page.goto(scoreboard_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            await asyncio.sleep(1.0)

            tables = await page.query_selector_all("table.tScore")
            if not tables:
                return None

            away_name = _team_code_to_name(away_code)
            home_name = _team_code_to_name(home_code)

            for table in tables:
                rows = await table.query_selector_all("tbody tr")
                if len(rows) < 2:
                    continue

                first_row_cells = await rows[0].query_selector_all("th, td")
                first_cell_text = ""
                if first_row_cells:
                    first_cell_text = (await first_row_cells[0].text_content() or "").strip()

                if first_cell_text not in (away_name, home_name):
                    continue

                team_data = {}
                for side, row in zip(("away", "home"), rows, strict=True):
                    parsed = await self._parse_scoreboard_row_cells(row, side, away_code, home_code)
                    if parsed:
                        team_data[side] = parsed

                if "away" in team_data and "home" in team_data:
                    return team_data

            return None

        finally:
            await page.goto(review_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

    @staticmethod
    async def _parse_scoreboard_row_cells(
        row: Page, side: str, away_code: str, home_code: str
    ) -> dict[str, Any] | None:
        cells = await row.query_selector_all("th, td")
        cell_values = []
        for c in cells:
            val = (await c.text_content() or "").strip()
            cell_values.append(val)

        if len(cell_values) < 4:
            return None

        line_score = []
        for v in cell_values[1:13]:
            if v == "-" or v == "":
                line_score.append(None)
            else:
                try:
                    line_score.append(int(v))
                except (ValueError, TypeError):
                    line_score.append(None)

        r_val = None
        h_val = None
        e_val = None
        for i in range(len(cell_values) - 1, 0, -1):
            val = cell_values[i]
            if val == "-" or val == "":
                continue
            try:
                int(val)
            except (ValueError, TypeError):
                continue
            if e_val is None:
                e_val = int(val)
            elif h_val is None:
                h_val = int(val)
            elif r_val is None:
                r_val = int(val)
                break

        return {
            "code": away_code if side == "away" else home_code,
            "name": cell_values[0],
            "score": r_val,
            "hits": h_val,
            "errors": e_val,
            "line_score": line_score,
        }

    async def _extract_team_info(self, page: Page, game_id: str, season_year: int | None) -> dict[str, dict[str, Any]]:
        """
        Extracts team info.

        Args:
            page: Playwright page object.
            game_id: Game ID.
            season_year: Season Year.

        Returns:
            Dictionary mapping.

        """
        script = r"""
        () => {
            const getRows = (t, extractTh) => Array.from(t.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll(extractTh ? 'td, th' : 'td')).map(td => {
                    const img = td.querySelector('img');
                    if (img && img.alt) return img.alt;
                    if (img && img.src && img.src.includes('team')) {
                        const match = img.src.match(/\/([A-Z]{2})\.png/i);
                        if (match) return match[1];
                    }
                    const clone = td.cloneNode(true);
                    Array.from(clone.querySelectorAll('span, em, strong, p')).forEach(el => {
                        const txt = (el.textContent || '').trim();
                        if (['승', '패', '무', '세'].includes(txt)) {
                            el.remove();
                        }
                    });
                    let text = (clone.textContent || '').replace(/\u00a0/g, ' ').trim();
                    text = text.replace(/^[승패무세]\s*/, '').replace(/\s*[승패무세]$/, '').trim();
                    text = text.replace(/[\d]+승\s*[\d]+패\s*[\d]+무/, '').trim();
                    return text;
                }).filter(text => text !== '')
            );

            let teamTable = document.getElementById('tblScordboard1') || document.getElementById('tblScoreboard1');
            let inningTable = document.getElementById('tblScordboard2') || document.getElementById('tblScoreboard2');
            let totalTable = document.getElementById('tblScordboard3') || document.getElementById('tblScoreboard3');

            if (teamTable && inningTable && totalTable) {
                const teamRows = getRows(teamTable, true);
                const inningRows = getRows(inningTable, false);
                const totalRows = getRows(totalTable, false);

                if (teamRows.length >= 2 && inningRows.length >= 2 && totalRows.length >= 2) {
                    const headers = ["TEAM", ...Array.from({length: inningRows[0].length}, (_,k)=>String(k+1)), "R", "H", "E"];
                    const rows = [];
                    for (let i=0; i<2; i++) {
                        const teamName = teamRows[i][0] || "Unknown";
                        const innings = inningRows[i];
                        const totals = totalRows[i].slice(0, 3);
                        rows.push([teamName, ...innings, ...totals]);
                    }
                    return { headers, rows };
                }
            }

            const tables = Array.from(document.querySelectorAll('table'));
            teamTable = null; inningTable = null; totalTable = null;

            for (const table of tables) {
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => (th.textContent || '').replace(/\u00a0/g, ' ').trim().toUpperCase());

                if (!teamTable && (headers.some(h => h.includes('TEAM')) || headers.includes('팀') || headers.includes(' '))) {
                    if (headers.length <= 4) teamTable = table;
                }
                if (!inningTable && headers.includes('1') && headers.includes('2') && headers.includes('3')) inningTable = table;
                if (!totalTable && headers.includes('R') && headers.includes('H')) totalTable = table;
            }

            if (!teamTable || !inningTable || !totalTable) return null;

            const getRowsFallback = (t) => Array.from(t.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => {
                    const img = td.querySelector('img');
                    if (img && img.alt) return img.alt;
                    if (img && img.src && img.src.includes('team')) {
                        const match = img.src.match(/\/([A-Z]{2})\.png/i);
                        if (match) return match[1];
                    }
                    const clone = td.cloneNode(true);
                    Array.from(clone.querySelectorAll('span, em, strong, p')).forEach(el => {
                        const txt = (el.textContent || '').trim();
                        if (['승', '패', '무', '세'].includes(txt)) {
                            el.remove();
                        }
                    });
                    let text = (clone.textContent || '').replace(/\u00a0/g, ' ').trim();
                    // Keep the record part if it looks like "X승 Y패 Z무" to avoid total wipe
                    if (text.includes('승') && text.includes('패')) {
                         // Don't treat this as the team name if possible
                         return "";
                    }
                    text = text.replace(/^[승패무세]\s*/, '').replace(/\s*[승패무세]$/, '').trim();
                    return text;
                })
            );

            const teamRows = getRowsFallback(teamTable);
            const inningRows = getRowsFallback(inningTable);
            const totalRows = getRowsFallback(totalTable);

            if (teamRows.length >= 2 && inningRows.length >= 2 && totalRows.length >= 2) {
                const headers = ["TEAM", ...Array.from({length: inningRows[0].length}, (_,k)=>String(k+1)), "R", "H", "E"];
                const rows = [];
                for (let i=0; i<2; i++) {
                    let teamName = teamRows[i][0] || "";
                    if (teamName === "TEAM") teamName = ""; // Skip header re-read
                    const innings = inningRows[i];
                    const totals = totalRows[i].slice(0, 3);
                    rows.push([teamName, ...innings, ...totals]);
                }
                return { headers, rows };
            }
            return null;
        }
        """

        result = await page.evaluate(script)
        away_info: dict[str, Any]
        home_info: dict[str, Any]

        if result and len(result["rows"]) >= 2:
            headers = result["headers"]
            rows = result["rows"]
            away_info = self._parse_scoreboard_row(headers, rows[0], season_year)
            home_info = self._parse_scoreboard_row(headers, rows[1], season_year)
        else:
            away_info = home_info = None

        if not away_info and not home_info:
            live_scores = await self._extract_live_scores(page)
            if live_scores:
                away_code = live_scores["away"]["code"] or game_id[8:10]
                home_code = live_scores["home"]["code"] or game_id[10:12]
                sb_scores = await self._fetch_scoreboard_inning_scores(page, game_id, away_code, home_code)
                if sb_scores:
                    away_info = sb_scores["away"]
                    home_info = sb_scores["home"]
                else:
                    away_info = live_scores["away"]
                    home_info = live_scores["home"]

        # Fallback to gameId decoding for missing/generic team info (common in All-Star)
        away_segment = game_id[8:10] if len(game_id) >= 10 else None
        home_segment = game_id[10:12] if len(game_id) >= 12 else None

        if not away_info or not away_info.get("code") or away_info.get("name") in (None, "", "Unknown"):
            away_info = {
                "name": away_info.get("name") if away_info else away_segment,
                "code": team_code_from_game_id_segment(away_segment, season_year),
                "score": away_info.get("score") if away_info else None,
                "hits": away_info.get("hits") if away_info else None,
                "errors": away_info.get("errors") if away_info else None,
                "line_score": away_info.get("line_score") if away_info else [],
            }
        if not home_info or not home_info.get("code") or home_info.get("name") in (None, "", "Unknown"):
            home_info = {
                "name": home_info.get("name") if home_info else home_segment,
                "code": team_code_from_game_id_segment(home_segment, season_year),
                "score": home_info.get("score") if home_info else None,
                "hits": home_info.get("hits") if home_info else None,
                "errors": home_info.get("errors") if home_info else None,
                "line_score": home_info.get("line_score") if home_info else [],
            }

        return {"away": away_info, "home": home_info}

    def _resolve_hitter_id(
        self,
        player_name: str,
        team_code: str | None,
        season_year: int | None,
        uniform_no: str | None,
    ) -> int | None:
        """
        Resolves hitter id.

        Args:
            player_name: Player Name.
            team_code: Team code identifier.
            season_year: Season Year.
            uniform_no: Uniform No.

        Returns:
            The result of the operation.

        """
        if not (self.resolver and team_code and season_year):
            return None
        p_id = self.resolver.resolve_id(
            player_name,
            team_code,
            season_year,
            uniform_no=uniform_no,
            is_pitcher=False,
        )
        if p_id:
            logger.info("   [RESOLVED] %s (%s) -> %s", player_name, team_code, p_id)
        return p_id

    @staticmethod
    def _select_hitter_extra_row(
        *,
        extra_has_names: bool,
        extra_map: dict[str, dict[str, Any]],
        extra_rows: list[dict[str, Any]],
        player_name: str,
        idx: int,
    ) -> dict[str, Any] | None:
        """
        Handles the select hitter extra row operation.

        Returns:
            The result of the operation.

        """
        if extra_has_names:
            return extra_map.get(player_name)
        base_idx = idx - 1
        return extra_rows[base_idx] if base_idx < len(extra_rows) else None

    @staticmethod
    def _apply_hitter_inning_derivatives(stats: dict[str, Any], inning_rows: list[dict[str, Any]], idx: int) -> None:
        """
        Handles the apply hitter inning derivatives operation.

        Args:
            stats: Statistics data.
            inning_rows: Inning Rows.
            idx: Idx.

        """
        if not inning_rows or idx - 1 >= len(inning_rows):
            return
        derived = GameDetailCrawler._derive_hitter_stats_from_inning_cells(inning_rows[idx - 1]["cells"])
        for key, value in derived.items():
            if stats.get(key) in (0, None):
                stats[key] = value

    @staticmethod
    def _backfill_hitter_plate_appearances(stats: dict[str, Any]) -> None:
        """
        Backfills hitter plate appearances.

        Args:
            stats: Statistics data.

        """
        if stats.get("plate_appearances") not in (0, None):
            return
        stats["plate_appearances"] = (
            (stats.get("at_bats") or 0)
            + (stats.get("walks") or 0)
            + (stats.get("hbp") or 0)
            + (stats.get("sacrifice_hits") or 0)
            + (stats.get("sacrifice_flies") or 0)
        )

    @staticmethod
    def _build_hitter_payload(
        ctx: HitterPayloadContext,
    ) -> dict[str, Any]:
        """
        Builds hitter payload.

        Args:
            ctx: Ctx.

        Returns:
            Dictionary mapping.

        """
        batting_order = GameDetailCrawler._parse_batting_order(ctx.row["cells"])
        position = GameDetailCrawler._parse_position(ctx.row["cells"])
        return {
            "player_id": ctx.p_id,
            "player_name": ctx.player_name,
            "uniform_no": ctx.uniform_no,
            "team_code": ctx.team_code,
            "team_side": ctx.team_side,
            "batting_order": batting_order,
            "position": position,
            "is_starter": batting_order is not None and batting_order <= 9,
            "appearance_seq": ctx.idx,
            "stats": ctx.stats,
            "extras": ctx.extras or None,
        }

    async def _extract_hitters(
        self,
        ctx: BoxscoreCrawlContext,
        team_side: str,
        team_code: str | None,
    ) -> list[dict[str, Any]]:
        """
        Extracts hitters.

        Args:
            ctx: Ctx.
            team_side: Team Side.
            team_code: Team code identifier.

        Returns:
            List of results.

        """
        season_year = ctx.season_year
        roster_map = ctx.roster_map
        page = ctx.page
        selectors = (
            [GAME_DETAIL.away_hitter_primary, GAME_DETAIL.away_hitter_extra]
            if team_side == "away"
            else [GAME_DETAIL.home_hitter_primary, GAME_DETAIL.home_hitter_extra]
        )
        tables = []
        for selector in selectors:
            table_rows = await self._extract_table_rows(page, selector)
            if table_rows:
                tables.append(table_rows)

        base_rows = tables[0] if tables else []
        inning_rows = await self._extract_table_rows(
            page,
            GAME_DETAIL.away_hitter_inning if team_side == "away" else GAME_DETAIL.home_hitter_inning,
        )
        extra_rows = tables[1] if len(tables) > 1 else []

        extra_has_names = any(r.get("playerName") for r in extra_rows)

        extra_map = {}
        if extra_has_names:
            extra_map = {row["playerName"]: row for row in extra_rows if row["playerName"]}

        results: list[dict[str, Any]] = []
        team_total_stats = {}

        for idx, row in enumerate(base_rows, start=1):
            player_name = row["playerName"]
            if not player_name:
                continue

            if player_name in {"합계", "팀합계"}:
                self._populate_hitter_stats(team_total_stats, {}, row["cells"])
                continue

            player_name, uniform_no = self._parse_name_and_uniform(player_name, row.get("cells", {}))

            p_id = safe_int_or_none(row.get("playerId"))

            stats = {}
            extras = {}
            self._populate_hitter_stats(stats, extras, row["cells"])

            self._apply_hitter_inning_derivatives(stats, inning_rows, idx)

            if p_id is None:
                p_id = self._resolve_hitter_id(player_name, team_code, season_year, uniform_no)

            extra_row = self._select_hitter_extra_row(
                extra_has_names=extra_has_names,
                extra_map=extra_map,
                extra_rows=extra_rows,
                player_name=player_name,
                idx=idx,
            )

            if extra_row:
                self._populate_hitter_stats(stats, extras, extra_row["cells"])

            self._backfill_hitter_plate_appearances(stats)

            if not p_id:
                p_id, uniform_no = self._resolve_from_roster_map(roster_map, player_name, uniform_no)

            payload = self._build_hitter_payload(
                HitterPayloadContext(
                    row=row,
                    idx=idx,
                    player_name=player_name,
                    p_id=p_id,
                    uniform_no=uniform_no,
                    team_code=team_code,
                    team_side=team_side,
                    stats=stats,
                    extras=extras,
                ),
            )
            results.append(payload)

        return results, team_total_stats

    @staticmethod
    def _resolve_hanwha_park_junyoung(row: dict[str, Any], rows: list[dict[str, Any]], idx: int) -> int:
        """
        Resolves hanwha park junyoung.

        Args:
            row: Row.
            rows: Rows.
            idx: Idx.

        Returns:
            Integer result.

        """
        matching_rows = [candidate for candidate in rows if candidate.get("playerName") == "박준영"]
        if len(matching_rows) > 1:
            try:
                rel_idx = [candidate["playerName"] for candidate in rows].index("박준영")
                return 52731 if idx == rel_idx + 1 else 56709
            except ValueError:
                return 56709 if idx > 4 else 52731

        era_val = 5.0
        try:
            era_str = row["cells"].get("평균자책점") or row["cells"].get("ERA")
            if era_str:
                era_val = float(era_str)
        except ValueError:
            pass
        return 56709 if era_val < 3.0 else 52731

    def _resolve_pitcher_from_resolver(
        self,
        ctx: PitcherResolutionContext,
    ) -> int | None:
        """
        Resolves pitcher from resolver.

        Args:
            ctx: Ctx.

        Returns:
            The result of the operation.

        """
        if not (self.resolver and ctx.team_code and ctx.season_year):
            return None
        if ctx.player_name == "박준영" and ctx.team_code == "HH" and ctx.season_year == 2026:
            return self._resolve_hanwha_park_junyoung(ctx.row, ctx.rows, ctx.idx)
        return self.resolver.resolve_id(
            ctx.player_name,
            ctx.team_code,
            ctx.season_year,
            uniform_no=ctx.uniform_no,
            is_pitcher=True,
        )

    async def _search_and_register_pitcher(self, player_name: str, team_code: str | None) -> int | None:
        """
        Searches for and register pitcher.

        Args:
            player_name: Player Name.
            team_code: Team code identifier.

        Returns:
            The result of the operation.

        """
        if not self.resolver:
            return None
        can_register_from_search = not getattr(self.resolver, "strict_game_resolution", False) and getattr(
            self.resolver,
            "allow_auto_register",
            True,
        )
        if not can_register_from_search:
            return None

        logger.info("🔍 Unknown player '%s' (%s) found. Searching KBO...", player_name, team_code)
        from src.crawlers.player_search_crawler import PlayerSearchCrawler

        search_crawler = PlayerSearchCrawler()
        new_profiles = await search_crawler.search_player(player_name)
        for profile in new_profiles:
            if profile.get("name") != player_name:
                continue
            p_id = int(profile["player_id"])
            from src.repositories.player_basic_repository import save_player_basic

            save_player_basic(profile)
            logger.info("✅ Registered new player: %s (%s)", player_name, p_id)
            return p_id
        return None

    async def _resolve_pitcher_id(
        self,
        ctx: PitcherResolutionContext,
    ) -> int | None:
        """
        Resolves pitcher id.

        Args:
            ctx: Ctx.

        Returns:
            The result of the operation.

        """
        p_id = self._resolve_pitcher_from_resolver(ctx)
        if p_id is None:
            p_id = await self._search_and_register_pitcher(ctx.player_name, ctx.team_code)
        if p_id:
            logger.info("   [RESOLVED] %s (%s) -> %s", ctx.player_name, ctx.team_code, p_id)
        return p_id

    @staticmethod
    def _build_pitcher_payload(
        ctx: PitcherPayloadContext,
    ) -> dict[str, Any]:
        """
        Builds pitcher payload.

        Args:
            ctx: Ctx.

        Returns:
            Dictionary mapping.

        """
        stats = {}
        extras = {}
        GameDetailCrawler._populate_pitcher_stats(stats, extras, ctx.row["cells"])

        innings_text = ctx.row["cells"].get("이닝") or ctx.row["cells"].get("IP")
        innings_outs = parse_innings_to_outs(innings_text)

        result_text = ctx.row["cells"].get("결과") or ctx.row["cells"].get("결")
        decision = GameDetailCrawler._parse_decision(result_text)
        if decision:
            stats["decision"] = decision

        return {
            "player_id": ctx.p_id,
            "player_name": ctx.player_name,
            "uniform_no": ctx.uniform_no,
            "team_code": ctx.team_code,
            "team_side": ctx.team_side,
            "is_starting": ctx.idx == 1,
            "appearance_seq": ctx.idx,
            "stats": {**stats, "innings_outs": innings_outs},
            "extras": extras or None,
        }

    async def _extract_pitchers(
        self,
        ctx: BoxscoreCrawlContext,
        team_side: str,
        team_code: str | None,
    ) -> list[dict[str, Any]]:
        """
        Extracts pitchers.

        Args:
            ctx: Ctx.
            team_side: Team Side.
            team_code: Team code identifier.

        Returns:
            List of results.

        """
        page = ctx.page
        season_year = ctx.season_year
        roster_map = ctx.roster_map
        selectors = (
            [
                GAME_DETAIL.away_pitcher_primary,
                GAME_DETAIL.away_pitcher_alt,
                GAME_DETAIL.away_pitcher_alt2,
            ]
            if team_side == "away"
            else [
                GAME_DETAIL.home_pitcher_primary,
                GAME_DETAIL.home_pitcher_alt,
                GAME_DETAIL.home_pitcher_alt2,
            ]
        )
        rows = []
        for selector in selectors:
            table_rows = await self._extract_table_rows(page, selector)
            if table_rows:
                rows = table_rows
                break

        results: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            player_name = row["playerName"]
            if not player_name or player_name in {"합계", "팀합계"}:
                continue

            player_name, uniform_no = self._parse_name_and_uniform(player_name, row.get("cells", {}))

            p_id = safe_int_or_none(row.get("playerId"))

            if p_id is None:
                p_id = await self._resolve_pitcher_id(
                    PitcherResolutionContext(
                        row=row,
                        rows=rows,
                        idx=idx,
                        player_name=player_name,
                        team_code=team_code,
                        season_year=season_year,
                        uniform_no=uniform_no,
                    ),
                )

            if not p_id:
                p_id, uniform_no = self._resolve_from_roster_map(roster_map, player_name, uniform_no)

            payload = self._build_pitcher_payload(
                PitcherPayloadContext(
                    row=row,
                    idx=idx,
                    player_name=player_name,
                    p_id=p_id,
                    uniform_no=uniform_no,
                    team_code=team_code,
                    team_side=team_side,
                ),
            )
            results.append(payload)

        return results

    async def _extract_table_rows(self, page: Page, selector: str) -> list[dict[str, Any]]:
        """
        Extracts table rows.

        Args:
            page: Playwright page object.
            selector: Selector.

        Returns:
            List of results.

        """
        if not selector:
            return []

        script = r"""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];
            const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());

            // Find '선수명' index
            let nameIndex = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i] === '선수명') {
                    nameIndex = i;
                    break;
                }
            }

            return Array.from(table.querySelectorAll('tbody tr')).map((tr, index) => {
                const cells = Array.from(tr.querySelectorAll('th,td'));
                const values = {};
                for (let i = 0; i < cells.length; i++) {
                    const header = headers[i] || `COL_${i}`;
                    values[header] = cells[i].textContent.trim();
                }
                const link = tr.querySelector('a[href*="playerId="], a[href*="p_id="], a[href*="pCode="], a[href*="pcode="], a[href*="PlayerDetail"]');
                let playerId = null;
                let playerName = null;
                let uniformNo = null;

                // Try to find uniform number in the first cell or a cell with specific header
                if (cells.length > 0) {
                    const firstVal = cells[0].textContent.trim();
                    if (/^\d+$/.test(firstVal)) {
                        uniformNo = firstVal;
                    }
                }

                if (link) {
                    playerName = link.textContent.trim();
                    const href = link.getAttribute('href');
                    try {
                        const url = new URL(href, window.location.origin);
                        playerId = url.searchParams.get('playerId') ||
                                   url.searchParams.get('p_id') ||
                                   url.searchParams.get('pCode') ||
                                   url.searchParams.get('pcode');
                    } catch (e) {
                        playerId = null;
                    }
                    if (!playerId && href) {
                        const m = href.match(/(?:playerId|p_id|pCode|pcode|id)=(\d+)/i);
                        playerId = m ? m[1] : null;
                    }
                }

                // Fallback: Use name column if link not found
                if (!playerName && nameIndex !== -1 && cells.length > nameIndex) {
                    playerName = cells[nameIndex].textContent.trim();
                }

                return { index, cells: values, playerId, playerName, uniformNo };
            });
        }
        """

        return await page.evaluate(script, selector)

    async def _extract_game_summary(self, page: Page) -> list[dict[str, str]]:
        """Extracts game summary details from #tblEtc (Winning hit, HR, Errors, Umpires, etc.)."""
        selector = GAME_DETAIL.etc_table
        if not await page.query_selector(selector):
            return []

        script = r"""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];

            const results = [];
            const rows = table.querySelectorAll('tbody tr');

            rows.forEach(tr => {
                const th = tr.querySelector('th');
                const td = tr.querySelector('td');
                if (th && td) {
                    const category = (th.textContent || '').trim();
                    const content = (td.textContent || '').trim();
                    if (content && content !== '없음') {
                         results.push({
                             'summary_type': category,
                             'detail_text': content
                         });
                    }
                }
            });
            return results;
        }
        """
        return await page.evaluate(script, selector)

    async def _load_roster_map_from_lineup(
        self,
        page: Page,
        game_id: str,
        game_date: str,
        review_url: str,
        *,
        lightweight: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Loads roster from lineup.

        Args:
            page: Playwright page object.
            game_id: Game ID.
            game_date: Game Date.
            review_url: Review URL.
            lightweight: Lightweight mode.

        Returns:
            Dictionary mapping.

        """
        roster_map: dict[str, list[dict[str, Any]]] = {}
        for section in ("ENTRY", "LINEUP"):
            lineup_url = f"{self.base_url}?gameDate={game_date}&gameId={game_id}&section={section}"

            async def _navigate_lineup(_url: str = lineup_url) -> None:
                """Handles the navigate lineup operation."""
                await self.policy.delay_async()
                await page.goto(_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                with contextlib.suppress(PlaywrightError, TimeoutError):
                    await page.wait_for_selector(
                        GAME_DETAIL.lineup_link,
                        timeout=SEL_TIMEOUT,
                    )

            try:
                await self.policy.run_with_retry_async(_navigate_lineup)
                roster_map = await self._extract_roster_from_lineup(page)
                if roster_map:
                    break
            except DETAIL_CRAWLER_EXCEPTIONS:
                logger.exception("⚠️  Failed lineup roster crawl for %s (%s)", game_id, section)

        if lightweight:
            return roster_map

        # Always return to REVIEW page for box score extraction.
        try:

            async def _navigate_back() -> None:
                """Handles the navigate back operation."""
                await self.policy.delay_async()
                await page.goto(review_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

            await self.policy.run_with_retry_async(_navigate_back)
            await self._wait_for_boxscore(page, game_id=game_id)
        except DETAIL_CRAWLER_EXCEPTIONS:
            logger.exception("⚠️  Failed to return to review page for %s", game_id)
        return roster_map

    @staticmethod
    def _log_unresolved_player_ids(
        game_id: str,
        hitters: dict[str, list[dict[str, Any]]],
        pitchers: dict[str, list[dict[str, Any]]],
    ) -> None:
        """
        Logs unresolved player ids.

        Args:
            game_id: Game ID.
            hitters: Hitters.
            pitchers: Pitchers.

        """
        unresolved = []
        for team_side in ("away", "home"):
            unresolved.extend(
                (row.get("player_name"), row.get("team_code"), row.get("uniform_no"))
                for row in hitters.get(team_side, [])
                if row.get("player_name") and not row.get("player_id")
            )
            unresolved.extend(
                (row.get("player_name"), row.get("team_code"), row.get("uniform_no"))
                for row in pitchers.get(team_side, [])
                if row.get("player_name") and not row.get("player_id")
            )
        if not unresolved:
            return
        logger.warning("⚠️  Unresolved player_id entries for %s: %s", game_id, len(unresolved))
        for name, team_code, uniform_no in unresolved:
            logger.info("   - name=%s, team_code=%s, uniform_no=%s", name, team_code or "N/A", uniform_no or "N/A")

    @staticmethod
    def _derive_hitter_stats_from_inning_cells(cells: dict[str, str]) -> dict[str, int]:
        """Counts stats from inning breakdown cells (e.g. '삼진', '4구')."""
        derived = {"strikeouts": 0, "walks": 0, "hbp": 0, "sacrifice_hits": 0, "sacrifice_flies": 0}
        for val in cells.values():
            if not val or val == "&nbsp;":
                continue
            if "삼진" in val or "스낫" in val or "루낫" in val or "낫아웃" in val:
                derived["strikeouts"] += 1
            if "4구" in val or "볼넷" in val or "고의4구" in val:
                derived["walks"] += 1
            if "사구" in val or "몸에 맞는 볼" in val:
                derived["hbp"] += 1
            if "희번" in val or "희생번트" in val:
                derived["sacrifice_hits"] += 1
            if "희비" in val or "희생플라이" in val:
                derived["sacrifice_flies"] += 1
        return derived

    @staticmethod
    def _populate_hitter_stats(stats: dict[str, Any], extras: dict[str, Any], cells: dict[str, str]) -> None:
        """
        Handles the populate hitter stats operation.

        Args:
            stats: Statistics data.
            extras: Extras.
            cells: Cells.

        """
        for header, value in cells.items():
            key = HITTER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ("", "-", None):
                continue
            if key in HITTER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            else:
                stats[key] = safe_int_or_none(value)

    @staticmethod
    def _populate_pitcher_stats(stats: dict[str, Any], extras: dict[str, Any], cells: dict[str, str]) -> None:
        """
        Handles the populate pitcher stats operation.

        Args:
            stats: Statistics data.
            extras: Extras.
            cells: Cells.

        """
        for header, value in cells.items():
            key = PITCHER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ("", "-", None):
                continue
            if key in PITCHER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            elif key == "innings":
                stats["innings_outs"] = parse_innings_to_outs(value)
            else:
                stats[key] = safe_int_or_none(value)

    def _parse_scoreboard_row(
        self,
        headers: list[str],
        row: list[str],
        season_year: int | None = None,
    ) -> dict[str, Any]:
        """
        Parses scoreboard row.

        Args:
            headers: Headers.
            row: Row.
            season_year: Season Year.

        Returns:
            Dictionary mapping.

        """
        if not row:
            return {
                "name": None,
                "code": None,
                "line_score": [],
                "score": None,
                "hits": None,
                "errors": None,
            }

        name = row[0]
        if name:
            name = name.replace("승", "").replace("패", "").replace("무", "").replace("세", "").strip()

        # Validate row structure against headers: last 3 should be R/H/E
        if headers and len(headers) >= 4 and len(headers) == len(row):
            _last3 = [h.upper() for h in headers[-3:]]
            if _last3[0] not in ("R", "RUN", "득점", "R/H/E"):
                logger.debug("Unexpected scoreboard header before R: %s", headers[-3])
            if _last3[1] not in ("H", "HIT", "안타"):
                logger.debug("Unexpected scoreboard header before H: %s", headers[-2])
            if _last3[2] not in ("E", "ERR", "실책", "E/H"):
                logger.debug("Unexpected scoreboard header before E: %s", headers[-1])

        line = row[1:-3] if len(row) > 4 else []
        totals = row[-3:] if len(row) >= 3 else []

        score = safe_int_or_none(totals[0]) if totals else None
        hits = safe_int_or_none(totals[1]) if len(totals) > 1 else None
        errors = safe_int_or_none(totals[2]) if len(totals) > 2 else None

        line_numeric = [safe_int_or_none(item) for item in line]

        return {
            "name": name,
            "code": resolve_team_code(name, season_year),
            "line_score": line_numeric,
            "score": score,
            "hits": hits,
            "errors": errors,
        }

    @staticmethod
    def _parse_batting_order(cells: dict[str, str]) -> int | None:
        """
        Parses batting order.

        Args:
            cells: Cells.

        Returns:
            The result of the operation.

        """
        for key in ("타순", "NO", "No", "순", "타순(교체)", "COL_0"):
            if key in cells:
                value = re.search(r"\d+", cells[key])
                if value:
                    return int(value.group())
        return None

    @staticmethod
    def _parse_position(cells: dict[str, str]) -> str | None:
        """
        Parses position.

        Args:
            cells: Cells.

        Returns:
            The result of the operation.

        """
        for key in ("POS", "포지션", "수비위치", "COL_1"):
            if key in cells:
                return cells[key] or None
        return None

    @staticmethod
    def _parse_decision(text: str | None) -> str | None:
        """
        Parses decision.

        Args:
            text: Input text content.

        Returns:
            The result of the operation.

        """
        if not text:
            return None
        text = text.strip()
        result = None
        if "승" in text:
            result = "W"
        elif "패" in text:
            result = "L"
        elif "세" in text:
            result = "S"
        elif "홀드" in text or "H" in text:
            result = "H"
        if result not in ("W", "L", "S", "H"):
            return None
        return result

    @staticmethod
    def _parse_duration_minutes(duration: str | None) -> int | None:
        """
        Parses duration minutes.

        Args:
            duration: Duration.

        Returns:
            The result of the operation.

        """
        if not duration:
            return None
        parts = duration.strip().split(":")
        if len(parts) != 2:
            return None
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except ValueError:
            return None

    @staticmethod
    def _parse_season_year(game_date: str) -> int | None:
        """
        Parses season year.

        Args:
            game_date: Game Date.

        Returns:
            The result of the operation.

        """
        digits = "".join(ch for ch in str(game_date) if ch.isdigit())
        if len(digits) >= 4:
            try:
                return int(digits[:4])
            except ValueError:
                return None
        return None

    async def _extract_roster_from_lineup(self, page: Page) -> dict[str, list[dict[str, Any]]]:
        """
        Extracts a map of {PlayerName: [{id, uniform_no}, ...]} from the LINEUP page.

        Used to resolve player IDs when the Review page boxscore lacks links (legacy games).
        """
        script = r"""
        () => {
            const map = {};
            const addToMap = (name, id, uniform) => {
                const cleanName = name.trim();
                if (!cleanName) return;

                if (!map[cleanName]) {
                    map[cleanName] = [];
                }

                // Avoid duplicates
                const exists = map[cleanName].some(p => p.id === id);
                if (!exists) {
                    map[cleanName].push({id, uniform});
                }
            };

            // Find all anchor tags that look like player links
            const links = document.querySelectorAll('a[href*="PlayerDetail"], a[href*="playerId="], a[href*="p_id="], a[href*="pCode="], a[href*="pcode="]');

            links.forEach(a => {
                const name = a.textContent.trim();
                const href = a.getAttribute('href');
                if (!href) return;

                const idMatch = href.match(/(?:playerId|p_id|pCode|pcode|id)=(\d+)/i);
                if (name && idMatch) {
                    let uniform = null;

                    // strategy 1: Check nearby lists or text for "No.XX"
                    const parentLi = a.closest('li');
                    if (parentLi) {
                        const text = parentLi.textContent;
                        const uniMatch = text.match(/No\.(\d+)/);
                        if (uniMatch) uniform = uniMatch[1];
                    }

                    // strategy 2: Check previous sibling or parent structure (table columns)
                    // (Simplification: just take what we found)

                    addToMap(name, idMatch[1], uniform);
                }
            });
            return map;
        }
        """
        try:
            return await page.evaluate(script)
        except PlaywrightError as e:
            logger.warning("Error executing roster extraction script: %s", e, exc_info=True)
            return {}


async def main() -> None:  # pragma: no cover
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--game_id", help="KBO Game ID (e.g., 20251013SKSS0)")
    parser.add_argument("--date", help="Game Date (YYYYMMDD)")
    parser.add_argument("--save", action="store_true", help="Save to local database")
    args = parser.parse_args()

    if not args.game_id:
        logger.info("Usage: python3 -m src.crawlers.game_detail_crawler --game_id <ID> [--date <YYYYMMDD>] [--save]")
        return

    game_id = normalize_kbo_game_id(args.game_id)
    game_date = args.date or game_id[:8]

    logger.info("🚀 Starting crawl for game %s (%s)...", game_id, game_date)
    crawler = GameDetailCrawler()
    game_data = await crawler.crawl_game(game_id, game_date)
    if game_data and args.save:
        logger.info(
            "Direct --save is intended for one-off parser checks. "
            "Operational collection should use src.cli.collect_games or src.cli.run_daily_update.",
        )
        from src.repositories.game_repository import save_game_detail

        success = save_game_detail(game_data)
        if success:
            logger.info("✅ Successfully saved and triggered sync for %s", game_id)
        else:
            logger.error("❌ Failed to save %s", game_id)
    elif not game_data:
        logger.error("❌ Failed to crawl %s", game_id)
    else:
        logger.info(game_data)


if __name__ == "__main__":  # pragma: no cover
    import asyncio

    asyncio.run(main())
