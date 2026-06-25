"""Team-level batting stats crawler."""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING, Any

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.db.engine import SessionLocal
from src.repositories.team_stats_repository import TeamSeasonBattingRepository
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_retry import LONG_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_mapping import get_team_mapping_for_year
from src.utils.team_stats_helpers import (
    _parse_one_team_row,
    get_cell_value,
    parse_numeric,
    parse_team_stats_html,
)

if TYPE_CHECKING:
    from bs4.element import Tag

logger = logging.getLogger(__name__)

TEAM_BATTING_CRAWL_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, OSError)
TEAM_BATTING_FALLBACK_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)

TEAM_BATTING_URLS = [
    "https://www.koreabaseball.com/Record/Team/Hitter/Basic.aspx",
    "https://www.koreabaseball.com/Record/Team/Hitter/BasicOld.aspx",
]

HEADER_MAP = {
    "팀": "team_name",
    "팀명": "team_name",
    "경기": "games",
    "g": "games",
    "타석": "plate_appearances",
    "pa": "plate_appearances",
    "타수": "at_bats",
    "ab": "at_bats",
    "득점": "runs",
    "r": "runs",
    "안타": "hits",
    "h": "hits",
    "2루타": "doubles",
    "2b": "doubles",
    "3루타": "triples",
    "3b": "triples",
    "홈런": "home_runs",
    "hr": "home_runs",
    "타점": "rbi",
    "rbi": "rbi",
    "도루": "stolen_bases",
    "sb": "stolen_bases",
    "도실": "caught_stealing",
    "cs": "caught_stealing",
    "볼넷": "walks",
    "bb": "walks",
    "삼진": "strikeouts",
    "so": "strikeouts",
    "타율": "avg",
    "avg": "avg",
    "출루율": "obp",
    "obp": "obp",
    "장타율": "slg",
    "slg": "slg",
    "ops": "ops",
}

FLOAT_FIELDS = {"avg", "obp", "slg", "ops"}
BATTING_FIELDS = {
    "games",
    "plate_appearances",
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "stolen_bases",
    "caught_stealing",
    "walks",
    "strikeouts",
    "avg",
    "obp",
    "slg",
    "ops",
}


class TeamBattingStatsCrawler:
    """Collects and persists team-level batting stats for a season."""

    def __init__(self, league: str = "REGULAR", policy: RequestPolicy | None = None) -> None:
        """Initializes a new instance."""
        self.league = league
        self.repo = TeamSeasonBattingRepository()
        self.policy = policy or RequestPolicy()

    def crawl(self, season: int, *, persist: bool = True, headless: bool = True) -> list[dict[str, Any]]:
        """Crawls crawl.

        Args:
            season: Season year.

        Returns:
            List of results.

        """
        team_mapping = get_team_mapping_for_year(season)
        stats = []
        try:
            stats = self._collect_from_site(season, team_mapping, headless=headless)
        except TEAM_BATTING_CRAWL_EXCEPTIONS as crawl_err:
            logger.warning("Team batting crawl failed: %s. Falling back...", crawl_err)

        if not stats:
            logger.warning("⚠️ KBO 팀 타격 페이지 오류. DB에서 폴백 집계를 시작합니다 (시즌: %s)...", season)
            try:
                with SessionLocal() as session:
                    aggregator = TeamStatAggregator(session)
                    from src.aggregators.team_stat_aggregator import TeamAggregationQuery

                    stats = aggregator.aggregate_batting(TeamAggregationQuery(season=season, dry_run=not persist))

                    # 팀명 보충 (aggregator는 코드만 가지고 있음)
                    reverse_mapping = {v: k for k, v in team_mapping.items()}
                    for s in stats:
                        s["team_name"] = reverse_mapping.get(s["team_id"], s["team_id"])

                    # 순위 데이터도 함께 재계산 (통합 폴백 로직)
                    logger.warning("⚠️ 팀 순위 데이터도 함께 재계산합니다 (시즌: %s)...", season)
                    try:
                        from src.cli.calculate_standings import StandingsCalculator

                        calc = StandingsCalculator(session)
                        calc.calculate_year(season)
                    except TEAM_BATTING_FALLBACK_EXCEPTIONS:
                        logger.exception("Standings calculation fallback error")
            except TEAM_BATTING_FALLBACK_EXCEPTIONS:
                logger.exception("[ERROR] 팀 타격 집계 폴백 실패")
                raise

        elif persist:
            self.repo.upsert_many(stats)
        return stats

    def _collect_from_site(
        self,
        season: int,
        team_mapping: dict[str, str],
        *,
        headless: bool,
    ) -> list[dict[str, Any]]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(**self.policy.build_context_kwargs(locale="ko-KR"))
            install_sync_resource_blocking(context)
            page = context.new_page()
            for url in TEAM_BATTING_URLS:
                try:
                    self.policy.delay()
                    self.policy.run_with_retry(page.goto, url, wait_until="networkidle", timeout=LONG_TIMEOUT)
                    self.policy.delay()
                    self._select_season(page, season)
                    self.policy.delay()
                    html = page.content()
                    stats = parse_team_batting_html(html, season, self.league, team_mapping)
                    if stats:
                        context.close()
                        browser.close()
                        return stats
                except (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError) as exc:
                    logger.warning("Failed to parse %s: %s", url, exc)
            context.close()
            browser.close()
        return []

    @staticmethod
    def _select_season(page: Page, season: int) -> bool:
        selectors = [
            "#cphContents_cphContents_cphContents_ddlSeason_ddlSeason",
            "#cphContents_cphContents_cphContents_ddlSeason",
            'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]',
        ]
        for selector in selectors:
            dropdown = page.query_selector(selector)
            if not dropdown:
                continue
            try:
                page.select_option(selector, str(season))
                page.wait_for_load_state("networkidle")
            except (PlaywrightError, PlaywrightTimeoutError):
                logger.warning("Failed to select season dropdown, trying next selector")
                continue
            else:
                return True
        return False


def parse_team_batting_html(
    html: str,
    season: int,
    league: str,
    team_mapping: dict[str, str],
) -> list[dict[str, Any]]:
    """Parses team batting html.

    Args:
        html: Html.
        season: Season year.
        league: League.
        team_mapping: Team Mapping.

    Returns:
        List of results.

    """
    return parse_team_stats_html(
        html,
        season,
        league,
        team_mapping,
        HEADER_MAP,
        BATTING_FIELDS,
        FLOAT_FIELDS,
    )


def _add_batting_values(payload: dict[str, Any], cells: list, indexes: dict[str, int]) -> dict[str, Any]:
    extras: dict[str, Any] = {}
    for header_key, idx in indexes.items():
        if header_key == "team_name":
            continue
        value_str = get_cell_value(cells, idx)
        if value_str is None:
            continue
        value = parse_numeric(value_str, as_float=header_key in FLOAT_FIELDS)
        if header_key in BATTING_FIELDS:
            payload[header_key] = value
        else:
            extras[header_key] = value
    return extras


def _parse_team_batting_row(
    row: Tag,
    indexes: dict[str, int],
    season: int,
    league: str,
    team_mapping: dict[str, str],
) -> dict[str, Any] | None:
    return _parse_one_team_row(row, indexes, season, league, team_mapping, BATTING_FIELDS, FLOAT_FIELDS, None)


def main() -> None:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Crawl team-level batting stats.")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g., 2023)")
    parser.add_argument("--league", type=str, default="REGULAR", help="League code (default: REGULAR)")
    parser.add_argument("--no-save", action="store_true", help="Only print stats without saving to DB")
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser with UI (default: headless)",
    )
    args = parser.parse_args()

    crawler = TeamBattingStatsCrawler(league=args.league)
    stats = crawler.crawl(args.season, persist=not args.no_save, headless=not args.headed)
    logger.info("Collected %s team batting rows for season %s", len(stats), args.season)


if __name__ == "__main__":
    main()
