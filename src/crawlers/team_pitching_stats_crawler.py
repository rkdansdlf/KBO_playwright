"""
Team-level pitching stats crawler.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import Tag
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.db.engine import SessionLocal
from src.repositories.team_stats_repository import TeamSeasonPitchingRepository
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_retry import LONG_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_mapping import get_team_mapping_for_year
from src.utils.team_stats_helpers import get_cell_value, parse_numeric, resolve_team_id
from src.utils.type_helpers import parse_innings

logger = logging.getLogger(__name__)

TEAM_PITCHING_URLS = [
    "https://www.koreabaseball.com/Record/Team/Pitcher/Basic.aspx",
    "https://www.koreabaseball.com/Record/Team/Pitcher/BasicOld.aspx",
]

HEADER_MAP = {
    "팀": "team_name",
    "팀명": "team_name",
    "경기": "games",
    "g": "games",
    "승": "wins",
    "w": "wins",
    "패": "losses",
    "l": "losses",
    "무": "ties",
    "세": "saves",
    "sv": "saves",
    "홀드": "holds",
    "hd": "holds",
    "이닝": "innings_pitched",
    "ip": "innings_pitched",
    "실점": "runs_allowed",
    "r": "runs_allowed",
    "자책": "earned_runs",
    "er": "earned_runs",
    "피안타": "hits_allowed",
    "h": "hits_allowed",
    "피홈런": "home_runs_allowed",
    "hr": "home_runs_allowed",
    "볼넷": "walks_allowed",
    "bb": "walks_allowed",
    "탈삼진": "strikeouts",
    "so": "strikeouts",
    "방어율": "era",
    "era": "era",
    "whip": "whip",
    "피안타율": "avg_against",
    "avg": "avg_against",
}

FLOAT_FIELDS = {"innings_pitched", "era", "whip", "avg_against"}
PITCHING_FIELDS = {
    "games",
    "wins",
    "losses",
    "ties",
    "saves",
    "holds",
    "innings_pitched",
    "runs_allowed",
    "earned_runs",
    "hits_allowed",
    "home_runs_allowed",
    "walks_allowed",
    "strikeouts",
    "era",
    "whip",
    "avg_against",
}
TEAM_PITCHING_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


class TeamPitchingStatsCrawler:
    """Collects and persists team-level pitching stats for a season."""

    def __init__(self, league: str = "REGULAR", policy: RequestPolicy | None = None) -> None:
        self.league = league
        self.repo = TeamSeasonPitchingRepository()
        self.policy = policy or RequestPolicy()

    def crawl(self, season: int, *, persist: bool = True, headless: bool = True) -> list[dict[str, Any]]:
        mapping = get_team_mapping_for_year(season)
        stats = []
        try:
            stats = self._collect_from_site(season, mapping, headless=headless)
        except (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError) as crawl_err:
            logger.warning("KBO team pitching crawl failed: %s. Falling back...", crawl_err)

        if not stats:
            logger.warning("⚠️ KBO 팀 투구 페이지 오류. DB에서 폴백 집계를 시작합니다 (시즌: %s)...", season)
            try:
                with SessionLocal() as session:
                    aggregator = TeamStatAggregator(session)
                    stats = aggregator.aggregate_pitching(season, dry_run=not persist)

                    # 팀명 보충
                    reverse_mapping = {v: k for k, v in mapping.items()}
                    for s in stats:
                        s["team_name"] = reverse_mapping.get(s["team_id"], s["team_id"])

                    # 순위 데이터도 함께 재계산 (통합 폴백 로직)
                    logger.warning("⚠️ 팀 순위 데이터도 함께 재계산합니다 (시즌: %s)...", season)
                    try:
                        from src.cli.calculate_standings import StandingsCalculator

                        calc = StandingsCalculator(session)
                        calc.calculate_year(season)
                    except TEAM_PITCHING_EXCEPTIONS:
                        logger.exception("[ERROR] 순위 연산 폴백 중 오류 발생")
            except TEAM_PITCHING_EXCEPTIONS:
                logger.exception("[ERROR] 팀 투구 집계 폴백 실패")
                raise

        elif persist:
            self.repo.upsert_many(stats)
        return stats

    def _collect_from_site(self, season: int, team_mapping: dict[str, str], *, headless: bool) -> list[dict[str, Any]]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(**self.policy.build_context_kwargs(locale="ko-KR"))
            install_sync_resource_blocking(context)
            page = context.new_page()
            for url in TEAM_PITCHING_URLS:
                try:
                    self.policy.delay()
                    self.policy.run_with_retry(page.goto, url, wait_until="networkidle", timeout=LONG_TIMEOUT)
                    self.policy.delay()
                    self._select_season(page, season)
                    self.policy.delay()
                    html = page.content()
                    stats = parse_team_pitching_html(html, season, self.league, team_mapping)
                    if stats:
                        context.close()
                        browser.close()
                        return stats
                except TEAM_PITCHING_EXCEPTIONS:
                    logger.exception("[WARN] Failed to parse %s", url)
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
                logger.warning("Failed to select option, trying next")
                continue
            else:
                return True
        return False


def parse_team_pitching_html(
    html: str,
    season: int,
    league: str,
    team_mapping: dict[str, str],
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.tData01") or soup.select_one("table")
    if not table:
        return []

    header_cells = table.select("thead tr th")
    if not header_cells:
        header_cells = table.select("tr th")
    headers = [cell.get_text(strip=True).lower() for cell in header_cells]
    indexes = _build_column_map(headers)
    if "team_name" not in indexes:
        return []

    rows = _extract_stat_rows(table)
    return [
        payload
        for row in rows
        if (payload := _parse_team_pitching_row(row, indexes, season, league, team_mapping)) is not None
    ]


def _extract_stat_rows(table: Tag) -> list[Tag]:
    rows = table.select("tbody tr")
    if rows:
        return rows
    return [row for row in table.select("tr") if row.find_all("td")]


def _parse_team_pitching_row(
    row: Tag,
    indexes: dict[str, int],
    season: int,
    league: str,
    team_mapping: dict[str, str],
) -> dict[str, Any] | None:
    cells = row.find_all("td")
    if len(cells) < len(indexes):
        return None
    team_name = get_cell_value(cells, indexes["team_name"])
    if not team_name:
        return None
    payload: dict[str, Any] = {
        "team_id": resolve_team_id(team_name, team_mapping) or team_name,
        "team_name": team_name,
        "season": season,
        "league": league,
    }
    extras = _add_pitching_values(payload, cells, indexes)
    if extras:
        payload["extra_stats"] = extras
    return payload


def _add_pitching_values(payload: dict[str, Any], cells: list[Tag], indexes: dict[str, int]) -> dict[str, Any]:
    extras: dict[str, Any] = {}
    for header_key, idx in indexes.items():
        if header_key == "team_name":
            continue
        value_str = get_cell_value(cells, idx)
        if value_str is None:
            continue
        parsed_value = _parse_pitching_value(header_key, value_str)
        if header_key in PITCHING_FIELDS:
            payload[header_key] = parsed_value
        else:
            extras[header_key] = parsed_value
    return extras


def _parse_pitching_value(header_key: str, value_str: str) -> int | float | str | None:
    if header_key == "innings_pitched":
        return parse_innings(value_str)
    return parse_numeric(value_str, header_key in FLOAT_FIELDS)


def _build_column_map(headers: list[str]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    for idx, raw in enumerate(headers):
        key = raw.strip().lower()
        normalized = HEADER_MAP.get(key)
        if normalized:
            indexes[normalized] = idx
    if "team_name" not in indexes:
        indexes["team_name"] = 1 if len(headers) > 1 else 0
    return indexes


def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl team-level pitching stats.")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g., 2023)")
    parser.add_argument("--league", type=str, default="REGULAR", help="League code (default: REGULAR)")
    parser.add_argument("--no-save", action="store_true", help="Only print stats without saving to DB")
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser with UI (default: headless)",
    )
    args = parser.parse_args()

    crawler = TeamPitchingStatsCrawler(league=args.league)
    stats = crawler.crawl(args.season, persist=not args.no_save, headless=not args.headed)
    logger.info("Collected %s team pitching rows for season %s", len(stats), args.season)


if __name__ == "__main__":
    main()
