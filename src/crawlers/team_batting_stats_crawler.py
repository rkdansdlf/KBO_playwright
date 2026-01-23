"""
Team-level batting stats crawler.
"""
from __future__ import annotations

import argparse
import time
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

from src.repositories.team_stats_repository import TeamSeasonBattingRepository
from src.utils.team_mapping import get_team_mapping_for_year
from src.utils.request_policy import RequestPolicy
from src.utils.playwright_blocking import install_sync_resource_blocking

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

    def __init__(self, league: str = "REGULAR", policy: RequestPolicy | None = None):
        self.league = league
        self.repo = TeamSeasonBattingRepository()
        self.policy = policy or RequestPolicy()

    def crawl(self, season: int, *, persist: bool = True, headless: bool = True) -> List[Dict[str, Any]]:
        team_mapping = get_team_mapping_for_year(season)
        stats = self._collect_from_site(season, team_mapping, headless=headless)
        if persist and stats:
            self.repo.upsert_many(stats)
        return stats

    def _collect_from_site(
        self,
        season: int,
        team_mapping: Dict[str, str],
        *,
        headless: bool,
    ) -> List[Dict[str, Any]]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(**self.policy.build_context_kwargs(locale="ko-KR"))
            install_sync_resource_blocking(context)
            page = context.new_page()
            for url in TEAM_BATTING_URLS:
                try:
                    self.policy.delay()
                    self.policy.run_with_retry(page.goto, url, wait_until="networkidle", timeout=60000)
                    time.sleep(0.5)
                    self._select_season(page, season)
                    time.sleep(0.5)
                    html = page.content()
                    stats = parse_team_batting_html(html, season, self.league, team_mapping)
                    if stats:
                        context.close()
                        browser.close()
                        return stats
                except Exception as exc:
                    print(f"[WARN] Failed to parse {url}: {exc}")
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
                return True
            except Exception:
                continue
        return False


def parse_team_batting_html(
    html: str,
    season: int,
    league: str,
    team_mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Parse batting stats from a Team batting HTML page."""
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

    rows = table.select("tbody tr")
    if not rows:
        rows = [row for row in table.select("tr") if row.find_all("td")]

    stats: List[Dict[str, Any]] = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < len(indexes):
            continue
        team_name = _get_cell_value(cells, indexes["team_name"])
        if not team_name:
            continue
        team_id = _resolve_team_id(team_name, team_mapping)
        payload: Dict[str, Any] = {
            "team_id": team_id or team_name,
            "team_name": team_name,
            "season": season,
            "league": league,
        }

        extras: Dict[str, Any] = {}
        for header_key, idx in indexes.items():
            if header_key == "team_name":
                continue
            value_str = _get_cell_value(cells, idx)
            if value_str is None:
                continue
            field_name = header_key
            value = _parse_numeric(value_str, header_key in FLOAT_FIELDS)
            if field_name in BATTING_FIELDS:
                payload[field_name] = value
            else:
                extras[field_name] = value

        if extras:
            payload["extra_stats"] = extras
        stats.append(payload)
    return stats


def _build_column_map(headers: List[str]) -> Dict[str, int]:
    indexes: Dict[str, int] = {}
    for idx, raw in enumerate(headers):
        key = raw.strip().lower()
        normalized = HEADER_MAP.get(key)
        if normalized:
            indexes[normalized] = idx
        elif key == "ops":
            indexes["ops"] = idx
    if "team_name" not in indexes:
        # Heuristic fallback: assume second column holds team name
        indexes["team_name"] = 1 if len(headers) > 1 else 0
    return indexes


def _get_cell_value(cells, index: int) -> Optional[str]:
    if index >= len(cells):
        return None
    return cells[index].get_text(strip=True)


def _resolve_team_id(team_name: str, team_mapping: Dict[str, str]) -> Optional[str]:
    key = team_name.strip()
    if key in team_mapping:
        return team_mapping[key]
    normalized = key.replace(" ", "")
    if normalized in team_mapping:
        return team_mapping[normalized]
    return None


def _parse_numeric(value: str, as_float: bool) -> Optional[float | int]:
    cleaned = value.replace(",", "").replace("%", "")
    if cleaned in ("", "-", "N/A"):
        return None
    try:
        return float(cleaned) if as_float else int(float(cleaned))
    except ValueError:
        try:
            return float(cleaned)
        except ValueError:
            return None


def main():
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
    print(f"Collected {len(stats)} team batting rows for season {args.season}")


if __name__ == "__main__":
    main()
