"""
Team-level pitching stats crawler.
"""
from __future__ import annotations

import argparse
import time
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

from src.repositories.team_stats_repository import TeamSeasonPitchingRepository
from src.utils.team_mapping import get_team_mapping_for_year
from src.utils.request_policy import RequestPolicy
from src.utils.playwright_blocking import install_sync_resource_blocking

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


class TeamPitchingStatsCrawler:
    """Collects and persists team-level pitching stats for a season."""

    def __init__(self, league: str = "REGULAR", policy: RequestPolicy | None = None):
        self.league = league
        self.repo = TeamSeasonPitchingRepository()
        self.policy = policy or RequestPolicy()

    def crawl(self, season: int, *, persist: bool = True, headless: bool = True) -> List[Dict[str, Any]]:
        mapping = get_team_mapping_for_year(season)
        stats = self._collect_from_site(season, mapping, headless=headless)
        if persist and stats:
            self.repo.upsert_many(stats)
        return stats

    def _collect_from_site(self, season: int, team_mapping: Dict[str, str], *, headless: bool) -> List[Dict[str, Any]]:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(**self.policy.build_context_kwargs(locale="ko-KR"))
            install_sync_resource_blocking(context)
            page = context.new_page()
            for url in TEAM_PITCHING_URLS:
                try:
                    self.policy.delay()
                    self.policy.run_with_retry(page.goto, url, wait_until="networkidle", timeout=60000)
                    time.sleep(0.5)
                    self._select_season(page, season)
                    time.sleep(0.5)
                    html = page.content()
                    stats = parse_team_pitching_html(html, season, self.league, team_mapping)
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


def parse_team_pitching_html(
    html: str,
    season: int,
    league: str,
    team_mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
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
            parsed_value = (
                _parse_innings(value_str)
                if header_key == "innings_pitched"
                else _parse_numeric(value_str, header_key in FLOAT_FIELDS)
            )
            if header_key in PITCHING_FIELDS:
                payload[header_key] = parsed_value
            else:
                extras[header_key] = parsed_value

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
    if "team_name" not in indexes:
        indexes["team_name"] = 1 if len(headers) > 1 else 0
    return indexes


def _get_cell_value(cells, index: int) -> Optional[str]:
    if index >= len(cells):
        return None
    return cells[index].get_text(strip=True)


def _resolve_team_id(team_name: str, mapping: Dict[str, str]) -> Optional[str]:
    key = team_name.strip()
    if key in mapping:
        return mapping[key]
    normalized = key.replace(" ", "")
    if normalized in mapping:
        return mapping[normalized]
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


def _parse_innings(value: str) -> Optional[float]:
    cleaned = value.replace(",", "")
    if cleaned in ("", "-", "N/A"):
        return None
    if "." in cleaned:
        whole, frac = cleaned.split(".", 1)
        try:
            base = int(whole)
        except ValueError:
            return None
        if frac == "1":
            return base + (1.0 / 3.0)
        if frac == "2":
            return base + (2.0 / 3.0)
        return float(cleaned)
    try:
        return float(cleaned)
    except ValueError:
        return None


def main():
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
    print(f"Collected {len(stats)} team pitching rows for season {args.season}")


if __name__ == "__main__":
    main()
