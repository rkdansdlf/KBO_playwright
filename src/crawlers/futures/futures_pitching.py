from __future__ import annotations

import logging
from typing import Any

"""
Futures League Pitching Stats Crawler
Fetches year-by-year Futures pitching statistics from player profile pages.
"""

import re

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError

from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import LONG_TIMEOUT, SHORT_TIMEOUT
from src.utils.team_codes import resolve_kbo_legacy_team_code
from src.utils.throttle import throttle
from src.utils.type_helpers import (
    parse_innings_to_outs,
    safe_float_or_none,
    safe_int_or_none,
)

logger = logging.getLogger(__name__)
FUTURES_PITCHER_KEYS = [
    "season",
    "era",
    "games",
    "complete_games",
    "shutouts",
    "wins",
    "losses",
    "saves",
    "holds",
    "tbf",
    "innings_pitched",
    "innings_outs",
    "hits_allowed",
    "home_runs_allowed",
    "walks_allowed",
    "hit_batters",
    "strikeouts",
    "runs_allowed",
    "earned_runs",
    "team_code",
]

HEADER_MAP = {
    "연도": "season",
    "년도": "season",
    "시즌": "season",
    "year": "season",
    "팀명": "team_name",
    "팀": "team_name",
    "era": "era",
    "평균자책": "era",
    "평균자책점": "era",
    "경기": "games",
    "g": "games",
    "완투": "complete_games",
    "cg": "complete_games",
    "완봉": "shutouts",
    "sho": "shutouts",
    "승": "wins",
    "w": "wins",
    "패": "losses",
    "l": "losses",
    "세": "saves",
    "sv": "saves",
    "세이브": "saves",
    "홀드": "holds",
    "hld": "holds",
    "타자": "tbf",
    "tbf": "tbf",
    "이닝": "IP",
    "ip": "IP",
    "피안타": "hits_allowed",
    "h": "hits_allowed",
    "피홈런": "home_runs_allowed",
    "hr": "home_runs_allowed",
    "볼넷": "walks_allowed",
    "bb": "walks_allowed",
    "사구": "hit_batters",
    "hbp": "hit_batters",
    "삼진": "strikeouts",
    "so": "strikeouts",
    "실점": "runs_allowed",
    "r": "runs_allowed",
    "자책": "earned_runs",
    "er": "earned_runs",
}


def _norm_header(txt: str) -> str:
    """Normalize header text to standard key."""
    t = re.sub(r"\s+", "", txt).lower()
    return HEADER_MAP.get(t, txt.strip())



def _parse_table(table) -> list[dict]:
    """Parse a table element into list of season pitching records."""
    headers = [_norm_header(th.get_text(strip=True)) for th in table.select("thead th, thead td")]

    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [_norm_header(cell.get_text(strip=True)) for cell in first_row.find_all(["th", "td"])]

    out = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue

        # Skip total/summary rows
        if any(keyword in " ".join(cells).lower() for keyword in ["통산", "합계", "career", "total"]):
            continue

        row = {}
        for h, v in zip(headers, cells, strict=False):
            key = _norm_header(h)

            if key == "season":
                m = re.search(r"\d{4}", v)
                if not m:
                    row["season"] = None
                else:
                    row["season"] = int(m.group())
            elif key == "team_name":
                row["team_name"] = v
            elif key == "era":
                row["era"] = safe_float_or_none(v)
            elif key == "IP":
                row["IP"] = v
            elif key in (
                "games",
                "complete_games",
                "shutouts",
                "wins",
                "losses",
                "saves",
                "holds",
                "tbf",
                "hits_allowed",
                "home_runs_allowed",
                "walks_allowed",
                "hit_batters",
                "strikeouts",
                "runs_allowed",
                "earned_runs",
            ):
                row[key] = safe_int_or_none(v)

        # Skip rows without valid season
        season = row.get("season")
        if not season:
            continue

        # Parse innings and outs
        ip_str = row.get("IP")
        outs = parse_innings_to_outs(ip_str)
        row["innings_outs"] = outs
        row["innings_pitched"] = round(outs / 3.0, 3) if outs is not None else None

        # Resolve team code
        team_name = row.get("team_name")
        row["team_code"] = resolve_kbo_legacy_team_code(team_name, season_year=season)

        out.append(row)

    return out


def _pick_futures_pitching_table(soup: BeautifulSoup) -> Any | None:
    """Find the Futures pitching record table."""
    # Method 1: Find by ID
    tbl = soup.find("table", id="tblPitcherRecord")
    if tbl:
        return tbl

    # Method 2: Header-based heuristic
    for t in soup.find_all("table"):
        headers = [_norm_header(th.get_text(strip=True)) for th in t.select("thead th, thead td")]
        if not headers:
            continue
        if {"season", "era", "games", "wins", "losses"}.issubset(set(headers)):
            return t

    return None


async def fetch_and_parse_futures_pitching(
    player_id: str,
    profile_url: str,
    pool: AsyncPlaywrightPool | None = None,
) -> list[dict]:
    """
    Fetch Futures pitching stats from player profile page.
    """
    active_pool = pool or AsyncPlaywrightPool(max_pages=1, context_kwargs={"locale": "ko-KR"})
    owns_pool = pool is None
    await active_pool.start()
    try:
        page = await active_pool.acquire()
        try:
            if not await compliance.is_allowed(profile_url):
                logger.info("[COMPLIANCE] Blocked futures pitching: %s", profile_url)
                return []

            await throttle.wait()
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT)
            await throttle.wait()

            try:
                futures_tab = await page.wait_for_selector('text="퓨처스"', timeout=SHORT_TIMEOUT)
                if futures_tab:
                    await futures_tab.click()
                    await throttle.wait()
            except PlaywrightError:
                logger.debug("Pitching tab not found or already selected")

            html = await page.content()
        finally:
            await active_pool.release(page)
    finally:
        if owns_pool:
            await active_pool.close()

    soup = BeautifulSoup(html, "lxml")
    table = _pick_futures_pitching_table(soup)

    if not table:
        return []

    rows = _parse_table(table)

    # Trim to only requested keys, filling missing with None
    trimmed = []
    for r in rows:
        item = {k: r.get(k) for k in FUTURES_PITCHER_KEYS}
        trimmed.append(item)

    return trimmed
