"""futures pitching 모듈."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

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
from src.utils.team_codes import TEAM_NAME_TO_CODE
from src.utils.throttle import throttle
from src.utils.type_helpers import (
    parse_innings_to_outs,
    safe_float_or_none,
    safe_int_or_none,
)

if TYPE_CHECKING:
    from bs4.element import Tag

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
    """Normalize header text to standard key.

    Args:
        txt: Txt.

    """
    t = re.sub(r"\s+", "", txt).lower()

    return HEADER_MAP.get(t, txt.strip())


def _parse_table(table: Tag) -> list[dict]:
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
        if any(k in " ".join(cells).lower() for k in ["통산", "합계", "career", "total"]):
            continue
        row = _parse_pitching_cell_row(headers, cells)
        if row.get("season"):
            ip_str = row.get("IP")
            row["innings_outs"] = parse_innings_to_outs(ip_str) if ip_str else 0
            out.append(row)
    return out


def _parse_pitching_cell_row(headers: list[str], cells: list[str]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h, v in zip(headers, cells, strict=False):
        key = _norm_header(h)
        if key == "season":
            m = re.search(r"\d{4}", v)
            row["season"] = int(m.group()) if m else None
        elif key == "team_name":
            row["team_name"] = v
            code = TEAM_NAME_TO_CODE.get(v.strip())
            if not code:
                code = TEAM_NAME_TO_CODE.get(v.strip().upper())
            row["team_code"] = code
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
    return row


def _pick_futures_pitching_table(soup: BeautifulSoup) -> Tag | None:
    """Find the Futures pitching record table.

    Args:
        soup: Soup.

    """
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
    _player_id: str,
    profile_url: str,
    pool: AsyncPlaywrightPool | None = None,
) -> list[dict]:
    """Fetch Futures pitching stats from player profile page.

    Args:
        _player_id: Player ID.
        profile_url: Profile URL.
        pool: Connection pool for async operations.

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
