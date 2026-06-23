from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

"""
Futures League Batting Stats Crawler
Fetches year-by-year Futures batting statistics from player profile pages.
"""

import asyncio
import re

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError

from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import LONG_TIMEOUT, SHORT_TIMEOUT
from src.utils.throttle import throttle
from src.utils.type_helpers import safe_float_or_none, safe_int_or_none

if TYPE_CHECKING:
    from bs4.element import Tag

logger = logging.getLogger(__name__)
FUTURES_KEYS = ["season", "AVG", "G", "AB", "R", "H", "2B", "3B", "HR", "RBI", "SB", "BB", "HBP", "SO", "SLG", "OBP"]

HEADER_MAP = {
    # Korean/English mixed → standardized keys
    "연도": "season",
    "년도": "season",
    "시즌": "season",
    "year": "season",
    "경기": "G",
    "g": "G",
    "타수": "AB",
    "ab": "AB",
    "득점": "R",
    "r": "R",
    "안타": "H",
    "h": "H",
    "2루타": "2B",
    "2b": "2B",
    "3루타": "3B",
    "3b": "3B",
    "홈런": "HR",
    "hr": "HR",
    "타점": "RBI",
    "rbi": "RBI",
    "도루": "SB",
    "sb": "SB",
    "볼넷": "BB",
    "bb": "BB",
    "사구": "HBP",
    "hbp": "HBP",
    "죽사구": "HBP",
    "삼진": "SO",
    "so": "SO",
    "타율": "AVG",
    "avg": "AVG",
    "장타율": "SLG",
    "slg": "SLG",
    "출루율": "OBP",
    "obp": "OBP",
}


def _norm_header(txt: str) -> str:
    """Normalize header text to standard key."""
    t = re.sub(r"\s+", "", txt).lower()
    return HEADER_MAP.get(t, txt.strip())


def _compute_missing(row: dict) -> dict[str, Any]:
    """Compute missing derived stats (SLG, OBP) if possible."""
    H = row.get("H")  # noqa: N806
    _2B = row.get("2B")  # noqa: N806
    _3B = row.get("3B")  # noqa: N806
    HR = row.get("HR")  # noqa: N806
    AB = row.get("AB")  # noqa: N806
    BB = row.get("BB")  # noqa: N806
    HBP = row.get("HBP")  # noqa: N806
    SF = row.get("SF")  # noqa: N806

    # Compute SLG if missing
    if ("SLG" not in row or row.get("SLG") is None) and None not in (H, _2B, _3B, HR, AB) and AB and AB > 0:
        _1B = H - sum(v or 0 for v in [_2B, _3B, HR])  # noqa: N806
        tb = (_1B or 0) + 2 * (_2B or 0) + 3 * (_3B or 0) + 4 * (HR or 0)
        row["SLG"] = round(tb / AB, 3)

    # Compute OBP if missing
    if "OBP" not in row or row.get("OBP") is None:
        denom = (AB or 0) + (BB or 0) + (HBP or 0) + (SF or 0)
        if denom > 0:
            row["OBP"] = round(((H or 0) + (BB or 0) + (HBP or 0)) / denom, 3)

    return row


def _parse_table(table: Tag) -> list[dict]:
    headers = _extract_table_headers(table)
    out = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells:
            continue
        if any(k in " ".join(cells).lower() for k in ["통산", "합계", "career", "total"]):
            continue
        row = _parse_batting_row(headers, cells)
        if row.get("season"):
            out.append(_compute_missing(row))
    return out


def _extract_table_headers(table: Tag) -> list[str]:
    headers = [_norm_header(th.get_text(strip=True)) for th in table.select("thead th, thead td")]
    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [_norm_header(cell.get_text(strip=True)) for cell in first_row.find_all(["th", "td"])]
    return headers


def _parse_batting_row(headers: list[str], cells: list[str]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for h, v in zip(headers, cells, strict=False):
        key = _norm_header(h)
        if key == "season":
            m = re.search(r"\d{4}", v)
            row["season"] = int(m.group()) if m else None
        elif key in ("AVG", "SLG", "OBP"):
            row[key] = safe_float_or_none(v)
        elif key in ("G", "AB", "R", "H", "2B", "3B", "HR", "RBI", "SB", "BB", "HBP", "SO", "SF"):
            row[key] = safe_int_or_none(v)
    return row


def _pick_futures_table(soup: BeautifulSoup) -> Tag | None:
    """
    Find the Futures stats table safely:
    1. Look for table near '퓨처스' label
    2. Fallback: find table with season, AVG, OBP, SLG headers
    """
    # Method 1: Find '퓨처스' label and get next table
    label = soup.find(
        lambda tag: tag.name in ["h2", "h3", "h4", "button", "a", "li", "span"] and "퓨처스" in tag.get_text(),
    )
    if label:
        nxt = label.find_next("table")
        if nxt:
            return nxt

    # Method 2: Header-based heuristic
    for t in soup.find_all("table"):
        headers = [_norm_header(th.get_text(strip=True)) for th in t.select("thead th, thead td")]
        if not headers:
            continue
        if {"season", "AVG", "OBP", "SLG"}.issubset(set(headers)):
            return t

    return None


async def fetch_and_parse_futures_batting(
    _player_id: str,
    profile_url: str,
    pool: AsyncPlaywrightPool | None = None,
) -> list[dict]:
    """
    Fetch Futures batting stats from player profile page.

    Args:
        player_id: KBO player ID (string)
        profile_url: Full URL to player profile page

    Returns:
        List of dicts, each representing one season's stats
    """
    active_pool = pool or AsyncPlaywrightPool(max_pages=1, context_kwargs={"locale": "ko-KR"})
    owns_pool = pool is None
    await active_pool.start()
    try:
        page = await active_pool.acquire()
        try:
            if not await compliance.is_allowed(profile_url):
                logger.info("[COMPLIANCE] Blocked futures batting: %s", profile_url)
                return []

            await throttle.wait()
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT)
            await throttle.wait()  # Wait for dynamic content using throttle instead of sleep

            # Try to click Futures tab if it exists
            try:
                futures_tab = await page.wait_for_selector('text="퓨처스"', timeout=SHORT_TIMEOUT)
                if futures_tab:
                    await futures_tab.click()
                    await throttle.wait()
            except PlaywrightError:
                logger.debug("Batting tab not found or already selected")

            html = await page.content()
        finally:
            await active_pool.release(page)
    finally:
        if owns_pool:
            await active_pool.close()

    soup = BeautifulSoup(html, "lxml")
    table = _pick_futures_table(soup)

    if not table:
        return []  # No Futures stats for this player

    rows = _parse_table(table)

    # Trim to only requested keys, filling missing with None
    trimmed = []
    for r in rows:
        item = {k: r.get(k) for k in FUTURES_KEYS}
        trimmed.append(item)

    return trimmed


async def main() -> None:
    """Test the Futures batting crawler."""
    # Test with player 51868
    player_id = "51868"
    # IMPORTANT: Use HitterTotal for year-by-year stats, not HitterDetail (single season)
    profile_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"

    rows = await fetch_and_parse_futures_batting(player_id, profile_url)
    logger.info("\nParsed %s Futures season records for player %s", len(rows), player_id)

    for row in rows:
        logger.info("  Season %s: AVG=%s, G=%s, H=%s", row.get("season"), row.get("AVG"), row.get("G"), row.get("H"))


if __name__ == "__main__":
    asyncio.run(main())
