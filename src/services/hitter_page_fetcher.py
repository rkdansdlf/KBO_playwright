"""
Fetch HITTER section HTML from KBO GameCenter and extract SH/SF values.

The HITTER section of KBO GameCenter pages includes dedicated columns
for 희타 (sacrifice hits) and 희비 (sacrifice flies) in the batter stat tables.
These columns are often omitted from the REVIEW section, resulting in
game_batting_stats having 0 for those fields.

NOTE: This only works for games where the KBO GameCenter page renders tables
server-side (2024+). For legacy games (2020-2023), the page uses JavaScript
to render all tables — Playwright is required to extract them.

This module provides both:
1. HTTP-based fetch + parse (for modern games)
2. Pure parse function that accepts rendered HTML (for Playwright-based usage)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from src.urls import GAME_CENTER
from src.utils.request_policy import RequestPolicy
from src.utils.type_helpers import to_int

logger = logging.getLogger(__name__)

_policy = RequestPolicy()

KBO_GAME_CENTER_URL = GAME_CENTER

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


def build_hitter_url(game_id: str, game_date: str) -> str:
    """Build the HITTER section URL for a KBO game.

    game_date should be in YYYY-MM-DD or YYYYMMDD format.
    """
    date_compact = game_date.replace("-", "")
    return f"{KBO_GAME_CENTER_URL}?gameDate={date_compact}&gameId={game_id}&section=HITTER"


def _get_column_index_map(table_tag: BeautifulSoup) -> dict[str, int]:
    """Map column header text to cell index for a stats table."""
    header_row = table_tag.select_one("thead tr")
    if not header_row:
        return {}
    headers = header_row.find_all("th")
    return {th.get_text(strip=True): i for i, th in enumerate(headers)}


def parse_hitter_sh_sf(html: str, game_id: str) -> dict[int | str, dict[str, int]]:
    """Parse SH/SF values from HITTER section HTML.

    Reads lineup tables (tblAwayHitter1 / tblHomeHitter1) for player IDs
    and stat tables (tblAwayHitter3 / tblHomeHitter3) for 희타/희비 columns.
    Matches by row index.

    Returns:
        dict mapping player_id (int) or player_name (str, fallback)
        -> {"sh": int, "sf": int}
    """
    soup = BeautifulSoup(html, "html.parser")
    result: dict[int | str, dict[str, int]] = {}

    for side_prefix in ("Away", "Home"):
        lineup_table = soup.select_one(f"#tbl{side_prefix}Hitter1")
        stats_table = soup.select_one(f"#tbl{side_prefix}Hitter3")

        if not lineup_table or not stats_table:
            logger.debug("HITTER tables not found for %s in %s", side_prefix, game_id)
            continue

        col_map = _get_column_index_map(stats_table)
        sh_idx = col_map.get("희타")
        sf_idx = col_map.get("희비")

        if sh_idx is None and sf_idx is None:
            logger.debug("No 희타/희비 columns in HITTER table for %s", game_id)
            continue

        lineup_rows = lineup_table.select("tbody tr")
        stats_rows = stats_table.select("tbody tr")

        if not lineup_rows or not stats_rows:
            continue

        for i in range(min(len(lineup_rows), len(stats_rows))):
            link = lineup_rows[i].select_one("a")
            if not link:
                continue

            href = link.get("href", "")
            player_id_match = re.search(r"playerId=(\d+)", href)

            if player_id_match:
                player_key: int | str = int(player_id_match.group(1))
            else:
                player_name = link.get_text(strip=True) or link.get("title", "")
                if not player_name:
                    continue
                player_key = player_name

            stat_cells = stats_rows[i].find_all("td")
            sh = (
                to_int(stat_cells[sh_idx].get_text(strip=True))
                if sh_idx is not None and sh_idx < len(stat_cells)
                else 0
            )
            sf = (
                to_int(stat_cells[sf_idx].get_text(strip=True))
                if sf_idx is not None and sf_idx < len(stat_cells)
                else 0
            )

            if sh > 0 or sf > 0:
                entry = result.setdefault(player_key, {"sh": 0, "sf": 0})
                entry["sh"] += sh
                entry["sf"] += sf

    return result


def fetch_hitter_page_sync(
    game_id: str,
    game_date: str,
    client: httpx.Client | None = None,
) -> str | None:
    """Fetch HITTER section HTML synchronously.

    Returns the raw HTML string, or None on failure.
    """
    url = build_hitter_url(game_id, game_date)
    try:
        if client is not None:
            response = client.get(url)
        else:
            with httpx.Client(headers=DEFAULT_HEADERS, follow_redirects=True, timeout=15.0) as cl:
                response = cl.get(url)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.warning("HITTER page %s returned %s", game_id, e.response.status_code)
        return None
    except httpx.TimeoutException:
        logger.warning("HITTER page %s timed out", game_id)
        return None
    except httpx.HTTPError as e:
        logger.warning("HITTER page %s fetch failed: %s", game_id, e)
        return None
    else:
        return response.text


def fetch_and_parse_hitter_sh_sf(
    game_id: str,
    game_date: str,
    client: httpx.Client | None = None,
) -> dict[int | str, dict[str, int]]:
    """Convenience: fetch HITTER page and parse SH/SF in one call."""
    html = fetch_hitter_page_sync(game_id, game_date, client=client)
    if not html:
        return {}
    return parse_hitter_sh_sf(html, game_id)


def derive_sh_sf_from_hitter_page(
    session: Session,
    game_id: str,
    game_date: str,
    client: httpx.Client | None = None,
) -> int:
    """Derive SH/SF from HITTER page and update game_batting_stats in-place.

    Returns number of updated rows.
    """
    hitter_sh_sf = fetch_and_parse_hitter_sh_sf(game_id, game_date, client=client)
    if not hitter_sh_sf:
        return 0

    from sqlalchemy import text

    updated = 0
    for key, counts in hitter_sh_sf.items():
        updates = []
        params: dict[str, Any] = {"game_id": game_id}
        if counts["sh"] > 0:
            updates.append("sacrifice_hits = :sh")
            params["sh"] = counts["sh"]
        if counts["sf"] > 0:
            updates.append("sacrifice_flies = :sf")
            params["sf"] = counts["sf"]
        if not updates:
            continue

        set_clause = ", ".join(updates)
        if isinstance(key, int):
            params["player_id"] = key
            where_clause = "player_id = :player_id"
        else:
            params["player_name"] = key
            where_clause = "player_name = :player_name"

        sql = text(f"""
            UPDATE game_batting_stats
            SET {set_clause}
            WHERE game_id = :game_id
              AND {where_clause}
        """)  # noqa: S608
        result = session.execute(sql, params)
        updated += result.rowcount or 0

    if updated:
        logger.info(
            "Derived SH/SF from HITTER page for game %s: %d rows updated",
            game_id,
            updated,
        )
    return updated


def derive_sh_sf_hybrid(
    session: Session,
    game_id: str,
    game_date: str,
    client: httpx.Client | None = None,
    pbp_delay: float = 0.0,
) -> int:
    """Hybrid derivation: try PBP first, fall back to HITTER page.

    Args:
        session: SQLAlchemy session
        game_id: KBO game ID
        game_date: Game date in YYYY-MM-DD or YYYYMMDD format
        client: Optional shared httpx.Client
        pbp_delay: Seconds to wait between PBP check and HITTER fetch

    Returns:
        Number of updated rows (0 if none).
    """
    from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats

    pbp_updated = apply_sh_sf_to_batting_stats(session, game_id)
    if pbp_updated > 0:
        return pbp_updated

    if pbp_delay > 0:
        _policy.delay()

    return derive_sh_sf_from_hitter_page(session, game_id, game_date, client=client)
