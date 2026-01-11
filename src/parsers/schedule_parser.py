"""
Parse offline schedule HTML into structured game schedule rows.
"""
from __future__ import annotations

import re
from io import StringIO
from typing import Dict, List, Optional, Any

import pandas as pd
from bs4 import BeautifulSoup

from src.utils.team_codes import team_code_from_game_id_segment


LINK_PATTERN = re.compile(r"gameId=([0-9A-Z]+)")


def parse_schedule_html(
    html: str,
    default_year: Optional[int] = None,
    season_type: str = "regular",
) -> List[Dict[str, Any]]:
    """
    Extract schedule entries from a saved schedule page.

    Args:
        html: Raw HTML string.
        default_year: Optional year to fallback if it can't be inferred.
    """
    soup = BeautifulSoup(html, "html.parser")
    games: Dict[str, Dict[str, Any]] = {}

    for anchor in soup.select("a[href*='gameId=']"):
        href = anchor.get("href") or ""
        match = LINK_PATTERN.search(href)
        if not match:
            continue
        game_id = match.group(1)
        if game_id in games:
            continue
        year = default_year or int(game_id[:4])
        month = int(game_id[4:6])

        away_segment = game_id[8:10] if len(game_id) >= 10 else None
        home_segment = game_id[10:12] if len(game_id) >= 12 else None

        games[game_id] = {
            "game_id": game_id,
            "season_year": year,
            "season_type": season_type,
            "game_date": game_id[:8],
            "away_team_code": team_code_from_game_id_segment(away_segment, year),
            "home_team_code": team_code_from_game_id_segment(home_segment, year),
            "doubleheader_no": int(game_id[-1]) if game_id[-1].isdigit() else 0,
            "game_status": "scheduled",
            "crawl_status": "pending",
        }

    return list(games.values())


__all__ = ["parse_schedule_html"]
