"""Parse offline schedule HTML into structured game schedule rows."""

from __future__ import annotations

import re
from typing import Any

from src.constants import GAME_ID_FULL_LEN, GAME_ID_MIN_LEN
from src.parsers.base_parser import BaseHtmlParser
from src.utils.schedule_validation import split_schedule_game_id
from src.utils.team_codes import team_code_from_game_id_segment

LINK_PATTERN = re.compile(r"gameId=([0-9A-Z]+)")


class ScheduleParser(BaseHtmlParser[list[dict[str, Any]]]):
    """Parser for KBO offline and online schedule HTML pages."""

    def __init__(
        self,
        html: str,
        default_year: int | None = None,
        season_type: str = "regular",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize ScheduleParser."""
        merged_meta = dict(metadata or {})
        if default_year is not None:
            merged_meta["default_year"] = default_year
        merged_meta["season_type"] = season_type
        super().__init__(html=html, source_key="kbo_schedule", metadata=merged_meta)
        self.default_year = default_year
        self.season_type = season_type

    def parse(self) -> list[dict[str, Any]]:
        """Extract schedule entries from schedule HTML page.

        Returns:
            List of game schedule dictionaries.

        """
        games: dict[str, dict[str, Any]] = {}

        for anchor in self.soup.select("a[href*='gameId=']"):
            href = anchor.get("href") or ""
            if not isinstance(href, str):
                continue
            match = LINK_PATTERN.search(href)
            if not match:
                continue
            game_id = match.group(1)
            if game_id in games:
                continue
            year = self.default_year or int(game_id[:4])

            id_parts = split_schedule_game_id(game_id)
            away_segment: str | None
            home_segment: str | None
            doubleheader_no: int
            if id_parts:
                _, away_segment, home_segment, dh_str = id_parts
                doubleheader_no = int(dh_str)
            else:
                away_segment = game_id[8:10] if len(game_id) >= GAME_ID_MIN_LEN else None
                home_segment = game_id[10:12] if len(game_id) >= GAME_ID_FULL_LEN else None
                doubleheader_no = int(game_id[-1]) if game_id[-1].isdigit() else 0

            games[game_id] = {
                "game_id": game_id,
                "season_year": year,
                "season_type": self.season_type,
                "game_date": game_id[:8],
                "away_team_code": team_code_from_game_id_segment(away_segment, year),
                "home_team_code": team_code_from_game_id_segment(home_segment, year),
                "doubleheader_no": doubleheader_no,
                "game_status": "scheduled",
                "crawl_status": "pending",
                "stadium": "",
            }

        return list(games.values())


def parse_schedule_html(
    html: str,
    default_year: int | None = None,
    season_type: str = "regular",
) -> list[dict[str, Any]]:
    """Extract schedule entries from a saved schedule page.

    Args:
        html: Raw HTML string.
        default_year: Optional year to fallback if it cannot be inferred from gameId.
        season_type: Season type (e.g. 'regular', 'exhibition', 'postseason').

    Returns:
        List of game schedule dictionaries.

    """
    return ScheduleParser(html, default_year=default_year, season_type=season_type).parse()


__all__ = ["LINK_PATTERN", "ScheduleParser", "parse_schedule_html"]
