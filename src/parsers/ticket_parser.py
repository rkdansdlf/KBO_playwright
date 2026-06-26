"""Parser for team ticket pages. Extracts seat grades and prices (weekday/weekend)."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from src.constants import KST
from src.parsers.base_parser import BaseStadiumParser

TEAM_CODE_FROM_SOURCE_KEY = {
    "lg_twins_ticket": ("LG", "JAMSIL"),
    "hanwha_eagles_ticket": ("HH", "HANBAT"),
    "samsung_lions_ticket": ("SS", "DAEGU"),
    "kt_wiz_ticket": ("KT", "SUWON"),
    "doosan_bears_ticket": ("OB", "JAMSIL"),
    "lotte_giants_ticket": ("LT", "SAJIK"),
    "kia_tigers_ticket": ("HT", "GWANGJU"),
    "nc_dinos_ticket": ("NC", "CHANGWON"),
    "ssg_landers_ticket": ("SK", "MUNHAK"),
    "kiwoom_heroes_ticket": ("WO", "GOCHEOK"),
}

PRICE_PATTERN = re.compile(
    r"(?<![가-힣])(?<!주말\s)(?<!주말)([가-힣]+(?:석|존|zone|Zone))\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원|￦|KRW)?",
    re.MULTILINE,
)
WEEKEND_PATTERN = re.compile(r"주말\s*([가-힣]+(?:석|존|zone|Zone))\s*:?\s*(\d{1,3}(?:,\d{3})*)", re.MULTILINE)


class TicketParser(BaseStadiumParser):
    """TicketParser class."""

    SOURCE_KEY_MAP = TEAM_CODE_FROM_SOURCE_KEY

    def parse(self) -> list[dict[str, Any]]:
        """
        Parses parse.

        Returns:
            List of results.

        """
        team_info = self.SOURCE_KEY_MAP.get(self.source_key)
        if not team_info:
            import logging

            logging.getLogger(__name__).warning("No team mapping for source_key=%s", self.source_key)
            return []

        team_code, stadium_id = team_info
        season = datetime.now(KST).year
        meta_season = self.metadata.get("season")
        if meta_season:
            season = int(meta_season)

        prices_dict = {}
        for match in PRICE_PATTERN.finditer(self.text):
            grade, price_str = match.group(1), match.group(2).replace(",", "")
            key = (team_code, season, grade, "weekday", "general")
            prices_dict[key] = {
                "team_id": team_code,
                "stadium_id": stadium_id,
                "season": season,
                "seat_grade": grade,
                "day_type": "weekday",
                "price": int(price_str),
                "currency": "KRW",
                "audience_type": "general",
                "effective_from": None,
                "effective_to": None,
            }

        for match in WEEKEND_PATTERN.finditer(self.text):
            grade, price_str = match.group(1), match.group(2).replace(",", "")
            key_weekday = (team_code, season, grade, "weekday", "general")
            key_weekend = (team_code, season, grade, "weekend", "general")
            if key_weekday in prices_dict:
                prices_dict[key_weekend] = {
                    "team_id": team_code,
                    "stadium_id": stadium_id,
                    "season": season,
                    "seat_grade": grade,
                    "day_type": "weekend",
                    "price": int(price_str),
                    "currency": "KRW",
                    "audience_type": "general",
                    "effective_from": None,
                    "effective_to": None,
                }

        return list(prices_dict.values())


def parse_ticket_page(html: str, source_key: str, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Parses ticket page.

    Args:
        html: Html.
        source_key: Source Key.
        metadata: Metadata.

    Returns:
        List of results.

    """
    return TicketParser(html, source_key, metadata).parse()


if __name__ == "__main__":
    import sys

    html = sys.stdin.read() if not sys.stdin.isatty() else "<html><body><p>테이블석 : 150,000원</p></body></html>"
    result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
    for item in result:
        print(item)
