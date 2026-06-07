"""
Parser for team ticket pages. Extracts seat grades and prices (weekday/weekend).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

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

PRICE_PATTERN = re.compile(r"(?<!주말\s)([가-힣]+(?:석|존|zone|Zone))\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원|￦|KRW)?", re.MULTILINE)
WEEKEND_PATTERN = re.compile(r"주말\s*([가-힣]+(?:석|존|zone|Zone))\s*:?\s*(\d{1,3}(?:,\d{3})*)", re.MULTILINE)


def parse_ticket_page(html: str, source_key: str, metadata: dict | None = None) -> list[dict[str, Any]]:
    team_info = TEAM_CODE_FROM_SOURCE_KEY.get(source_key)
    if not team_info:
        logger.warning(f"No team mapping for source_key={source_key}")
        return []

    team_code, stadium_id = team_info
    season = datetime.now().year
    meta_season = (metadata or {}).get("season")
    if meta_season:
        season = int(meta_season)

    soup = BeautifulSoup(html, "html.parser")
    text_content = soup.get_text(separator=" ", strip=True)

    prices = {}
    for match in PRICE_PATTERN.finditer(text_content):
        grade, price_str = match.group(1), match.group(2).replace(",", "")
        key = (team_code, season, grade, "weekday", "general")
        if key not in prices:
            prices[key] = {
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

    weekend_prices = {}
    for match in WEEKEND_PATTERN.finditer(text_content):
        grade, price_str = match.group(1), match.group(2).replace(",", "")
        key_weekday = (team_code, season, grade, "weekday", "general")
        key_weekend = (team_code, season, grade, "weekend", "general")
        if key_weekday in prices and key_weekend not in weekend_prices:
            p = prices[key_weekday]
            weekend_entry = dict(p)
            weekend_entry["day_type"] = "weekend"
            weekend_entry["price"] = int(price_str)
            weekend_prices[key_weekend] = weekend_entry

    return list(prices.values()) + list(weekend_prices.values())


if __name__ == "__main__":
    import sys
    html = sys.stdin.read() if not sys.stdin.isatty() else "<html><body><p>테이블석 : 150,000원</p></body></html>"
    result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
    for item in result:
        print(item)
