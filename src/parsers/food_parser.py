"""
Parser for stadium food vendor pages. Extracts vendor names and menu items with prices.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MENU_PATTERN = re.compile(r"([가-힣a-zA-Z0-9\s]{2,30})\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)")

STADIUM_FROM_SOURCE_KEY = {
    "lotte_giants_fnb": "SAJIK",
    "nc_dinos_food_seat": "CHANGWON",
    "gujangfood_com": "UNKNOWN",
}


def parse_food(html: str, source_key: str, metadata: dict | None = None) -> list[dict[str, Any]]:
    stadium_id = STADIUM_FROM_SOURCE_KEY.get(source_key, "UNKNOWN")

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    vendors = []
    menu_matches = MENU_PATTERN.findall(text)
    menus = []
    for name, price_str in menu_matches:
        menus.append(
            {
                "menu_name": name.strip(),
                "price": int(price_str.replace(",", "")),
                "category": "etc",
            }
        )

    if menus:
        vendors.append(
            {
                "vendor": {
                    "stadium_id": stadium_id,
                    "vendor_name": f"{stadium_id} 구장 매점",
                    "order_method": "onsite",
                    "confidence": "low",
                },
                "menus": menus,
            }
        )

    return vendors


if __name__ == "__main__":
    import sys

    html = sys.stdin.read() if not sys.stdin.isatty() else "<html><body><p>떡볶이: 3,000원</p></body></html>"
    result = parse_food(html, "lotte_giants_fnb")
    for item in result:
        logger.info(item)
