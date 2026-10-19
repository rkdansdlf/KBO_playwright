"""Registry mapping source keys to parser functions."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from bs4 import BeautifulSoup

from .team_event_parser import parse_team_events
from .ticket_parser import parse_ticket_page

ParserFunc = Callable[[str, str, dict[str, Any] | None], list[dict[str, Any]]]

_MIN_SECTION_NAME_LENGTH = 2


def _parse_ticket_map(_html: str, _source_key: str, _metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return []


def _parse_lotte_giants_food(
    html: str, _source_key: str, _metadata: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Parse Lotte Giants food page (giantsclub.com/food)."""
    menu_pattern = re.compile(r"([가-힣a-zA-Z0-9\s]{2,30})\s*:?\s*(\d{1,3}(?:,\d{3})*)\s*(?:원)")
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    vendors = []

    menu_matches = menu_pattern.findall(text)
    menus = []
    for name, price_str in menu_matches:
        menus.append(
            {
                "menu_name": name.strip(),
                "price": int(price_str.replace(",", "")),
                "category": "etc",
            },
        )

    if menus:
        vendors.append(
            {
                "vendor": {
                    "stadium_id": "SAJIK",
                    "vendor_name": "SAJIK 구장 매점",
                    "order_method": "onsite",
                    "confidence": "low",
                },
                "menus": menus,
            },
        )

    return vendors


def _parse_jamsil_parking(html: str, _source_key: str, _metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Parse Jamsil stadium parking page (stadium.seoul.go.kr)."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    sections = []
    seen = set()

    patterns = [
        re.compile(r"([가-힣]+(?:구역|주차|장|zone|Zone))"),
        re.compile(r"(P\d+|[A-Z]\d+)"),
    ]

    for pattern in patterns:
        for match in pattern.finditer(text):
            name = match.group(0).strip()
            if name in seen or len(name) < _MIN_SECTION_NAME_LENGTH:
                continue
            seen.add(name)
            sections.append(
                {
                    "stadium_id": "JAMSIL",
                    "section_name": name,
                    "section_code": name,
                    "capacity": None,
                    "zone_type": "parking",
                },
            )

    return sections


PARSER_REGISTRY: dict[str, ParserFunc] = {
    "lg_twins_events": parse_team_events,
    "hanwha_eagles_events": parse_team_events,
    "doosan_bears_events": parse_team_events,
    "ssg_landers_events": parse_team_events,
    "nc_dinos_events": parse_team_events,
    "kia_tigers_events": parse_team_events,
    "lotte_giants_events": parse_team_events,
    "samsung_lions_events": parse_team_events,
    "kt_wiz_events": parse_team_events,
    "kiwoom_heroes_events": parse_team_events,
    "kbo_ticket_map": _parse_ticket_map,
    "lg_twins_ticket": parse_ticket_page,
    "hanwha_eagles_ticket": parse_ticket_page,
    "samsung_lions_ticket": parse_ticket_page,
    "kt_wiz_ticket": parse_ticket_page,
    "doosan_bears_ticket": parse_ticket_page,
    "lotte_giants_ticket": parse_ticket_page,
    "kia_tigers_ticket": parse_ticket_page,
    "nc_dinos_ticket": parse_ticket_page,
    "ssg_landers_ticket": parse_ticket_page,
    "kiwoom_heroes_ticket": parse_ticket_page,
    "lotte_giants_fnb": _parse_lotte_giants_food,
    "jamsil_parking_official": _parse_jamsil_parking,
}


def get_parser(source_key: str) -> ParserFunc | None:
    """Get parser.

    Args:
        source_key: Source Key.
        source_key: Source Key.
        source_key: Source Key.

    Returns:
        The result of the operation.

    """
    return PARSER_REGISTRY.get(source_key)
