from __future__ import annotations

from typing import Any
from collections.abc import Callable

from .food_parser import parse_food
from .parking_parser import parse_parking
from .roster_parser import parse_mobile_roster
from .seat_parser import parse_seat_sections
from .team_event_parser import parse_team_events
from .ticket_parser import parse_ticket_page

ParserFunc = Callable[[str, str, dict | None], list[dict[str, Any]]]


def _parse_ticket_map(html: str, source_key: str, metadata: dict | None = None) -> list[dict[str, Any]]:
    # KBO map page is an intermediate discovery page; ticket URLs are extracted
    # at crawl time and individual team pages are parsed separately.
    return []


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
    "kbo_today_roster": parse_mobile_roster,
    "kbo_player_register": parse_mobile_roster,
    "kbo_player_movement": parse_mobile_roster,
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
    "lg_twins_seat": parse_seat_sections,
    "seoul_stadium_seat": parse_seat_sections,
    "ssg_landers_parking": parse_parking,
    "daegu_parking": parse_parking,
    "jamsil_parking_official": parse_parking,
    "lotte_giants_fnb": parse_food,
    "nc_dinos_food_seat": parse_food,
    "gujangfood_com": parse_food,
}


def get_parser(source_key: str) -> ParserFunc | None:
    return PARSER_REGISTRY.get(source_key)


PARSED_DOMAINS_NESTED = {"parking", "food"}


def is_nested_domain(target_domain: str) -> bool:
    return target_domain in PARSED_DOMAINS_NESTED
