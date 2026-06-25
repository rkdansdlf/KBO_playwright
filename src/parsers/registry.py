"""Registry mapping source keys to parser functions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .team_event_parser import parse_team_events
from .ticket_parser import parse_ticket_page

ParserFunc = Callable[[str, str, dict | None], list[dict[str, Any]]]


def _parse_ticket_map(_html: str, _source_key: str, _metadata: dict | None = None) -> list[dict[str, Any]]:
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
}


def get_parser(source_key: str) -> ParserFunc | None:
    return PARSER_REGISTRY.get(source_key)
