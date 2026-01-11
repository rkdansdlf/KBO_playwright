"""
Team history helpers loaded from Docs/team_history definitions.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Optional


@dataclass(frozen=True)
class TeamHistoryEntry:
    franchise_id: int
    team_code: str
    start_season: int
    end_season: Optional[int]


# This list mirrors Docs/schema/teams_history.md and Supabase public.team_history seed data.
_TEAM_HISTORY: tuple[TeamHistoryEntry, ...] = (
    TeamHistoryEntry(1, "SS", 1982, None),
    TeamHistoryEntry(2, "LT", 1982, None),
    TeamHistoryEntry(3, "MBC", 1982, 1989),
    TeamHistoryEntry(3, "LG", 1990, None),
    TeamHistoryEntry(4, "OB", 1982, 1995),
    TeamHistoryEntry(4, "DO", 1996, None),
    TeamHistoryEntry(5, "HT", 1982, 2000),
    TeamHistoryEntry(5, "KIA", 2001, None),
    TeamHistoryEntry(6, "SM", 1982, 1985),
    TeamHistoryEntry(6, "CB", 1985, 1987),
    TeamHistoryEntry(6, "TP", 1988, 1995),
    TeamHistoryEntry(6, "HU", 1996, 2007),
    TeamHistoryEntry(6, "WO", 2008, 2009),
    TeamHistoryEntry(6, "NX", 2010, 2018),
    TeamHistoryEntry(6, "KI", 2019, None),
    TeamHistoryEntry(7, "BE", 1986, 1992),
    TeamHistoryEntry(7, "HH", 1993, None),
    TeamHistoryEntry(8, "SL", 1990, 1999),
    TeamHistoryEntry(8, "SK", 2000, 2020),
    TeamHistoryEntry(8, "SSG", 2021, None),
    TeamHistoryEntry(9, "NC", 2011, None),
    TeamHistoryEntry(10, "KT", 2013, None),
)


FRANCHISE_CANONICAL_CODE = {
    1: "SS",
    2: "LT",
    3: "LG",
    4: "OB",
    5: "KIA",
    6: "WO",
    7: "HH",
    8: "SSG",
    9: "NC",
    10: "KT",
}


def iter_team_history() -> Iterable[TeamHistoryEntry]:
    return _TEAM_HISTORY


@lru_cache(maxsize=None)
def resolve_team_code_for_season(raw_code: str, season_year: int) -> Optional[str]:
    raw = raw_code.upper()
    for entry in _TEAM_HISTORY:
        if entry.team_code.upper() != raw:
            continue
        end_season = entry.end_season or season_year
        if entry.start_season <= season_year <= end_season:
            return FRANCHISE_CANONICAL_CODE.get(entry.franchise_id)
    return None
