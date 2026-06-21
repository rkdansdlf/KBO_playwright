"""
Team history helpers loaded from Docs/team_history definitions.
"""


# ruff: noqa: PLR2004from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cache


@dataclass(frozen=True)
class TeamHistoryEntry:
    franchise_id: int
    team_code: str
    start_season: int
    end_season: int | None


# This list mirrors the KBO Team History page and is the source of truth for
# season-aware franchise identity. Dissolved clubs stay separate from later
# expansion or replacement franchises even when KBO technical codes overlap.
_TEAM_HISTORY: tuple[TeamHistoryEntry, ...] = (
    TeamHistoryEntry(1, "SS", 1982, None),
    TeamHistoryEntry(2, "LT", 1982, None),
    TeamHistoryEntry(3, "MBC", 1982, 1989),
    TeamHistoryEntry(3, "LG", 1990, None),
    TeamHistoryEntry(4, "OB", 1982, 1998),
    TeamHistoryEntry(4, "DB", 1999, None),
    TeamHistoryEntry(5, "HT", 1982, 2000),
    TeamHistoryEntry(5, "KIA", 2001, None),
    TeamHistoryEntry(6, "SM", 1982, 1984),
    TeamHistoryEntry(6, "CB", 1985, 1987),
    TeamHistoryEntry(6, "TP", 1988, 1995),
    TeamHistoryEntry(6, "HU", 1996, 2007),
    TeamHistoryEntry(11, "WO", 2008, 2009),
    TeamHistoryEntry(11, "NX", 2010, 2018),
    TeamHistoryEntry(11, "KH", 2019, None),
    TeamHistoryEntry(7, "BE", 1986, 1993),
    TeamHistoryEntry(7, "HH", 1994, None),
    TeamHistoryEntry(12, "SL", 1991, 1999),
    TeamHistoryEntry(8, "SK", 2000, 2020),
    TeamHistoryEntry(8, "SSG", 2021, None),
    TeamHistoryEntry(9, "NC", 2011, None),
    TeamHistoryEntry(10, "KT", 2013, None),
)


FRANCHISE_CANONICAL_CODE = {
    1: "SS",
    2: "LT",
    3: "LG",
    4: "DB",  # Modern canonical is DB
    5: "KIA",
    6: "HU",
    7: "HH",
    8: "SSG",
    9: "NC",
    10: "KT",
    11: "KH",
    12: "SL",
}


def iter_team_history() -> Iterable[TeamHistoryEntry]:
    return _TEAM_HISTORY


def _entry_is_active_in_season(entry: TeamHistoryEntry, season_year: int) -> bool:
    end_season = entry.end_season if entry.end_season is not None else season_year
    return entry.start_season <= season_year <= end_season


@cache
def find_team_history_entry(raw_code: str, season_year: int | None = None) -> TeamHistoryEntry | None:
    """Return the season-correct history entry for a raw or legacy team code."""
    raw = raw_code.upper()
    if raw == "HD":
        raw = "HU"
    if raw == "SSG" and season_year is not None and 1991 <= season_year <= 1999:
        raw = "SL"
    franchise_id = None
    original_entry = None
    for entry in _TEAM_HISTORY:
        if entry.team_code.upper() == raw:
            franchise_id = entry.franchise_id
            original_entry = entry
            break

    if franchise_id is None:
        return None

    if season_year is None:
        return original_entry

    if original_entry and _entry_is_active_in_season(original_entry, season_year):
        return original_entry

    for entry in _TEAM_HISTORY:
        if entry.franchise_id == franchise_id and _entry_is_active_in_season(entry, season_year):
            return entry

    return None


def franchise_id_for_team_code(raw_code: str, season_year: int | None = None) -> int | None:
    entry = find_team_history_entry(raw_code, season_year)
    return entry.franchise_id if entry else None


def canonical_code_for_team_code(raw_code: str, season_year: int | None = None) -> str | None:
    entry = find_team_history_entry(raw_code, season_year)
    if not entry:
        return None
    return FRANCHISE_CANONICAL_CODE.get(entry.franchise_id)


@cache
def resolve_team_code_for_season(raw_code: str, season_year: int) -> str | None:
    entry = find_team_history_entry(raw_code, season_year)
    return entry.team_code.upper() if entry else None
