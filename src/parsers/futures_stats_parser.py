"""
Parsing helpers for Futures League season tables on player profile pages.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from src.parsers.retired_player_parser import (
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)


_HITTER_KEYWORDS = {"타수", "안타", "AVG", "타율", "타점"}
_PITCHER_KEYWORDS = {"ERA", "평균자책", "WHIP", "이닝", "승", "패"}


def _classify_tables(tables: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    hitter_tables: List[Dict[str, Any]] = []
    pitcher_tables: List[Dict[str, Any]] = []

    for table in tables:
        # Priority 1: Check for explicit type marker (set by crawler)
        table_type = table.get("_table_type")
        if table_type == "HITTER":
            hitter_tables.append(table)
            continue
        elif table_type == "PITCHER":
            pitcher_tables.append(table)
            continue

        # Priority 2: Try exact keyword match
        headers = table.get("headers") or []
        normalized = {header.strip() for header in headers}

        if _HITTER_KEYWORDS & normalized:
            hitter_tables.append(table)
            continue
        elif _PITCHER_KEYWORDS & normalized:
            pitcher_tables.append(table)
            continue

        # Priority 3: Fallback - inspect caption or row content for hints
        caption = (table.get("caption") or "").lower()
        summary = str(table.get("summary", "")).lower()
        combined_hint = caption + " " + summary

        # Hitter hints: check for batting-related keywords
        if any(keyword in combined_hint for keyword in ("타율", "타격", "타자", "hitter", "batting")):
            hitter_tables.append(table)
        # Pitcher hints: check for pitching-related keywords
        elif any(keyword in combined_hint for keyword in ("투수", "투구", "pitcher", "pitching", "평균자책")):
            pitcher_tables.append(table)

    return hitter_tables, pitcher_tables


def parse_futures_tables(tables: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Split futures tables into batting/pitching aggregates and parse them."""
    hitter_tables, pitcher_tables = _classify_tables(tables)

    batting = (
        parse_retired_hitter_tables(hitter_tables, league="FUTURES", level="KBO2")
        if hitter_tables
        else []
    )

    pitching: List[Dict[str, Any]] = []
    for table in pitcher_tables:
        pitching.extend(
            parse_retired_pitcher_table(table, league="FUTURES", level="KBO2")
        )

    return {"batting": batting, "pitching": pitching}


__all__ = ["parse_futures_tables"]

