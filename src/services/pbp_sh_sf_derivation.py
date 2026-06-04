"""
Derive sacrifice_hits (SH) and sacrifice_flies (SF) from game_events PBP data.

The Naver/KBO box score HTML often omits the SH/SF columns, causing
game_batting_stats.sacrifice_hits and .sacrifice_flies to remain 0.

This module derives those values from play-by-play event descriptions:
  - SH = description contains '희생번트' (sacrifice bunt)
  - SF = description contains '희생플라이' (sacrifice fly), outs_before < 2

For modern PBP data (2025+), uses batter_id join.
For legacy PBP data (pre-2025), falls back to batter_name matching.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Modern PBP (2025+): batter_id is populated
_DERIVE_SH_BY_ID_SQL = text("""
    SELECT batter_id, COUNT(*) as cnt
    FROM game_events
    WHERE game_id = :game_id
      AND description LIKE '%희생번트%'
      AND batter_id IS NOT NULL
    GROUP BY batter_id
""")

_DERIVE_SF_BY_ID_SQL = text("""
    SELECT batter_id, COUNT(*) as cnt
    FROM game_events
    WHERE game_id = :game_id
      AND description LIKE '%희생플라이%'
      AND outs < 2
      AND batter_id IS NOT NULL
    GROUP BY batter_id
""")

# Legacy PBP (pre-2025): batter_id may be NULL, match by name
_DERIVE_SH_BY_NAME_SQL = text("""
    SELECT e.batter_name, COUNT(*) as cnt
    FROM game_events e
    WHERE e.game_id = :game_id
      AND e.description LIKE '%희생번트%'
      AND e.batter_name IS NOT NULL
    GROUP BY e.batter_name
""")

_DERIVE_SF_BY_NAME_SQL = text("""
    SELECT e.batter_name, COUNT(*) as cnt
    FROM game_events e
    WHERE e.game_id = :game_id
      AND e.description LIKE '%희생플라이%'
      AND e.outs < 2
      AND e.batter_name IS NOT NULL
    GROUP BY e.batter_name
""")


def derive_sh_sf_for_game(session: Any, game_id: str) -> dict[int | str, dict[str, int]]:
    """Query game_events and return {player_id_or_name: {'sh': N, 'sf': N}}."""
    result: dict[int | str, dict[str, int]] = {}

    # Try modern approach (by batter_id)
    sh_rows = session.execute(_DERIVE_SH_BY_ID_SQL, {"game_id": game_id}).all()
    for row in sh_rows:
        result.setdefault(row.batter_id, {"sh": 0, "sf": 0})["sh"] = row.cnt

    sf_rows = session.execute(_DERIVE_SF_BY_ID_SQL, {"game_id": game_id}).all()
    for row in sf_rows:
        result.setdefault(row.batter_id, {"sh": 0, "sf": 0})["sf"] = row.cnt

    # If no ID-based results, try legacy name-based approach
    if not result:
        sh_rows = session.execute(_DERIVE_SH_BY_NAME_SQL, {"game_id": game_id}).all()
        for row in sh_rows:
            result.setdefault(row.batter_name, {"sh": 0, "sf": 0})["sh"] = row.cnt

        sf_rows = session.execute(_DERIVE_SF_BY_NAME_SQL, {"game_id": game_id}).all()
        for row in sf_rows:
            result.setdefault(row.batter_name, {"sh": 0, "sf": 0})["sf"] = row.cnt

    return result


def apply_sh_sf_to_batting_stats(session: Any, game_id: str) -> int:
    """Derive SH/SF from game_events and update game_batting_stats in-place.

    Returns the number of updated rows.
    """
    derived = derive_sh_sf_for_game(session, game_id)
    if not derived:
        return 0

    updated = 0
    for key, counts in derived.items():
        updates = []
        params: dict[str, Any] = {"game_id": game_id}
        if counts["sh"] > 0:
            updates.append("sacrifice_hits = :sh")
            params["sh"] = counts["sh"]
        if counts["sf"] > 0:
            updates.append("sacrifice_flies = :sf")
            params["sf"] = counts["sf"]
        if not updates:
            continue

        set_clause = ", ".join(updates)
        # Try matching by player_id (int key) or player_name (str key)
        if isinstance(key, int):
            params["player_id"] = key
            where_clause = "player_id = :player_id"
        else:
            params["player_name"] = key
            where_clause = "player_name = :player_name"

        sql = text(f"""
            UPDATE game_batting_stats
            SET {set_clause}
            WHERE game_id = :game_id
              AND {where_clause}
        """)
        result = session.execute(sql, params)
        updated += result.rowcount or 0

    if updated:
        logger.info("Derived SH/SF from PBP for game %s: %d rows updated", game_id, updated)
    return updated
