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

_DERIVE_EVENTS_SQL = text("""
    SELECT batter_id, batter_name, description
    FROM game_events
    WHERE game_id = :game_id
      AND (description LIKE '%희생번트%' OR description LIKE '%희생플라이%')
""")


def derive_sh_sf_for_game(session: Any, game_id: str) -> dict[int | str, dict[str, int]]:
    """Query game_events and return {player_id_or_name: {'sh': N, 'sf': N}}."""
    result: dict[int | str, dict[str, int]] = {}

    # Build name-to-id mapping from game_batting_stats for this game
    stats_rows = session.execute(
        text("SELECT player_id, player_name FROM game_batting_stats WHERE game_id = :game_id"),
        {"game_id": game_id},
    ).all()

    # Map player_name -> set of player_ids (in case of duplicate names, though rare)
    name_to_ids: dict[str, set[int]] = {}
    for r in stats_rows:
        if r.player_id and r.player_name:
            name_to_ids.setdefault(r.player_name.strip(), set()).add(r.player_id)

    # Unique name-to-id mapping
    name_to_id: dict[str, int] = {}
    for name, ids in name_to_ids.items():
        if len(ids) == 1:
            name_to_id[name] = next(iter(ids))

    # Query all events matching SH or SF descriptions
    event_rows = session.execute(_DERIVE_EVENTS_SQL, {"game_id": game_id}).all()

    for row in event_rows:
        desc = row.description or ""
        is_sh = "희생번트" in desc
        is_sf = "희생플라이" in desc
        if not is_sh and not is_sf:
            continue

        # Resolve the player key (player_id if available, fallback to name)
        key: int | str | None = None
        if row.batter_id is not None:
            key = int(row.batter_id)
        elif row.batter_name:
            name = row.batter_name.strip()
            key = name_to_id.get(name, name)
        else:
            continue

        result.setdefault(key, {"sh": 0, "sf": 0})
        if is_sh:
            result[key]["sh"] += 1
        if is_sf:
            result[key]["sf"] += 1

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
