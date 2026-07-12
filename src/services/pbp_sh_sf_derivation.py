"""Derive sacrifice_hits (SH) and sacrifice_flies (SF) from game_events PBP data.

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
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
SACRIFICE_FLY_MAX_OUTS = 2

_DERIVE_EVENTS_SQL = text("""
    SELECT batter_id, batter_name, description, outs
    FROM game_events
    WHERE game_id = :game_id
      AND (description LIKE '%희생번트%' OR description LIKE '%희생플라이%')
""")


def derive_sh_sf_for_game(session: Session, game_id: str) -> dict[int | str, dict[str, int]]:
    """Query game_events and return {player_id_or_name: {'sh': N, 'sf': N}}.

    Args:
        session: Session.
        game_id: Game ID.
        session: Session.
        game_id: Game ID.

    """
    result: dict[int | str, dict[str, int]] = {}

    name_to_id = _build_unique_batter_name_map(session, game_id)

    # Query all events matching SH or SF descriptions
    event_rows = session.execute(_DERIVE_EVENTS_SQL, {"game_id": game_id}).all()

    for row in event_rows:
        desc = cast("str", getattr(row, "description", None) or "")
        is_sh = "희생번트" in desc
        is_sf = "희생플라이" in desc
        if not is_sh and not is_sf:
            continue

        # SF only counts with fewer than 2 outs before the play
        outs = cast("int | None", getattr(row, "outs", None))
        if is_sf and outs is not None and outs >= SACRIFICE_FLY_MAX_OUTS:
            continue

        key = _resolve_derived_batter_key(row, name_to_id)
        if key is None:
            continue

        result.setdefault(key, {"sh": 0, "sf": 0})
        if is_sh:
            result[key]["sh"] += 1
        if is_sf:
            result[key]["sf"] += 1

    return result


def _build_unique_batter_name_map(session: Session, game_id: str) -> dict[str, int]:
    stats_rows = session.execute(
        text("SELECT player_id, player_name FROM game_batting_stats WHERE game_id = :game_id"),
        {"game_id": game_id},
    ).all()

    name_to_ids: dict[str, set[int]] = {}
    for row in stats_rows:
        if row.player_id and row.player_name:
            name_to_ids.setdefault(row.player_name.strip(), set()).add(row.player_id)

    collisions = {name: ids for name, ids in name_to_ids.items() if len(ids) > 1}
    if collisions:
        logger.warning("SH/SF derivation: name collisions in game %s: %s", game_id, collisions)

    return {name: next(iter(ids)) for name, ids in name_to_ids.items() if len(ids) == 1}


def _resolve_derived_batter_key(row: object, name_to_id: dict[str, int]) -> int | str | None:
    batter_id = cast("int | None", getattr(row, "batter_id", None))
    if batter_id is not None:
        return int(batter_id)
    batter_name = cast("str | None", getattr(row, "batter_name", None))
    if batter_name:
        name = batter_name.strip()
        return name_to_id.get(name, name)
    return None


def count_sh_sf_from_events(
    event_rows: list[object],
    name_to_id: dict[str, int],
) -> dict[int | str, dict[str, int]]:
    """Count SH/SF from pre-fetched event rows.

    Pure function: takes event rows and a name-to-id map,
    returns {player_key: {'sh': N, 'sf': N}}.

    Args:
        event_rows: Event Rows.
        name_to_id: Name to Id.

    """
    result: dict[int | str, dict[str, int]] = {}
    for row in event_rows:
        desc = cast("str", getattr(row, "description", None) or "")
        is_sh = "희생번트" in desc
        is_sf = "희생플라이" in desc
        if not is_sh and not is_sf:
            continue
        outs = cast("int | None", getattr(row, "outs", None))
        if is_sf and outs is not None and outs >= SACRIFICE_FLY_MAX_OUTS:
            continue
        key = _resolve_derived_batter_key(row, name_to_id)
        if key is None:
            continue
        result.setdefault(key, {"sh": 0, "sf": 0})
        if is_sh:
            result[key]["sh"] += 1
        if is_sf:
            result[key]["sf"] += 1
    return result


def apply_sh_sf_to_batting_stats(session: Session, game_id: str) -> int:
    """Derive SH/SF from game_events and update game_batting_stats in-place.

    Return the number of updated rows.

    Args:
        session: Session.
        game_id: Game ID.
        session: Session.
        game_id: Game ID.

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
        """)  # noqa: S608
        exec_result: Any = session.execute(sql, params)
        updated += cast("int", exec_result.rowcount) or 0

        # Also update player_game_batting if it has SH/SF columns
        try:
            sql2 = text(f"""
                UPDATE player_game_batting
                SET {set_clause}
                WHERE game_id = :game_id
                  AND {where_clause}
            """)  # noqa: S608
            session.execute(sql2, params)
        except (SQLAlchemyError, RuntimeError):
            pass  # player_game_batting may not exist in all environments

    if updated:
        logger.info("Derived SH/SF from PBP for game %s: %d rows updated", game_id, updated)
    return updated
