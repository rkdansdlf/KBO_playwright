"""
Situational Splits Calculator Service.

Computes RISP (Runners In Scoring Position) and L/R splits
by querying the game_events PBP data with game_batting_stats.
"""
import os
import sys
from typing import Dict, Any, Optional, List
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.engine import SessionLocal


class SituationalSplitCalculator:
    """
    Computes situational batting splits from game_events (PBP) data.
    """

    def __init__(self, session=None):
        self._session = session

    def _session_ctx(self):
        if self._session:
            return self._session
        return SessionLocal()

    # ------------------------------------------------------------------ #
    # RISP: Runners In Scoring Position (base_state & 6 > 0)
    # base_state bitmask: 1=1B, 2=2B, 4=3B  → RISP = 2B or 3B set → & 6
    # ------------------------------------------------------------------ #
    def _resolve_name(self, player_id: int, session) -> Optional[str]:
        """Returns the Korean name for a given player_id."""
        row = session.execute(
            text("SELECT name FROM player_basic WHERE player_id = :pid"),
            {"pid": player_id}
        ).fetchone()
        return row.name if row else None

    def get_risp_stats(
        self, player_id: int, season: int
    ) -> Dict[str, Any]:
        """Returns RISP batting stats for a player in a given season."""
        query = """
        SELECT
            COUNT(CASE WHEN e.result_code NOT IN ('BB','HBP','SH','SF') AND e.result_code IS NOT NULL THEN 1 END) AS risp_ab,
            COUNT(CASE WHEN e.result_code IN ('H1','H2','H3','HR') THEN 1 END)           AS risp_hits,
            COUNT(CASE WHEN e.result_code IN ('H1','H2','H3','HR','BB','HBP') THEN 1 END) AS risp_on_base
        FROM game_events e
        WHERE
            e.batter_name = :name
            AND SUBSTR(e.game_id, 1, 4) = :season
            AND (e.base_state & 6) > 0
        """
        with self._session_ctx() as session:
            name = self._resolve_name(player_id, session)
            if not name:
                return {"risp_avg": None, "risp_ab": 0, "risp_hits": 0}
            row = session.execute(
                text(query), {"name": name, "season": str(season)}
            ).fetchone()

        if not row or not row.risp_ab:
            return {"risp_avg": None, "risp_ab": 0, "risp_hits": 0}

        risp_avg = round(row.risp_hits / row.risp_ab, 3) if row.risp_ab else None
        obp_denom = (row.risp_ab or 0) + (row.risp_on_base or 0) - (row.risp_hits or 0)
        risp_obp = round(row.risp_on_base / obp_denom, 3) if obp_denom else None

        return {
            "risp_avg": risp_avg,
            "risp_obp": risp_obp,
            "risp_ab": row.risp_ab,
            "risp_hits": row.risp_hits,
        }

    # ------------------------------------------------------------------ #
    # L/R Splits: vs Left-handed / Right-handed Pitcher
    # ------------------------------------------------------------------ #
    def get_lr_splits(
        self, player_id: int, season: int
    ) -> Dict[str, Any]:
        """
        Returns batting splits vs LHP and RHP.
        Joins game_events → game_pitching_stats → player_basic (throws).
        Uses batter_name since batter_id may be NULL in game_events.
        """
        query = """
        SELECT
            pb.throws,
            COUNT(CASE WHEN e.result_code NOT IN ('BB','HBP','SH','SF') AND e.result_code IS NOT NULL THEN 1 END) AS ab,
            COUNT(CASE WHEN e.result_code IN ('H1','H2','H3','HR') THEN 1 END)             AS hits,
            COUNT(CASE WHEN e.result_code IN ('BB','HBP') THEN 1 END)                      AS on_base_events,
            COUNT(CASE WHEN e.result_code IN ('H1','H2','H3','HR','BB','HBP') THEN 1 END) AS obp_events
        FROM game_events e
        JOIN game_pitching_stats gps
            ON gps.game_id = e.game_id
            AND gps.player_name = e.pitcher_name
        JOIN player_basic pb
            ON pb.player_id = gps.player_id
        WHERE
            e.batter_name = :name
            AND SUBSTR(e.game_id, 1, 4) = :season
            AND pb.throws IN ('L', 'R')
        GROUP BY pb.throws
        """
        results: Dict[str, Any] = {"vs_lhp": {}, "vs_rhp": {}}

        with self._session_ctx() as session:
            name = self._resolve_name(player_id, session)
            if not name:
                return results
            rows = session.execute(
                text(query), {"name": name, "season": str(season)}
            ).fetchall()

        for row in rows:
            key = "vs_lhp" if row.throws == "L" else "vs_rhp"
            avg = round(row.hits / row.ab, 3) if row.ab else None
            obp_d = row.ab + row.on_base_events
            obp = round(row.obp_events / obp_d, 3) if obp_d else None
            results[key] = {"ab": row.ab, "hits": row.hits, "avg": avg, "obp": obp}

        return results

    # ------------------------------------------------------------------ #
    # Two-Out RBI: Clutch situational stat
    # ------------------------------------------------------------------ #
    def get_two_out_stats(
        self, player_id: int, season: int
    ) -> Dict[str, Any]:
        """Returns batting stats with 2 outs."""
        query = """
        SELECT
            COUNT(CASE WHEN result_code NOT IN ('BB','HBP','SH','SF') AND result_code IS NOT NULL THEN 1 END) AS ab,
            COUNT(CASE WHEN result_code IN ('H1','H2','H3','HR') THEN 1 END) AS hits,
            SUM(rbi)                                                          AS rbi
        FROM game_events
        WHERE
            batter_name = :name
            AND SUBSTR(game_id, 1, 4) = :season
            AND outs = 2
        """
        with self._session_ctx() as session:
            name = self._resolve_name(player_id, session)
            if not name:
                return {"two_out_avg": None, "two_out_ab": 0, "two_out_rbi": 0}
            row = session.execute(
                text(query), {"name": name, "season": str(season)}
            ).fetchone()

        if not row or not row.ab:
            return {"two_out_avg": None, "two_out_ab": 0, "two_out_rbi": 0}

        return {
            "two_out_avg": round(row.hits / row.ab, 3) if row.ab else None,
            "two_out_ab": row.ab,
            "two_out_rbi": row.rbi or 0,
        }

    # ------------------------------------------------------------------ #
    # Full profile helper
    # ------------------------------------------------------------------ #
    def get_full_splits(
        self, player_id: int, season: int
    ) -> Dict[str, Any]:
        return {
            "player_id": player_id,
            "season": season,
            "risp": self.get_risp_stats(player_id, season),
            "lr_splits": self.get_lr_splits(player_id, season),
            "two_out": self.get_two_out_stats(player_id, season),
        }


# --------------------------------------------------------------------------- #
# CLI: Quick test on a given player
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Situational Splits Calculator")
    parser.add_argument("--player_id", type=int, required=True, help="KBO Player ID")
    parser.add_argument("--season",    type=int, default=2025,   help="Season year")
    args = parser.parse_args()

    calc = SituationalSplitCalculator()
    result = calc.get_full_splits(args.player_id, args.season)
    print(json.dumps(result, ensure_ascii=False, indent=2))
