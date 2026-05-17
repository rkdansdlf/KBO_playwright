#!/usr/bin/env python3
"""
Repair historical Earned Run (ER) violations (ER > Opponent Runs).
Identifies and fixes impossible pitching stats.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

def repair_historical_er(dry_run: bool = True):
    with SessionLocal() as session:
        # Find games where team_er > opp_runs
        query = """
            SELECT 
                g.game_id, 
                g.game_date,
                gps.team_side,
                SUM(gps.earned_runs) as team_er,
                CASE WHEN gps.team_side = 'home' THEN g.away_score ELSE g.home_score END as opp_runs
            FROM game_pitching_stats gps
            JOIN game g ON gps.game_id = g.game_id
            WHERE g.game_status IN ('COMPLETED', 'DRAW')
            GROUP BY g.game_id, gps.team_side
            HAVING team_er > opp_runs
        """
        violations = session.execute(text(query)).mappings().all()
        print(f"🛠️ Found {len(violations)} ER violations to repair.")

        for v in violations:
            gid = v["game_id"]
            side = v["team_side"]
            print(f"   - Processing {gid} ({side})...")
            
            # For these historical games, the most likely error is that 'runs_allowed' was put into 'earned_runs'
            # OR there was a duplicate entry.
            # We will cap ER at Runs for these specific cases if we can't find a better fix.
            # But wait! A better fix is to re-crawl if it's recent, or manually fix if old.
            
            # Let's check individual pitchers
            pitchers = session.execute(
                text("SELECT id, player_name, earned_runs, runs_allowed FROM game_pitching_stats WHERE game_id = :gid AND team_side = :side"),
                {"gid": gid, "side": side}
            ).mappings().all()
            
            for p in pitchers:
                if p["earned_runs"] > p["runs_allowed"]:
                    print(f"     Pitcher {p['player_name']} has ER({p['earned_runs']}) > Runs({p['runs_allowed']}). Fixing.")
                    if not dry_run:
                        session.execute(
                            text("UPDATE game_pitching_stats SET earned_runs = runs_allowed, updated_at = CURRENT_TIMESTAMP WHERE id = :pid"),
                            {"id": p["id"]}
                        )
            
            if not dry_run:
                session.commit()
                print(f"     ✅ Repaired {gid}")

def main():
    parser = argparse.ArgumentParser(description="Repair historical ER violations")
    parser.add_argument("--execute", action="store_true", help="Execute updates")
    args = parser.parse_args()

    repair_historical_er(dry_run=not args.execute)

if __name__ == "__main__":
    main()
