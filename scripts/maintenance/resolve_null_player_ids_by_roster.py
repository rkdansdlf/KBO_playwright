#!/usr/bin/env python3
"""Resolve NULL player_id values using team_daily_roster context."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from sqlalchemy import text
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

def resolve_by_roster(apply: bool = False, since: str = "2024-01-01"):
    tables = ["game_batting_stats", "game_pitching_stats", "game_lineups"]

    with SessionLocal() as session:
        for table in tables:
            print(f"\n--- Processing {table} ---")

            # Join game stats -> game -> team_daily_roster by date/team/name
            # Use player_basic_id (correct column on team_daily_roster)
            # Guard: only match rows where exactly ONE distinct player_basic_id resolves
            #        to avoid overwriting with wrong ID when roster has duplicates
            query = text(f"""
                SELECT t.id, g.game_date, t.team_code, t.player_name,
                       MIN(r.player_basic_id) as resolved_id,
                       COUNT(DISTINCT r.player_basic_id) as match_count
                FROM {table} t
                JOIN game g ON g.game_id = t.game_id
                JOIN team_daily_roster r
                    ON r.roster_date = g.game_date
                    AND r.player_name = t.player_name
                    AND r.player_basic_id IS NOT NULL
                    AND (
                        r.team_code = t.team_code
                        OR r.team_code = CASE
                            WHEN t.team_code = 'KH'  THEN 'WO'
                            WHEN t.team_code = 'WO'  THEN 'KH'
                            WHEN t.team_code = 'KIA' THEN 'HT'
                            WHEN t.team_code = 'HT'  THEN 'KIA'
                            WHEN t.team_code = 'DB'  THEN 'OB'
                            WHEN t.team_code = 'OB'  THEN 'DB'
                            WHEN t.team_code = 'SSG' THEN 'SK'
                            WHEN t.team_code = 'SK'  THEN 'SSG'
                            ELSE t.team_code END
                    )
                WHERE t.player_id IS NULL
                  AND g.game_date >= :since
                GROUP BY t.id, g.game_date, t.team_code, t.player_name
                HAVING match_count = 1
            """)

            matches = session.execute(query, {"since": since}).fetchall()
            ambiguous_query = text(f"""
                SELECT COUNT(DISTINCT t.id)
                FROM {table} t
                JOIN game g ON g.game_id = t.game_id
                JOIN team_daily_roster r
                    ON r.roster_date = g.game_date
                    AND r.player_name = t.player_name
                    AND r.player_basic_id IS NOT NULL
                    AND (r.team_code = t.team_code OR r.team_code = CASE
                        WHEN t.team_code = 'KH'  THEN 'WO'
                        WHEN t.team_code = 'WO'  THEN 'KH'
                        WHEN t.team_code = 'KIA' THEN 'HT'
                        WHEN t.team_code = 'HT'  THEN 'KIA'
                        WHEN t.team_code = 'DB'  THEN 'OB'
                        WHEN t.team_code = 'OB'  THEN 'DB'
                        WHEN t.team_code = 'SSG' THEN 'SK'
                        WHEN t.team_code = 'SK'  THEN 'SSG'
                        ELSE t.team_code END)
                WHERE t.player_id IS NULL AND g.game_date >= :since
                GROUP BY t.id HAVING COUNT(DISTINCT r.player_basic_id) > 1
            """)
            ambiguous_count = session.execute(ambiguous_query, {"since": since}).scalar() or 0

            print(f"  Resolvable (unique match):  {len(matches)}")
            print(f"  Skipped (ambiguous roster): {ambiguous_count}")

            if not apply:
                for i, (row_id, g_date, t_code, p_name, res_id, _) in enumerate(matches[:5]):
                    print(f"  [DRY-RUN] {g_date} {t_code} {p_name} -> {res_id}")
                print(f"🔍 Would update {len(matches)} rows in {table}.")
                continue

            updated = 0
            for row_id, g_date, t_code, p_name, res_id, _ in matches:
                session.execute(
                    text(f"UPDATE {table} SET player_id = :pid WHERE id = :id"),
                    {"pid": res_id, "id": row_id}
                )
                updated += 1

            session.commit()
            print(f"✅ Updated {updated} rows in {table}.")

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Resolve NULL player_id via team_daily_roster")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default: dry-run)")
    parser.add_argument("--since", default="2024-01-01", help="Only process games on/after this date (YYYY-MM-DD)")
    args = parser.parse_args()
    resolve_by_roster(apply=args.apply, since=args.since)
