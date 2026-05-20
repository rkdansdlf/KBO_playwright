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

def resolve_by_roster(apply: bool = False):
    tables = ["game_batting_stats", "game_pitching_stats", "game_lineups"]
    
    with SessionLocal() as session:
        for table in tables:
            print(f"\n--- Processing {table} ---")
            # Find NULL player_id rows and join with team_daily_roster
            # We match by game_date, team_code, and player_name
            # Note: team_daily_roster.roster_date is DATE, game.game_date is DATE
            
            team_mapping = {
                "KH": "WO",
                "WO": "KH",
                "KIA": "HT",
                "HT": "KIA",
                "DB": "OB",
                "OB": "DB",
                "SSG": "SK",
                "SK": "SSG"
            }
            
            # We'll try both the original team_code and its alias
            query = text(f"""
                SELECT t.id, g.game_date, t.team_code, t.player_name, r.player_id as resolved_id
                FROM {table} t
                JOIN game g ON g.game_id = t.game_id
                JOIN team_daily_roster r ON r.roster_date = g.game_date 
                    AND (r.team_code = t.team_code OR r.team_code = CASE 
                        WHEN t.team_code = 'KH' THEN 'WO'
                        WHEN t.team_code = 'WO' THEN 'KH'
                        WHEN t.team_code = 'KIA' THEN 'HT'
                        WHEN t.team_code = 'HT' THEN 'KIA'
                        WHEN t.team_code = 'DB' THEN 'OB'
                        WHEN t.team_code = 'OB' THEN 'DB'
                        WHEN t.team_code = 'SSG' THEN 'SK'
                        WHEN t.team_code = 'SK' THEN 'SSG'
                        ELSE t.team_code END)
                    AND r.player_name = t.player_name
                WHERE t.player_id IS NULL
                AND g.game_date >= '2026-01-01'
            """)
            
            matches = session.execute(query).fetchall()
            print(f"Found {len(matches)} potential matches via roster.")
            
            updated = 0
            for row_id, g_date, t_code, p_name, res_id in matches:
                if apply:
                    session.execute(
                        text(f"UPDATE {table} SET player_id = :pid WHERE id = :id"),
                        {"pid": res_id, "id": row_id}
                    )
                    updated += 1
                else:
                    if updated < 5:
                        print(f"  [DRY-RUN] {g_date} {t_code} {p_name} -> {res_id}")
                    updated += 1
            
            if apply:
                session.commit()
                print(f"✅ Updated {updated} rows in {table}.")
            else:
                print(f"🔍 Would update {updated} rows in {table}.")

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    resolve_by_roster(apply=args.apply)
