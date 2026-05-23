#!/usr/bin/env python3
"""Resolve remaining 2025 NULL player_id values in game stats tables."""
from __future__ import annotations

import os
import sys
import shutil
from datetime import datetime
from pathlib import Path
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal, DATABASE_URL

def backup_db():
    if not DATABASE_URL.startswith("sqlite:///"):
        print("Not a SQLite DB, skipping backup.")
        return
    db_path = Path(DATABASE_URL.removeprefix("sqlite:///"))
    if not db_path.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.parent / "backups" / f"kbo_dev_before_2025_null_resolution_{stamp}.db"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    print(f"✅ DB backed up to: {backup_path}")

def run_resolution():
    backup_db()

    with SessionLocal() as session:
        # 1. Resolve 이주형 (KH) -> 50167
        print("Resolving 이주형 (KH) -> 50167")
        q1_batting = text("""
            UPDATE game_batting_stats
            SET player_id = 50167
            WHERE player_name = '이주형' AND team_code = 'KH' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r1_batting = session.execute(q1_batting)
        print(f"  game_batting_stats updated: {r1_batting.rowcount} rows")

        q1_lineup = text("""
            UPDATE game_lineups
            SET player_id = 50167
            WHERE player_name = '이주형' AND team_code = 'KH' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r1_lineup = session.execute(q1_lineup)
        print(f"  game_lineups updated: {r1_lineup.rowcount} rows")

        # 2. Resolve 최원준 (KIA) -> 66606
        print("Resolving 최원준 (KIA) -> 66606")
        q2_batting = text("""
            UPDATE game_batting_stats
            SET player_id = 66606
            WHERE player_name = '최원준' AND team_code = 'KIA' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r2_batting = session.execute(q2_batting)
        print(f"  game_batting_stats updated: {r2_batting.rowcount} rows")

        q2_lineup = text("""
            UPDATE game_lineups
            SET player_id = 66606
            WHERE player_name = '최원준' AND team_code = 'KIA' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r2_lineup = session.execute(q2_lineup)
        print(f"  game_lineups updated: {r2_lineup.rowcount} rows")

        # 3. Resolve 김태혁 (LT) -> 76430
        print("Resolving 김태혁 (LT) -> 76430")
        q3_batting = text("""
            UPDATE game_batting_stats
            SET player_id = 76430
            WHERE player_name = '김태혁' AND team_code = 'LT' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r3_batting = session.execute(q3_batting)
        print(f"  game_batting_stats updated: {r3_batting.rowcount} rows")

        q3_pitching = text("""
            UPDATE game_pitching_stats
            SET player_id = 76430
            WHERE player_name = '김태혁' AND team_code = 'LT' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r3_pitching = session.execute(q3_pitching)
        print(f"  game_pitching_stats updated: {r3_pitching.rowcount} rows")

        q3_lineup = text("""
            UPDATE game_lineups
            SET player_id = 76430
            WHERE player_name = '김태혁' AND team_code = 'LT' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r3_lineup = session.execute(q3_lineup)
        print(f"  game_lineups updated: {r3_lineup.rowcount} rows")

        # 4. Resolve 이승현 (SS) Pitching -> 51454 (starter) or 60146 (reliever)
        print("Resolving 이승현 (SS) Pitching")
        q4_starter = text("""
            UPDATE game_pitching_stats
            SET player_id = 51454
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '2025%' AND is_starting = 1
        """)
        r4_starter = session.execute(q4_starter)
        print(f"  game_pitching_stats starters updated: {r4_starter.rowcount} rows")

        q4_reliever = text("""
            UPDATE game_pitching_stats
            SET player_id = 60146
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '2025%' AND is_starting = 0
        """)
        r4_reliever = session.execute(q4_reliever)
        print(f"  game_pitching_stats relievers updated: {r4_reliever.rowcount} rows")

        # 5. Resolve 이승현 (SS) Batting/Lineups -> 51454 on 2025-07-10, 60146 on 2025-07-24
        print("Resolving 이승현 (SS) Batting/Lineups")
        q5_batting_1 = text("""
            UPDATE game_batting_stats
            SET player_id = 51454
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '20250710%'
        """)
        r5_batting_1 = session.execute(q5_batting_1)
        print(f"  game_batting_stats 20250710 updated: {r5_batting_1.rowcount} rows")

        q5_batting_2 = text("""
            UPDATE game_batting_stats
            SET player_id = 60146
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '20250724%'
        """)
        r5_batting_2 = session.execute(q5_batting_2)
        print(f"  game_batting_stats 20250724 updated: {r5_batting_2.rowcount} rows")

        q5_lineup_1 = text("""
            UPDATE game_lineups
            SET player_id = 51454
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '20250710%'
        """)
        r5_lineup_1 = session.execute(q5_lineup_1)
        print(f"  game_lineups 20250710 updated: {r5_lineup_1.rowcount} rows")

        q5_lineup_2 = text("""
            UPDATE game_lineups
            SET player_id = 60146
            WHERE player_name = '이승현' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '20250724%'
        """)
        r5_lineup_2 = session.execute(q5_lineup_2)
        print(f"  game_lineups 20250724 updated: {r5_lineup_2.rowcount} rows")

        # 6. Resolve 박시후 (SSG) -> 50812
        print("Resolving 박시후 (SSG) -> 50812")
        q6_pitching = text("""
            UPDATE game_pitching_stats
            SET player_id = 50812
            WHERE player_name = '박시후' AND team_code = 'SSG' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r6_pitching = session.execute(q6_pitching)
        print(f"  game_pitching_stats updated: {r6_pitching.rowcount} rows")

        # 7. Resolve 김태훈 (SS) Batting/Lineups -> 65040 (outfielder)
        print("Resolving 김태훈 (SS) Batting/Lineups -> 65040")
        q7_batting = text("""
            UPDATE game_batting_stats
            SET player_id = 65040
            WHERE player_name = '김태훈' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r7_batting = session.execute(q7_batting)
        print(f"  game_batting_stats updated: {r7_batting.rowcount} rows")

        q7_lineup = text("""
            UPDATE game_lineups
            SET player_id = 65040
            WHERE player_name = '김태훈' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r7_lineup = session.execute(q7_lineup)
        print(f"  game_lineups updated: {r7_lineup.rowcount} rows")

        # 8. Resolve 김태훈 (SS) Pitching -> 62360 (pitcher)
        print("Resolving 김태훈 (SS) Pitching -> 62360")
        q8_pitching = text("""
            UPDATE game_pitching_stats
            SET player_id = 62360
            WHERE player_name = '김태훈' AND team_code = 'SS' AND player_id IS NULL AND game_id LIKE '2025%'
        """)
        r8_pitching = session.execute(q8_pitching)
        print(f"  game_pitching_stats updated: {r8_pitching.rowcount} rows")

        # Commit all overrides so conservative resolver doesn't mix them up
        session.commit()
        print("🎉 Custom mappings committed successfully.")

if __name__ == "__main__":
    run_resolution()
