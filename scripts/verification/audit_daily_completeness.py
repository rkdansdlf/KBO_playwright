#!/usr/bin/env python3
"""
Audit script to verify that all COMPLETED games have essential detail data.
Checks game_batting_stats, game_pitching_stats, and game_play_by_play.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def audit_completeness(db_url: str, lookback_days: int) -> int:
    engine = create_engine(db_url)

    # Calculate dates in Python to be dialect-agnostic
    today = date.today()
    start_date = (today - timedelta(days=lookback_days)).isoformat()
    end_date = today.isoformat()

    print(f"🔍 Auditing COMPLETED games from {start_date} to {end_date}...")

    # We use subqueries to count related records for each completed game.
    # This identifies "silent" gaps where a game is marked finished but collection failed.
    query = text("""
        SELECT g.game_date, g.game_id,
               (SELECT COUNT(*) FROM game_batting_stats b WHERE b.game_id = g.game_id) as hitter_cnt,
               (SELECT COUNT(*) FROM game_pitching_stats p WHERE p.game_id = g.game_id) as pitcher_cnt,
               (SELECT COUNT(*) FROM game_play_by_play p WHERE p.game_id = g.game_id) as relay_cnt
        FROM game g
        WHERE g.game_date >= :start_date
          AND g.game_date < :end_date
          AND g.game_status = 'COMPLETED'
        ORDER BY g.game_date DESC;
    """)

    failures = []
    game_count = 0

    try:
        with engine.connect() as conn:
            results = conn.execute(query, {"start_date": start_date, "end_date": end_date}).fetchall()
            game_count = len(results)

            for row in results:
                # row structure: (game_date, game_id, hitter_cnt, pitcher_cnt, relay_cnt)
                g_date = str(row[0])
                g_id = row[1]
                h_cnt = row[2]
                p_cnt = row[3]
                r_cnt = row[4]

                missing = []
                if h_cnt == 0:
                    missing.append("batting_stats")
                if p_cnt == 0:
                    missing.append("pitching_stats")
                if r_cnt == 0:
                    missing.append("play_by_play")

                if missing:
                    failures.append(f"  - [{g_date}] {g_id}: missing {', '.join(missing)}")

    except Exception as e:
        print(f"❌ Database error during audit: {e}")
        return 2

    if failures:
        print(f"❌ Found {len(failures)} incomplete games out of {game_count} checked:")
        for f in failures:
            print(f)
        print("\nPossible causes: Crawler timeout, KBO site structure change, or database connection issues.")
        print("Action: Run backfill for the missing game IDs.")
        return 1

    if game_count == 0:
        print(f"ℹ️  No completed games found in the last {lookback_days} days.")
    else:
        print(f"✅ All {game_count} completed games have full detail data.")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Audit daily game data completeness")
    parser.add_argument("--db-url", help="Database URL (can be env:VAR_NAME)")
    parser.add_argument("--days", type=int, default=14, help="Lookback days (default: 14)")
    args = parser.parse_args()

    load_dotenv()
    db_url = args.db_url
    if db_url and db_url.startswith("env:"):
        db_url = os.getenv(db_url[4:])

    if not db_url:
        # Fallback to local dev DB
        db_url = os.getenv("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"

    sys.exit(audit_completeness(db_url, args.days))


if __name__ == "__main__":
    main()
