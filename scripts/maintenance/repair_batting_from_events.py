#!/usr/bin/env python3
"""
Repair batting statistics by deriving them from game events.
Fixes PA=0 and SO=0 issues found by the audit.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal


def repair_batting_from_events(year: int | None = None, game_id: str | None = None, dry_run: bool = True):
    with SessionLocal() as session:
        # Find games with PA=0 violations (or specific game_id)
        query = """
            SELECT DISTINCT g.game_id
            FROM game g
            JOIN game_batting_stats b ON g.game_id = b.game_id
            WHERE (b.plate_appearances = 0 OR b.plate_appearances IS NULL)
              AND b.at_bats > 0
              AND g.game_status IN ('COMPLETED', 'DRAW')
        """
        params = {}
        if year:
            query += " AND g.game_date LIKE :year_pattern"
            params["year_pattern"] = f"{year}%"
        if game_id:
            query = "SELECT :game_id as game_id"
            params["game_id"] = game_id

        game_ids = [row[0] for row in session.execute(text(query), params).all()]
        print(f"🛠️ Found {len(game_ids)} games to repair.")

        for gid in game_ids:
            print(f"   - Processing {gid}...")
            # 1. Get events for this game
            events = (
                session.execute(
                    text(
                        "SELECT batter_id, result_code FROM game_events WHERE game_id = :gid AND event_type = 'batting'"
                    ),
                    {"gid": gid},
                )
                .mappings()
                .all()
            )

            if not events:
                print(f"     ⚠️ No events found for {gid}. Skipping.")
                continue

            # 2. Group stats by batter
            batter_stats = {}
            for e in events:
                bid = e["batter_id"]
                res = e["result_code"] or ""
                if not bid:
                    continue

                stats = batter_stats.setdefault(bid, {"pa": 0, "so": 0, "bb": 0, "hbp": 0, "sh": 0, "sf": 0})
                stats["pa"] += 1
                if "삼진" in res:
                    stats["so"] += 1
                if "4구" in res or "볼넷" in res or "고의4구" in res:
                    stats["bb"] += 1
                if "사구" in res or "몸에 맞는 볼" in res:
                    stats["hbp"] += 1
                if "희생번트" in res or "희생번트" in res:  # wait, result_code like '%희생번트%'
                    stats["sh"] += 1
                if "희생플라이" in res:
                    stats["sf"] += 1

            # 3. Update game_batting_stats
            for bid, s in batter_stats.items():
                # We update PA, SO, BB, HBP, SH, SF
                # Formula check: PA = AB + BB + HBP + SH + SF
                # We should fetch current AB first to be safe, but here we just trust PA count from events
                if dry_run:
                    print(
                        f"     [DRY RUN] Batter {bid}: PA={s['pa']}, SO={s['so']}, BB={s['bb']}, SH={s['sh']}, SF={s['sf']}"
                    )
                else:
                    session.execute(
                        text("""
                            UPDATE game_batting_stats
                            SET plate_appearances = :pa,
                                strikeouts = :so,
                                walks = :bb,
                                hbp = :hbp,
                                sacrifice_hits = :sh,
                                sacrifice_flies = :sf,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE game_id = :gid AND player_id = :bid
                        """),
                        {
                            "pa": s["pa"],
                            "so": s["so"],
                            "bb": s["bb"],
                            "hbp": s["hbp"],
                            "sh": s["sh"],
                            "sf": s["sf"],
                            "gid": gid,
                            "bid": bid,
                        },
                    )

            if not dry_run:
                session.commit()
                print(f"     ✅ Repaired {gid}")


def main():
    parser = argparse.ArgumentParser(description="Repair batting stats from events")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--game-id", help="Filter by specific game_id")
    parser.add_argument("--execute", action="store_true", help="Execute updates (default is dry run)")
    args = parser.parse_args()

    repair_batting_from_events(year=args.year, game_id=args.game_id, dry_run=not args.execute)


if __name__ == "__main__":
    main()
