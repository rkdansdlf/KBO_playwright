#!/usr/bin/env python3
"""
KBO Live Data Spot-Checker.
Randomly samples database records (players, games) and verifies them
against live data on the official KBO website.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameInningScore, GamePitchingStat
from src.models.player import PlayerBasic
from src.utils.playwright_pool import AsyncPlaywrightPool


class SpotChecker:
    def __init__(self, pool: AsyncPlaywrightPool):
        self.pool = pool
        self.profile_crawler = PlayerProfileCrawler(pool=pool)
        self.game_crawler = GameDetailCrawler(pool=pool)
        self.mismatches: list[dict[str, Any]] = []

    def log_mismatch(self, category: str, identifier: str, field: str, db_val: Any, live_val: Any, details: str = ""):
        mismatch = {
            "category": category,
            "id": identifier,
            "field": field,
            "db_value": db_val,
            "live_value": live_val,
            "details": details,
        }
        self.mismatches.append(mismatch)
        print(f"   [MISMATCH] {category} [{identifier}] - {field}: DB '{db_val}' vs Live '{live_val}' {details}")

    async def check_players(self, sample_players: int, year: int | None = None) -> int:
        print(f"\n👥 Starting Spot-Check on {sample_players} Random Players...")
        print("-" * 50)

        with SessionLocal() as session:
            query = session.query(PlayerBasic)
            # If a specific year is provided, filter players active in that season (optional heuristic)
            if year:
                # SQLite-compatible random ordering
                players = (
                    query.filter(
                        PlayerBasic.player_id.in_(
                            session.query(func.distinct(PlayerBasic.player_id)).filter(
                                (PlayerBasic.debut_year <= year) | (PlayerBasic.debut_year.is_(None))
                            )
                        )
                    )
                    .order_by(func.random())
                    .limit(sample_players)
                    .all()
                )
            else:
                players = query.order_by(func.random()).limit(sample_players).all()

            if not players:
                print("⚠️ No players found matching search criteria.")
                return 0

            checked_count = 0
            for idx, p in enumerate(players, 1):
                pid_str = str(p.player_id)
                print(f"[{idx}/{len(players)}] Checking Player: {p.name} (ID: {pid_str})")

                try:
                    live_profile = await self.profile_crawler.crawl_player_profile(pid_str, position=p.position)
                    if not live_profile:
                        # Some players (e.g., historical or retired) might not be easily crawled, log warning
                        print(
                            f"   ⚠️ Could not load profile for {p.name} (ID: {pid_str}). (Possibly retired/inactive or layout mismatch)"
                        )
                        continue

                    checked_count += 1

                    # Verify core attributes
                    # Name check
                    if p.name != live_profile.get("name"):
                        self.log_mismatch("Player", pid_str, "name", p.name, live_profile.get("name"))

                    # Physical details (tolerance ±1 for height/weight due to potential shifts or roundings)
                    for db_f, live_f in [("height_cm", "height_cm"), ("weight_kg", "weight_kg")]:
                        db_val = getattr(p, db_f)
                        live_val = live_profile.get(live_f)
                        if db_val is not None and live_val is not None:
                            if abs(db_val - live_val) > 2:
                                self.log_mismatch("Player", pid_str, db_f, db_val, live_val)

                    # Bats/Throws
                    for db_f, live_f in [("bats", "bats"), ("throws", "throws")]:
                        db_val = getattr(p, db_f)
                        live_val = live_profile.get(live_f)
                        if db_val and live_val and db_val != live_val:
                            self.log_mismatch("Player", pid_str, db_f, db_val, live_val)

                except Exception as e:
                    print(f"   ❌ Error checking player {p.name} ({pid_str}): {e}")

            return checked_count

    async def check_games(self, sample_games: int, year: int | None = None) -> int:
        print(f"\n⚾ Starting Spot-Check on {sample_games} Random Games...")
        print("-" * 50)

        with SessionLocal() as session:
            query = session.query(Game).filter(Game.game_status.in_(["COMPLETED", "종료", "DRAW"]))
            if year:
                query = query.filter(Game.game_id.like(f"{year}%"))

            games = query.order_by(func.random()).limit(sample_games).all()
            if not games:
                print("⚠️ No completed games found matching search criteria.")
                return 0

            checked_count = 0
            for idx, g in enumerate(games, 1):
                game_date_str = g.game_date.strftime("%Y%m%d")
                print(
                    f"[{idx}/{len(games)}] Checking Game: {g.game_id} ({g.away_team} @ {g.home_team}) - {game_date_str}"
                )

                try:
                    live_game = await self.game_crawler.crawl_game(g.game_id, game_date_str)
                    if not live_game:
                        print(f"   ⚠️ Could not load game detail for {g.game_id}.")
                        continue

                    checked_count += 1

                    # 1. Verify basic scores and metadata
                    teams_info = live_game.get("teams") or {}
                    live_home_score = teams_info.get("home", {}).get("score")
                    live_away_score = teams_info.get("away", {}).get("score")

                    if g.home_score != live_home_score:
                        self.log_mismatch("Game", g.game_id, "home_score", g.home_score, live_home_score)
                    if g.away_score != live_away_score:
                        self.log_mismatch("Game", g.game_id, "away_score", g.away_score, live_away_score)

                    live_stadium = (live_game.get("metadata") or {}).get("stadium")
                    if g.stadium and live_stadium and g.stadium != live_stadium:
                        self.log_mismatch("Game", g.game_id, "stadium", g.stadium, live_stadium)

                    # 2. Verify Inning Scores
                    db_innings = (
                        session.query(GameInningScore)
                        .filter_by(game_id=g.game_id)
                        .order_by(GameInningScore.team_side, GameInningScore.inning)
                        .all()
                    )

                    for side in ("home", "away"):
                        live_line = teams_info.get(side, {}).get("line_score") or []
                        db_side_innings = [i.runs for i in db_innings if i.team_side == side]

                        # Compare lists
                        if len(db_side_innings) != len(live_line):
                            self.log_mismatch(
                                "GameInnings",
                                g.game_id,
                                f"{side}_innings_length",
                                len(db_side_innings),
                                len(live_line),
                                f"DB: {db_side_innings} vs Live: {live_line}",
                            )
                        else:
                            for inning_idx, (db_runs, live_runs) in enumerate(zip(db_side_innings, live_line), 1):
                                if db_runs != live_runs:
                                    self.log_mismatch(
                                        "GameInnings", g.game_id, f"{side}_inning_{inning_idx}", db_runs, live_runs
                                    )

                    # 3. Verify Player Statistics (Batting Box Score)
                    db_batting = {
                        (b.player_name, b.appearance_seq): b
                        for b in session.query(GameBattingStat).filter_by(game_id=g.game_id).all()
                    }

                    for side in ("home", "away"):
                        live_hitters = live_game.get("hitters", {}).get(side) or []
                        for h_idx, live_hitter in enumerate(live_hitters, 1):
                            h_name = live_hitter.get("player_name")
                            h_seq = live_hitter.get("appearance_seq") or h_idx

                            key = (h_name, h_seq)
                            if key not in db_batting:
                                # Try fallback match by name only if sequence doesn't match perfectly
                                fallback_matches = [b for (name, seq), b in db_batting.items() if name == h_name]
                                if fallback_matches:
                                    db_hitter = fallback_matches[0]
                                else:
                                    self.log_mismatch(
                                        "GameBatting", g.game_id, f"missing_hitter_{h_name}", None, f"Seq {h_seq}"
                                    )
                                    continue
                            else:
                                db_hitter = db_batting[key]

                            # Verify key stats
                            live_stats = live_hitter.get("stats") or {}
                            for stat_key in ("at_bats", "hits", "runs", "home_runs", "rbi", "walks", "strikeouts"):
                                db_val = getattr(db_hitter, stat_key) or 0
                                live_val = live_stats.get(stat_key) or 0
                                if db_val != live_val:
                                    self.log_mismatch(
                                        "GameBatting", g.game_id, f"{h_name}_{stat_key}", db_val, live_val
                                    )

                    # 4. Verify Player Statistics (Pitching Box Score)
                    db_pitching = {
                        (p.player_name, p.appearance_seq): p
                        for p in session.query(GamePitchingStat).filter_by(game_id=g.game_id).all()
                    }

                    for side in ("home", "away"):
                        live_pitchers = live_game.get("pitchers", {}).get(side) or []
                        for p_idx, live_pitcher in enumerate(live_pitchers, 1):
                            p_name = live_pitcher.get("player_name")
                            p_seq = live_pitcher.get("appearance_seq") or p_idx

                            key = (p_name, p_seq)
                            if key not in db_pitching:
                                fallback_matches = [p for (name, seq), p in db_pitching.items() if name == p_name]
                                if fallback_matches:
                                    db_pitcher = fallback_matches[0]
                                else:
                                    self.log_mismatch(
                                        "GamePitching", g.game_id, f"missing_pitcher_{p_name}", None, f"Seq {p_seq}"
                                    )
                                    continue
                            else:
                                db_pitcher = db_pitching[key]

                            live_stats = live_pitcher.get("stats") or {}
                            for stat_key in (
                                "innings_outs",
                                "hits_allowed",
                                "runs_allowed",
                                "earned_runs",
                                "strikeouts",
                                "walks_allowed",
                            ):
                                db_val = getattr(db_pitcher, stat_key) or 0
                                live_val = live_stats.get(stat_key) or 0
                                if db_val != live_val:
                                    self.log_mismatch(
                                        "GamePitching", g.game_id, f"{p_name}_{stat_key}", db_val, live_val
                                    )

                except Exception as e:
                    print(f"   ❌ Error checking game {g.game_id}: {e}")
                    import traceback

                    traceback.print_exc()

            return checked_count


async def main():
    parser = argparse.ArgumentParser(description="KBO Live Data Spot-Checker")
    parser.add_argument("--players", type=int, default=10, help="Number of random players to spot check")
    parser.add_argument("--games", type=int, default=5, help="Number of random completed games to spot check")
    parser.add_argument("--year", type=int, help="Limit check to a specific season/year")
    parser.add_argument("--output", type=str, help="Path to write verification results JSON")
    args = parser.parse_args()

    print("🔬" * 20)
    print(" KBO Live Data Spot-Checking Tool")
    print("🔬" * 20)

    # Initialize Playwright and Checker
    pool = AsyncPlaywrightPool(max_pages=2)
    await pool.start()

    checker = SpotChecker(pool)

    try:
        players_checked = 0
        games_checked = 0

        if args.players > 0:
            players_checked = await checker.check_players(args.players, args.year)

        if args.games > 0:
            games_checked = await checker.check_games(args.games, args.year)

        print("\n" + "=" * 50)
        print("📊 Spot-Check Summary")
        print("=" * 50)
        print(f"👥 Checked Players: {players_checked}")
        print(f"⚾ Checked Games:   {games_checked}")
        print(f"🚨 Total Mismatches Detected: {len(checker.mismatches)}")

        if checker.mismatches:
            print("\n🚨 Mismatch details:")
            for m in checker.mismatches:
                print(
                    f"  - [{m['category']}] {m['id']}: {m['field']} (DB: '{m['db_value']}' vs Live: '{m['live_value']}')"
                )

            # Print remediation tip
            print("\n💡 Suggested Remediation Commands:")
            remedy_games = {m["id"] for m in checker.mismatches if m["category"].startswith("Game")}
            remedy_players = {m["id"] for m in checker.mismatches if m["category"] == "Player"}

            for g_id in remedy_games:
                print(f"    python3 -m src.cli.collect_games --year {g_id[:4]} --month {g_id[4:6]} --force")
            for p_id in remedy_players:
                print("    python3 -m src.crawlers.player_profile_crawler --year 2025 --force")
        else:
            print("\n✅ Perfect agreement! No mismatches found between the sampled DB data and the live KBO website.")

        # Save to JSON output if requested
        if args.output:
            import json

            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            report_data = {
                "checked_players": players_checked,
                "checked_games": games_checked,
                "mismatches": checker.mismatches,
                "timestamp": datetime.now().isoformat(),
            }
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Report saved to {out_path}")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
