"""
Backfill 2020-2023 missing player statistics.
Uses PlayerDailyStatsCrawler to fetch high-fidelity transactional data (HR, BB, PA, etc.)
and updates the local transactional tables.
"""

import argparse
import asyncio
from collections import defaultdict

from sqlalchemy import text

from src.crawlers.player_daily_stats_crawler import PlayerDailyStatsCrawler
from src.db.engine import SessionLocal

TEAM_NAME_TO_CODE = {
    "두산": "DB",
    "OB": "DB",
    "삼성": "SS",
    "LG": "LG",
    "키움": "KH",
    "넥센": "KH",
    "히어로즈": "KH",
    "우리": "KH",
    "SSG": "SSG",
    "SK": "SK",
    "롯데": "LT",
    "한화": "HH",
    "KIA": "KIA",
    "HT": "KIA",
    "해태": "KIA",
    "KT": "KT",
    "NC": "NC",
}


def get_opponent_code(name: str) -> str | None:
    return TEAM_NAME_TO_CODE.get(name)


async def backfill_batting_stats(year: int, player_ids: list[int] | None = None):
    print(f"🛠️  Backfilling Batting Stats for {year}...")
    crawler = PlayerDailyStatsCrawler(headless=True)
    session = SessionLocal()

    try:
        query = f"""
            SELECT DISTINCT ps.player_id, pb.name
            FROM player_season_batting ps
            JOIN player_basic pb ON ps.player_id = pb.player_id
            WHERE ps.season = {year} AND ps.league = 'REGULAR'
            AND ps.player_id >= 50000
        """
        if player_ids:
            p_list = ",".join(map(str, player_ids))
            query += f" AND ps.player_id IN ({p_list})"

        players = session.execute(text(query)).fetchall()
        print(f"🔍 Found {len(players)} players to process.")

        for p_id, p_name in players:
            print(f"   👤 Processing {p_name} ({p_id})...")
            games = await crawler.crawl_player_season(p_id, False, year)
            if not games:
                continue

            day_opp_counts = defaultdict(int)
            update_count = 0
            for game in games:
                g_date = game["game_date"]
                stats = game["stats"]
                opp_name = game["opponent"]
                opp_code = get_opponent_code(opp_name)

                day_opp_counts[(g_date, opp_code)] += 1
                seq = day_opp_counts[(g_date, opp_code)]

                db_game_ids = session.execute(
                    text("""
                    SELECT game_id FROM game
                    WHERE game_date = :gdate
                    AND (home_team = :opp OR away_team = :opp)
                    ORDER BY game_id ASC
                """),
                    {"gdate": g_date, "opp": opp_code},
                ).fetchall()

                if not db_game_ids:
                    continue

                target_gid = db_game_ids[seq - 1][0] if len(db_game_ids) >= seq else db_game_ids[0][0]

                session.execute(
                    text("""
                    UPDATE game_batting_stats
                    SET plate_appearances = :pa, home_runs = :hr, walks = :bb, hbp = :hbp,
                        strikeouts = :so, gdp = :gdp, doubles = :d2, triples = :d3,
                        runs = :r, rbi = :rbi, stolen_bases = :sb, caught_stealing = :cs
                    WHERE player_id = :pid AND game_id = :gid
                """),
                    {
                        "pa": stats["plate_appearances"],
                        "hr": stats["home_runs"],
                        "bb": stats["walks"],
                        "hbp": stats["hbp"],
                        "so": stats["strikeouts"],
                        "gdp": stats["gdp"],
                        "d2": stats["doubles"],
                        "d3": stats["triples"],
                        "r": stats["runs"],
                        "rbi": stats["rbi"],
                        "sb": stats["stolen_bases"],
                        "cs": stats["caught_stealing"],
                        "pid": p_id,
                        "gid": target_gid,
                    },
                )
                update_count += 1

            session.commit()
            print(f"      ✅ Updated {update_count} games.")

    finally:
        session.close()


async def backfill_pitching_stats(year: int, player_ids: list[int] | None = None):
    print(f"🛠️  Backfilling Pitching Stats for {year}...")
    crawler = PlayerDailyStatsCrawler(headless=True)
    session = SessionLocal()

    try:
        query = f"""
            SELECT DISTINCT ps.player_id, pb.name
            FROM player_season_pitching ps
            JOIN player_basic pb ON ps.player_id = pb.player_id
            WHERE ps.season = {year} AND ps.league = 'REGULAR'
            AND ps.player_id >= 50000
        """
        if player_ids:
            p_list = ",".join(map(str, player_ids))
            query += f" AND ps.player_id IN ({p_list})"

        players = session.execute(text(query)).fetchall()
        print(f"🔍 Found {len(players)} pitchers to process.")

        for p_id, p_name in players:
            print(f"   👤 Processing {p_name} ({p_id})...")
            games = await crawler.crawl_player_season(p_id, True, year)
            if not games:
                continue

            day_opp_counts = defaultdict(int)
            update_count = 0
            for game in games:
                g_date = game["game_date"]
                stats = game["stats"]
                opp_name = game["opponent"]
                opp_code = get_opponent_code(opp_name)

                day_opp_counts[(g_date, opp_code)] += 1
                seq = day_opp_counts[(g_date, opp_code)]

                db_game_ids = session.execute(
                    text("""
                    SELECT game_id FROM game
                    WHERE game_date = :gdate
                    AND (home_team = :opp OR away_team = :opp)
                    ORDER BY game_id ASC
                """),
                    {"gdate": g_date, "opp": opp_code},
                ).fetchall()

                if not db_game_ids:
                    continue

                target_gid = db_game_ids[seq - 1][0] if len(db_game_ids) >= seq else db_game_ids[0][0]

                session.execute(
                    text("""
                    UPDATE game_pitching_stats
                    SET decision = :dec, wins = :w, losses = :l, saves = :s,
                        batters_faced = :tbf, innings_outs = :outs, hits_allowed = :h,
                        home_runs_allowed = :hr, walks_allowed = :bb, hit_batters = :hbp,
                        strikeouts = :so, runs_allowed = :r, earned_runs = :er
                    WHERE player_id = :pid AND game_id = :gid
                """),
                    {
                        "dec": stats["decision"],
                        "w": stats["wins"],
                        "l": stats["losses"],
                        "s": stats["saves"],
                        "tbf": stats["batters_faced"],
                        "outs": stats["innings_outs"],
                        "h": stats["hits_allowed"],
                        "hr": stats["home_runs_allowed"],
                        "bb": stats["walks_allowed"],
                        "hbp": stats["hbp_allowed"],
                        "so": stats["strikeouts"],
                        "r": stats["runs_allowed"],
                        "er": stats["earned_runs"],
                        "pid": p_id,
                        "gid": target_gid,
                    },
                )
                update_count += 1

            session.commit()
            print(f"      ✅ Updated {update_count} games.")

    finally:
        session.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2020)
    parser.add_argument("--player-id", type=int, default=None)
    parser.add_argument("--type", choices=["batting", "pitching", "both"], default="both")
    args = parser.parse_args()

    p_ids = [args.player_id] if args.player_id else None

    if args.type in ["batting", "both"]:
        await backfill_batting_stats(args.year, player_ids=p_ids)
    if args.type in ["pitching", "both"]:
        await backfill_pitching_stats(args.year, player_ids=p_ids)


if __name__ == "__main__":
    asyncio.run(main())
