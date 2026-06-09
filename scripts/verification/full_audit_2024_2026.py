import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
DB_PATH = Path("data/kbo_dev.db")


def run_audit():
    if not DB_PATH.exists():
        logger.info(f"DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    current_year = datetime.now().year
    years = [str(y) for y in range(current_year - 2, current_year + 1)]

    logger.info(f"=== KBO Stats Audit ({years[0]}-{years[-1]}) ===")

    for year in years:
        logger.info(f"\n--- Year: {year} ---")

        # 1. Batting Mismatch
        query_batting = f"""
        WITH game_agg AS (
            SELECT
                b.player_id,
                COUNT(DISTINCT b.game_id) as g_games,
                SUM(b.hits) as g_hits,
                SUM(b.at_bats) as g_ab,
                SUM(b.home_runs) as g_hr,
                SUM(b.rbi) as g_rbi
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE strftime('%Y', g.game_date) = '{year}'
            GROUP BY b.player_id
        ),
        season_agg AS (
            SELECT
                player_id,
                SUM(games) as s_games,
                SUM(hits) as s_hits,
                SUM(at_bats) as s_ab,
                SUM(home_runs) as s_hr,
                SUM(rbi) as s_rbi
            FROM player_season_batting
            WHERE season = {year}
            GROUP BY player_id
        )
        SELECT
            p.name,
            s.player_id,
            s.s_games, g.g_games,
            s.s_hits, g.g_hits,
            s.s_hr, g.g_hr
        FROM season_agg s
        LEFT JOIN game_agg g ON s.player_id = g.player_id
        JOIN player_basic p ON s.player_id = p.player_id
        WHERE s.s_hits != COALESCE(g.g_hits, 0)
           OR s.s_games != COALESCE(g.g_games, 0)
        LIMIT 10;
        """

        df_batting = pd.read_sql_query(query_batting, conn)
        logger.info("Batting Mismatches (Top 10):")
        if df_batting.empty:
            logger.info("  None found in this sample.")
        else:
            logger.info(df_batting.to_string(index=False))

        # Count total batting mismatches
        count_query = f"""
        SELECT COUNT(*)
        FROM player_season_batting s
        LEFT JOIN (
            SELECT player_id, SUM(hits) as g_hits, COUNT(DISTINCT b.game_id) as g_games
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE strftime('%Y', g.game_date) = '{year}'
            GROUP BY player_id
        ) g ON s.player_id = g.player_id
        WHERE s.season = {year} AND (COALESCE(s.hits, 0) != COALESCE(g.g_hits, 0) OR COALESCE(s.games, 0) != COALESCE(g.g_games, 0));
        """
        total_mismatch = conn.execute(count_query).fetchone()[0]
        total_players = conn.execute(f"SELECT COUNT(*) FROM player_season_batting WHERE season = {year}").fetchone()[0]
        logger.info(f"\nTotal Batting Mismatches: {total_mismatch} / {total_players} players")

        # 2. Pitching Mismatch
        query_pitching = f"""
        WITH game_agg AS (
            SELECT
                p_stats.player_id,
                COUNT(DISTINCT p_stats.game_id) as g_games,
                SUM(p_stats.innings_outs) as g_outs,
                SUM(p_stats.earned_runs) as g_er,
                SUM(p_stats.wins) as g_w,
                SUM(p_stats.losses) as g_l
            FROM game_pitching_stats p_stats
            JOIN game g ON p_stats.game_id = g.game_id
            WHERE strftime('%Y', g.game_date) = '{year}'
            GROUP BY p_stats.player_id
        ),
        season_agg AS (
            SELECT
                player_id,
                SUM(games) as s_games,
                SUM(innings_outs) as s_outs,
                SUM(earned_runs) as s_er,
                SUM(wins) as s_w,
                SUM(losses) as s_l
            FROM player_season_pitching
            WHERE season = {year}
            GROUP BY player_id
        )
        SELECT
            pb.name,
            s.player_id,
            s.s_games, g.g_games,
            s.s_outs, g.g_outs,
            s.s_w, g.g_w
        FROM season_agg s
        LEFT JOIN game_agg g ON s.player_id = g.player_id
        JOIN player_basic pb ON s.player_id = pb.player_id
        WHERE COALESCE(s.s_outs, 0) != COALESCE(g.g_outs, 0)
           OR COALESCE(s.s_games, 0) != COALESCE(g.g_games, 0)
        LIMIT 10;
        """
        df_pitching = pd.read_sql_query(query_pitching, conn)
        logger.info("\nPitching Mismatches (Top 10):")
        if df_pitching.empty:
            logger.info("  None found in this sample.")
        else:
            logger.info(df_pitching.to_string(index=False))

        # Count total pitching mismatches
        count_pitching_query = f"""
        SELECT COUNT(*)
        FROM player_season_pitching s
        LEFT JOIN (
            SELECT p_stats.player_id, SUM(p_stats.innings_outs) as g_outs, COUNT(DISTINCT p_stats.game_id) as g_games
            FROM game_pitching_stats p_stats
            JOIN game g ON p_stats.game_id = g.game_id
            WHERE strftime('%Y', g.game_date) = '{year}'
            GROUP BY p_stats.player_id
        ) g ON s.player_id = g.player_id
        WHERE s.season = {year} AND (COALESCE(s.innings_outs, 0) != COALESCE(g.g_outs, 0) OR COALESCE(s.games, 0) != COALESCE(g.g_games, 0));
        """
        total_p_mismatch = conn.execute(count_pitching_query).fetchone()[0]
        total_p_players = conn.execute(f"SELECT COUNT(*) FROM player_season_pitching WHERE season = {year}").fetchone()[
            0
        ]
        logger.info(f"Total Pitching Mismatches: {total_p_mismatch} / {total_p_players} players")

    conn.close()


if __name__ == "__main__":
    run_audit()
