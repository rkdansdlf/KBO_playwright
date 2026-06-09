import logging
import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from sqlalchemy import text

from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_player_candidates(session, name, season, team_code):
    """Get all candidate player IDs for a name in a given season/team."""
    # Check batting
    batters = session.execute(
        text("""
        SELECT psb.player_id, 'B' as type, pb.position as main_pos
        FROM player_season_batting psb
        JOIN player_basic pb ON psb.player_id = pb.player_id
        WHERE pb.name = :name AND psb.season = :season AND psb.team_code = :team_code
    """),
        {"name": name, "season": season, "team_code": team_code},
    ).fetchall()

    # Check pitching
    pitchers = session.execute(
        text("""
        SELECT psp.player_id, 'P' as type, pb.position as main_pos
        FROM player_season_pitching psp
        JOIN player_basic pb ON psp.player_id = pb.player_id
        WHERE pb.name = :name AND psp.season = :season AND psp.team_code = :team_code
    """),
        {"name": name, "season": season, "team_code": team_code},
    ).fetchall()

    candidates = {}
    for r in batters:
        candidates[r.player_id] = candidates.get(r.player_id, set())
        candidates[r.player_id].add("B")
    for r in pitchers:
        candidates[r.player_id] = candidates.get(r.player_id, set())
        candidates[r.player_id].add("P")

    return candidates


def repair_batting_collisions():
    session = SessionLocal()
    try:
        # 1. Find collision groups
        collisions = session.execute(
            text("""
            SELECT game_id, player_id, player_name, COUNT(*) as cnt
            FROM game_batting_stats
            GROUP BY game_id, player_id
            HAVING cnt > 1
        """)
        ).fetchall()

        logger.info(f"Found {len(collisions)} batting collision groups.")

        repaired_count = 0
        for game_id, current_player_id, name, _count in collisions:
            season = int(game_id[:4])

            # Get all rows for this collision
            rows = session.execute(
                text("""
                SELECT id, team_code, position
                FROM game_batting_stats
                WHERE game_id = :game_id AND player_id = :current_player_id
            """),
                {"game_id": game_id, "current_player_id": current_player_id},
            ).fetchall()

            team_code = rows[0].team_code
            candidates = get_player_candidates(session, name, season, team_code)

            if len(candidates) <= 1:
                logger.warning(
                    f"  Game {game_id}: {name} has {len(candidates)} candidates in season data. Cannot disambiguate easily."
                )
                continue

            logger.info(
                f"  Processing Game {game_id}: {name} ({len(candidates)} candidates: {list(candidates.keys())})"
            )

            # Disambiguation logic based on position
            # '투' (P) matches Pitcher candidate
            # Other positions match Batter candidate
            for row in rows:
                is_pitcher_slot = row.position == "투"
                possible_ids = []

                if is_pitcher_slot:
                    # Look for candidate who is a pitcher
                    possible_ids = [pid for pid, types in candidates.items() if "P" in types]
                else:
                    # Look for candidate who is a batter
                    possible_ids = [pid for pid, types in candidates.items() if "B" in types]

                if len(possible_ids) == 1:
                    target_id = possible_ids[0]
                    if target_id != current_player_id:
                        logger.info(
                            f"    -> Updating row {row.id} (pos {row.position}): {current_player_id} -> {target_id}"
                        )
                        session.execute(
                            text("UPDATE game_batting_stats SET player_id = :target_id WHERE id = :id"),
                            {"target_id": target_id, "id": row.id},
                        )
                        repaired_count += 1
                else:
                    logger.warning(
                        f"    Could not uniquely disambiguate row {row.id} (pos {row.position}). Candidates: {possible_ids}"
                    )

        session.commit()
        logger.info(f"Repaired {repaired_count} batting rows.")

        # 2. Game Lineups
        lineup_collisions = session.execute(
            text("""
            SELECT game_id, player_id, player_name, COUNT(*) as cnt
            FROM game_lineups
            GROUP BY game_id, player_id
            HAVING cnt > 1
        """)
        ).fetchall()

        logger.info(f"Found {len(lineup_collisions)} lineup collision groups.")

        repaired_lineup_count = 0
        for game_id, current_player_id, name, _count in lineup_collisions:
            season = int(game_id[:4])
            rows = session.execute(
                text("""
                SELECT id, team_code, position
                FROM game_lineups
                WHERE game_id = :game_id AND player_id = :current_player_id
            """),
                {"game_id": game_id, "current_player_id": current_player_id},
            ).fetchall()

            team_code = rows[0].team_code
            candidates = get_player_candidates(session, name, season, team_code)

            if len(candidates) <= 1:
                continue

            for row in rows:
                is_pitcher_slot = row.position == "투"
                possible_ids = [pid for pid, types in candidates.items() if ("P" if is_pitcher_slot else "B") in types]

                if len(possible_ids) == 1:
                    target_id = possible_ids[0]
                    if target_id != current_player_id:
                        session.execute(
                            text("UPDATE game_lineups SET player_id = :target_id WHERE id = :id"),
                            {"target_id": target_id, "id": row.id},
                        )
                        repaired_lineup_count += 1

        session.commit()
        logger.info(f"Repaired {repaired_lineup_count} lineup rows.")

    except Exception as e:  # noqa: BLE001
        session.rollback()
        logger.error(f"Error repairing batting collisions: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    repair_batting_collisions()
