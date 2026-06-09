"""
Phase 1: Cleanup Ghost Duplicate Game Records (2010-2023)
Identifies and removes duplicate game records where one of the duplicates
has no associated player stats (Ghost data) or has redundant stats.
"""

from sqlalchemy import func

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat


def _delete_game_and_metadata(session, game_obj):
    """Helper to delete game and all its non-cascading children."""
    if game_obj.metadata_entry:
        session.delete(game_obj.metadata_entry)
    for r in game_obj.summary:
        session.delete(r)
    for r in game_obj.plays:
        session.delete(r)
    for r in game_obj.innings:
        session.delete(r)
    for r in game_obj.lineups:
        session.delete(r)
    for r in game_obj.events:
        session.delete(r)
    for r in game_obj.aliases:
        session.delete(r)
    session.delete(game_obj)


def cleanup_ghost_games():
    session = SessionLocal()
    try:
        print("🕵️  Searching for duplicate games (2010-2023)...")

        # 1. Identify duplicate (date, home, away) pairs
        duplicates = (
            session.query(Game.game_date, Game.home_team, Game.away_team, func.count(Game.id))
            .filter(func.strftime("%Y", Game.game_date).between("2010", "2023"))
            .group_by(Game.game_date, Game.home_team, Game.away_team)
            .having(func.count(Game.id) > 1)
            .all()
        )

        if not duplicates:
            print("✅ No duplicate games found in the 2010-2023 period.")
            return

        print(f"🔍 Found {len(duplicates)} duplicate game groups. Analyzing ghosts...")

        deleted_count = 0

        for g_date, home, away, _ in duplicates:
            # Get all game records for this pair
            games = session.query(Game).filter_by(game_date=g_date, home_team=home, away_team=away).all()

            game_stats_info = []
            for g in games:
                bat_count = session.query(func.count(GameBattingStat.id)).filter_by(game_id=g.game_id).scalar()
                pitch_count = session.query(func.count(GamePitchingStat.id)).filter_by(game_id=g.game_id).scalar()
                game_stats_info.append(
                    {
                        "game": g,
                        "bat_count": bat_count,
                        "pitch_count": pitch_count,
                        "total_stats": bat_count + pitch_count,
                    }
                )

            # Sort by total stats descending
            game_stats_info.sort(key=lambda x: x["total_stats"], reverse=True)

            # Keep the one with the most stats
            master = game_stats_info[0]
            ghosts = game_stats_info[1:]

            for ghost in ghosts:
                game_obj = ghost["game"]
                if ghost["total_stats"] == 0:
                    print(f"   🗑️ Deleting ghost game {game_obj.game_id} ({g_date} {away}@{home}) - 0 stats found.")
                    _delete_game_and_metadata(session, game_obj)
                    deleted_count += 1
                else:
                    # Both have stats. Identical redundant data case.
                    if master["total_stats"] > 0:
                        print(
                            f"   🗑️ Deleting redundant game WITH STATS {game_obj.game_id} ({g_date} {away}@{home}) - stats count {ghost['total_stats']}."
                        )

                        # Explicitly delete stats of the redundant record
                        for s in game_obj.batting_stats:
                            session.delete(s)
                        for s in game_obj.pitching_stats:
                            session.delete(s)

                        _delete_game_and_metadata(session, game_obj)
                        deleted_count += 1

        session.commit()
        print(f"✅ Cleanup complete. Total {deleted_count} records removed.")

    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    cleanup_ghost_games()
