import argparse

from sqlalchemy import delete, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GameLineup, GamePitchingStat
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)


def merge_player(session: Session, source_id: int, target_id: int):
    """Merges source_id into target_id for all game and season stat models, then deletes source_id."""
    print(f"Merging temporary ID {source_id} -> Official KBO ID {target_id}")

    # 1. Update Game Batting Stats
    session.execute(update(GameBattingStat).where(GameBattingStat.player_id == source_id).values(player_id=target_id))

    # 2. Update Game Pitching Stats
    session.execute(update(GamePitchingStat).where(GamePitchingStat.player_id == source_id).values(player_id=target_id))

    # 3. Update Game Lineups
    session.execute(update(GameLineup).where(GameLineup.player_id == source_id).values(player_id=target_id))

    # 4. Update Season Stats (Batting, Pitching, Fielding, Baserunning)
    # If update fails due to UniqueConstraint, it means official profile already has that season record.
    # In that case, we delete the temporary profile's season record to satisfy FK when deleting PlayerBasic.
    for model, label in [
        (PlayerSeasonBatting, "Batting"),
        (PlayerSeasonPitching, "Pitching"),
        (PlayerSeasonFielding, "Fielding"),
        (PlayerSeasonBaserunning, "Baserunning"),
    ]:
        try:
            updated = session.execute(
                update(model).where(model.player_id == source_id).values(player_id=target_id)
            ).rowcount
            print(f"  - Season {label} updated: {updated}")
        except SQLAlchemyError:
            deleted = session.execute(delete(model).where(model.player_id == source_id)).rowcount
            print(f"  - Season {label} merged (deleted pseudo): {deleted}")

    session.commit()  # Commit updates/deletes in other tables first

    # 5. Delete temporary PlayerBasic profile
    session.execute(delete(PlayerBasic).where(PlayerBasic.player_id == source_id))
    session.commit()
    print(f"✅ Deleted temporary profile {source_id}")


def run_merge(auto_confirm: bool = False):
    session = SessionLocal()
    try:
        # Find all pseudo-IDs (>= 900000)
        pseudos = session.execute(select(PlayerBasic).where(PlayerBasic.player_id >= 900000)).scalars().all()

        if not pseudos:
            print("ℹ️ No temporary pseudo-IDs found in the database. Everything is clean!")
            return

        print(f"🔍 Found {len(pseudos)} temporary profiles.")

        for pseudo in pseudos:
            print(f"\nEvaluating Pseudo Profile: [{pseudo.player_id}] {pseudo.name} (Team: {pseudo.team})")

            # Find matching official profiles
            stmt = select(PlayerBasic).where(
                PlayerBasic.name == pseudo.name, PlayerBasic.team == pseudo.team, PlayerBasic.player_id < 900000
            )
            candidates = session.execute(stmt).scalars().all()

            if not candidates:
                print("  ⚠️ No official KBO matching profile found yet. Skipping.")
                continue

            if len(candidates) > 1:
                # Need to disambiguate by uniform_no or other metrics
                matched = [c for c in candidates if c.uniform_no == pseudo.uniform_no]
                if len(matched) == 1:
                    candidates = matched
                else:
                    print("  ⚠️ Ambiguous multiple official profiles found. Skipping.")
                    for c in candidates:
                        print(f"      - {c.player_id}: {c.name} (Uni: {c.uniform_no})")
                    continue

            target = candidates[0]
            print(f"  🎯 Found Official Match: [{target.player_id}] {target.name} (Uni: {target.uniform_no})")

            if not auto_confirm:
                ans = input(f"Proceed with merging {pseudo.player_id} into {target.player_id}? (y/n): ")
                if ans.lower() != "y":
                    print("  Skipping.")
                    continue

            merge_player(session, pseudo.player_id, target.player_id)

    except Exception as e:
        session.rollback()
        print(f"❌ Fatal Error during merge: {e}")
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Merge pseudo player IDs to official KBO IDs")
    parser.add_argument("--yes", "-y", action="store_true", help="Auto-confirm all unique merges")
    args = parser.parse_args()

    run_merge(auto_confirm=args.yes)


if __name__ == "__main__":
    main()
