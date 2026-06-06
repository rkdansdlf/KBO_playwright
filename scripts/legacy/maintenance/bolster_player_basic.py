from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GamePitchingStat
from src.models.player import PlayerBasic


def bolster_player_basic():
    with SessionLocal() as session:
        # 1. Find IDs in advanced stats but missing in player_basic
        # We check both Baserunning and Fielding
        from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding

        missing_ids = set()
        br_ids = (
            session.query(PlayerSeasonBaserunning.player_id)
            .filter(~PlayerSeasonBaserunning.player_id.in_(session.query(PlayerBasic.player_id)))
            .distinct()
            .all()
        )
        for (pid,) in br_ids:
            missing_ids.add(pid)

        fld_ids = (
            session.query(PlayerSeasonFielding.player_id)
            .filter(~PlayerSeasonFielding.player_id.in_(session.query(PlayerBasic.player_id)))
            .distinct()
            .all()
        )
        for (pid,) in fld_ids:
            missing_ids.add(pid)

        print(f"🔍 Found {len(missing_ids)} missing players in player_basic: {missing_ids}")

        for pid in missing_ids:
            if not pid:
                continue

            # Try to find name and team from GameBattingStat
            name = "Unknown"
            team = "Unknown"

            bat = session.query(GameBattingStat.player_name, GameBattingStat.team_code).filter_by(player_id=pid).first()
            if bat:
                name, team = bat
            else:
                pit = (
                    session.query(GamePitchingStat.player_name, GamePitchingStat.team_code)
                    .filter_by(player_id=pid)
                    .first()
                )
                if pit:
                    name, team = pit

            print(f"   ✨ Creating stub for {name} ({pid}) [{team}]")
            stub = PlayerBasic(player_id=pid, name=name, team=team, status="STUB", status_source="AUTO_BOLSTER")
            session.add(stub)

        session.commit()
        print("✅ Bolstering complete.")


if __name__ == "__main__":
    bolster_player_basic()
