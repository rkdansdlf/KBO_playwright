from sqlalchemy import func

from src.db.engine import SessionLocal
from src.models.standings import TeamStandingsDaily
from src.models.team_history import TeamHistory


def sync_history_ranking():
    with SessionLocal() as session:
        # Get latest standings for 2026
        max_date = (
            session.query(func.max(TeamStandingsDaily.standings_date))
            .filter(TeamStandingsDaily.standings_date.like("2026%"))
            .scalar()
        )

        if not max_date:
            print("No standings found for 2026.")
            return

        print(f"Syncing rankings from {max_date} to team_history (2026)...")

        standings = (
            session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.standings_date == max_date)
            .order_by(TeamStandingsDaily.win_pct.desc(), TeamStandingsDaily.wins.desc())
            .all()
        )

        for idx, s in enumerate(standings, start=1):
            # Update team_history.ranking where season=2026 and team_code=s.team_code
            hist = (
                session.query(TeamHistory)
                .filter(TeamHistory.season == 2026, TeamHistory.team_code == s.team_code)
                .first()
            )

            if hist:
                hist.ranking = idx
                print(f"   {s.team_code}: {idx}")
            else:
                print(f"   ⚠️ TeamHistory record not found for {s.team_code} (2026)")

        session.commit()
        print("✅ Sync complete.")


if __name__ == "__main__":
    sync_history_ranking()
