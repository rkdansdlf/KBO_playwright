"""
Phase 2: Repair Season Metadata Boundaries (2010-2019)
Corrects incorrectly assigned season_ids for exhibition and postseason games
that were erroneously marked as Regular Season.
"""

from datetime import datetime

from src.db.engine import SessionLocal
from src.models.game import Game

# (Start Date, End Date, Exhibition ID, Postseason Bucket ID)
BOUNDARIES = {
    2023: ("2023-04-01", "2023-10-17", 248, 252),
    2022: ("2022-04-02", "2022-10-11", 242, 246),
    2021: ("2021-04-03", "2021-10-30", 236, 240),
    2020: ("2020-05-05", "2020-10-31", 230, 234),
    2019: ("2019-03-23", "2019-10-01", 224, 265),
    2018: ("2018-03-24", "2018-10-14", 218, 222),
    2017: ("2017-03-31", "2017-10-03", 212, 216),
    2016: ("2016-04-01", "2016-10-09", 206, 210),
    2015: ("2015-03-28", "2015-10-06", 200, 204),
    2014: ("2014-03-29", "2014-10-17", 194, 198),
    2013: ("2013-03-30", "2013-10-05", 188, 192),
    2012: ("2012-04-07", "2012-10-06", 182, 186),
    2011: ("2011-04-02", "2011-10-06", 176, 180),
    2010: ("2010-03-27", "2010-09-26", 170, 174),
}


def repair_season_boundaries():
    session = SessionLocal()
    try:
        print("🛠️  Repairing season boundaries (2010-2019)...")

        for year, (start, end, exhibition_id, postseason_id) in BOUNDARIES.items():
            start_date = datetime.strptime(start, "%Y-%m-%d").date()
            end_date = datetime.strptime(end, "%Y-%m-%d").date()

            # 1. Repair Exhibition Games (Before Start)
            exhibition_games = (
                session.query(Game)
                .filter(Game.game_date < start_date)
                .filter(Game.game_date >= datetime(year, 1, 1).date())
                .all()
            )

            ex_fixed = 0
            for g in exhibition_games:
                if g.season_id != exhibition_id:
                    g.season_id = exhibition_id
                    ex_fixed += 1

            # 2. Repair Postseason Games (After End)
            postseason_games = (
                session.query(Game)
                .filter(Game.game_date > end_date)
                .filter(Game.game_date <= datetime(year, 12, 31).date())
                .all()
            )

            ps_fixed = 0
            for g in postseason_games:
                if g.season_id != postseason_id:
                    # Generic bucket for now to avoid regular season bleed
                    g.season_id = postseason_id
                    ps_fixed += 1

            print(f"📅 {year} Season: {ex_fixed} exhibition fixed, {ps_fixed} postseason fixed.")

        session.commit()
        print("✅ Season metadata repair complete.")

    except Exception as e:
        print(f"❌ Error during repair: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    repair_season_boundaries()
