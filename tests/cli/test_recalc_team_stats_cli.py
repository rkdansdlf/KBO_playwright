import unittest
from datetime import date
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Setup test imports
from src.models.base import Base
from src.models.team import Team
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.cli.recalc_team_stats import run_recalc


class TestRecalcTeamStatsCLI(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite DB
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        # Patch SessionLocal across necessary modules
        import src.db.engine
        import src.cli.recalc_team_stats
        import src.repositories.team_stats_repository

        self.orig_engine_session = src.db.engine.SessionLocal
        self.orig_cli_session = src.cli.recalc_team_stats.SessionLocal
        self.orig_repo_session = src.repositories.team_stats_repository.SessionLocal

        src.db.engine.SessionLocal = self.Session
        src.cli.recalc_team_stats.SessionLocal = self.Session
        src.repositories.team_stats_repository.SessionLocal = self.Session

        # Mock Dialect name for repositories to match sqlite
        import src.repositories.team_stats_repository

        self.orig_dialect = src.repositories.team_stats_repository.Engine.dialect.name
        src.repositories.team_stats_repository.Engine.dialect.name = "sqlite"

        # Populate initial test metadata (Teams)
        with self.Session() as session:
            t1 = Team(team_id="LG", team_name="트윈스", team_short_name="LG", city="서울", is_active=True)
            t2 = Team(team_id="OB", team_name="베어스", team_short_name="두산", city="서울", is_active=True)
            session.add_all([t1, t2])
            session.commit()

    def tearDown(self):
        # Restore original sessions
        import src.db.engine
        import src.cli.recalc_team_stats
        import src.repositories.team_stats_repository

        src.db.engine.SessionLocal = self.orig_engine_session
        src.cli.recalc_team_stats.SessionLocal = self.orig_cli_session
        src.repositories.team_stats_repository.SessionLocal = self.orig_repo_session
        src.repositories.team_stats_repository.Engine.dialect.name = self.orig_dialect

    def test_recalc_dry_run_does_not_modify_db(self):
        # Insert mock player data
        with self.Session() as session:
            pb = PlayerBasic(player_id=99999, name="선수A")
            session.add(pb)
            p = PlayerSeasonBatting(
                id=1,
                player_id=99999,
                season=2025,
                team_code="LG",
                games=10,
                plate_appearances=20,
                at_bats=18,
                hits=6,
                league="REGULAR",
            )
            session.add(p)
            session.commit()

        # Run dry run
        exit_code = run_recalc(season=2025, dry_run=True)
        self.assertEqual(exit_code, 0)

        # Assert no rows written to team_season_batting
        with self.Session() as session:
            count = session.query(TeamSeasonBatting).count()
            self.assertEqual(count, 0)

    def test_recalc_active_run_upserts_correctly_and_is_idempotent(self):
        # Insert mock player data
        with self.Session() as session:
            pb = PlayerBasic(player_id=99999, name="선수A")
            session.add(pb)
            p_bat = PlayerSeasonBatting(
                id=1,
                player_id=99999,
                season=2025,
                team_code="LG",
                games=10,
                plate_appearances=20,
                at_bats=18,
                hits=6,
                league="REGULAR",
            )
            p_pit = PlayerSeasonPitching(
                id=1,
                player_id=99999,
                season=2025,
                team_code="LG",
                games=5,
                innings_outs=15,
                earned_runs=2,
                league="REGULAR",
            )
            session.add_all([p_bat, p_pit])
            session.commit()

        # First run: writes to DB
        exit_code = run_recalc(season=2025, dry_run=False)
        self.assertEqual(exit_code, 0)

        with self.Session() as session:
            bat_count = session.query(TeamSeasonBatting).count()
            pit_count = session.query(TeamSeasonPitching).count()
            self.assertEqual(bat_count, 1)
            self.assertEqual(pit_count, 1)

            bat_row = session.query(TeamSeasonBatting).first()
            pit_row = session.query(TeamSeasonPitching).first()

            self.assertEqual(bat_row.team_name, "트윈스")
            self.assertEqual(bat_row.at_bats, 18)
            self.assertEqual(bat_row.hits, 6)
            self.assertAlmostEqual(bat_row.avg, 0.333, places=3)
            self.assertEqual(bat_row.extra_stats.get("source"), "player_rollup")

            self.assertEqual(pit_row.team_name, "트윈스")
            self.assertEqual(pit_row.innings_pitched, 5.0)
            self.assertEqual(pit_row.earned_runs, 2)
            self.assertAlmostEqual(pit_row.era, 3.60, places=2)
            self.assertEqual(pit_row.extra_stats.get("source"), "player_rollup")

        # Second run: must run cleanly (upsert/idempotent check)
        exit_code2 = run_recalc(season=2025, dry_run=False)
        self.assertEqual(exit_code2, 0)

        with self.Session() as session:
            self.assertEqual(session.query(TeamSeasonBatting).count(), 1)
            self.assertEqual(session.query(TeamSeasonPitching).count(), 1)


if __name__ == "__main__":
    unittest.main()
