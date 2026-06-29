import unittest
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

from src.cli.recalc_team_stats import (
    _log_dry_run_batting,
    _log_dry_run_pitching,
    _run_batting_recalc,
    _run_pitching_recalc,
    main,
    run_recalc,
)

# Setup test imports
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching


class TestRecalcTeamStatsCLI(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite DB
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        # Patch SessionLocal across necessary modules
        import src.cli.recalc_team_stats
        import src.db.engine
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
        import src.cli.recalc_team_stats
        import src.db.engine
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


class TestRecalcTeamStatsMain:
    def _run_main(self, argv, mock_recalc):
        with (
            patch("src.cli.recalc_team_stats.run_recalc", mock_recalc),
            patch("sys.argv", argv),
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            return exc_info.value.code

    def test_main_with_season(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025"], mock)
        assert code == 0
        mock.assert_called_once()

    def test_main_with_year_alias(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--year", "2025"], mock)
        assert code == 0

    def test_main_with_team_id(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--team-id", "LG"], mock)
        assert code == 0
        _, kwargs = mock.call_args
        assert kwargs.get("team_id") == "LG"

    def test_main_with_team_alias(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--team", "OB"], mock)
        assert code == 0
        _, kwargs = mock.call_args
        assert kwargs.get("team_id") == "OB"

    def test_main_dry_run(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--dry-run"], mock)
        assert code == 0
        args, kwargs = mock.call_args
        assert kwargs["dry_run"] is True

    def test_main_batting_only(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--batting-only"], mock)
        assert code == 0
        args, kwargs = mock.call_args
        assert kwargs["batting_only"] is True

    def test_main_pitching_only(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--pitching-only"], mock)
        assert code == 0
        args, kwargs = mock.call_args
        assert kwargs["pitching_only"] is True

    def test_main_legacy_type_batting(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--type", "batting"], mock)
        assert code == 0
        args, kwargs = mock.call_args
        assert kwargs["batting_only"] is True

    def test_main_legacy_type_pitching(self):
        mock = MagicMock(return_value=0)
        code = self._run_main(["recalc_team_stats", "--season", "2025", "--type", "pitching"], mock)
        assert code == 0
        args, kwargs = mock.call_args
        assert kwargs["pitching_only"] is True

    def test_main_season_required(self):
        with patch("sys.argv", ["recalc_team_stats"]):
            with pytest.raises(SystemExit):
                main()


class TestLogDryRun:
    def test_log_dry_run_batting(self, caplog):
        results = [
            {
                "team_id": "LG",
                "team_name": "트윈스",
                "games": 10,
                "at_bats": 100,
                "hits": 30,
                "avg": 0.300,
                "obp": 0.350,
                "slg": 0.450,
                "ops": 0.800,
            },
        ]
        with caplog.at_level(logging.INFO):
            _log_dry_run_batting(results)
        assert "DRY-RUN" in caplog.text
        assert "LG" in caplog.text

    def test_log_dry_run_batting_no_team_name(self, caplog):
        results = [
            {
                "team_id": "LG",
                "games": 10,
                "at_bats": 100,
                "hits": 30,
                "avg": 0.300,
                "obp": 0.350,
                "slg": 0.450,
                "ops": 0.800,
            },
        ]
        with caplog.at_level(logging.INFO):
            _log_dry_run_batting(results)
        assert "DRY-RUN" in caplog.text

    def test_log_dry_run_pitching(self, caplog):
        results = [
            {
                "team_id": "LG",
                "team_name": "트윈스",
                "games": 10,
                "wins": 5,
                "losses": 3,
                "ties": 0,
                "innings_pitched": 50.0,
                "earned_runs": 20,
                "era": 3.60,
                "whip": 1.20,
            },
        ]
        with caplog.at_level(logging.INFO):
            _log_dry_run_pitching(results)
        assert "DRY-RUN" in caplog.text
        assert "LG" in caplog.text

    def test_log_dry_run_pitching_no_team_name(self, caplog):
        results = [
            {
                "team_id": "LG",
                "games": 10,
                "wins": 5,
                "losses": 3,
                "ties": 0,
                "innings_pitched": 50.0,
                "earned_runs": 20,
                "era": 3.60,
                "whip": 1.20,
            },
        ]
        with caplog.at_level(logging.INFO):
            _log_dry_run_pitching(results)
        assert "DRY-RUN" in caplog.text


if __name__ == "__main__":
    unittest.main()
