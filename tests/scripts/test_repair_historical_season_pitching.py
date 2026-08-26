"""Unit tests for scripts/maintenance/repair_historical_season_pitching.py."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance.repair_historical_season_pitching import (
    repair_pitching_season_records,
)
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonPitching


class TestRepairPitchingSeasonRecords:
    @pytest.fixture
    def sqlite_session(self, tmp_path):
        db_path = tmp_path / "test_pitching_repair.db"
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(
            engine,
            tables=[PlayerBasic.__table__, PlayerSeasonPitching.__table__],
        )
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        # Add players
        p1 = PlayerBasic(player_id=71260, name="이재우")
        p2 = PlayerBasic(player_id=99999, name="미등록선수")
        p3 = PlayerBasic(player_id=71261, name="이재우")
        session.add_all([p1, p2, p3])

        # Add corrupted FINAL_VERIFICATION rows
        fv1 = PlayerSeasonPitching(
            player_id=71260,
            season=2005,
            source="FINAL_VERIFICATION",
            team_code="DB",
            games=76,
            wins=305,
            losses=269,
            saves=69,
            holds=0,
            innings_pitched=89.2,
            earned_runs=19,
            era=1.91,
        )
        fv2 = PlayerSeasonPitching(
            player_id=99999,
            season=2005,
            source="FINAL_VERIFICATION",
            team_code="HT",
            games=10,
            wins=150,
            losses=120,
            saves=50,
            holds=0,
            innings_pitched=20.0,
            earned_runs=10,
            era=4.50,
        )
        # Add verified PROFILE row for 이재우 (separate player ID record as in KBO profile crawl)
        prof1 = PlayerSeasonPitching(
            player_id=71261,
            season=2005,
            source="PROFILE",
            team_code="DB",
            games=76,
            wins=7,
            losses=5,
            saves=1,
            holds=28,
            innings_pitched=89.2,
            earned_runs=19,
            era=1.91,
        )
        session.add_all([fv1, fv2, prof1])
        session.commit()

        yield session, f"sqlite:///{db_path}"
        session.close()

    def test_dry_run_no_changes(self, sqlite_session):
        session, db_url = sqlite_session
        count = repair_pitching_season_records(db_url=db_url, apply_changes=False)
        assert count == 2

        # In dry-run, fv1 should still have wins=305
        row1 = session.query(PlayerSeasonPitching).filter_by(player_id=71260, source="FINAL_VERIFICATION").one()
        assert row1.wins == 305

    def test_apply_repairs_and_resets(self, sqlite_session, tmp_path):
        session, db_url = sqlite_session
        with patch("scripts.maintenance.repair_historical_season_pitching.BACKUP_DIR", tmp_path):
            count = repair_pitching_season_records(db_url=db_url, apply_changes=True)
            assert count == 2

        session.expire_all()
        # 이재우 should be mapped to PROFILE (wins=7, losses=5, saves=1, holds=28)
        row1 = session.query(PlayerSeasonPitching).filter_by(player_id=71260, source="FINAL_VERIFICATION").one()
        assert row1.wins == 7
        assert row1.losses == 5
        assert row1.saves == 1
        assert row1.holds == 28

        # 미등록선수 (no PROFILE) should be reset to 0-0-0-0
        row2 = session.query(PlayerSeasonPitching).filter_by(player_id=99999, source="FINAL_VERIFICATION").one()
        assert row2.wins == 0
        assert row2.losses == 0
        assert row2.saves == 0
        assert row2.holds == 0
