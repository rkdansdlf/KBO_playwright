"""Unit tests for scripts/maintenance/repair_historical_season_pitching.py."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance import repair_historical_season_pitching as repair_module
from scripts.maintenance.repair_historical_season_pitching import (
    repair_pitching_season_records,
)
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonPitching


def test_repair_queries_use_oracle_string_literals() -> None:
    sql = Path(repair_module.__file__).read_text(encoding="utf-8")

    assert "p.source = 'PROFILE'" in sql
    assert "p.source = 'FINAL_VERIFICATION'" in sql
    assert "p.league = 'REGULAR'" in sql
    assert "p.level = 'KBO1'" in sql
    assert 'p.source = "PROFILE"' not in sql
    assert 'p.source = "FINAL_VERIFICATION"' not in sql


def test_invalid_database_url_does_not_fallback_to_local(monkeypatch) -> None:
    monkeypatch.setattr(repair_module, "create_engine", MagicMock(side_effect=ValueError("invalid URL")))

    with pytest.raises(ValueError, match="invalid URL"):
        repair_pitching_season_records(db_url="oracle+oracledb://invalid", apply_changes=False)


def test_ambiguous_profile_name_is_skipped() -> None:
    target = SimpleNamespace(wins=99, losses=88, saves=77, holds=66)
    session = MagicMock()
    session.get.return_value = target
    lookup = repair_module.ProfileLookup(
        by_player={},
        by_unique_name={},
        ambiguous_names={(2005, "동명이인")},
    )

    counts = repair_module._apply_pitching_repairs(
        session,
        [(1, 123, 2005, "FINAL_VERIFICATION", "DB", 99, 88, 77, 66, "동명이인")],
        lookup,
        apply_changes=True,
    )

    assert counts[:3] == (0, 0, 1)
    assert (target.wins, target.losses, target.saves, target.holds) == (99, 88, 77, 66)


def test_no_profile_evidence_is_skipped() -> None:
    target = SimpleNamespace(wins=99, losses=88, saves=77, holds=66)
    session = MagicMock()
    session.get.return_value = target
    lookup = repair_module.ProfileLookup(by_player={}, by_unique_name={}, ambiguous_names=set())

    counts = repair_module._apply_pitching_repairs(
        session,
        [(1, 123, 2005, "FINAL_VERIFICATION", "DB", 99, 88, 77, 66, "미등록선수")],
        lookup,
        apply_changes=True,
    )

    assert counts[:4] == (0, 0, 0, 1)
    assert (target.wins, target.losses, target.saves, target.holds) == (99, 88, 77, 66)


def test_exact_player_match_takes_priority_over_ambiguous_name() -> None:
    target = SimpleNamespace(wins=99, losses=88, saves=77, holds=66)
    session = MagicMock()
    session.get.return_value = target
    lookup = repair_module.ProfileLookup(
        by_player={
            (2005, 123): repair_module.ProfileRecord(
                stats=(7, 5, 1, 28),
                team_code="DB",
            ),
        },
        by_unique_name={},
        ambiguous_names={(2005, "동명이인")},
    )

    counts = repair_module._apply_pitching_repairs(
        session,
        [(1, 123, 2005, "FINAL_VERIFICATION", "DB", 99, 88, 77, 66, "동명이인")],
        lookup,
        apply_changes=True,
    )

    assert counts[:3] == (1, 0, 0)
    assert (target.wins, target.losses, target.saves, target.holds) == (7, 5, 1, 28)


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

    def test_dry_run_writes_candidate_report(self, sqlite_session, tmp_path):
        session, db_url = sqlite_session
        report_path = tmp_path / "repair_report.json"

        count = repair_pitching_season_records(
            db_url=db_url,
            apply_changes=False,
            report_path=report_path,
        )

        assert count == 2
        report = json.loads(report_path.read_text(encoding="utf-8"))
        assert report["mode"] == "dry-run"
        assert report["mapped_to_profile"] == 1
        assert report["changed_profile_values"] == 1
        assert report["no_op_profile_matches"] == 0
        assert report["ambiguous_skipped"] == 0
        assert report["no_evidence_skipped"] == 1
        mapped_row = next(row for row in report["rows"] if row["player_id"] == 71260)
        assert mapped_row["resolution"] == "unique_name"
        assert mapped_row["team_code_match"] is True
        assert mapped_row["would_change"] is True
        assert mapped_row["original_values"] == {"wins": 305, "losses": 269, "saves": 69, "holds": 0}
        assert mapped_row["profile_values"] == {"wins": 7, "losses": 5, "saves": 1, "holds": 28}

    def test_apply_repairs_and_skips_without_evidence(self, sqlite_session, tmp_path):
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

        # 미등록선수 (no PROFILE evidence) must remain unchanged
        row2 = session.query(PlayerSeasonPitching).filter_by(player_id=99999, source="FINAL_VERIFICATION").one()
        assert row2.wins == 150
        assert row2.losses == 120
        assert row2.saves == 50
        assert row2.holds == 0
