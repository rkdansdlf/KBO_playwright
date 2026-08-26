"""Unit tests for scripts/maintenance/backfill_historical_stadium_codes.py."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance.backfill_historical_stadium_codes import (
    HISTORICAL_STADIUM_NAMES,
    backfill_stadiums,
    clean_stadium_name,
)
from src.models.base import Base
from src.models.game import Game, GameMetadata


class TestCleanStadiumName:
    def test_clean_dirty_mudeung(self):
        assert clean_stadium_name("7월 17일 DH2 무등 야구장", "HT") == "광주"

    def test_clean_dirty_jamsil(self):
        assert clean_stadium_name("4월 17일 서울종합운동장 야구장", "LG") == "잠실"

    def test_clean_dirty_incheon(self):
        assert clean_stadium_name("30 숭의야구장", "TP") == "인천"

    def test_empty_stadium_inferred_from_home_team(self):
        assert clean_stadium_name("", "SS") == "대구"
        assert clean_stadium_name("   ", "LT") == "부산"
        assert clean_stadium_name(None, "HT") == "광주"
        assert clean_stadium_name(None, "SL") == "전주"

    def test_normal_stadium_unchanged(self):
        assert clean_stadium_name("잠실", "OB") == "잠실"
        assert clean_stadium_name("동대문", "MBC") == "동대문"


class TestBackfillStadiums:
    @pytest.fixture
    def sqlite_session(self, tmp_path):
        db_path = tmp_path / "test_stadium_backfill.db"
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(engine, tables=[Game.__table__, GameMetadata.__table__])
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

        # Insert test historical game
        g1 = Game(
            game_id="19900717LGHT1",
            game_date=date(1990, 7, 17),
            home_team="HT",
            away_team="LG",
            home_score=3,
            away_score=5,
            stadium="7월 17일 DH2 무등 야구장",
            season_id=199000,
        )
        g2 = Game(
            game_id="19950401LTSS0",
            game_date=date(1995, 4, 1),
            home_team="SS",
            away_team="LT",
            home_score=5,
            away_score=2,
            stadium="대구",
            season_id=199500,
        )
        session.add_all([g1, g2])
        session.commit()
        yield session, f"sqlite:///{db_path}"
        session.close()

    def test_backfill_dry_run(self, sqlite_session):
        session, db_url = sqlite_session
        processed = backfill_stadiums(1990, 1995, db_url=db_url, apply_changes=False)
        assert processed == 2

        # In dry run, stadium shouldn't change and metadata not created
        g1 = session.query(Game).filter(Game.game_id == "19900717LGHT1").one()
        assert g1.stadium == "7월 17일 DH2 무등 야구장"
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == "19900717LGHT1").one_or_none()
        assert meta is None

    def test_backfill_apply(self, sqlite_session):
        session, db_url = sqlite_session
        processed = backfill_stadiums(1990, 1995, db_url=db_url, apply_changes=True)
        assert processed == 2

        # Check cleaned stadium
        session.expire_all()
        g1 = session.query(Game).filter(Game.game_id == "19900717LGHT1").one()
        assert g1.stadium == "광주"

        # Check metadata created
        m1 = session.query(GameMetadata).filter(GameMetadata.game_id == "19900717LGHT1").one()
        assert m1.stadium_code == "MUDEUNG"
        assert m1.stadium_name == "광주 무등경기장 야구장"
        assert m1.source_payload["raw_stadium"] == "7월 17일 DH2 무등 야구장"

        m2 = session.query(GameMetadata).filter(GameMetadata.game_id == "19950401LTSS0").one()
        assert m2.stadium_code == "DAEGU"
        assert m2.stadium_name == "대구시민운동장 야구장"
