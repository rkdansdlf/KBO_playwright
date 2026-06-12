from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.repositories.save_kbo_pitching import save_pitching_stats


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


@pytest.fixture(autouse=True)
def patch_session_local(session):
    with patch("src.repositories.save_kbo_pitching.SessionLocal", return_value=session):
        yield


class TestSaveKboPitching:
    def test_save_pitching_stats(self, session):
        pitching_data = [
            {
                "player_id": 100,
                "season": 2025,
                "team_code": "LG",
                "games": 20,
                "innings_pitched": 150,
                "hits_allowed": 120,
                "runs_allowed": 50,
                "strikeouts": 100,
                "home_runs_allowed": 8,
                "walks_allowed": 30,
                "wild_pitches": 5,
                "intentional_walks": 1,
                "hit_batters": 2,
            }
        ]
        count = save_pitching_stats(pitching_data)
        assert count == 1

    def test_save_pitching_empty(self, session):
        assert save_pitching_stats([]) == 0

    def test_save_pitching_upsert(self, session):
        data = [
            {
                "player_id": 200,
                "season": 2025,
                "team_code": "SSG",
                "games": 10,
                "innings_pitched": 50.0,
                "hits_allowed": 40,
                "runs_allowed": 20,
                "strikeouts": 30,
                "home_runs_allowed": 5,
                "walks_allowed": 15,
                "wild_pitches": 2,
                "intentional_walks": 0,
                "hit_batters": 1,
            }
        ]
        save_pitching_stats(data)
        data[0]["games"] = 99
        save_pitching_stats(data)
        from src.models.player import PlayerSeasonBatting

        record = session.query(PlayerSeasonBatting).filter_by(player_id=200, season=2025).first()
        assert record.games == 99

    def test_maps_pitching_fields(self, session):
        data = [
            {
                "player_id": 300,
                "season": 2025,
                "team_code": "LG",
                "league": "REGULAR",
                "level": "KBO1",
                "games": 25,
                "innings_pitched": 180.0,
                "hits_allowed": 150,
                "runs_allowed": 60,
                "strikeouts": 130,
                "home_runs_allowed": 10,
                "walks_allowed": 40,
                "wild_pitches": 7,
                "intentional_walks": 2,
                "hit_batters": 3,
            }
        ]
        save_pitching_stats(data)
        from src.models.player import PlayerSeasonBatting

        record = session.query(PlayerSeasonBatting).filter_by(player_id=300, season=2025).first()
        assert record.games == 25
        assert record.plate_appearances == 180
        assert record.at_bats == 150
        assert record.runs == 60
        assert record.hits == 130
        assert record.home_runs == 10
        assert record.walks == 40
        assert record.strikeouts == 7
