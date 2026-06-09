from __future__ import annotations

from collections import Counter
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerSeasonPitching
from src.repositories.player_season_pitching_repository import (
    LAST_FILTER_COUNTS,
    cleanup_invalid_pitching_data,
    get_last_filter_counts,
    get_pitching_stats_by_season,
    get_pitching_stats_count,
    save_pitching_stats_to_db,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    PlayerBasic.__table__.create(engine)
    PlayerSeasonPitching.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.player_season_pitching_repository.SessionLocal", return_value=session),
        patch("src.repositories.player_season_pitching_repository.get_database_type", return_value="sqlite"),
        patch(
            "src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter()),
        ),
    ):
        LAST_FILTER_COUNTS.clear()
        yield


class TestSavePitchingStats:
    def test_empty_payloads(self):
        result = save_pitching_stats_to_db([])
        assert result == 0

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_save_single(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test Pitcher"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "games": 10, "wins": 5}],
            Counter(),
        )

        result = save_pitching_stats_to_db([{"player_id": 1001, "season": 2024}])
        assert result == 1

        stats = session.query(PlayerSeasonPitching).all()
        assert len(stats) == 1
        assert stats[0].wins == 5

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_save_multiple(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerBasic(player_id=2, name="B"))
        session.commit()

        mock_filter.return_value = (
            [
                {"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5, "wins": 3},
                {"player_id": 2, "season": 2024, "league": "REGULAR", "games": 8, "wins": 6},
            ],
            Counter(),
        )

        result = save_pitching_stats_to_db([{}, {}])
        assert result == 2

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_upsert_existing(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10, "wins": 5}],
            Counter(),
        )
        save_pitching_stats_to_db([{}])

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 11, "wins": 6}],
            Counter(),
        )
        result = save_pitching_stats_to_db([{}])
        assert result == 1

        stats = session.query(PlayerSeasonPitching).all()
        assert len(stats) == 1
        assert stats[0].wins == 6

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_extra_stats_promotion(self, mock_filter, session):
        session.add(PlayerBasic(player_id=2001, name="Test"))
        session.commit()

        mock_filter.return_value = (
            [{
                "player_id": 2001, "season": 2024, "league": "REGULAR",
                "games": 5, "extra_stats": {"metrics": {"complete_games": 2, "shutouts": 1}},
            }],
            Counter(),
        )
        save_pitching_stats_to_db([{}])

        stats = session.query(PlayerSeasonPitching).first()
        assert stats.complete_games == 2
        assert stats.shutouts == 1

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_get_last_filter_counts(self, mock_filter):
        mock_filter.return_value = ([], Counter({"invalid": 3}))
        save_pitching_stats_to_db([{"player_id": 1, "season": 2024}])
        counts = get_last_filter_counts()
        assert counts.get("invalid") == 3


class TestQueryAndCleanup:
    def test_get_pitching_stats_count(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonPitching(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()

        count = get_pitching_stats_count(session)
        assert count == 1

    def test_get_pitching_stats_by_season(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonPitching(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.add(PlayerSeasonPitching(player_id=1, season=2025, league="REGULAR", level="KBO1"))
        session.commit()

        results = get_pitching_stats_by_season(2024, session)
        assert len(results) == 1

    def test_cleanup_invalid_data_clean(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonPitching(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()

        deleted = cleanup_invalid_pitching_data(session)
        assert deleted == 0
