from __future__ import annotations

from collections import Counter
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.repositories.safe_batting_repository import (
    LAST_FILTER_COUNTS,
    cleanup_invalid_batting_data,
    get_batting_stats_by_season,
    get_batting_stats_count,
    get_last_filter_counts,
    save_batting_stats_safe,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    PlayerBasic.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.safe_batting_repository.SessionLocal", return_value=session),
        patch("src.repositories.safe_batting_repository.get_database_type", return_value="sqlite"),
        patch(
            "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter()),
        ),
    ):
        LAST_FILTER_COUNTS.clear()
        yield


class TestSaveBattingStats:
    def test_empty_payloads(self):
        result = save_batting_stats_safe([])
        assert result == 0

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_save_single(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test Hitter"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "games": 10, "hits": 25}],
            Counter(),
        )

        result = save_batting_stats_safe([{}])
        assert result == 1

        stats = session.query(PlayerSeasonBatting).all()
        assert len(stats) == 1
        assert stats[0].hits == 25

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_save_multiple(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerBasic(player_id=2, name="B"))
        session.commit()

        mock_filter.return_value = (
            [
                {"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5, "hits": 10},
                {"player_id": 2, "season": 2024, "league": "REGULAR", "games": 8, "hits": 20},
            ],
            Counter(),
        )

        result = save_batting_stats_safe([{}, {}])
        assert result == 2

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_upsert_existing(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10, "hits": 25}],
            Counter(),
        )
        save_batting_stats_safe([{}])

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 11, "hits": 30}],
            Counter(),
        )
        result = save_batting_stats_safe([{}])
        assert result == 1

        stats = session.query(PlayerSeasonBatting).all()
        assert len(stats) == 1
        assert stats[0].hits == 30

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_skip_null_player_id(self, mock_filter, session):
        mock_filter.return_value = (
            [
                {"player_id": None, "season": 2024, "league": "REGULAR", "games": 5},
                {"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5},
            ],
            Counter(),
        )
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()

        result = save_batting_stats_safe([{}, {}])
        assert result == 1

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_get_last_filter_counts(self, mock_filter):
        mock_filter.return_value = ([], Counter({"invalid": 3}))
        save_batting_stats_safe([{"player_id": 1, "season": 2024}])
        counts = get_last_filter_counts()
        assert counts.get("invalid") == 3


class TestQueryAndCleanup:
    def test_get_batting_stats_count(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()

        count = get_batting_stats_count(session)
        assert count == 1

    def test_get_batting_stats_by_season(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.add(PlayerSeasonBatting(player_id=1, season=2025, league="REGULAR", level="KBO1"))
        session.commit()

        results = get_batting_stats_by_season(2024, session)
        assert len(results) == 1

    def test_cleanup_invalid_data_clean(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()

        deleted = cleanup_invalid_batting_data(session)
        assert deleted == 0
