from __future__ import annotations

from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerSeasonPitching
from src.repositories.player_season_pitching_repository import (
    LAST_FILTER_COUNTS,
    _build_pitching_row,
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
def patch_deps():
    with (
        patch("src.repositories.player_season_pitching_repository.get_database_type", return_value="sqlite"),
        patch(
            "src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter()),
        ),
    ):
        LAST_FILTER_COUNTS.clear()
        yield


class TestSavePitchingStats:
    def test_empty_payloads(self, session):
        result = save_pitching_stats_to_db([], session)
        assert result == 0

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_save_single(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test Pitcher"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "games": 10, "wins": 5}],
            Counter(),
        )

        result = save_pitching_stats_to_db([{"player_id": 1001, "season": 2024}], session)
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

        result = save_pitching_stats_to_db([{}, {}], session)
        assert result == 2

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_upsert_existing(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1001, name="Test"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10, "wins": 5}],
            Counter(),
        )
        save_pitching_stats_to_db([{}], session)

        mock_filter.return_value = (
            [{"player_id": 1001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 11, "wins": 6}],
            Counter(),
        )
        result = save_pitching_stats_to_db([{}], session)
        assert result == 1

        stats = session.query(PlayerSeasonPitching).all()
        assert len(stats) == 1
        assert stats[0].wins == 6

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_saves_same_player_as_two_team_rows(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1002, name="Split Pitcher"))
        session.commit()

        mock_filter.return_value = (
            [
                {"player_id": 1002, "season": 2021, "league": "REGULAR", "team_code": "LG", "wins": 2},
                {"player_id": 1002, "season": 2021, "league": "REGULAR", "team_code": "KH", "wins": 1},
            ],
            Counter(),
        )

        result = save_pitching_stats_to_db([{}, {}], session)

        assert result == 2
        assert session.query(PlayerSeasonPitching).filter_by(player_id=1002).count() == 2

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_extra_stats_promotion(self, mock_filter, session):
        session.add(PlayerBasic(player_id=2001, name="Test"))
        session.commit()

        mock_filter.return_value = (
            [
                {
                    "player_id": 2001,
                    "season": 2024,
                    "league": "REGULAR",
                    "games": 5,
                    "extra_stats": {"metrics": {"complete_games": 2, "shutouts": 1}},
                },
            ],
            Counter(),
        )
        save_pitching_stats_to_db([{}], session)

        stats = session.query(PlayerSeasonPitching).first()
        assert stats.complete_games == 2
        assert stats.shutouts == 1

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_get_last_filter_counts(self, mock_filter, session):
        mock_filter.return_value = ([], Counter({"invalid": 3}))
        save_pitching_stats_to_db([{"player_id": 1, "season": 2024}], session)
        counts = get_last_filter_counts()
        assert counts.get("invalid") == 3

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_filter_all_invalid(self, mock_filter, session):
        mock_filter.return_value = ([], Counter({"missing_player_id": 2}))
        result = save_pitching_stats_to_db([{"season": 2024}, {"season": 2025}], session)
        assert result == 0

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_upsert_execute_sqlalchemy_error(self, mock_filter, session):
        session.add(PlayerBasic(player_id=3001, name="Err"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 3001, "season": 2024, "league": "REGULAR", "games": 1}],
            Counter(),
        )

        with patch.object(session, "execute", side_effect=SQLAlchemyError("fail", "fail", Exception("fail"))):
            with patch.object(session, "rollback"):
                result = save_pitching_stats_to_db([{}], session)
                assert result == 0

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_merge_path_when_stmt_is_none(self, mock_filter, session):

        session.add(PlayerBasic(player_id=5001, name="Merge"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 5001, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10, "wins": 5}],
            Counter(),
        )

        with patch(
            "src.repositories.player_season_pitching_repository._build_pitching_upsert_stmt",
            return_value=None,
        ):
            result = save_pitching_stats_to_db([{}], session)
            assert result == 1

        stats = session.query(PlayerSeasonPitching).first()
        assert stats is not None
        assert stats.wins == 5

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_merge_path_update_existing(self, mock_filter, session):
        session.add(PlayerBasic(player_id=6001, name="Merge2"))
        session.commit()
        session.add(PlayerSeasonPitching(player_id=6001, season=2024, league="REGULAR", level="KBO1", wins=3))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 6001, "season": 2024, "league": "REGULAR", "level": "KBO1", "wins": 7}],
            Counter(),
        )

        with patch(
            "src.repositories.player_season_pitching_repository._build_pitching_upsert_stmt",
            return_value=None,
        ):
            result = save_pitching_stats_to_db([{}], session)
            assert result == 1

        stats = session.query(PlayerSeasonPitching).filter_by(player_id=6001).first()
        assert stats.wins == 7


class TestBuildPitchingRow:
    def test_prefer_payload_value_falls_through_to_metrics(self):
        metrics = {"complete_games": 3}
        result = _build_pitching_row({"player_id": 1, "season": 2024, "extra_stats": {"metrics": metrics}})
        assert result["complete_games"] == 3

    def test_prefer_payload_value_payload_takes_precedence(self):
        result = _build_pitching_row(
            {"player_id": 1, "season": 2024, "complete_games": 5, "extra_stats": {"metrics": {"complete_games": 3}}},
        )
        assert result["complete_games"] == 5

    def test_extra_stats_not_dict(self):
        result = _build_pitching_row({"player_id": 1, "season": 2024, "extra_stats": "not_a_dict"})
        assert "complete_games" not in result

    def test_metrics_not_dict(self):
        result = _build_pitching_row({"player_id": 1, "season": 2024, "extra_stats": {"metrics": "bad"}})
        assert "complete_games" not in result

    def test_none_values_stripped(self):
        result = _build_pitching_row({"player_id": 1, "season": 2024, "league": "REGULAR"})
        assert "wins" not in result
        assert "era" not in result
        assert "player_id" in result


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

    def test_cleanup_invalid_data_deletes_null_player_id(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.return_value = 1
        deleted = cleanup_invalid_pitching_data(mock_session)
        assert deleted == 1

    def test_cleanup_invalid_data_deletes_null_season(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.return_value = 1
        deleted = cleanup_invalid_pitching_data(mock_session)
        assert deleted == 1

    def test_cleanup_invalid_data_sqlalchemy_error(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.side_effect = SQLAlchemyError(
            "fail",
            "fail",
            Exception("fail"),
        )
        deleted = cleanup_invalid_pitching_data(mock_session)
        assert deleted == 0
