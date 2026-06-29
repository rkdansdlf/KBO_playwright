from __future__ import annotations

from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
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

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_filter_all_invalid(self, mock_filter):
        mock_filter.return_value = ([], Counter({"missing_player_id": 2}))
        result = save_pitching_stats_to_db([{"season": 2024}, {"season": 2025}])
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
            with patch.object(session, "rollback") as mock_rollback:
                result = save_pitching_stats_to_db([{}])
                assert result == 0
                mock_rollback.assert_called()

    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_commit_sqlalchemy_error(self, mock_filter, session):
        session.add(PlayerBasic(player_id=4001, name="CommitErr"))
        session.commit()

        mock_filter.return_value = (
            [{"player_id": 4001, "season": 2024, "league": "REGULAR", "games": 1}],
            Counter(),
        )

        with patch.object(session, "execute"):
            with patch.object(session, "commit", side_effect=SQLAlchemyError("fail", "fail", Exception("fail"))):
                with patch.object(session, "rollback") as mock_rollback:
                    result = save_pitching_stats_to_db([{}])
                    assert result == 0
                    mock_rollback.assert_called_once()

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
            result = save_pitching_stats_to_db([{}])
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
            result = save_pitching_stats_to_db([{}])
            assert result == 1

        stats = session.query(PlayerSeasonPitching).filter_by(player_id=6001).first()
        assert stats.wins == 7

    @patch("src.repositories.player_season_pitching_repository.SessionLocal")
    @patch("src.repositories.player_season_pitching_repository.get_database_type", return_value="mysql")
    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_mysql_upsert_branch(self, mock_filter, mock_db_type, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_filter.return_value = (
            [{"player_id": 7001, "season": 2024, "league": "REGULAR", "games": 5}],
            Counter(),
        )

        result = save_pitching_stats_to_db([{}])
        assert result == 1
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("src.repositories.player_season_pitching_repository.SessionLocal")
    @patch("src.repositories.player_season_pitching_repository.get_database_type", return_value="postgresql")
    @patch("src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads")
    def test_postgresql_upsert_branch(self, mock_filter, mock_db_type, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_filter.return_value = (
            [{"player_id": 8001, "season": 2024, "league": "REGULAR", "games": 5}],
            Counter(),
        )

        result = save_pitching_stats_to_db([{}])
        assert result == 1
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()


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

    def test_get_pitching_stats_count_no_session(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.count.return_value = 42
        with patch("src.repositories.player_season_pitching_repository.SessionLocal", return_value=mock_session):
            count = get_pitching_stats_count()
            assert count == 42

    def test_get_pitching_stats_by_season_no_session(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.filter_by.return_value.all.return_value = [MagicMock()]
        with patch("src.repositories.player_season_pitching_repository.SessionLocal", return_value=mock_session):
            results = get_pitching_stats_by_season(2024)
            assert len(results) == 1

    def test_cleanup_invalid_data_no_session(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.return_value = 2
        with patch("src.repositories.player_season_pitching_repository.SessionLocal", return_value=mock_session):
            deleted = cleanup_invalid_pitching_data()
            assert deleted == 2
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()

    def test_cleanup_invalid_data_sqlalchemy_error_no_session(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.side_effect = SQLAlchemyError(
            "fail",
            "fail",
            Exception("fail"),
        )
        with patch("src.repositories.player_season_pitching_repository.SessionLocal", return_value=mock_session):
            deleted = cleanup_invalid_pitching_data()
            assert deleted == 0
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()
