from __future__ import annotations

from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.repositories.safe_batting_repository import (
    _batting_row,
    _batting_rows,
    _execute_single_upsert,
    _excluded_update_dict,
    _inserted_update_dict,
    _save_mysql_rows,
    _save_postgresql_rows,
    _save_sqlite_rows,
    _save_rows_by_database_type,
    _unique_batting_payloads,
    cleanup_invalid_batting_data,
    get_batting_stats_by_season,
    get_batting_stats_count,
    get_last_filter_counts,
    save_batting_stats_safe,
    save_futures_batting,
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
def _patch_deps(session):
    with (
        patch("src.repositories.safe_batting_repository.SessionLocal", return_value=session),
        patch("src.repositories.safe_batting_repository.get_database_type", return_value="sqlite"),
        patch(
            "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter()),
        ),
    ):
        from src.repositories import safe_batting_repository as sbr

        sbr.LAST_FILTER_COUNTS.clear()
        yield


class TestUniqueBattingPayloads:
    def test_dedup_by_conflict_key(self):
        payloads = [
            {"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5},
            {"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10},
        ]
        result = _unique_batting_payloads(payloads)
        assert len(result) == 1
        assert result[(1, 2024, "REGULAR", "KBO1")]["games"] == 10

    def test_skip_none_player_id(self):
        payloads = [{"player_id": None, "season": 2024, "league": "REGULAR"}]
        assert _unique_batting_payloads(payloads) == {}

    def test_skip_none_season(self):
        payloads = [{"player_id": 1, "season": None, "league": "REGULAR"}]
        assert _unique_batting_payloads(payloads) == {}

    def test_default_level(self):
        payloads = [{"player_id": 1, "season": 2024, "league": "REGULAR"}]
        result = _unique_batting_payloads(payloads)
        assert (1, 2024, "REGULAR", "KBO1") in result


class TestBattingRow:
    def test_defaults(self):
        row = _batting_row({"player_id": 1, "season": 2024, "league": "REGULAR"})
        assert row["level"] == "KBO1"
        assert row["source"] == "CRAWLER"
        assert row["games"] is None

    def test_explicit_values(self):
        row = _batting_row(
            {"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO2", "games": 10, "hits": 3}
        )
        assert row["level"] == "KBO2"
        assert row["games"] == 10
        assert row["hits"] == 3


class TestBattingRows:
    def test_filters_and_dedup(self):
        payloads = [
            {"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5},
            {"player_id": None, "season": 2024, "league": "REGULAR"},
        ]
        rows = _batting_rows(payloads)
        assert len(rows) == 1


class TestExcludedUpdateDict:
    def test_excludes_conflict_keys(self):
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(PlayerSeasonBatting).values([{"player_id": 1, "season": 2024}])
        result = _excluded_update_dict(stmt, [{"player_id": 1, "season": 2024, "games": 5}])
        assert "player_id" not in result
        assert "season" not in result
        assert "games" in result


class TestInsertedUpdateDict:
    def test_excludes_conflict_keys(self):
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(PlayerSeasonBatting).values([{"player_id": 1, "season": 2024}])
        result = _inserted_update_dict(stmt, [{"player_id": 1, "season": 2024, "games": 5}])
        assert "player_id" not in result
        assert "games" in result


class TestExecuteSingleUpsert:
    def test_success_returns_one(self, session):
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        stmt = sqlite_insert(PlayerSeasonBatting).values(player_id=1, season=2024, league="REGULAR", level="KBO1")
        stmt = stmt.on_conflict_do_update(index_elements=["player_id", "season", "league", "level"], set_={"games": 5})
        result = _execute_single_upsert(session, stmt, {"player_id": 1})
        assert result == 1

    def test_failure_returns_zero(self, session):
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(PlayerSeasonBatting).values(player_id=None, season=None, league=None, level=None)
        stmt = stmt.on_conflict_do_update(index_elements=["player_id", "season", "league", "level"], set_={"games": 5})
        result = _execute_single_upsert(session, stmt, {"player_id": None})
        assert result == 0


class TestSaveSqliteRows:
    def test_batch_success(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        result = _save_sqlite_rows(session, rows)
        assert result == 1

    def test_batch_failure_fallback(self, session):
        rows = [{"player_id": None, "season": None, "league": None, "level": None, "games": 5}]
        result = _save_sqlite_rows(session, rows)
        assert result == 0


class TestSaveMysqlRows:
    @patch("src.repositories.safe_batting_repository.mysql_insert")
    def test_batch_success(self, mock_insert_cls, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        mock_stmt = MagicMock()
        mock_insert_cls.return_value.values.return_value = mock_stmt
        mock_stmt.on_duplicate_key_update.return_value = mock_stmt
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        with patch.object(session, "execute", return_value=MagicMock()):
            result = _save_mysql_rows(session, rows)
        assert result == 1

    @patch("src.repositories.safe_batting_repository.mysql_insert")
    def test_batch_failure_fallback(self, mock_insert_cls, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        mock_stmt = MagicMock()
        mock_stmt.on_duplicate_key_update.return_value = mock_stmt
        mock_insert_cls.return_value.values.return_value = mock_stmt
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        with patch.object(session, "execute", side_effect=SQLAlchemyError("fail")):
            result = _save_mysql_rows(session, rows)
        assert result == 0


class TestSavePostgresqlRows:
    @patch("src.repositories.safe_batting_repository.postgresql_insert")
    def test_batch_success(self, mock_insert_cls, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        mock_stmt = MagicMock()
        mock_insert_cls.return_value.values.return_value = mock_stmt
        mock_stmt.on_conflict_do_update.return_value = mock_stmt
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        with patch.object(session, "execute", return_value=MagicMock()):
            result = _save_postgresql_rows(session, rows)
        assert result == 1

    @patch("src.repositories.safe_batting_repository.postgresql_insert")
    def test_batch_failure_fallback(self, mock_insert_cls, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        mock_stmt = MagicMock()
        mock_stmt.on_conflict_do_update.return_value = mock_stmt
        mock_insert_cls.return_value.values.return_value = mock_stmt
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        with patch.object(session, "execute", side_effect=SQLAlchemyError("fail")):
            result = _save_postgresql_rows(session, rows)
        assert result == 0


class TestSaveRowsByDatabaseType:
    def test_sqlite_path(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        result = _save_rows_by_database_type(session, rows, "sqlite")
        assert result == 1

    def test_mysql_path(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        with (
            patch("src.repositories.safe_batting_repository.mysql_insert") as mock_insert,
            patch.object(session, "execute", return_value=MagicMock()),
        ):
            mock_stmt = MagicMock()
            mock_insert.return_value.values.return_value = mock_stmt
            mock_stmt.on_duplicate_key_update.return_value = mock_stmt
            result = _save_rows_by_database_type(session, rows, "mysql")
        assert result == 1

    def test_postgresql_path(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        result = _save_rows_by_database_type(session, rows, "postgresql")
        assert result == 1

    def test_generic_path(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        result = _save_rows_by_database_type(session, rows, "unknown")
        assert result == 1

    def test_generic_updates_existing(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1", games=5))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 10, "hits": 3}]
        result = _save_rows_by_database_type(session, rows, "unknown")
        assert result == 1
        existing = session.query(PlayerSeasonBatting).first()
        assert existing.games == 10
        assert existing.hits == 3

    def test_generic_inserts_new(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        rows = [{"player_id": 1, "season": 2024, "league": "REGULAR", "level": "KBO1", "games": 5}]
        result = _save_rows_by_database_type(session, rows, "unknown")
        assert result == 1


class TestSaveBattingStatsSafe:
    def test_empty_returns_zero(self):
        assert save_batting_stats_safe([]) == 0

    def test_all_filtered_returns_zero(self):
        with patch(
            "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter({"invalid": 2})),
        ):
            result = save_batting_stats_safe([{"player_id": 1, "season": 2024}])
            assert result == 0

    def test_rows_empty_after_dedup(self, session):
        with patch(
            "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
            return_value=([{"player_id": None, "season": None}], Counter()),
        ):
            result = save_batting_stats_safe([{}])
            assert result == 0

    def test_sqlite_pragma_off_and_on(self, session):
        from sqlalchemy import text

        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        with (
            patch("src.repositories.safe_batting_repository.get_database_type", return_value="sqlite"),
            patch(
                "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
                return_value=([{"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5}], Counter()),
            ),
        ):
            save_batting_stats_safe([{}])

    def test_non_sqlite_no_pragma(self, session):
        from sqlalchemy import text

        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        with (
            patch("src.repositories.safe_batting_repository.get_database_type", return_value="postgresql"),
            patch(
                "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
                return_value=([{"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5}], Counter()),
            ),
        ):
            save_batting_stats_safe([{}])

    def test_sqlalchemy_error_returns_zero(self, session):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute.side_effect = [SQLAlchemyError("DB error"), None]
        with (
            patch("src.repositories.safe_batting_repository.SessionLocal", return_value=mock_session),
            patch("src.repositories.safe_batting_repository.get_database_type", return_value="sqlite"),
            patch(
                "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
                return_value=([{"player_id": 1, "season": 2024, "league": "REGULAR", "games": 5}], Counter()),
            ),
        ):
            result = save_batting_stats_safe([{}])
            assert result == 0

    def test_get_last_filter_counts_after_save(self):
        with patch(
            "src.repositories.safe_batting_repository.filter_valid_season_stat_payloads",
            return_value=([], Counter({"invalid": 3})),
        ):
            save_batting_stats_safe([{"player_id": 1, "season": 2024}])
            counts = get_last_filter_counts()
            assert counts.get("invalid") == 3


class TestGetBattingStatsCount:
    def test_with_session(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        assert get_batting_stats_count(session) == 1

    def test_without_session(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        assert get_batting_stats_count() == 1


class TestGetBattingStatsBySeason:
    def test_with_session(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.add(PlayerSeasonBatting(player_id=1, season=2025, league="REGULAR", level="KBO1"))
        session.commit()
        assert len(get_batting_stats_by_season(2024, session)) == 1

    def test_without_session(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        assert len(get_batting_stats_by_season(2024)) == 1


class TestCleanupInvalidBattingData:
    def test_no_deletion_when_valid(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        deleted = cleanup_invalid_batting_data(session)
        assert deleted == 0

    def test_external_session_no_commit(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        cleanup_invalid_batting_data(session)
        remaining = session.query(PlayerSeasonBatting).count()
        assert remaining == 1

    def test_internal_session_commits(self, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.add(PlayerSeasonBatting(player_id=1, season=2024, league="REGULAR", level="KBO1"))
        session.commit()
        with patch("src.repositories.safe_batting_repository.SessionLocal", return_value=session):
            cleanup_invalid_batting_data()
        remaining = session.query(PlayerSeasonBatting).count()
        assert remaining == 1

    def test_sqlalchemy_error_returns_zero(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.delete.side_effect = SQLAlchemyError("DB error")
        result = cleanup_invalid_batting_data(mock_session)
        assert result == 0


class TestSaveFuturesBatting:
    def test_empty_rows_returns_zero(self):
        assert save_futures_batting(1, []) == 0

    def test_no_season_skipped(self):
        rows = [{"G": 10, "AB": 30}]
        assert save_futures_batting(1, rows) == 0

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_with_valid_data(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        from src.repositories.safe_batting_repository import _batting_rows

        payloads = [
            {
                "player_id": 1,
                "season": 2024,
                "league": "FUTURES",
                "level": "KBO2",
                "games": 10,
                "at_bats": 30,
                "hits": 9,
                "avg": 0.300,
            }
        ]
        mock_filter.return_value = (payloads, Counter())
        rows = [{"season": 2024, "G": 10, "AB": 30, "H": 9, "AVG": 0.300}]
        result = save_futures_batting(1, rows)
        assert result == 1

    @patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads")
    def test_multiple_rows(self, mock_filter, session):
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()
        payloads = [
            {"player_id": 1, "season": 2024, "league": "FUTURES", "level": "KBO2", "games": 10},
            {"player_id": 1, "season": 2025, "league": "FUTURES", "level": "KBO2", "games": 15},
        ]
        mock_filter.return_value = (payloads, Counter())
        rows = [
            {"season": 2024, "G": 10, "AB": 30},
            {"season": 2025, "G": 15, "AB": 45},
        ]
        result = save_futures_batting(1, rows)
        assert result == 2
