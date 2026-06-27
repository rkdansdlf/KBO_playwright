from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.sync.sync_base import (
    GAME_SIGNATURE_CHILD_TABLES,
    GameSyncEligibility,
    OCISyncBase,
    SyncBatchConfig,
    _build_composite_signature_query,
    _dedupe_records_for_conflict_keys,
    _execute_signature_query,
    _has_both_team_sides,
    _is_game_dirty,
    _is_game_dirty_by_child_tables,
    _is_game_dirty_by_game_section,
    _is_game_dirty_by_metadata_section,
    _load_game_ids_with_rows,
    _load_team_sides,
    _log_sync_eligibility,
    _row_to_record,
    _serialize_scalar,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
    filter_publishable_game_ids,
    load_game_sync_signatures,
)

_is_transient_oci_error = OCISyncBase._is_transient_oci_error

pytestmark = pytest.mark.usefixtures("_db_engine")


class TestGameSyncEligibilityCounts:
    def test_counts_returns_dict(self):
        e = GameSyncEligibility(
            skipped_schedule_only=["g1", "g2"],
            skipped_incomplete_detail=["g3"],
            skipped_empty_relay=["g4", "g5"],
            skipped_cancelled=["g6"],
        )
        assert e.counts() == {
            "skipped_schedule_only": 2,
            "skipped_incomplete_detail": 1,
            "skipped_empty_relay": 2,
            "skipped_cancelled": 1,
        }


class TestSerializeScalarEdgeCases:
    def test_float(self):
        assert _serialize_scalar(3.14) == 3.14

    def test_bool(self):
        assert _serialize_scalar(True) is True


class TestDedupeRecordsEdgeCases:
    def test_single_record(self):
        records = [{"id": 1, "val": "a"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 1

    def test_all_duplicates(self):
        records = [{"id": 1, "val": "a"}, {"id": 1, "val": "b"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 1


class TestRowToRecordTransform:
    def test_transform_fn_applied(self):
        mock = MagicMock()
        mock._mapping = {"a": 1, "b": 2}

        def transform(data):
            data["c"] = 3
            return data

        result = _row_to_record(mock, ["a", "b"], transform_fn=transform)
        assert result["c"] == 3


class TestLoadTeamSides:
    def test_empty_game_ids(self):
        session = MagicMock(spec=Session)
        result = _load_team_sides(session, MagicMock(), [])
        assert result == {}

    def test_filters_nulls(self, _db_engine):
        from src.models.base import Base
        from src.models.game import GameBattingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        stat = GameBattingStat(
            game_id="20250601_01",
            team_side="home",
            player_id="p1",
            player_name="Test Player",
            appearance_seq=1,
        )
        session.add(stat)
        session.commit()

        result = _load_team_sides(session, GameBattingStat, ["20250601_01"])
        assert "20250601_01" in result
        assert "home" in result["20250601_01"]
        session.close()


class TestLoadGameIdsWithRows:
    def test_empty_game_ids(self):
        session = MagicMock(spec=Session)
        result = _load_game_ids_with_rows(session, MagicMock(), [])
        assert result == set()


class TestHasBothTeamSides:
    def test_both_sides(self):
        side_map = {"g1": {"home", "away"}}
        assert _has_both_team_sides(side_map, "g1") is True

    def test_one_side(self):
        side_map = {"g1": {"home"}}
        assert _has_both_team_sides(side_map, "g1") is False

    def test_missing_game(self):
        assert _has_both_team_sides({}, "g1") is False


class TestIsGameDirtyByGameSection:
    def test_different_home_score(self):
        local = {"game": {"home_score": 5}}
        remote = {"game": {"home_score": 3}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_score(self):
        local = {"game": {"away_score": 2}}
        remote = {"game": {"away_score": 4}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_home_pitcher(self):
        local = {"game": {"home_pitcher": "p1"}}
        remote = {"game": {"home_pitcher": "p2"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_pitcher(self):
        local = {"game": {"away_pitcher": "p1"}}
        remote = {"game": {"away_pitcher": "p2"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_home_team(self):
        local = {"game": {"home_team": "SSG"}}
        remote = {"game": {"home_team": "LG"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_team(self):
        local = {"game": {"away_team": "SSG"}}
        remote = {"game": {"away_team": "LG"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_local_updated_at_none_not_dirty(self):
        local = {"game": {"updated_at": None}}
        remote = {"game": {"updated_at": "2025-01-01"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is False

    def test_remote_updated_at_none_is_dirty(self):
        local = {"game": {"updated_at": "2025-01-01"}}
        remote = {"game": {"updated_at": None}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True


class TestIsGameDirtyByMetadataSection:
    def test_different_row_count(self):
        local = {"game_metadata": {"row_count": 1}}
        remote = {"game_metadata": {"row_count": 2}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True

    def test_different_start_time(self):
        local = {"game_metadata": {"start_time": "2025-01-01T12:00:00"}}
        remote = {"game_metadata": {"start_time": "2025-01-01T13:00:00"}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True

    def test_local_max_updated_none_not_dirty(self):
        local = {"game_metadata": {"max_updated_at": None}}
        remote = {"game_metadata": {"max_updated_at": "2025-01-01"}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is False

    def test_remote_max_updated_none_is_dirty(self):
        local = {"game_metadata": {"max_updated_at": "2025-01-01"}}
        remote = {"game_metadata": {"max_updated_at": None}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True


class TestIsGameDirtyByChildTables:
    def test_different_row_count(self):
        local = {"game_inning_scores": {"row_count": 1}}
        remote = {"game_inning_scores": {"row_count": 2}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is True

    def test_newer_max_updated(self):
        local = {"game_inning_scores": {"max_updated_at": "2025-06-02"}}
        remote = {"game_inning_scores": {"max_updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is True

    def test_identical_not_dirty(self):
        local = {"game_inning_scores": {"row_count": 1, "max_updated_at": "2025-06-01"}}
        remote = {"game_inning_scores": {"row_count": 1, "max_updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is False


class TestFilterGameIdsByYearEdgeCases:
    def test_mixed_prefixes(self):
        result = filter_game_ids_by_year(["20240101", "20250101", "20241231", "20231201"], 2024)
        assert result == ["20240101", "20241231"]


class TestBuildCompositeSignatureQueryEdgeCases:
    def test_all_tables_included(self):
        sql = _build_composite_signature_query(None)
        for table in GAME_SIGNATURE_CHILD_TABLES:
            assert table in sql


class TestBuildGameSyncEligibilityCompleted:
    def test_completed_with_detail(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game, GameBattingStat, GamePitchingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="home", player_id="p1", player_name="P1", appearance_seq=1)
        )
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="away", player_id="p2", player_name="P2", appearance_seq=2)
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="home", player_id="p3", player_name="P3", appearance_seq=1
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="away", player_id="p4", player_name="P4", appearance_seq=2
            )
        )
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.parent_game_ids
        assert "20250601_01" in e.detail_game_ids
        session.close()

    def test_completed_without_relay_skipped(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game, GameBattingStat, GamePitchingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="home", player_id="p1", player_name="P1", appearance_seq=1)
        )
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="away", player_id="p2", player_name="P2", appearance_seq=2)
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="home", player_id="p3", player_name="P3", appearance_seq=1
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="away", player_id="p4", player_name="P4", appearance_seq=2
            )
        )
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_empty_relay
        session.close()


class TestFilterPublishableGameIds:
    def test_returns_parent_ids(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.commit()

        result = filter_publishable_game_ids(session, ["20250601_01"])
        assert result == ["20250601_01"]
        session.close()


class TestOCISyncBaseUnit:
    def test_init(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            assert sync.sqlite_session == sqlite_session
            assert sync._season_map_cache is None
            assert sync._franchise_id_mapping_cache is None

    def test_chunked(self):
        result = OCISyncBase._chunked(["a", "b", "c", "d", "e"], 2)
        assert result == [["a", "b"], ["c", "d"], ["e"]]

    def test_chunked_empty(self):
        result = OCISyncBase._chunked([], 2)
        assert result == []

    def test_quote_identifier_valid(self):
        assert OCISyncBase._quote_identifier("valid_table") == '"valid_table"'

    def test_quote_identifier_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            OCISyncBase._quote_identifier("")

    def test_quote_identifier_invalid_start(self):
        with pytest.raises(ValueError, match="unsafe SQL identifier"):
            OCISyncBase._quote_identifier("123table")

    def test_quote_identifier_invalid_chars(self):
        with pytest.raises(ValueError, match="unsafe SQL identifier"):
            OCISyncBase._quote_identifier("table name")

    def test_target_table_exists_no_engine(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = None
            assert sync._target_table_exists(MagicMock()) is True

    def test_resolve_sync_columns(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = MagicMock()

            class FakeModel:
                __tablename__ = "test_table"

                class __table__:
                    columns = []

            result = sync._resolve_sync_columns(FakeModel, ["id"])
            assert isinstance(result, list)

    def test_sync_in_batches_empty(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = MagicMock()
            sync._raw_oci_connection_with_retries = MagicMock()
            sync._bulk_copy_upsert = MagicMock()

            config = SyncBatchConfig(
                model=MagicMock(),
                query=MagicMock(),
                total_count=0,
                columns=["a"],
                conflict_keys=["a"],
                transform_fn=None,
                batch_size=100,
                update_timestamp=True,
            )
            config.query.offset.return_value.limit.return_value.all.return_value = []
            result = sync._sync_in_batches(config)
            assert result == 0

    def test_reset_target_sequence_non_postgres(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("sqlite:///:memory:", sqlite_session)
            result = sync._reset_target_sequence_for_table("test_table")
            assert result is False

    def test_direct_insert_upsert_no_conflict_keys(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"a": 1, "b": 2}
            sync._direct_insert_upsert(
                "test_table",
                record,
                [],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_direct_insert_upsert_no_update_cols(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"id": 1}
            sync._direct_insert_upsert(
                "test_table",
                record,
                ["id"],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_direct_insert_upsert_with_update_cols(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"id": 1, "name": "test"}
            sync._direct_insert_upsert(
                "test_table",
                record,
                ["id"],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()


class TestExecuteSignatureQuery:
    def test_without_game_ids(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result

        sql = "SELECT * FROM game"
        result = _execute_signature_query(mock_session, sql)
        assert result == mock_result

    def test_with_game_ids(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result

        sql = "SELECT * FROM game WHERE game_id IN :game_ids"
        result = _execute_signature_query(mock_session, sql, game_ids=["g1", "g2"])
        assert result == mock_result


class TestDetectDirtyGameIdsEdgeCases:
    def test_dirty_by_child_tables(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game_inning_scores": {"row_count": 2}}},
                {"g1": {"game_inning_scores": {"row_count": 1}}},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == ["g1"]

    def test_not_dirty(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game": {"game_status": "COMPLETED", "updated_at": "2025-01-01"}}},
                {"g1": {"game": {"game_status": "COMPLETED", "updated_at": "2025-01-01"}}},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == []


class TestIsDirtyEdgeCases:
    def test_identical_game_not_dirty(self):
        local = {"game": {"home_score": 5, "away_score": 3, "updated_at": "2025-01-01"}}
        remote = {"game": {"home_score": 5, "away_score": 3, "updated_at": "2025-01-01"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is False

    def test_local_newer_updated_at(self):
        local = {"game": {"updated_at": "2025-06-02"}}
        remote = {"game": {"updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_both_updated_at_none(self):
        local = {"game": {"updated_at": None}}
        remote = {"game": {"updated_at": None}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is False

    def test_metadata_skipped_in_child_tables(self):
        local = {"game_metadata": {"row_count": 5}, "game_inning_scores": {"row_count": 1}}
        remote = {"game_metadata": {"row_count": 999}, "game_inning_scores": {"row_count": 1}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is False

    def test_dirty_by_metadata_max_updated(self):
        local = {"game_metadata": {"max_updated_at": "2025-06-02"}}
        remote = {"game_metadata": {"max_updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True

    def test_is_game_dirty_metadata_only(self):
        local = {"game": {}, "game_metadata": {"max_updated_at": "2025-06-02"}}
        remote = {"game": {}, "game_metadata": {"max_updated_at": "2025-06-01"}}
        assert _is_game_dirty("g1", local, remote) is True


class TestSerializeScalarFullCoverage:
    def test_none_returns_none(self):
        assert _serialize_scalar(None) is None

    def test_datetime_isoformat(self):
        dt = datetime(2025, 1, 1, 12, 0, 0)
        assert _serialize_scalar(dt) == "2025-01-01T12:00:00"

    def test_string_passthrough(self):
        assert _serialize_scalar("hello") == "hello"

    def test_int_passthrough(self):
        assert _serialize_scalar(42) == 42


class TestDedupeRecordsExtra:
    def test_empty_conflict_keys_returns_all(self):
        records = [{"id": 1}, {"id": 2}]
        assert _dedupe_records_for_conflict_keys(records, []) == records

    def test_null_in_key_preserves_record(self):
        records = [
            {"game_id": "g1", "seq": None, "val": "a"},
            {"game_id": "g1", "seq": None, "val": "b"},
        ]
        result = _dedupe_records_for_conflict_keys(records, ["game_id", "seq"])
        assert len(result) == 2

    def test_mixed_null_and_duplicate(self):
        records = [
            {"game_id": "g1", "seq": None, "val": "a"},
            {"game_id": "g1", "seq": 1, "val": "b"},
            {"game_id": "g1", "seq": 1, "val": "c"},
        ]
        result = _dedupe_records_for_conflict_keys(records, ["game_id", "seq"])
        assert len(result) == 2

    def test_empty_records(self):
        assert _dedupe_records_for_conflict_keys([], ["id"]) == []


class TestRowToRecordFallback:
    def test_row_without_mapping_uses_getattr(self):
        class FakeRow:
            col_a = "val_a"
            col_b = 42

        result = _row_to_record(FakeRow(), ["col_a", "col_b"])
        assert result["col_a"] == "val_a"
        assert result["col_b"] == 42

    def test_json_serialization_of_nested(self):
        mock = MagicMock()
        mock._mapping = {"data": [1, 2, 3], "nested": {"a": 1}}
        result = _row_to_record(mock, ["data", "nested"])
        assert result["data"] == "[1, 2, 3]"
        assert result["nested"] == '{"a": 1}'

    def test_created_at_now_default(self):
        mock = MagicMock()
        mock._mapping = {"name": "test"}
        result = _row_to_record(mock, ["name", "created_at", "updated_at"])
        assert result["created_at"] is not None
        assert result["updated_at"] is not None


class TestFilterGameIdsByYearExtra:
    def test_none_year_returns_all(self):
        game_ids = ["20240101", "20250101", "20231201"]
        assert filter_game_ids_by_year(game_ids, None) == game_ids

    def test_empty_game_ids(self):
        assert filter_game_ids_by_year([], 2024) == []

    def test_non_matching_year(self):
        result = filter_game_ids_by_year(["20240101", "20241231"], 2025)
        assert result == []


class TestLogSyncEligibility:
    def test_empty_eligibility_no_log(self, caplog):
        eligibility = GameSyncEligibility()
        with caplog.at_level(logging.WARNING, logger="src.sync.sync_base"):
            _log_sync_eligibility(eligibility)
        assert "skipped_schedule_only" not in caplog.text

    def test_samples_with_games(self, caplog):
        eligibility = GameSyncEligibility(
            skipped_schedule_only=["g1", "g2"],
            skipped_incomplete_detail=["g3"],
            skipped_empty_relay=["g4"],
            skipped_cancelled=["g5"],
        )
        with caplog.at_level(logging.WARNING, logger="src.sync.sync_base"):
            _log_sync_eligibility(eligibility)
        assert "skipped_schedule_only=2" in caplog.text
        assert "skipped_incomplete_detail=1" in caplog.text
        assert "skipped_empty_relay=1" in caplog.text
        assert "skipped_cancelled=1" in caplog.text


class TestIsTransientOCIError:
    def test_operational_error_is_transient(self):
        from sqlalchemy.exc import OperationalError

        err = OperationalError("stmt", {}, Exception("connection failed"))
        assert _is_transient_oci_error(err) is True

    def test_dbapi_error_with_invalidated_connection(self):
        from sqlalchemy.exc import DBAPIError

        err = DBAPIError("stmt", (), Exception("test"))
        err.connection_invalidated = True
        assert _is_transient_oci_error(err) is True

    def test_dbapi_error_without_invalidated(self):
        from sqlalchemy.exc import DBAPIError

        err = DBAPIError("stmt", (), Exception("test"))
        err.connection_invalidated = False
        assert _is_transient_oci_error(err) is False

    def test_timeout_message_is_transient(self):
        err = Exception("server closed the connection unexpectedly")
        assert _is_transient_oci_error(err) is True

    def test_non_transient_error(self):
        err = Exception("syntax error in SQL")
        assert _is_transient_oci_error(err) is False


class TestBuildGameSyncEligibilityCancelled:
    def test_cancelled_game_skipped(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="CANCELLED",
        )
        session.add(g)
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_cancelled
        assert "20250601_01" in e.parent_game_ids
        session.close()


class TestBuildGameSyncEligibilityScheduledWithScore:
    def test_scheduled_with_score_becomes_parent(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="SCHEDULED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.parent_game_ids
        assert "20250601_01" not in e.skipped_schedule_only
        assert "20250601_01" not in e.skipped_incomplete_detail
        session.close()


class TestLoadTeamSidesNullHandling:
    def test_null_team_side_skipped(self):
        session = MagicMock(spec=Session)
        session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [
            ("g1", None),
        ]

        from src.models.game import GameBattingStat

        result = _load_team_sides(session, GameBattingStat, ["g1"])
        assert "g1" not in result
