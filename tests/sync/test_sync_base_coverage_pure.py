from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.sync.sync_base import (
    GAME_SIGNATURE_CHILD_TABLES,
    OCISyncBase,
    GameSyncEligibility,
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


@pytest.fixture
def sync_base():
    with patch("src.sync.sync_base.create_engine"):
        sqlite_session = MagicMock(spec=Session)
        sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
        sync.oci_engine = MagicMock()
        sync.target_session = MagicMock()
        yield sync


class TestOCISyncBaseTestConnection:
    def test_connection_success(self, sync_base):
        sync_base.target_session.execute.return_value = MagicMock()
        result = sync_base.test_connection()
        assert result is True
        sync_base.target_session.execute.assert_called_once()

    def test_connection_failure_non_transient(self, sync_base):
        sync_base.target_session.execute.side_effect = SQLAlchemyError("connection lost")
        sync_base._rollback_target_session = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(return_value=False)
        result = sync_base.test_connection()
        assert result is False
        sync_base._rollback_target_session.assert_called_once()

    def test_connection_failure_transient_reconnect(self, sync_base):
        sync_base.target_session.execute.side_effect = OperationalError("stmt", {}, Exception("server closed"))
        sync_base._rollback_target_session = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(return_value=True)
        sync_base._reconnect_oci = MagicMock()
        result = sync_base.test_connection()
        assert result is False
        sync_base._rollback_target_session.assert_called_once()
        sync_base._reconnect_oci.assert_called_once()

    def test_connection_failure_transient_reconnect_fails(self, sync_base):
        sync_base.target_session.execute.side_effect = OperationalError("stmt", {}, Exception("server closed"))
        sync_base._rollback_target_session = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(return_value=True)
        sync_base._reconnect_oci = MagicMock(side_effect=RuntimeError("reconnect failed"))
        result = sync_base.test_connection()
        assert result is False
        sync_base._reconnect_oci.assert_called_once()


class TestOCISyncBaseGetSeasonMap:
    def test_cache_hit(self, sync_base):
        cached = {(2025, 0): 1}
        sync_base._season_map_cache = cached
        result = sync_base._get_season_map()
        assert result is cached

    def test_first_query_succeeds(self, sync_base):
        mock_row = MagicMock()
        mock_row.season_year = 2025
        mock_row.league_type_code = 0
        mock_row.season_id = 1
        sync_base.target_session.execute.return_value.all.return_value = [mock_row]
        result = sync_base._get_season_map()
        assert result == {(2025, 0): 1}
        assert sync_base._season_map_cache == {(2025, 0): 1}

    def test_multiple_queries_fallback(self, sync_base):
        second_mock_row = MagicMock()
        second_mock_row.season_year = 2024
        second_mock_row.league_type_code = 1
        second_mock_row.season_id = 2
        calls = [SQLAlchemyError("first table missing"), [second_mock_row]]

        def execute_side_effect(*_args, **_kwargs):
            exc = calls.pop(0)
            if isinstance(exc, Exception):
                raise exc
            result = MagicMock()
            result.all.return_value = exc
            return result

        sync_base.target_session.execute.side_effect = execute_side_effect
        result = sync_base._get_season_map()
        assert result == {(2024, 1): 2}

    def test_all_queries_fail_returns_empty(self, sync_base):
        sync_base.target_session.execute.side_effect = SQLAlchemyError("table not found")
        result = sync_base._get_season_map()
        assert result == {}
        assert sync_base._season_map_cache == {}


class TestOCISyncBaseGetFranchiseIdMapping:
    def test_cache_hit(self, sync_base):
        cached = {1: 10}
        sync_base._franchise_id_mapping_cache = cached
        result = sync_base._get_franchise_id_mapping()
        assert result is cached

    def test_empty_original_codes(self, sync_base):
        with patch("src.models.franchise.Franchise"):
            mock_f = MagicMock()
            mock_f.original_code = None
            sync_base.sqlite_session.query.return_value.all.return_value = [mock_f]
            result = sync_base._get_franchise_id_mapping()
            assert result == {}

    def test_with_data(self, sync_base):
        with patch("src.models.franchise.Franchise"):
            mock_sf = MagicMock()
            mock_sf.id = 1
            mock_sf.original_code = "SSG"
            sync_base.sqlite_session.query.return_value.all.return_value = [mock_sf]
            mock_oci_row = MagicMock()
            mock_oci_row.original_code = "SSG"
            mock_oci_row.id = 10
            sync_base.target_session.query.return_value.filter.return_value.all.return_value = [mock_oci_row]
            result = sync_base._get_franchise_id_mapping()
            assert result == {1: 10}


class TestOCISyncBaseBulkCopyUpsert:
    def test_empty_records_returns_none(self, sync_base):
        result = sync_base._bulk_copy_upsert("test_table", [], ["id"])
        assert result is None

    def test_success(self, sync_base):
        sync_base._do_bulk_copy_upsert = MagicMock()
        sync_base._bulk_copy_upsert("test_table", [{"id": 1, "name": "test"}], ["id"])
        sync_base._do_bulk_copy_upsert.assert_called_once()

    def test_retry_then_success(self, sync_base):
        do_bulk = MagicMock()
        do_bulk.side_effect = [OperationalError("stmt", {}, Exception("connection lost")), None]
        sync_base._do_bulk_copy_upsert = do_bulk
        sync_base._reconnect_oci = MagicMock()
        with patch("src.sync.sync_base.time.sleep"):
            sync_base._bulk_copy_upsert("test_table", [{"id": 1}], ["id"])
            assert do_bulk.call_count == 2
            sync_base._reconnect_oci.assert_called_once()

    def test_all_retries_exhausted_raises(self, sync_base):
        do_bulk = MagicMock()
        do_bulk.side_effect = OperationalError("stmt", {}, Exception("persistent error"))
        sync_base._do_bulk_copy_upsert = do_bulk
        sync_base._reconnect_oci = MagicMock()
        with (
            patch("src.sync.sync_base.time.sleep"),
            pytest.raises(OperationalError),
        ):
            sync_base._bulk_copy_upsert("test_table", [{"id": 1}], ["id"])
        assert do_bulk.call_count == 3
        assert sync_base._reconnect_oci.call_count == 2


class TestOCISyncBaseReconnectOci:
    def test_basic_reconnect(self, sync_base):
        old_session = sync_base.target_session
        old_session.close = MagicMock()
        sync_base.oci_engine.dispose = MagicMock()
        with patch("src.sync.sync_base.sessionmaker") as mock_sm:
            mock_sm.return_value = MagicMock(return_value=MagicMock())
            sync_base._reconnect_oci()
            old_session.close.assert_called_once()
            sync_base.oci_engine.dispose.assert_called_once()

    def test_cleanup_exception_ignored(self, sync_base):
        old_session = sync_base.target_session
        old_session.close = MagicMock(side_effect=RuntimeError("close failed"))
        sync_base.oci_engine.dispose = MagicMock()
        with patch("src.sync.sync_base.sessionmaker") as mock_sm:
            mock_sm.return_value = MagicMock(return_value=MagicMock())
            sync_base._reconnect_oci()
            sync_base.oci_engine.dispose.assert_not_called()


class TestOCISyncBaseRawOciConnectionWithRetries:
    def test_success(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        sync_base.oci_engine.raw_connection.return_value = mock_conn
        result = sync_base._raw_oci_connection_with_retries(label="test")
        assert result is mock_conn
        mock_cursor.execute.assert_called_once()

    def test_transient_then_success(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        sync_base.oci_engine.raw_connection.side_effect = [
            OperationalError("stmt", {}, Exception("server closed")),
            mock_conn,
        ]
        sync_base._reconnect_oci = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(side_effect=[True, True])
        with patch("src.sync.sync_base.time.sleep"):
            result = sync_base._raw_oci_connection_with_retries(label="test")
            assert result is mock_conn
            sync_base._reconnect_oci.assert_called_once()

    def test_non_transient_raises(self, sync_base):
        sync_base.oci_engine.raw_connection.side_effect = RuntimeError("non transient")
        sync_base._is_transient_oci_error = MagicMock(return_value=False)
        with pytest.raises(RuntimeError, match="non transient"):
            sync_base._raw_oci_connection_with_retries(label="test")


class TestOCISyncBaseRunTargetSessionWithRetries:
    def test_success(self, sync_base):
        op = MagicMock(return_value="ok")
        result = sync_base._run_target_session_with_retries(op, label="test")
        assert result == "ok"
        op.assert_called_once()

    def test_transient_then_success(self, sync_base):
        op = MagicMock()
        op.side_effect = [OperationalError("stmt", {}, Exception("server closed")), "ok"]
        sync_base._rollback_target_session = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(side_effect=[True, True])
        sync_base._reconnect_oci = MagicMock()
        with patch("src.sync.sync_base.time.sleep"):
            result = sync_base._run_target_session_with_retries(op, label="test")
            assert result == "ok"
            assert op.call_count == 2
            sync_base._rollback_target_session.assert_called_once()

    def test_non_transient_raises(self, sync_base):
        op = MagicMock(side_effect=RuntimeError("non transient"))
        sync_base._rollback_target_session = MagicMock()
        sync_base._is_transient_oci_error = MagicMock(return_value=False)
        with pytest.raises(RuntimeError, match="non transient"):
            sync_base._run_target_session_with_retries(op, label="test")
        sync_base._rollback_target_session.assert_called_once()


class TestOCISyncBaseRollbackTargetSession:
    def test_rollback_success(self, sync_base):
        sync_base.target_session.rollback = MagicMock()
        sync_base._rollback_target_session(label="test")
        sync_base.target_session.rollback.assert_called_once()

    def test_rollback_exception_ignored(self, sync_base):
        sync_base.target_session.rollback = MagicMock(side_effect=SQLAlchemyError("rollback failed"))
        sync_base._rollback_target_session(label="test")


class TestOCISyncBaseEnsureTableAndClose:
    def test_ensure_table(self, sync_base):
        with patch("src.models.base.Base.metadata.create_all") as mock_create_all:
            mock_table = MagicMock()
            mock_model = MagicMock()
            mock_model.__table__ = mock_table
            sync_base._ensure_table(mock_model)
            mock_create_all.assert_called_once_with(sync_base.oci_engine, tables=[mock_table])

    def test_close(self, sync_base):
        sync_base.target_session.close = MagicMock()
        sync_base.oci_engine.dispose = MagicMock()
        sync_base.close()
        sync_base.target_session.close.assert_called_once()
        sync_base.oci_engine.dispose.assert_called_once()


class TestOCISyncBaseResetTargetSequence:
    def test_postgres_with_sequence(self, sync_base):
        sync_base.target_session.get_bind.return_value.dialect.name = "postgresql"
        sync_base.target_session.execute.return_value.scalar.return_value = "test_table_id_seq"
        result = sync_base._reset_target_sequence_for_table("test_table")
        assert result is True
        assert sync_base.target_session.execute.call_count >= 2
        sync_base.target_session.commit.assert_called_once()

    def test_postgres_no_sequence(self, sync_base):
        sync_base.target_session.get_bind.return_value.dialect.name = "postgresql"
        sync_base.target_session.execute.return_value.scalar.return_value = None
        result = sync_base._reset_target_sequence_for_table("test_table")
        assert result is False

    def test_non_postgres_returns_false(self, sync_base):
        sync_base.target_session.get_bind.return_value.dialect.name = "sqlite"
        result = sync_base._reset_target_sequence_for_table("test_table")
        assert result is False


class TestOCISyncBaseSyncInBatches:
    def test_empty_config_returns_zero(self, sync_base):
        sync_base.oci_engine = MagicMock()
        sync_base._raw_oci_connection_with_retries = MagicMock()
        sync_base._bulk_copy_upsert = MagicMock()
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
        result = sync_base._sync_in_batches(config)
        assert result == 0

    def test_copies_batches_successfully(self, sync_base):
        sync_base.oci_engine = MagicMock()
        mock_conn = MagicMock()
        sync_base._raw_oci_connection_with_retries = MagicMock(return_value=mock_conn)
        sync_base._bulk_copy_upsert = MagicMock()
        fake_row1 = MagicMock()
        fake_row1._mapping = {"a": 1}
        fake_row2 = MagicMock()
        fake_row2._mapping = {"a": 2}
        model = MagicMock()
        model.__tablename__ = "test"
        config = SyncBatchConfig(
            model=model,
            query=MagicMock(),
            total_count=2,
            columns=["a"],
            conflict_keys=["a"],
            transform_fn=None,
            batch_size=100,
            update_timestamp=True,
        )
        config.query.offset.return_value.limit.return_value.all.return_value = [fake_row1, fake_row2]
        result = sync_base._sync_in_batches(config)
        assert result == 2
        sync_base._bulk_copy_upsert.assert_called_once()

    def test_fallback_to_row_by_row_on_bulk_error(self, sync_base):
        sync_base.oci_engine = MagicMock()
        mock_conn = MagicMock()
        sync_base._raw_oci_connection_with_retries = MagicMock(return_value=mock_conn)
        sync_base._bulk_copy_upsert = MagicMock(side_effect=OperationalError("stmt", {}, Exception("copy failed")))
        sync_base._direct_insert_upsert = MagicMock()
        fake_row = MagicMock()
        fake_row._mapping = {"a": 1}
        model = MagicMock()
        model.__tablename__ = "test"
        config = SyncBatchConfig(
            model=model,
            query=MagicMock(),
            total_count=1,
            columns=["a"],
            conflict_keys=["a"],
            transform_fn=None,
            batch_size=100,
            update_timestamp=True,
        )
        config.query.offset.return_value.limit.return_value.all.return_value = [fake_row]
        result = sync_base._sync_in_batches(config)
        assert result == 1
        sync_base._bulk_copy_upsert.assert_called_once()
        sync_base._direct_insert_upsert.assert_called_once()

    def test_fallback_close_connection_on_error(self, sync_base):
        sync_base.oci_engine = MagicMock()
        mock_conn = MagicMock()
        sync_base._raw_oci_connection_with_retries = MagicMock(return_value=mock_conn)
        sync_base._bulk_copy_upsert = MagicMock(side_effect=OperationalError("stmt", {}, Exception("copy failed")))
        sync_base._direct_insert_upsert = MagicMock()
        model = MagicMock()
        model.__tablename__ = "test"
        fake_row = MagicMock()
        fake_row._mapping = {"a": 1}
        config = SyncBatchConfig(
            model=model,
            query=MagicMock(),
            total_count=1,
            columns=["a"],
            conflict_keys=["a"],
            transform_fn=None,
            batch_size=100,
            update_timestamp=True,
        )
        config.query.offset.return_value.limit.return_value.all.return_value = [fake_row]
        sync_base._sync_in_batches(config)
        mock_conn.close.assert_called()


class TestOCISyncBaseDoBulkCopyUpsert:
    def test_with_unique_cols_and_update_timestamp(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        records = [{"id": 1, "name": "test"}]
        sync_base._do_bulk_copy_upsert("test_table", records, ["id"], update_timestamp=True, connection=mock_conn)
        assert mock_cursor.execute.call_count >= 3
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_without_unique_cols(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        records = [{"id": 1, "name": "test"}]
        sync_base._do_bulk_copy_upsert("test_table", records, [], update_timestamp=True, connection=mock_conn)
        assert mock_cursor.execute.call_count >= 3

    def test_no_update_cols(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        records = [{"id": 1}]
        sync_base._do_bulk_copy_upsert("test_table", records, ["id"], update_timestamp=True, connection=mock_conn)
        assert mock_cursor.execute.call_count >= 3

    def test_error_path_rollback_and_raise(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = RuntimeError("execution failed")
        records = [{"id": 1, "name": "test"}]
        with pytest.raises(RuntimeError, match="execution failed"):
            sync_base._do_bulk_copy_upsert("test_table", records, ["id"], update_timestamp=True, connection=mock_conn)
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_uses_internal_connection_when_none_passed(self, sync_base):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        sync_base.oci_engine.raw_connection.return_value = mock_conn
        records = [{"id": 1, "name": "test"}]
        sync_base._do_bulk_copy_upsert("test_table", records, ["id"], update_timestamp=True, connection=None)
        assert mock_cursor.execute.call_count >= 3
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestOCISyncBaseSyncSimpleTable:
    def test_target_table_missing_returns_zero(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=False)
        model = MagicMock()
        model.__tablename__ = "test"
        result = sync_base.sync_simple_table(model, ["id"])
        assert result == 0

    def test_no_compatible_columns_returns_zero(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=[])
        model = MagicMock()
        model.__tablename__ = "test"
        result = sync_base.sync_simple_table(model, ["id"])
        assert result == 0

    def test_empty_query_result_returns_zero(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=["a"])
        sync_base.sqlite_session.query.return_value.count.return_value = 0
        model = MagicMock()
        model.__tablename__ = "test"
        result = sync_base.sync_simple_table(model, ["id"])
        assert result == 0

    def test_with_filters_applies_filter_to_query(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=["a", "b"])
        sync_base.sqlite_session.query.return_value.count.return_value = 1
        sync_base._sync_in_batches = MagicMock(return_value=1)
        model = MagicMock()
        model.__tablename__ = "test"
        my_filter = MagicMock()
        result = sync_base.sync_simple_table(model, ["id"], filters=[my_filter])
        assert result == 1
        sync_base.sqlite_session.query.return_value.filter.assert_called_once_with(my_filter)

    def test_with_dedupe_keys_passes_through(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=["a", "b"])
        sync_base.sqlite_session.query.return_value.count.return_value = 1
        sync_base._sync_in_batches = MagicMock(return_value=1)
        model = MagicMock()
        model.__tablename__ = "test"
        result = sync_base.sync_simple_table(model, ["id"], dedupe_keys=["a", "b"])
        assert result == 1

    def test_exclude_cols_without_id_appends_it(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=["a"])
        sync_base.sqlite_session.query.return_value.count.return_value = 0
        model = MagicMock()
        model.__tablename__ = "test"
        sync_base.sync_simple_table(model, ["id"], exclude_cols=["other"])
        sync_base._resolve_sync_columns.assert_called_with(model, ["other", "id"])

    def test_update_timestamp_explicit_false(self, sync_base):
        sync_base._target_table_exists = MagicMock(return_value=True)
        sync_base._resolve_sync_columns = MagicMock(return_value=["a"])
        sync_base.sqlite_session.query.return_value.count.return_value = 1
        sync_base._sync_in_batches = MagicMock(return_value=1)
        model = MagicMock()
        model.__tablename__ = "test"
        result = sync_base.sync_simple_table(model, ["id"], update_timestamp=False)
        assert result == 1


class TestBuildGameSyncEligibilityEdgeCases:
    def test_empty_game_ids_returns_empty_eligibility(self):
        session = MagicMock(spec=Session)
        e = build_game_sync_eligibility(session, [])
        assert e.parent_game_ids == []
        assert e.skipped_schedule_only == []

    def test_cancelled_with_score_in_parent_not_schedule_only(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="CANCELLED",
                home_score=0,
                away_score=0,
            )
        )
        session.commit()
        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_cancelled
        assert "20250601_01" in e.parent_game_ids
        assert "20250601_01" not in e.skipped_schedule_only
        session.close()

    def test_scheduled_without_score_skipped_as_schedule_only(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="SCHEDULED",
            )
        )
        session.commit()
        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_schedule_only
        assert "20250601_01" not in e.parent_game_ids
        session.close()

    def test_completed_with_relay_adds_relay_ids(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game, GameBattingStat, GamePitchingStat, GameEvent

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="COMPLETED",
                home_score=5,
                away_score=3,
            )
        )
        session.add(
            GameBattingStat(
                game_id="20250601_01",
                team_side="home",
                player_id="p1",
                player_name="P1",
                appearance_seq=1,
            )
        )
        session.add(
            GameBattingStat(
                game_id="20250601_01",
                team_side="away",
                player_id="p2",
                player_name="P2",
                appearance_seq=2,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01",
                team_side="home",
                player_id="p3",
                player_name="P3",
                appearance_seq=1,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01",
                team_side="away",
                player_id="p4",
                player_name="P4",
                appearance_seq=2,
            )
        )
        session.add(
            GameEvent(
                game_id="20250601_01",
                event_seq=1,
                event_type="hit",
                inning=1,
                description="single",
            )
        )
        session.commit()
        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.relay_game_ids
        assert "20250601_01" in e.detail_game_ids
        assert "20250601_01" not in e.skipped_empty_relay
        assert "20250601_01" not in e.skipped_incomplete_detail
        session.close()

    def test_completed_without_detail_skipped_incomplete(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="COMPLETED",
                home_score=5,
                away_score=3,
            )
        )
        session.commit()
        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.parent_game_ids
        assert "20250601_01" not in e.detail_game_ids
        assert "20250601_01" in e.skipped_incomplete_detail
        assert "20250601_01" in e.skipped_empty_relay
        session.close()


class TestLoadGameSyncSignaturesEdgeCases:
    def test_returns_empty_with_no_games(self, _db_engine):
        session = sessionmaker(bind=_db_engine)()
        sigs = load_game_sync_signatures(session)
        assert sigs == {}
        session.close()

    def test_with_game_data_sections(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="COMPLETED",
                home_score=5,
                away_score=3,
                home_pitcher="p1",
                away_pitcher="p2",
            )
        )
        session.commit()
        sigs = load_game_sync_signatures(session)
        assert "20250601_01" in sigs
        game_sig = sigs["20250601_01"]["game"]
        assert game_sig["home_team"] == "SSG"
        assert game_sig["away_team"] == "LG"
        assert game_sig["home_score"] == 5
        assert game_sig["away_score"] == 3
        assert game_sig["home_pitcher"] == "p1"
        assert game_sig["away_pitcher"] == "p2"
        session.close()

    def test_with_game_ids_filter(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="COMPLETED",
            )
        )
        session.add(
            Game(
                game_id="20250602_01",
                game_date=datetime(2025, 6, 2),
                home_team="LG",
                away_team="SSG",
                game_status="SCHEDULED",
            )
        )
        session.commit()
        sigs = load_game_sync_signatures(session, game_ids=["20250601_01"])
        assert "20250601_01" in sigs
        assert "20250602_01" not in sigs
        session.close()


class TestFilterGameIdsByYearEdgeCases:
    def test_mixed_prefixes_and_empty(self):
        assert filter_game_ids_by_year([], 2024) == []
        assert filter_game_ids_by_year(["20240101", "20230101"], 2022) == []


class TestDetectDirtyGameIdsEdgeCases:
    def test_local_game_missing_in_remote_is_dirty(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game": {"game_status": "COMPLETED"}}},
                {},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == ["g1"]

    def test_no_local_signatures_returns_empty(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch("src.sync.sync_base.load_game_sync_signatures", side_effect=[{}, {}]):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == []


class TestFilterPublishableGameIdsEdgeCases:
    def test_empty_input(self):
        session = MagicMock(spec=Session)
        result = filter_publishable_game_ids(session, [])
        assert result == []

    def test_with_publishable_ids(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()
        session.add(
            Game(
                game_id="20250601_01",
                game_date=datetime(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
                game_status="SCHEDULED",
                home_score=1,
                away_score=0,
            )
        )
        session.commit()
        result = filter_publishable_game_ids(session, ["20250601_01"])
        assert result == ["20250601_01"]
        session.close()


class TestIsTransientOCIDetailed:
    def test_connection_reset_message(self):
        err = Exception("connection reset by peer")
        assert OCISyncBase._is_transient_oci_error(err) is True

    def test_timeout_expired_message(self):
        err = Exception("timeout expired")
        assert OCISyncBase._is_transient_oci_error(err) is True

    def test_syntax_error_non_transient(self):
        err = Exception("syntax error at or near")
        assert OCISyncBase._is_transient_oci_error(err) is False

    def test_ssl_syscall_message(self):
        err = Exception("ssl syscall error: connection closed")
        assert OCISyncBase._is_transient_oci_error(err) is True
