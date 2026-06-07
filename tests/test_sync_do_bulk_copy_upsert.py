from __future__ import annotations

import csv
import io as io_module
import random
from unittest.mock import MagicMock

import pytest

from src.sync.oci_sync import OCISync

# ── helpers ───────────────────────────────────────────────────────────


def _build_syncer():
    syncer = object.__new__(OCISync)
    syncer.oci_engine = MagicMock()
    syncer.sqlite_session = None
    syncer.target_session = None
    return syncer


def _mock_cursor(syncer):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    syncer.oci_engine.raw_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    return mock_cursor, mock_conn


def _capture_sql_calls(mock_cursor):
    """Return all strings passed to cursor.execute() and cursor.copy_expert()."""
    sql_calls = []
    for c in mock_cursor.execute.call_args_list:
        sql_calls.append(c[0][0])
    for c in mock_cursor.copy_expert.call_args_list:
        sql_calls.append(c[0][0])
    return sql_calls


# ── _do_bulk_copy_upsert ──────────────────────────────────────────────


class TestDoBulkCopyUpsert:
    def test_creates_temp_table(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("test_tbl", [{"id": 1}], ["id"], True, csv, io_module, random)
        create_calls = [c[0][0] for c in cursor.execute.call_args_list if "CREATE TEMP TABLE" in c[0][0]]
        assert len(create_calls) == 1
        assert "CREATE TEMP TABLE" in create_calls[0]
        assert "LIKE test_tbl INCLUDING DEFAULTS" in create_calls[0]

    def test_insert_with_conflict_update(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1, "name": "a"}], ["id"], True, csv, io_module, random)
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert "ON CONFLICT" in insert_sql
        assert '"id"' in insert_sql
        assert 'DO UPDATE SET "name" = EXCLUDED."name"' in insert_sql

    def test_insert_without_conflict(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1}], [], True, csv, io_module, random)
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert "ON CONFLICT" not in insert_sql

    def test_do_nothing_when_no_update_cols(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert "DO NOTHING" in insert_sql

    def test_update_timestamp_appended(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert(
            "t",
            [{"id": 1, "name": "a"}],
            ["id"],
            update_timestamp=True,
            csv=csv,
            io=io_module,
            random=random,
        )
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert '"updated_at" = CURRENT_TIMESTAMP' in insert_sql

    def test_update_timestamp_skipped_when_in_keys(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert(
            "t",
            [{"id": 1, "updated_at": "2025-01-01"}],
            ["id"],
            update_timestamp=True,
            csv=csv,
            io=io_module,
            random=random,
        )
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert "CURRENT_TIMESTAMP" not in insert_sql

    def test_update_timestamp_false_no_append(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert(
            "t",
            [{"id": 1, "name": "a"}],
            ["id"],
            update_timestamp=False,
            csv=csv,
            io=io_module,
            random=random,
        )
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert "CURRENT_TIMESTAMP" not in insert_sql

    def test_columns_double_quoted(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"my-col": 1, "another col": "x"}], ["my-col"], True, csv, io_module, random)
        sqls = _capture_sql_calls(cursor)
        insert_sql = [s for s in sqls if "INSERT INTO t" in s][0]
        assert '"my-col"' in insert_sql
        assert '"another col"' in insert_sql

    def test_temp_table_naming(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("my_table", [{"id": 1}], ["id"], True, csv, io_module, random)
        create_calls = [c[0][0] for c in cursor.execute.call_args_list if "CREATE TEMP TABLE" in c[0][0]]
        assert "temp_my_table_" in create_calls[0]

    def test_drops_temp_table(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        drop_calls = [c[0][0] for c in cursor.execute.call_args_list if "DROP TABLE" in c[0][0]]
        assert len(drop_calls) == 1

    def test_copy_command(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1, "name": "a"}], ["id"], True, csv, io_module, random)
        assert cursor.copy_expert.call_count == 1
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert "COPY temp_" in copy_sql
        assert '"id", "name"' in copy_sql
        assert "FROM STDIN" in copy_sql
        assert "FORMAT CSV" in copy_sql
        assert "DELIMITER '\t'" in copy_sql or "DELIMITER E'\\t'" in copy_sql

    def test_commits_on_success(self):
        syncer = _build_syncer()
        _, conn = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        conn.commit.assert_called_once()

    def test_rollback_on_error(self):
        syncer = _build_syncer()
        cursor, conn = _mock_cursor(syncer)
        cursor.execute.side_effect = RuntimeError("db error")
        with pytest.raises(RuntimeError, match="db error"):
            syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        conn.rollback.assert_called_once()

    def test_closes_cursor_and_connection(self):
        syncer = _build_syncer()
        cursor, conn = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        cursor.close.assert_called_once()
        conn.close.assert_called_once()

    def test_csv_tab_delimited(self):
        syncer = _build_syncer()
        cursor, _ = _mock_cursor(syncer)
        syncer._do_bulk_copy_upsert("t", [{"id": 1, "val": "hello"}], ["id"], True, csv, io_module, random)
        copy_sql = cursor.copy_expert.call_args[0][0]
        assert "FORMAT CSV" in copy_sql
        assert "DELIMITER" in copy_sql

    def test_error_closes_cursor_and_connection(self):
        syncer = _build_syncer()
        cursor, conn = _mock_cursor(syncer)
        cursor.execute.side_effect = RuntimeError("fail")
        with pytest.raises(RuntimeError):
            syncer._do_bulk_copy_upsert("t", [{"id": 1}], ["id"], True, csv, io_module, random)
        cursor.close.assert_called_once()
        conn.close.assert_called_once()
