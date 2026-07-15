from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.sync.sync_base import BulkCopyUpsertOptions, OCISyncBase, normalize_oracle_url


@pytest.fixture
def oracle_sync() -> OCISyncBase:
    sync = OCISyncBase.__new__(OCISyncBase)
    sync.sqlite_session = MagicMock()
    sync.target_session = MagicMock()
    sync.oci_engine = MagicMock()
    sync._oracle_columns_cache = {}
    return sync


def _connection() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    return connection, cursor


def _compact_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_normalize_oracle_url_encodes_unquoted_password_characters() -> None:
    url = "oracle+oracledb://user:p@ss:word@db.example/service"

    assert normalize_oracle_url(url) == "oracle+oracledb://user:p%40ss%3Aword@db.example/service"


def test_normalize_oracle_url_preserves_non_oracle_url() -> None:
    url = "postgresql://user:p@ss:word@db.example/service"

    assert normalize_oracle_url(url) == url


def test_oracle_initialization_normalizes_url_and_wallet_credentials(monkeypatch) -> None:
    sqlite_session = MagicMock()
    oracle_url = "oracle+oracledb://user:p%40ss%3Aword@db.example/service"
    engine = MagicMock()
    engine.dialect = SimpleNamespace(name="oracle")
    target_session = MagicMock()
    monkeypatch.setenv("TNS_ADMIN", "/wallet")

    with (
        patch("src.sync.sync_base.create_engine", return_value=engine) as create_engine,
        patch("src.sync.sync_base.sessionmaker", return_value=MagicMock(return_value=target_session)),
    ):
        sync = OCISyncBase(oracle_url, sqlite_session)

    assert sync.oci_engine is engine
    assert sync.target_session is target_session
    create_engine.assert_called_once_with(
        oracle_url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=240,
        pool_timeout=30,
        connect_args={
            "config_dir": "/wallet",
            "wallet_location": "/wallet",
            "wallet_password": "p@ss:word",
        },
    )


@pytest.mark.parametrize(
    ("target_columns", "has_target_timestamps"),
    [
        ({"id", "name", "payload", "created_at", "updated_at"}, True),
        ({"id", "name", "payload"}, False),
    ],
)
def test_direct_oracle_insert_adds_target_timestamps_and_serializes_json(
    oracle_sync,
    target_columns,
    has_target_timestamps,
) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value=target_columns)
    record = {"id": 1, "name": "홍길동", "payload": {"runs": [1, 2]}}

    oracle_sync._direct_insert_upsert_oracle(
        "game_events",
        record,
        ["id"],
        update_timestamp=True,
        connection=connection,
    )

    sql, params = cursor.execute.call_args.args
    compact_sql = _compact_sql(sql)
    assert "MERGE INTO game_events t" in compact_sql
    assert params == {
        "id": 1,
        "name": "홍길동",
        "payload": json.dumps({"runs": [1, 2]}, ensure_ascii=False),
    }
    if has_target_timestamps:
        assert '"CREATED_AT"' in compact_sql
        assert '"UPDATED_AT"' in compact_sql
        assert compact_sql.count("CURRENT_TIMESTAMP") == 3
    else:
        assert '"CREATED_AT"' not in compact_sql
        assert '"UPDATED_AT"' not in compact_sql
        assert "CURRENT_TIMESTAMP" not in compact_sql
    connection.commit.assert_called_once()
    cursor.close.assert_called_once()


def test_direct_oracle_merge_update_timestamp_false_omits_timestamp_update(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id", "name"})

    oracle_sync._direct_insert_upsert_oracle(
        "game_events",
        {"id": 1, "name": "updated"},
        ["id"],
        update_timestamp=False,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.execute.call_args.args[0])
    assert 'WHEN MATCHED THEN UPDATE SET t."NAME" = :name' in compact_sql
    assert "CURRENT_TIMESTAMP" not in compact_sql


def test_direct_oracle_merge_without_update_columns_has_no_matched_clause(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    oracle_sync._direct_insert_upsert_oracle(
        "game_events",
        {"id": 1},
        ["id"],
        update_timestamp=True,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.execute.call_args.args[0])
    assert "MERGE INTO game_events t" in compact_sql
    assert "WHEN MATCHED THEN UPDATE" not in compact_sql
    assert "WHEN NOT MATCHED THEN INSERT" in compact_sql


def test_direct_oracle_insert_without_conflict_keys_uses_insert(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id", "name"})

    oracle_sync._direct_insert_upsert_oracle(
        "game_events",
        {"id": 1, "name": "new"},
        [],
        update_timestamp=True,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.execute.call_args.args[0])
    assert compact_sql.startswith('INSERT INTO game_events ("ID", "NAME") VALUES (:id, :name)')
    assert "MERGE INTO" not in compact_sql


@pytest.mark.parametrize(
    ("target_columns", "has_target_timestamps"),
    [
        ({"id", "name", "payload", "created_at", "updated_at"}, True),
        ({"id", "name", "payload"}, False),
    ],
)
def test_bulk_oracle_merge_adds_target_timestamps_and_serializes_json(
    oracle_sync,
    target_columns,
    has_target_timestamps,
) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value=target_columns)
    records = [
        {"id": 1, "name": "first", "payload": {"ok": True}},
        {"id": 2, "name": "second", "payload": ["a", "b"]},
    ]

    oracle_sync._do_bulk_merge_oracle(
        "game_events",
        records,
        ["id"],
        update_timestamp=True,
        connection=connection,
    )

    sql, params = cursor.executemany.call_args.args
    compact_sql = _compact_sql(sql)
    assert "MERGE INTO game_events t" in compact_sql
    assert params == [
        {"id": 1, "name": "first", "payload": '{"ok": true}'},
        {"id": 2, "name": "second", "payload": '["a", "b"]'},
    ]
    if has_target_timestamps:
        assert '"CREATED_AT"' in compact_sql
        assert '"UPDATED_AT"' in compact_sql
        assert compact_sql.count("CURRENT_TIMESTAMP") == 3
    else:
        assert '"CREATED_AT"' not in compact_sql
        assert '"UPDATED_AT"' not in compact_sql
        assert "CURRENT_TIMESTAMP" not in compact_sql
    connection.commit.assert_called_once()
    cursor.close.assert_called_once()


def test_bulk_oracle_merge_update_timestamp_false_omits_timestamp_update(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id", "name"})

    oracle_sync._do_bulk_merge_oracle(
        "game_events",
        [{"id": 1, "name": "updated"}],
        ["id"],
        update_timestamp=False,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.executemany.call_args.args[0])
    assert 'WHEN MATCHED THEN UPDATE SET t."NAME" = :name' in compact_sql
    assert "CURRENT_TIMESTAMP" not in compact_sql


def test_bulk_oracle_merge_without_unique_columns_uses_insert(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id", "name"})

    oracle_sync._do_bulk_merge_oracle(
        "game_events",
        [{"id": 1, "name": "new"}],
        [],
        update_timestamp=True,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.executemany.call_args.args[0])
    assert compact_sql.startswith('INSERT INTO game_events ("ID", "NAME") VALUES (:id, :name)')
    assert "MERGE INTO" not in compact_sql


def test_bulk_oracle_merge_without_update_columns_has_no_matched_clause(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    oracle_sync._do_bulk_merge_oracle(
        "game_events",
        [{"id": 1}],
        ["id"],
        update_timestamp=True,
        connection=connection,
    )

    compact_sql = _compact_sql(cursor.executemany.call_args.args[0])
    assert "WHEN MATCHED THEN UPDATE" not in compact_sql
    assert "WHEN NOT MATCHED THEN INSERT" in compact_sql


def test_bulk_copy_upsert_routes_oracle_records_to_merge(oracle_sync) -> None:
    oracle_sync.oci_engine.dialect.name = "oracle"
    oracle_sync._do_bulk_merge_oracle = MagicMock()

    oracle_sync._bulk_copy_upsert(
        "game_events",
        BulkCopyUpsertOptions(
            records=[{"id": 1, "payload": {"key": "값"}}],
            unique_cols=["id"],
        ),
    )

    oracle_sync._do_bulk_merge_oracle.assert_called_once_with(
        "game_events",
        [{"id": 1, "payload": '{"key": "값"}'}],
        ["id"],
        update_timestamp=True,
        connection=None,
    )


def test_direct_oracle_insert_rolls_back_and_closes_cursor_on_error(oracle_sync) -> None:
    connection, cursor = _connection()
    cursor.execute.side_effect = RuntimeError("insert failed")
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    with pytest.raises(RuntimeError, match="insert failed"):
        oracle_sync._direct_insert_upsert_oracle(
            "game_events",
            {"id": 1},
            [],
            update_timestamp=True,
            connection=connection,
        )

    connection.rollback.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_not_called()


def test_bulk_oracle_merge_rolls_back_and_closes_cursor_on_error(oracle_sync) -> None:
    connection, cursor = _connection()
    cursor.executemany.side_effect = RuntimeError("merge failed")
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    with pytest.raises(RuntimeError, match="merge failed"):
        oracle_sync._do_bulk_merge_oracle(
            "game_events",
            [{"id": 1}],
            [],
            update_timestamp=True,
            connection=connection,
        )

    connection.rollback.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_not_called()


def test_bulk_oracle_merge_closes_internal_connection_on_error(oracle_sync) -> None:
    connection, cursor = _connection()
    cursor.executemany.side_effect = RuntimeError("merge failed")
    oracle_sync.oci_engine.raw_connection.return_value = connection
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    with pytest.raises(RuntimeError, match="merge failed"):
        oracle_sync._do_bulk_merge_oracle(
            "game_events",
            [{"id": 1}],
            [],
            update_timestamp=True,
        )

    connection.rollback.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_bulk_oracle_merge_closes_internal_connection_after_success(oracle_sync) -> None:
    connection, cursor = _connection()
    oracle_sync.oci_engine.raw_connection.return_value = connection
    oracle_sync._oracle_columns = MagicMock(return_value={"id"})

    oracle_sync._do_bulk_merge_oracle(
        "game_events",
        [{"id": 1}],
        [],
        update_timestamp=True,
    )

    connection.commit.assert_called_once()
    cursor.close.assert_called_once()
    connection.close.assert_called_once()
