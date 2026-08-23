"""Unit tests for src.sync.verifier."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

from src.sync.table_dag import TableMeta
from src.sync.verifier import SyncConsistencyVerifier


def test_verify_table_match() -> None:
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, name TEXT)")
    sqlite_conn.execute("INSERT INTO test_tbl (id, name) VALUES (1, 'A'), (2, 'B')")

    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (2,)

    verifier = SyncConsistencyVerifier(sqlite_conn, mock_engine)
    meta = TableMeta("test_tbl", level=0)
    item = verifier.verify_table(meta)

    assert item.table_name == "test_tbl"
    assert item.sqlite_count == 2
    assert item.oci_count == 2
    assert item.diff == 0
    assert item.status == "MATCH"


def test_verify_table_mismatch() -> None:
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, name TEXT)")
    sqlite_conn.execute("INSERT INTO test_tbl (id, name) VALUES (1, 'A')")

    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (0,)

    verifier = SyncConsistencyVerifier(sqlite_conn, mock_engine)
    meta = TableMeta("test_tbl", level=0)
    item = verifier.verify_table(meta)

    assert item.sqlite_count == 1
    assert item.oci_count == 0
    assert item.diff == 1
    assert item.status == "OCI_EMPTY"


def test_verify_all_report() -> None:
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.execute("CREATE TABLE tbl_1 (id INTEGER PRIMARY KEY)")
    sqlite_conn.execute("CREATE TABLE tbl_2 (id INTEGER PRIMARY KEY)")
    sqlite_conn.execute("INSERT INTO tbl_1 VALUES (1)")
    sqlite_conn.execute("INSERT INTO tbl_2 VALUES (1)")

    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (1,)

    verifier = SyncConsistencyVerifier(sqlite_conn, mock_engine)
    tables = [TableMeta("tbl_1", level=0), TableMeta("tbl_2", level=0)]
    report = verifier.verify_all(tables)

    assert report.tables_checked == 2
    assert report.matching_tables == 2
    assert report.overall_status == "PASS"
