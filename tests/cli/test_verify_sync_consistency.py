from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.verify_sync_consistency import (
    _collect_count_mismatches,
    _log_count_results,
    check_table_counts,
    get_row_count,
    main,
    run_consistency_audit,
)


class TestCheckTableCounts:
    def test_returns_list(self):
        sqlite_conn = MagicMock()
        oci_conn = MagicMock()
        sqlite_conn.execute.return_value.scalar.return_value = 100
        oci_conn.execute.return_value.scalar.return_value = 100
        result = check_table_counts(sqlite_conn, oci_conn)
        assert isinstance(result, list)


class TestGetRowCount:
    def test_returns_count(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 42
        result = get_row_count(conn, "game")
        assert result == 42


class TestCollectCountMismatches:
    def test_no_mismatches(self):
        results = [{"table_name": "game", "sqlite_count": 100, "oci_count": 100, "status": "MATCH", "delta": 0}]
        mismatches, _ = _collect_count_mismatches(results)
        assert len(mismatches) == 0

    def test_finds_mismatches(self):
        results = [{"table_name": "game", "sqlite_count": 100, "oci_count": 95, "status": "MISMATCH", "delta": 5}]
        mismatches, _ = _collect_count_mismatches(results)
        assert len(mismatches) > 0


class TestLogCountResults:
    def test_logs_results(self, caplog):
        results = [{"table_name": "game", "sqlite_count": 100, "oci_count": 100, "status": "MATCH", "delta": 0}]
        with caplog.at_level(logging.INFO):
            _log_count_results(results)


class TestRunConsistencyAudit:
    def test_returns_true_when_all_match(self):
        with (
            patch("src.cli.verify_sync_consistency.check_table_counts") as mock_counts,
            patch("src.cli.verify_sync_consistency._collect_count_mismatches") as mock_mismatches,
            patch("src.cli.verify_sync_consistency._collect_deep_mismatches") as mock_deep,
            patch("src.cli.verify_sync_consistency._send_consistency_mismatch_alert"),
            patch("src.cli.verify_sync_consistency.get_oci_url", return_value="sqlite:///fake.db"),
        ):
            mock_counts.return_value = []
            mock_mismatches.return_value = ([], [])
            mock_deep.return_value = ([], [])

            result = run_consistency_audit()
            assert result is True


class TestVerifySyncConsistency:
    def test_default_run(self):
        with patch("src.cli.verify_sync_consistency.run_consistency_audit") as mock:
            mock.return_value = True
            with pytest.raises(SystemExit):
                main()

    def test_failure_exits_nonzero(self):
        with patch("src.cli.verify_sync_consistency.run_consistency_audit") as mock:
            mock.return_value = False
            with pytest.raises(SystemExit):
                main()
