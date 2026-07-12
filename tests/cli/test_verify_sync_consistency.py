from __future__ import annotations

import logging
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.cli.verify_sync_consistency import (
    _collect_count_mismatches,
    _compute_count_status,
    _compute_match_rate,
    _format_count_alert_line,
    _format_deep_alert_line,
    _format_game_season_fk_alert,
    _format_missing_keys,
    _log_count_results,
    _log_game_season_fk_results,
    _stringify_row,
    check_game_season_fk,
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


class TestPureHelpers:
    @pytest.mark.parametrize(
        ("delta", "expected"),
        [(0, "OK"), (-1, "OK (OCI+)"), (1, "MISMATCH")],
    )
    def test_compute_count_status(self, delta, expected):
        assert _compute_count_status(delta) == expected

    def test_stringify_row_formats_dates_and_scalars(self):
        row = [date(2025, 4, 1), datetime(2025, 4, 1, 12, 30), 123, None]
        assert _stringify_row(row) == ("2025-04-01", "2025-04-01T12:30:00", "123", "None")

    def test_compute_match_rate(self):
        assert _compute_match_rate(2, 5) == pytest.approx(60.0)
        assert _compute_match_rate(0, 0) == 100.0

    def test_format_missing_keys_unwraps_singletons(self):
        result = _format_missing_keys({("a",), ("b", "c")}, limit=10)
        assert "a" in result
        assert ("b", "c") in result

    def test_alert_formatters(self):
        res = {"table_name": "game", "sqlite_count": 10, "oci_count": 8, "delta": 2}
        assert _format_count_alert_line(res) == "• <b>game</b>: SQLite=10 vs OCI=8 (Delta=2)"
        assert _format_deep_alert_line("game", 80, "g1") == (
            "• <b>game</b>: Key ID match rate is 80% (Sample missing keys: g1)"
        )
        assert _format_game_season_fk_alert("SQLite", 3) == (
            "• SQLite: 3 game rows with invalid season_id (not in kbo_seasons)"
        )


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


class TestGameSeasonFk:
    def test_check_game_season_fk_returns_counts(self):
        sqlite_conn = MagicMock()
        oci_conn = MagicMock()
        sqlite_conn.execute.return_value.scalar.return_value = 0
        oci_conn.execute.return_value.scalar.return_value = 2

        result = check_game_season_fk(sqlite_conn, oci_conn)

        assert result == [
            {"db": "SQLite", "orphan_game_count": 0},
            {"db": "OCI", "orphan_game_count": 2},
        ]

    def test_check_game_season_fk_marks_query_errors(self):
        sqlite_conn = MagicMock()
        oci_conn = MagicMock()
        sqlite_conn.execute.side_effect = RuntimeError("missing table")
        oci_conn.execute.return_value.scalar.return_value = 0

        result = check_game_season_fk(sqlite_conn, oci_conn)

        assert result[0] == {"db": "SQLite", "orphan_game_count": -1}
        assert result[1] == {"db": "OCI", "orphan_game_count": 0}

    def test_log_game_season_fk_results_returns_alerts(self, caplog):
        rows = [
            {"db": "SQLite", "orphan_game_count": -1},
            {"db": "OCI", "orphan_game_count": 0},
            {"db": "Mirror", "orphan_game_count": 4},
        ]

        with caplog.at_level(logging.INFO):
            alerts = _log_game_season_fk_results(rows)

        assert alerts == ["• Mirror: 4 game rows with invalid season_id (not in kbo_seasons)"]


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
