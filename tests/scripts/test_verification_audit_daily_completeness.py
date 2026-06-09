from datetime import date
from unittest.mock import MagicMock, patch

from scripts.verification.audit_daily_completeness import (
    _coerce_date,
    _format_scope,
    _parse_statuses,
    audit_completeness,
)


class TestCoerceDate:
    def test_yyyymmdd(self):
        assert _coerce_date("20250401") == date(2025, 4, 1)

    def test_iso(self):
        assert _coerce_date("2025-04-01") == date(2025, 4, 1)

    def test_none(self):
        assert _coerce_date(None) is None

    def test_invalid(self):
        import pytest

        with pytest.raises(ValueError):
            _coerce_date("invalid")


class TestParseStatuses:
    def test_empty_without_incomplete(self):
        result = _parse_statuses("", include_incomplete=False)
        assert "COMPLETED" in result

    def test_with_incomplete(self):
        result = _parse_statuses("DRAW", include_incomplete=True)
        assert "DRAW" in result

    def test_custom(self):
        result = _parse_statuses("LIVE, FINAL", include_incomplete=False)
        assert "LIVE" in result


class TestFormatScope:
    def test_with_date(self):
        result = _format_scope(date(2025, 4, 1), 7, ["COMPLETED"], strict=False)
        assert "2025-04-01" in result

    def test_without_date(self):
        result = _format_scope(None, 14, ["COMPLETED"], strict=False)
        assert "14" in result

    def test_strict_mode(self):
        result = _format_scope(None, 7, ["COMPLETED"], strict=True)
        assert "strict" in result


class TestAuditCompleteness:
    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_no_rows(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []

        result = audit_completeness("sqlite:///test", 7, statuses=["COMPLETED"])
        assert result == 0

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_with_rows_pass(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        class MockRow:
            def __init__(self):
                self.game_date = "2025-04-01"
                self.game_id = "G1"
                self.hitter_cnt = 10
                self.pitcher_cnt = 5
                self.relay_cnt = 50

            def __getitem__(self, i):
                return [self.game_date, self.game_id, self.hitter_cnt, self.pitcher_cnt, self.relay_cnt][i]

            def __iter__(self):
                return iter([self.game_date, self.game_id, self.hitter_cnt, self.pitcher_cnt, self.relay_cnt])

        mock_conn.execute.return_value.fetchall.return_value = [MockRow()]

        result = audit_completeness("sqlite:///test", 7, statuses=["COMPLETED"])
        assert result == 0
