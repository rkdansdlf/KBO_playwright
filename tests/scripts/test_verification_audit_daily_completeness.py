import os
from datetime import date
from unittest.mock import MagicMock, patch

from scripts.verification.audit_daily_completeness import (
    _coerce_date,
    _format_scope,
    _has_required_home_innings,
    _parse_statuses,
    audit_completeness,
    main,
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

    def test_empty_with_incomplete_includes_completed_and_incomplete(self):
        result = _parse_statuses("", include_incomplete=True)
        assert "COMPLETED" in result
        assert "DRAW" in result
        assert "SCHEDULED" in result
        assert "UNRESOLVED_MISSING" in result

    def test_custom_with_incomplete(self):
        result = _parse_statuses("DRAW", include_incomplete=True)
        assert result == ["DRAW", "SCHEDULED", "UNRESOLVED_MISSING"]

    def test_custom(self):
        result = _parse_statuses("LIVE, FINAL", include_incomplete=False)
        assert "LIVE" in result


class TestRequiredHomeInnings:
    def test_home_win_allows_eight_home_innings(self):
        assert _has_required_home_innings(home_score=5, away_score=2, inning_home_cnt=8)

    def test_home_loss_requires_nine_home_innings(self):
        assert not _has_required_home_innings(home_score=2, away_score=5, inning_home_cnt=8)


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
    def test_rolling_no_rows_pass(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []

        result = audit_completeness("sqlite:///test", 7, statuses=["COMPLETED"])
        assert result == 0

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_non_monday_target_date_no_rows_fails(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []

        result = audit_completeness(
            "sqlite:///test",
            7,
            target_date=date(2025, 4, 8),
            statuses=["COMPLETED"],
        )
        assert result == 1

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_monday_target_date_no_rows_passes(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []

        result = audit_completeness(
            "sqlite:///test",
            7,
            target_date=date(2025, 4, 7),
            statuses=["COMPLETED"],
        )
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

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_monday_target_date_with_incomplete_row_still_fails_strict(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [
            (
                "G1",
                "2025-04-07",
                None,
                None,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ),
        ]

        result = audit_completeness(
            "sqlite:///test",
            7,
            target_date=date(2025, 4, 7),
            statuses=["COMPLETED"],
            strict=True,
        )
        assert result == 1

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_monday_target_date_with_complete_row_passes_strict(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [
            (
                "G1",
                "2025-04-07",
                3,
                2,
                1,
                9,
                9,
                9,
                9,
                10,
                10,
                3,
                3,
                0,
                20,
            ),
        ]

        result = audit_completeness(
            "sqlite:///test",
            7,
            target_date=date(2025, 4, 7),
            statuses=["COMPLETED"],
            strict=True,
        )
        assert result == 0

    @patch("scripts.verification.audit_daily_completeness.create_engine")
    def test_home_win_with_eight_home_innings_passes_strict(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = [
            (
                "G1",
                "2025-04-08",
                5,
                2,
                1,
                9,
                8,
                9,
                9,
                10,
                10,
                3,
                3,
                0,
                20,
            ),
        ]

        result = audit_completeness(
            "sqlite:///test",
            7,
            target_date=date(2025, 4, 8),
            statuses=["COMPLETED"],
            strict=True,
        )
        assert result == 0


class TestMain:
    def test_missing_env_db_url_returns_2(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("scripts.verification.audit_daily_completeness.load_dotenv"),
            patch("scripts.verification.audit_daily_completeness.audit_completeness") as mock_audit,
            patch("sys.argv", ["audit_daily_completeness.py", "--db-url", "env:OCI_DB_URL"]),
        ):
            assert main() == 2
            mock_audit.assert_not_called()

    def test_invalid_date_returns_2(self):
        with (
            patch.dict(os.environ, {"DATABASE_URL": "sqlite:///test"}, clear=True),
            patch("scripts.verification.audit_daily_completeness.load_dotenv"),
            patch("scripts.verification.audit_daily_completeness.audit_completeness") as mock_audit,
            patch("sys.argv", ["audit_daily_completeness.py", "--date", "invalid"]),
        ):
            assert main() == 2
            mock_audit.assert_not_called()
