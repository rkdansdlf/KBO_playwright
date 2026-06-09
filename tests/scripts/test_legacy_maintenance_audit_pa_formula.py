from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.audit_pa_formula import (
    _get_fix_candidates,
    audit_year,
    fix_year_formula,
)


class TestAuditYear:
    @patch("scripts.legacy.maintenance.audit_pa_formula.SessionLocal")
    def test_no_violations(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []
        mock_session.execute.return_value.scalar.return_value = 0

        result = audit_year(2025)
        assert result["year"] == 2025
        assert result["violation_rows"] == 0


class TestGetFixCandidates:
    @patch("scripts.legacy.maintenance.audit_pa_formula.SessionLocal")
    def test_empty(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        session = mock_session
        result = _get_fix_candidates(2025, session)
        assert result == []


class TestFixYearFormula:
    @patch("scripts.legacy.maintenance.audit_pa_formula.SessionLocal")
    def test_dry_run_no_candidates(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        result = fix_year_formula(2025, dry_run=True)
        assert result == 0
