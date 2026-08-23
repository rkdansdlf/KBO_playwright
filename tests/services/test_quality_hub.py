"""Unit tests for QualityHub service and UnifiedQualityReport."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.services.quality_hub import (
    FreshnessSummary,
    QualityGateSummary,
    QualityHub,
    RegressionPackSummary,
    StandingsSummary,
    UnifiedQualityReport,
)


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


class TestQualityHub:
    """Test QualityHub audit operations and report formatting."""

    def test_run_quality_gate_summary(self, session: Session) -> None:
        hub = QualityHub(session)
        mock_gate = MagicMock()
        mock_gate.validate_season_batting.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_pitching.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_pa_formula.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_team_batting.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_team_pitching.return_value = {"ok": True, "mismatches": []}

        with patch("src.services.quality_hub.QualityGate", return_value=mock_gate):
            summary = hub.run_quality_gate(season=2025)

        assert summary.ok is True
        assert summary.season == 2025
        assert summary.mismatch_count == 0
        assert not summary.errors

    def test_run_quality_gate_detects_mismatches(self, session: Session) -> None:
        hub = QualityHub(session)
        mock_gate = MagicMock()
        mock_gate.validate_season_batting.return_value = {"ok": False, "mismatches": [{"player": "A"}]}
        mock_gate.validate_season_pitching.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_pa_formula.return_value = {"ok": False, "error": "Formula mismatch"}
        mock_gate.validate_season_team_batting.return_value = {"ok": True, "mismatches": []}
        mock_gate.validate_season_team_pitching.return_value = {"ok": True, "mismatches": []}

        with patch("src.services.quality_hub.QualityGate", return_value=mock_gate):
            summary = hub.run_quality_gate(season=2025)

        assert summary.ok is False
        assert summary.batting_ok is False
        assert summary.pa_formula_ok is False
        assert summary.mismatch_count == 1
        assert "PA Formula: Formula mismatch" in summary.errors

    def test_run_full_audit_clean_pass(self, session: Session) -> None:
        hub = QualityHub(session)

        with (
            patch.object(
                hub,
                "run_quality_gate",
                return_value=QualityGateSummary(
                    season=2025,
                    league="REGULAR",
                    batting_ok=True,
                    pitching_ok=True,
                    pa_formula_ok=True,
                    team_batting_ok=True,
                    team_pitching_ok=True,
                    mismatch_count=0,
                ),
            ),
            patch.object(
                hub,
                "run_regression_pack",
                return_value=RegressionPackSummary(
                    ok=True,
                    check_count=10,
                    failure_count=0,
                ),
            ),
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(
                    target_date="2025-06-01",
                    ok=True,
                    checked_teams=10,
                ),
            ),
            patch.object(
                hub,
                "run_freshness_check",
                return_value=FreshnessSummary(
                    ok=True,
                    issue_count=0,
                ),
            ),
        ):
            report = hub.run_full_audit(season=2025, target_date=date(2025, 6, 1))

        assert report.overall_status == "PASS"
        assert report.quality_score == 100
        assert not report.remediation_hints

        # Verify serialization
        data = report.to_dict()
        assert data["overall_status"] == "PASS"
        assert data["quality_score"] == 100
        assert data["quality_gate"]["season"] == 2025

    def test_run_full_audit_with_remediations(self, session: Session) -> None:
        hub = QualityHub(session)

        with (
            patch.object(
                hub,
                "run_quality_gate",
                return_value=QualityGateSummary(
                    season=2025,
                    league="REGULAR",
                    batting_ok=True,
                    pitching_ok=True,
                    pa_formula_ok=False,
                    team_batting_ok=False,
                    team_pitching_ok=True,
                    mismatch_count=3,
                ),
            ),
            patch.object(
                hub,
                "run_regression_pack",
                return_value=RegressionPackSummary(
                    ok=False,
                    check_count=10,
                    failure_count=2,
                ),
            ),
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(
                    target_date="2025-06-01",
                    ok=True,
                    checked_teams=10,
                ),
            ),
            patch.object(
                hub,
                "run_freshness_check",
                return_value=FreshnessSummary(
                    ok=True,
                    issue_count=0,
                ),
            ),
        ):
            report = hub.run_full_audit(season=2025, target_date=date(2025, 6, 1))

        assert report.overall_status in ("WARN", "FAIL")
        assert len(report.remediation_hints) > 0
        assert any("audit_pa_formula" in hint for hint in report.remediation_hints)

        # Markdown output check
        md = hub.format_markdown(report)
        assert "KBO Data Quality Report" in md
        assert "Recommended Remediation" in md
