"""Unit tests for QualityHub service and UnifiedQualityReport."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
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
                # A recorded baseline with nothing failing: the clean run this
                # test describes. Without one the hub cannot distinguish it from
                # a run that has never been measured, and says so in its hints.
                return_value=RegressionPackSummary(
                    ok=True,
                    check_count=10,
                    failure_count=0,
                    baseline_available=True,
                    baseline_violation_count=0,
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

    def test_run_full_audit_passes_compact_date_to_freshness_gate(self, session: Session) -> None:
        """Freshness must receive YYYYMMDD, not an ISO string it cannot parse."""
        hub = QualityHub(session)
        captured: dict[str, str | None] = {}

        def _capture(days: int = 7, target_date: str | None = None) -> FreshnessSummary:
            captured["target_date"] = target_date
            return FreshnessSummary(ok=True, issue_count=0)

        with (
            patch.object(
                hub,
                "run_quality_gate",
                return_value=QualityGateSummary(
                    season=2026,
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
                return_value=RegressionPackSummary(ok=True, check_count=1, failure_count=0),
            ),
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(target_date="2026-09-25", ok=True, checked_teams=10),
            ),
            patch.object(hub, "run_freshness_check", side_effect=_capture),
        ):
            report = hub.run_full_audit(season=2026, target_date=date(2026, 9, 25))

        assert captured["target_date"] == "20260925"
        assert report.overall_status == "PASS"

    def test_run_full_audit_real_freshness_gate_does_not_raise(self, session: Session) -> None:
        """Regression: the real freshness gate used to reject the audit date format."""
        hub = QualityHub(session)

        with (
            patch.object(
                hub,
                "run_quality_gate",
                return_value=QualityGateSummary(
                    season=2026,
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
                return_value=RegressionPackSummary(ok=True, check_count=1, failure_count=0),
            ),
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(target_date="2026-09-25", ok=True, checked_teams=10),
            ),
        ):
            report = hub.run_full_audit(season=2026, target_date=date(2026, 9, 25))

        assert report.freshness is not None
        assert report.overall_status == "PASS"

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


class TestTheScoreDistinguishesNewDamageFromKnownBacklog:
    """A pack that fails for the same reason every day should stop scoring.

    The season PA violations cannot be repaired from here -- the source page is
    blocked by robots.txt and the game-level rows behind those seasons are absent
    -- so the pack reports ``ok=False`` indefinitely and the daily gate has sat in
    WARN since it was first recorded. That is the correct signal, but it is also a
    constant one: nothing in the score moves when the damage grows.

    The existing deduction is ``failure_count * 7.0``, a count of failed *checks*.
    A player newly losing their walk counts fails a check that was already failing,
    so the score does not move. These tests pin the property that does: the same
    number of violations costing more once some of them are new.
    """

    @staticmethod
    def _audit(hub: QualityHub, regression: RegressionPackSummary, *, asked: bool = False) -> UnifiedQualityReport:
        """Run the audit, optionally as a hub that was given a baseline path.

        The instruction to record a baseline is only offered to a hub that was
        asked to compare -- the API and the CLI construct one without a path and
        have no use for the advice.
        """
        gate = QualityGateSummary(
            season=2025,
            league="REGULAR",
            batting_ok=True,
            pitching_ok=True,
            pa_formula_ok=True,
            team_batting_ok=True,
            team_pitching_ok=True,
            mismatch_count=0,
        )
        with (
            patch.object(hub, "run_quality_gate", return_value=gate),
            patch.object(hub, "run_regression_pack", return_value=regression),
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(target_date="2025-06-01", ok=True, checked_teams=10),
            ),
            patch.object(hub, "run_freshness_check", return_value=FreshnessSummary(ok=True, issue_count=0)),
        ):
            return hub.run_full_audit(season=2025, target_date=date(2025, 6, 1))

    def test_a_backlog_alone_costs_exactly_what_it_cost_before(self, session: Session) -> None:
        """Nothing about a known backlog is new information, so nothing changes.

        A backlog is the existing rule's own signal -- ``failure_count`` already
        counts the checks that carry it -- so charging it a second time would
        lower the score every day without any of that being about new damage. The
        measure is kept for the operator (``baseline_violation_count``) rather
        than spent on the score.
        """
        hub = QualityHub(session)

        report = self._audit(
            hub,
            RegressionPackSummary(
                ok=False,
                check_count=13,
                failure_count=3,
                baseline_available=True,
                new_violation_count=0,
                baseline_violation_count=760,
            ),
        )

        # 35 - min(35, 3 * 7) = 14 of 35, and the other three gates are clean,
        # which is the score this run would have had before the baseline existed.
        assert report.quality_score == 79, f"the backlog moved the score: {report.quality_score}"

    def test_the_same_violations_cost_more_once_some_are_new(self, session: Session) -> None:
        """Identical failure count, identical check count, worse score.

        This is the whole point. Without it the daily gate cannot tell a quiet
        backlog from a bleeding one, and an operator watching the score sees no
        difference between the two states.
        """
        hub = QualityHub(session)
        common = {"ok": False, "check_count": 13, "failure_count": 3, "baseline_violation_count": 760}

        backlog_only = self._audit(hub, RegressionPackSummary(baseline_available=True, new_violation_count=0, **common))
        with_new = self._audit(hub, RegressionPackSummary(baseline_available=True, new_violation_count=46, **common))

        assert backlog_only.quality_score == 79
        assert with_new.quality_score < backlog_only.quality_score, (
            f"new damage did not cost anything: {backlog_only.quality_score} -> {with_new.quality_score}"
        )

    def test_a_missing_baseline_says_so_and_costs_nothing(self, session: Session) -> None:
        """The first run must not report the whole backlog as new damage.

        Without a recorded baseline every historical violation is unknown to it,
        so a missing file scored as an empty one would claim 2,603 new rows on the
        first night and turn a known backlog into a fresh alarm.
        """
        hub = QualityHub(session)

        without_baseline = self._audit(
            hub,
            RegressionPackSummary(ok=False, check_count=13, failure_count=3, baseline_available=False),
            asked=True,
        )
        backlog_only = self._audit(
            hub,
            RegressionPackSummary(
                ok=False,
                check_count=13,
                failure_count=3,
                baseline_available=True,
                new_violation_count=0,
                baseline_violation_count=760,
            ),
        )

        assert without_baseline.quality_score >= backlog_only.quality_score, (
            "having no baseline was scored worse than having a quiet one"
        )
        assert without_baseline.regression_pack is not None
        assert without_baseline.regression_pack.baseline_available is False
        assert any("baseline" in hint.lower() for hint in without_baseline.remediation_hints), (
            f"a run without a baseline gave no instruction to record one: {without_baseline.remediation_hints}"
        )

    def test_the_counts_reach_the_serialized_report(self, session: Session) -> None:
        """The incident message is built from ``to_dict``, so the numbers must survive it.

        ``api/dependencies.py`` hands this same object to the API, which is the
        other place an operator can read the verdict.
        """
        hub = QualityHub(session)

        report = self._audit(
            hub,
            RegressionPackSummary(
                ok=False,
                check_count=13,
                failure_count=3,
                baseline_available=True,
                new_violation_count=46,
                baseline_violation_count=760,
                newly_unrecoverable_count=0,
            ),
        )
        payload = report.to_dict()["regression_pack"]

        assert payload["baseline_available"] is True
        assert payload["new_violation_count"] == 46
        assert payload["baseline_violation_count"] == 760
        assert payload["newly_unrecoverable_count"] == 0

    def test_a_summary_built_before_the_new_fields_still_describes_itself(self) -> None:
        """Older callers construct this with three fields and must not break.

        ``run_unified_quality_check`` and the tests that predate this change build
        the summary directly; the new fields default rather than being required.
        """
        legacy = RegressionPackSummary(ok=False, check_count=13, failure_count=3)

        assert legacy.baseline_available is False
        assert legacy.new_violation_count == 0
        assert legacy.baseline_violation_count == 0
