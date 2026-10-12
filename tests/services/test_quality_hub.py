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
        if asked:
            hub.baseline_path = Path("recorded.json")
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


class TestTheHubReadsABaselineWhenGivenOne:
    """The score rules are inert unless something fills in the counts.

    ``_new_damage_deduction`` reads ``new_violation_count``, which stays zero
    unless ``run_regression_pack`` actually compares against a file. These tests
    cover that plumbing rather than the arithmetic, which the class above pins.
    """

    def test_a_row_absent_from_the_recorded_baseline_is_counted_as_new(self, session: Session, tmp_path) -> None:
        from src.validators.data_quality_regression_pack import write_baseline

        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                     sacrifice_hits, sacrifice_flies, league, level, source)
                VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'KBO1', 'LEGACY_CRAWLER')
                """,
            ),
        )
        session.commit()
        baseline = tmp_path / "baseline.json"
        write_baseline(session.connection(), baseline, season=2026)
        (tmp_path / "unrecoverable.json").write_text('{"version": 1, "rows": []}')

        recorded = QualityHub(session, baseline_path=baseline).run_regression_pack(season=2026)
        assert recorded.baseline_available is True
        assert recorded.new_violation_count == 0
        assert recorded.baseline_violation_count == 1

        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                     sacrifice_hits, sacrifice_flies, league, level, source)
                VALUES (5009, 2026, 20, 16, 1, 0, 0, 0, 'REGULAR', 'KBO1', 'LEGACY_CRAWLER')
                """,
            ),
        )
        session.commit()

        after = QualityHub(session, baseline_path=baseline).run_regression_pack(season=2026)
        assert after.new_violation_count == 1, "a row the baseline never named was not counted"
        assert after.baseline_violation_count == 1

    def test_a_hub_without_a_baseline_path_leaves_the_comparison_off(self, session: Session) -> None:
        """Existing callers pass a session alone and must keep working unchanged."""
        summary = QualityHub(session).run_regression_pack(season=2026)

        assert summary.baseline_available is False
        assert summary.new_violation_count == 0

    def test_a_baseline_file_that_cannot_be_read_does_not_reprice_the_run(self, session: Session, tmp_path) -> None:
        """A corrupt file is not an empty one.

        Treating it as empty would make every historical row look new, and those
        numbers are subtracted from a score.
        """
        missing = tmp_path / "absent.json"
        summary = QualityHub(session, baseline_path=missing).run_regression_pack(season=2026)

        assert summary.baseline_available is False
        assert summary.new_violation_count == 0


class TestTheBaselineScopeIsEnforcedBeforeItIsTrusted:
    """A comparison across different scopes is not a small discrepancy.

    452 of the 760 players in the current season PA set fail in more than one
    season, so a baseline recorded over all of history read by a ``--year`` run
    would mark 714 of them repaired on every single night. That verdict reads as
    progress, which is why the mismatch is refused rather than reported.
    """

    def test_a_baseline_recorded_over_other_seasons_is_not_read_as_a_verdict(self, session: Session, tmp_path) -> None:
        from src.validators.data_quality_regression_pack import compare_against_baseline, write_baseline

        for pid, season in ((5001, 2026), (5002, 2025), (5003, 2024)):
            session.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, level, source)
                    VALUES (:pid, :season, 15, 13, 1, 0, 0, 0, 'REGULAR', 'KBO1', 'LEGACY_CRAWLER')
                    """,
                ),
                {"pid": pid, "season": season},
            )
        session.commit()
        whole_history = tmp_path / "whole.json"
        write_baseline(session.connection(), whole_history)
        (tmp_path / "unrecoverable.json").write_text('{"version": 1, "rows": []}')

        verdict = compare_against_baseline(session.connection(), whole_history, season=2026)
        assert verdict.is_scope_mismatch is True

        summary = QualityHub(session, baseline_path=whole_history).run_regression_pack(season=2026)
        assert summary.baseline_available is False, (
            "a whole-history baseline was accepted by a single-season run, which "
            "reports most of the backlog as repaired"
        )
        assert summary.new_violation_count == 0

    def test_a_baseline_recorded_for_the_same_season_is_accepted(self, session: Session, tmp_path) -> None:
        from src.validators.data_quality_regression_pack import write_baseline

        session.execute(
            text(
                """
                INSERT INTO player_season_batting
                    (player_id, season, plate_appearances, at_bats, walks, hbp,
                     sacrifice_hits, sacrifice_flies, league, level, source)
                VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'KBO1', 'LEGACY_CRAWLER')
                """,
            ),
        )
        session.commit()
        same_season = tmp_path / "scoped.json"
        write_baseline(session.connection(), same_season, season=2026)
        (tmp_path / "unrecoverable.json").write_text('{"version": 1, "rows": []}')

        summary = QualityHub(session, baseline_path=same_season).run_regression_pack(season=2026)
        assert summary.baseline_available is True
        assert summary.new_violation_count == 0
        assert summary.baseline_violation_count == 1


class TestAnUnreadableBaselineIsNotSilent:
    """A file that cannot be read is a fact, not a guess.

    ``compare_against_baseline`` raises rather than returning an empty verdict
    because both possible guesses hide a real break: read as empty, the entire
    historical backlog becomes fresh damage and the score collapses on the first
    night; swallowed, a truncated file looks like a quiet one. The hub turns the
    error into "not compared", and this holds it to that rather than to whatever
    happens to be the broad ``except`` clause it is written under.
    """

    def test_a_corrupt_baseline_is_reported_as_not_compared(self, session: Session, tmp_path) -> None:
        corrupt = tmp_path / "baseline.json"
        corrupt.write_text("{ not json")

        summary = QualityHub(session, baseline_path=corrupt).run_regression_pack(season=2026)

        assert summary.baseline_available is False
        assert summary.new_violation_count == 0
        assert summary.baseline_violation_count == 0

    def test_a_baseline_without_a_checks_section_is_also_not_compared(self, session: Session, tmp_path) -> None:
        """A well-formed file with the wrong shape is as unusable as a broken one."""
        wrong_shape = tmp_path / "baseline.json"
        wrong_shape.write_text('{"version": 1}')

        summary = QualityHub(session, baseline_path=wrong_shape).run_regression_pack(season=2026)

        assert summary.baseline_available is False

    def test_a_missing_file_is_not_compared_rather_than_treated_as_empty(self, session: Session, tmp_path) -> None:
        """The first-run case, and the one that would invent 2,603 rows."""
        summary = QualityHub(session, baseline_path=tmp_path / "never_recorded.json").run_regression_pack(season=2026)

        assert summary.baseline_available is False
        assert summary.new_violation_count == 0

    def test_an_unreadable_baseline_still_reaches_the_operator_as_a_hint(self, session: Session, tmp_path) -> None:
        """The run proceeds, but the operator is told the comparison did not happen."""
        from src.services.quality_hub import QualityGateSummary

        hub = QualityHub(session, baseline_path=tmp_path / "absent.json")
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
            patch.object(
                hub,
                "run_standings_check",
                return_value=StandingsSummary(target_date="2025-06-01", ok=True, checked_teams=10),
            ),
            patch.object(hub, "run_freshness_check", return_value=FreshnessSummary(ok=True, issue_count=0)),
        ):
            report = hub.run_full_audit(season=2025, target_date=date(2025, 6, 1))

        assert any("baseline" in hint.lower() for hint in report.remediation_hints), report.remediation_hints


class TestABrokenBaselineIsDistinguishedFromABrokenDatabase:
    """The comparison is wrapped in a narrow catch on purpose.

    ``except Exception`` would also swallow a dead connection and turn a gate
    that cannot read its own database into a gate that reports nothing wrong.
    The difference is invisible in the output -- both leave the counts at zero --
    so it is asserted here rather than left to the exception class staying
    correct.
    """

    def test_a_database_error_is_not_reported_as_a_missing_baseline(self, session: Session, tmp_path) -> None:
        """A failing query must surface, not resolve to "not compared"."""
        with patch(
            "src.services.quality_hub.compare_against_baseline",
            side_effect=OperationalError("SELECT 1", {}, Exception("server closed the connection unexpectedly")),
        ):
            with pytest.raises(OperationalError):
                QualityHub(session, baseline_path=tmp_path / "baseline.json").run_regression_pack(season=2026)

    def test_a_baseline_error_is_absorbed_as_not_compared(self, session: Session, tmp_path) -> None:
        """The one error class this path may swallow, because it has a meaning."""
        from src.validators.data_quality_regression_pack import BaselineUnreadableError

        with patch(
            "src.services.quality_hub.compare_against_baseline",
            side_effect=BaselineUnreadableError("baseline is unreadable"),
        ):
            summary = QualityHub(session, baseline_path=tmp_path / "baseline.json").run_regression_pack(season=2026)

        assert summary.baseline_available is False
