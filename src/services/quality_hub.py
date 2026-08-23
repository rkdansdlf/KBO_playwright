"""Unified Quality & Freshness Hub service for KBO data pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.validators.data_quality_regression_pack import run_regression_pack
from src.validators.quality_gate import QualityGate
from src.validators.standings_integrity import validate_standings_integrity

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class QualityGateSummary:
    """Summary of statistical quality gate checks."""

    season: int
    league: str
    batting_ok: bool
    pitching_ok: bool
    pa_formula_ok: bool
    team_batting_ok: bool
    team_pitching_ok: bool
    mismatch_count: int
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Return True if all statistical checks passed without errors."""
        return (
            self.batting_ok
            and self.pitching_ok
            and self.pa_formula_ok
            and self.team_batting_ok
            and self.team_pitching_ok
            and not self.errors
        )


@dataclass(frozen=True)
class RegressionPackSummary:
    """Summary of database regression invariants."""

    ok: bool
    check_count: int
    failure_count: int
    failures: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class StandingsSummary:
    """Summary of standings integrity validation."""

    target_date: str
    ok: bool
    checked_teams: int
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    note: str | None = None


@dataclass(frozen=True)
class FreshnessSummary:
    """Summary of data freshness and staleness checks."""

    ok: bool
    issue_count: int
    issues: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class UnifiedQualityReport:
    """Consolidated multi-gate quality and freshness report."""

    timestamp: str
    overall_status: str  # "PASS", "WARN", "FAIL"
    quality_score: int  # 0 to 100
    quality_gate: QualityGateSummary | None = None
    regression_pack: RegressionPackSummary | None = None
    standings: StandingsSummary | None = None
    freshness: FreshnessSummary | None = None
    remediation_hints: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize report to a dictionary."""
        return {
            "timestamp": self.timestamp,
            "overall_status": self.overall_status,
            "quality_score": self.quality_score,
            "quality_gate": asdict(self.quality_gate) if self.quality_gate else None,
            "regression_pack": asdict(self.regression_pack) if self.regression_pack else None,
            "standings": asdict(self.standings) if self.standings else None,
            "freshness": asdict(self.freshness) if self.freshness else None,
            "remediation_hints": list(self.remediation_hints),
        }


PASS_SCORE_THRESHOLD = 95
WARN_SCORE_THRESHOLD = 80


class QualityHub:
    """Central service orchestrating all KBO data quality, regression, and freshness gates."""

    def __init__(self, session: Session) -> None:
        """Initialize QualityHub with an active database session."""
        self.session = session

    def run_quality_gate(self, season: int, league: str = "REGULAR") -> QualityGateSummary:  # noqa: C901
        """Execute statistical quality gate checks for a given season.

        Args:
            season: Target season year.
            league: League type (default: "REGULAR").

        Returns:
            QualityGateSummary containing check statuses.

        """
        gate = QualityGate(self.session)
        errors: list[str] = []
        mismatch_count = 0

        batting_res = gate.validate_season_batting(season, league)
        if not batting_res.get("ok"):
            mismatch_count += len(batting_res.get("mismatches", []))
            if batting_res.get("error"):
                errors.append(f"Batting: {batting_res['error']}")

        pitching_res = gate.validate_season_pitching(season, league)
        if not pitching_res.get("ok"):
            mismatch_count += len(pitching_res.get("mismatches", []))
            if pitching_res.get("error"):
                errors.append(f"Pitching: {pitching_res['error']}")

        pa_res = gate.validate_season_pa_formula(season, league)
        if not pa_res.get("ok"):
            mismatch_count += len(pa_res.get("mismatches", []))
            if pa_res.get("error"):
                errors.append(f"PA Formula: {pa_res['error']}")

        tb_res = gate.validate_season_team_batting(season, league)
        if not tb_res.get("ok"):
            mismatch_count += len(tb_res.get("mismatches", []))
            if tb_res.get("error"):
                errors.append(f"Team Batting: {tb_res['error']}")

        tp_res = gate.validate_season_team_pitching(season, league)
        if not tp_res.get("ok"):
            mismatch_count += len(tp_res.get("mismatches", []))
            if tp_res.get("error"):
                errors.append(f"Team Pitching: {tp_res['error']}")

        return QualityGateSummary(
            season=season,
            league=league,
            batting_ok=bool(batting_res.get("ok")),
            pitching_ok=bool(pitching_res.get("ok")),
            pa_formula_ok=bool(pa_res.get("ok")),
            team_batting_ok=bool(tb_res.get("ok")),
            team_pitching_ok=bool(tp_res.get("ok")),
            mismatch_count=mismatch_count,
            errors=errors,
        )

    def run_regression_pack(
        self,
        season: int | None = None,
        target_date: str | None = None,
    ) -> RegressionPackSummary:
        """Execute database invariant checks.

        Args:
            season: Optional season filter.
            target_date: Optional date filter.

        Returns:
            RegressionPackSummary.

        """
        conn = self.session.connection()
        report = run_regression_pack(conn, target_date=target_date, season=season)
        failures = [res.to_dict() for res in report.results if res.status == "fail"]
        return RegressionPackSummary(
            ok=report.ok,
            check_count=report.check_count,
            failure_count=report.failure_count,
            failures=failures,
        )

    def run_standings_check(self, target_date: date) -> StandingsSummary:
        """Validate daily standings snapshot integrity against game results.

        Args:
            target_date: Date of the standings snapshot.

        Returns:
            StandingsSummary.

        """
        res = validate_standings_integrity(self.session, target_date)
        return StandingsSummary(
            target_date=target_date.isoformat(),
            ok=bool(res.get("ok")),
            checked_teams=int(res.get("checked_teams", 0)),
            mismatches=list(res.get("mismatches", [])),
            note=res.get("note"),
        )

    def run_freshness_check(
        self,
        days: int = 7,
        target_date: str | None = None,
    ) -> FreshnessSummary:
        """Validate data freshness for recent game data.

        Args:
            days: Days threshold.
            target_date: Optional target date.

        Returns:
            FreshnessSummary.

        """
        from src.cli.reports.freshness_gate import evaluate_freshness_gate

        issues = evaluate_freshness_gate(self.session, target_date=target_date, days=days)
        return FreshnessSummary(
            ok=len(issues) == 0,
            issue_count=len(issues),
            issues=issues,
        )

    def run_full_audit(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        *,
        season: int | None = None,
        target_date: date | None = None,
        freshness_days: int = 7,
        include_quality_gate: bool = True,
        include_regression_pack: bool = True,
        include_standings: bool = True,
        include_freshness: bool = True,
    ) -> UnifiedQualityReport:
        """Run all requested quality and freshness gates and generate a consolidated report.

        Args:
            season: Target season (defaults to current year).
            target_date: Target date for standings/freshness (defaults to today).
            freshness_days: Lookback days for freshness evaluation.
            include_quality_gate: Run statistical quality gate.
            include_regression_pack: Run database invariant regression pack.
            include_standings: Run standings integrity check.
            include_freshness: Run data freshness check.

        Returns:
            UnifiedQualityReport containing consolidated metrics and status.

        """
        now = datetime.now(KST)
        target_season = season or now.year
        check_date = target_date or now.date()

        q_summary: QualityGateSummary | None = None
        r_summary: RegressionPackSummary | None = None
        s_summary: StandingsSummary | None = None
        f_summary: FreshnessSummary | None = None
        remediations: list[str] = []

        total_weight = 0
        earned_score = 0.0

        if include_quality_gate:
            total_weight += 35
            q_summary = self.run_quality_gate(target_season)
            if q_summary.ok:
                earned_score += 35.0
            else:
                deduction = min(35.0, q_summary.mismatch_count * 2.0 + len(q_summary.errors) * 5.0)
                earned_score += max(0.0, 35.0 - deduction)
                if not q_summary.pa_formula_ok:
                    remediations.append(f"python3 -m scripts.maintenance.audit_pa_formula --fix-year {target_season}")
                if not q_summary.team_batting_ok or not q_summary.team_pitching_ok:
                    remediations.append(f"python3 -m src.cli.recalc_team_stats --season {target_season}")

        if include_regression_pack:
            total_weight += 35
            r_summary = self.run_regression_pack(season=target_season)
            if r_summary.ok:
                earned_score += 35.0
            else:
                deduction = min(35.0, r_summary.failure_count * 7.0)
                earned_score += max(0.0, 35.0 - deduction)
                remediations.append(f"python3 -m scripts.maintenance.backfill_player_ids --year {target_season}")

        if include_standings:
            total_weight += 15
            s_summary = self.run_standings_check(check_date)
            if s_summary.ok:
                earned_score += 15.0
            else:
                deduction = min(15.0, len(s_summary.mismatches) * 3.0)
                earned_score += max(0.0, 15.0 - deduction)
                remediations.append("python3 -m src.cli.run_daily_update")

        if include_freshness:
            total_weight += 15
            f_summary = self.run_freshness_check(days=freshness_days, target_date=check_date.isoformat())
            if f_summary.ok:
                earned_score += 15.0
            else:
                deduction = min(15.0, f_summary.issue_count * 2.0)
                earned_score += max(0.0, 15.0 - deduction)
                remediations.append("python3 -m src.cli.run_daily_update")

        final_score = round((earned_score / total_weight) * 100) if total_weight > 0 else 100

        if final_score >= PASS_SCORE_THRESHOLD:
            overall_status = "PASS"
        elif final_score >= WARN_SCORE_THRESHOLD:
            overall_status = "WARN"
        else:
            overall_status = "FAIL"

        return UnifiedQualityReport(
            timestamp=now.isoformat(),
            overall_status=overall_status,
            quality_score=final_score,
            quality_gate=q_summary,
            regression_pack=r_summary,
            standings=s_summary,
            freshness=f_summary,
            remediation_hints=remediations,
        )

    def format_markdown(self, report: UnifiedQualityReport) -> str:
        """Render markdown summary of the unified quality report."""
        status_icon = "🟢" if report.overall_status == "PASS" else "🟡" if report.overall_status == "WARN" else "🔴"
        lines = [
            f"# {status_icon} KBO Data Quality Report ({report.overall_status})",
            f"- **Quality Score**: {report.quality_score} / 100",
            f"- **Timestamp**: {report.timestamp}",
            "",
            "## Gate Breakdown",
        ]

        if report.quality_gate:
            g = report.quality_gate
            g_icon = "✅" if g.ok else "❌"
            lines.append(f"- {g_icon} **Statistical Quality Gate** (Season {g.season}):")
            lines.append(f"  - Batting: {'OK' if g.batting_ok else 'Mismatch'}")
            lines.append(f"  - Pitching: {'OK' if g.pitching_ok else 'Mismatch'}")
            lines.append(f"  - PA Formula: {'OK' if g.pa_formula_ok else 'Mismatch'}")
            lines.append(f"  - Team Stats: {'OK' if g.team_batting_ok and g.team_pitching_ok else 'Mismatch'}")
            if g.mismatch_count > 0:
                lines.append(f"  - Total Mismatches: {g.mismatch_count}")

        if report.regression_pack:
            r = report.regression_pack
            r_icon = "✅" if r.ok else "❌"
            lines.append(f"- {r_icon} **DB Invariant Regression Pack**:")
            lines.append(f"  - Checks: {r.check_count}, Failures: {r.failure_count}")

        if report.standings:
            s = report.standings
            s_icon = "✅" if s.ok else "❌"
            lines.append(f"- {s_icon} **Standings Integrity** ({s.target_date}):")
            lines.append(f"  - Checked Teams: {s.checked_teams}, Mismatches: {len(s.mismatches)}")

        if report.freshness:
            f = report.freshness
            f_icon = "✅" if f.ok else "❌"
            lines.append(f"- {f_icon} **Data Freshness**:")
            lines.append(f"  - Issues Detected: {f.issue_count}")

        if report.remediation_hints:
            lines.extend(
                [
                    "",
                    "## Recommended Remediation",
                    "```bash",
                    *report.remediation_hints,
                    "```",
                ]
            )

        return "\n".join(lines)
