"""Historical Certification Runner executing 7 Invariant Layers across 45 Seasons."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

from src.certification.historical.invariants import (
    BaseHistoricalInvariant,
    BattingInvariants,
    BoxscoreReconciliationInvariant,
    GameStateInvariant,
    PitchingInvariants,
    ReferentialIntegrityInvariant,
    ScheduleCoverageInvariant,
    SeasonTotalsReconciliationInvariant,
)
from src.certification.historical.manifest import SeasonManifestRegistry
from src.certification.historical.models import (
    HistoricalAuditReport,
    HistoricalSeasonVerdict,
    InvariantSeverity,
    SeasonAuditResult,
)
from src.db.engine import Engine, get_db_session

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SQLAlchemyEngine

    from src.certification.context import CertificationContext


class HistoricalCertificationRunner:
    """Orchestrates 45-season historical invariant evaluation and verdict aggregation."""

    def __init__(self, invariants: list[BaseHistoricalInvariant] | None = None) -> None:
        """Initialize runner with default or custom historical invariants."""
        self.invariants = invariants or [
            ScheduleCoverageInvariant(),
            ReferentialIntegrityInvariant(),
            GameStateInvariant(),
            BattingInvariants(),
            PitchingInvariants(),
            BoxscoreReconciliationInvariant(),
            SeasonTotalsReconciliationInvariant(),
        ]

    def _resolve_engine(self, context: CertificationContext) -> SQLAlchemyEngine:
        """Resolve database engine with local SQLite fallback."""
        try:
            with get_db_session() as session:
                # Test connectivity
                session.execute(text("SELECT 1"))
                bind = session.get_bind()
                if bind is not None:
                    return bind  # type: ignore[return-value]
        except Exception:
            if context.target == "local":
                return create_engine("sqlite:///./data/kbo_dev.db")
            raise
        return Engine

    def run_historical_audit(
        self,
        context: CertificationContext,
        start_season: int = 1982,
        end_season: int = 2026,
    ) -> HistoricalAuditReport:
        """Execute historical certification across the specified season range."""
        start_time = time.perf_counter()
        started_iso = datetime.now(UTC).isoformat()
        seasons = list(range(start_season, end_season + 1))
        target_engine = self._resolve_engine(context)

        # 1. Run all invariants across all seasons in batch
        season_inv_map: dict[int, list] = {s: [] for s in seasons}
        for inv in self.invariants:
            inv_results = inv.evaluate_seasons(target_engine, seasons, context)
            for r in inv_results:
                if r.season in season_inv_map:
                    season_inv_map[r.season].append(r)

        # 2. Build SeasonAuditResult for each season
        season_results: list[SeasonAuditResult] = []
        total_violations = 0
        blocking_violations = 0
        total_declared_exceptions = 0
        undeclared_exceptions = 0
        required_checks_skipped = 0
        passed_count = 0
        passed_with_exc_count = 0
        failed_count = 0

        for s in seasons:
            manifest = SeasonManifestRegistry.get_manifest(s)
            invs = season_inv_map.get(s, [])

            s_violations = sum(inv.violation_count for inv in invs if inv.status == "FAIL")
            s_blocking = sum(
                inv.violation_count
                for inv in invs
                if inv.status == "FAIL" and inv.severity == InvariantSeverity.BLOCKER
            )
            s_exceptions = sum(
                1 for inv in invs if inv.status in {"PASS_WITH_EXCEPTION", "N_A", "NOT_COMPARABLE", "AS_OF_CUTOFF"}
            )
            s_skipped = sum(1 for inv in invs if inv.status == "SKIP")

            # Determine layer statuses
            layer_status: dict[str, str] = {}
            for inv in invs:
                layer_status[inv.layer] = inv.status

            # Compute season verdict
            if any(inv.status == "FAIL" for inv in invs):
                verdict = HistoricalSeasonVerdict.FAIL
                failed_count += 1
                undeclared_exceptions += sum(1 for inv in invs if inv.status == "FAIL")
            elif any(inv.status in {"PASS_WITH_EXCEPTION", "AS_OF_CUTOFF", "NOT_COMPARABLE", "N_A"} for inv in invs):
                verdict = HistoricalSeasonVerdict.PASS_WITH_DECLARED_EXCEPTIONS
                passed_with_exc_count += 1
            else:
                verdict = HistoricalSeasonVerdict.PASS
                passed_count += 1

            total_violations += s_violations
            blocking_violations += s_blocking
            total_declared_exceptions += s_exceptions
            required_checks_skipped += s_skipped

            season_results.append(
                SeasonAuditResult(
                    season=s,
                    status=manifest.status,
                    verdict=verdict,
                    layer_status=layer_status,
                    total_violations=s_violations,
                    declared_exceptions=s_exceptions,
                    invariants=invs,
                )
            )

        finished_iso = datetime.now(UTC).isoformat()
        total_duration_ms = (time.perf_counter() - start_time) * 1000.0

        # Mathematical certification formula:
        # Certified = (Blocking Violations == 0) and (Undeclared Exceptions == 0) and (Required Checks Skipped == 0)
        is_certified = (blocking_violations == 0) and (undeclared_exceptions == 0) and (required_checks_skipped == 0)

        # Overall verdict
        if not is_certified or failed_count > 0:
            overall = "NOT_CERTIFIED"
        elif passed_with_exc_count > 0:
            overall = "CERTIFIED_WITH_EXCEPTIONS"
        else:
            overall = "CERTIFIED"

        return HistoricalAuditReport(
            schema_version="1.0",
            contract="historical-v1",
            run_id=context.run_id,
            started_at=started_iso,
            finished_at=finished_iso,
            target=context.target,
            git_revision=context.git_revision,
            start_season=start_season,
            end_season=end_season,
            total_seasons=len(seasons),
            passed_seasons=passed_count,
            passed_with_exceptions=passed_with_exc_count,
            failed_seasons=failed_count,
            total_violations=total_violations,
            blocking_violations=blocking_violations,
            total_declared_exceptions=total_declared_exceptions,
            undeclared_exceptions=undeclared_exceptions,
            required_checks_skipped=required_checks_skipped,
            total_duration_ms=total_duration_ms,
            overall_verdict=overall,
            is_certified=is_certified,
            seasons=season_results,
        )


__all__ = [
    "HistoricalCertificationRunner",
]
