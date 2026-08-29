"""G10: Historical Data Certification Gate spanning 45 Seasons (1982~2026)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from src.certification.historical.reporter import HistoricalReporter
from src.certification.historical.runner import HistoricalCertificationRunner
from src.certification.models import GateResult, GateStatus

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class HistoricalCertificationGate:
    """G10: Validates 45 seasons of historical data against mathematical, relational, and aggregate invariants."""

    gate_id: str = "historical_data_45_seasons"
    name: str = "Historical Data Certification (1982~2026)"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["schema_migration"]

    def run(self, context: CertificationContext) -> GateResult:
        """Execute 45-season historical audit and collect evidence."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            runner = HistoricalCertificationRunner()
            report = runner.run_historical_audit(
                context=context,
                start_season=1982,
                end_season=2026,
            )

            metrics["total_seasons_examined"] = report.total_seasons
            metrics["passed_seasons"] = report.passed_seasons
            metrics["passed_with_exceptions"] = report.passed_with_exceptions
            metrics["failed_seasons"] = report.failed_seasons
            metrics["total_violations"] = report.total_violations
            metrics["total_declared_exceptions"] = report.total_declared_exceptions

            evidence["overall_verdict"] = report.overall_verdict

            # Save historical JSON report artifact
            hist_json_path = context.artifact_dir / "historical_audit.json"
            HistoricalReporter.save_json_report(report, hist_json_path)
            evidence["historical_report_artifact"] = str(hist_json_path)

            duration_ms = (time.perf_counter() - start) * 1000.0

            if report.failed_seasons > 0:
                is_local = context.target == "local" and not context.strict
                gate_status = GateStatus.WARN if is_local else GateStatus.FAIL
                msg = (
                    f"{report.failed_seasons} season(s) flagged historical differences in local SQLite "
                    f"({report.total_violations} violations)"
                    if is_local
                    else f"{report.failed_seasons} season(s) failed historical data certification "
                    f"({report.total_violations} violations)"
                )

                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=gate_status,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=msg,
                )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message=(
                    f"45 KBO seasons certified (passed={report.passed_seasons}, "
                    f"with_exceptions={report.passed_with_exceptions})"
                ),
            )

        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Historical data certification error: {err}",
            )


__all__ = [
    "HistoricalCertificationGate",
]
