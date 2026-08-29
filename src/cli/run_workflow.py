"""CLI command for executing multi-stage master workflow DAG pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from src.orchestration.master import MasterWorkflowOrchestrator


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Execute master platform workflow DAG pipelines.")
    parser.add_argument(
        "--workflow",
        type=str,
        default="daily_sync",
        choices=["daily_sync", "historical_recovery", "bulk_load"],
        help="Workflow pipeline to execute (default: daily_sync).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date for workflow execution (YYYYMMDD).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate execution without persisting data.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output workflow report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.workflow == "historical_recovery":
        orchestrator = MasterWorkflowOrchestrator.build_historical_recovery_workflow()
    elif args.workflow == "bulk_load":
        orchestrator = MasterWorkflowOrchestrator.build_bulk_load_workflow()
    else:
        orchestrator = MasterWorkflowOrchestrator.build_daily_sync_workflow()

    workflow_id = f"{args.workflow}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    context = {"date": args.date} if args.date else {}

    report = orchestrator.execute_workflow(workflow_id, context=context, dry_run=args.dry_run)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        mode_tag = "[DRY-RUN]" if args.dry_run else "[EXECUTE]"
        print(f"=== Master Workflow Execution {mode_tag} ({report.workflow_id}) ===")  # noqa: T201
        print(  # noqa: T201
            f"Overall Status: {report.overall_status} | "
            f"Completed: {report.completed_stages}/{report.total_stages} | "
            f"Failed: {report.failed_stages} | Skipped: {report.skipped_stages} | "
            f"Duration: {report.duration_seconds}s"
        )
        print("-" * 60)  # noqa: T201
        for res in report.stage_results:
            tag = f"[{res.status.value}]"
            err = f" (Error: {res.error_message})" if res.error_message else ""
            summary_line = (
                f"{tag:<14} Stage '{res.stage_id:<15}': {res.records_processed} records ({res.duration_seconds}s){err}"
            )
            print(summary_line)  # noqa: T201

    return 1 if report.overall_status in ("FAILED", "PARTIAL_FAILURE") else 0


if __name__ == "__main__":
    sys.exit(main())
