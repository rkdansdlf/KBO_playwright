"""CLI command for unified database maintenance and repair tasks."""

from __future__ import annotations

import argparse
import json
import sys

from src.maintenance.dto import MaintenanceRunReport
from src.maintenance.orchestrator import MaintenanceOrchestrator


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Run unified database maintenance tasks.")
    parser.add_argument(
        "--task",
        type=str,
        default="all",
        choices=["all", "pa_audit", "null_player_ids", "cleanup", "checkpoint"],
        help="Maintenance task to execute (default: all).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Target year for maintenance audits.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to the database (default is dry-run).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output maintenance results as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    orchestrator = MaintenanceOrchestrator()

    if args.task == "all":
        report = orchestrator.run_all(year=args.year, apply=args.apply)
    elif args.task == "pa_audit":
        res = orchestrator.run_pa_formula_audit(year=args.year, apply=args.apply)
        report = MaintenanceRunReport(
            total_tasks=1,
            successful_tasks=1 if res.status != "FAILED" else 0,
            failed_tasks=1 if res.status == "FAILED" else 0,
            total_rows_affected=res.rows_affected,
            duration_seconds=res.duration_seconds,
            results=[res],
        )
    elif args.task == "null_player_ids":
        res = orchestrator.run_null_player_ids_audit(year=args.year, apply=args.apply)
        report = MaintenanceRunReport(
            total_tasks=1,
            successful_tasks=1 if res.status != "FAILED" else 0,
            failed_tasks=1 if res.status == "FAILED" else 0,
            total_rows_affected=res.rows_affected,
            duration_seconds=res.duration_seconds,
            results=[res],
        )
    elif args.task == "cleanup":
        res = orchestrator.run_data_cleanup(dry_run=not args.apply)
        report = MaintenanceRunReport(
            total_tasks=1,
            successful_tasks=1 if res.status != "FAILED" else 0,
            failed_tasks=1 if res.status == "FAILED" else 0,
            total_rows_affected=res.rows_affected,
            duration_seconds=res.duration_seconds,
            results=[res],
        )
    else:  # checkpoint
        res = orchestrator.run_wal_checkpoint()
        report = MaintenanceRunReport(
            total_tasks=1,
            successful_tasks=1 if res.status != "FAILED" else 0,
            failed_tasks=1 if res.status == "FAILED" else 0,
            total_rows_affected=res.rows_affected,
            duration_seconds=res.duration_seconds,
            results=[res],
        )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        mode_str = "APPLY" if args.apply else "DRY-RUN"
        print(f"=== Unified Maintenance Execution [{mode_str}] ===")  # noqa: T201
        print(  # noqa: T201
            f"Total Tasks: {report.total_tasks} | Succeeded: {report.successful_tasks} | "
            f"Failed: {report.failed_tasks} | Total Rows Affected: {report.total_rows_affected} | "
            f"Elapsed: {report.duration_seconds}s"
        )
        print("-" * 60)  # noqa: T201
        for res in report.results:
            tag = f"[{res.status}]"
            err = f" (Error: {res.error_message})" if res.error_message else ""
            print(f"{tag:<12} {res.task_name:<25}: {res.rows_affected} rows ({res.duration_seconds}s){err}")  # noqa: T201

    return 1 if report.failed_tasks > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
