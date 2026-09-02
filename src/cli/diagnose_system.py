"""CLI command for comprehensive platform diagnostics and automated self-healing."""

from __future__ import annotations

import argparse
import json
import sys

from src.diagnostics.engine import SystemDiagnosticsEngine


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Run comprehensive system health diagnostics.")
    parser.add_argument(
        "--subsystem",
        type=str,
        default="all",
        choices=["all", "database", "scheduler", "crawler", "pipeline", "rag_vector"],
        help="Subsystem to diagnose (default: all).",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt automated self-healing for detected anomalies.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output diagnostics report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    engine = SystemDiagnosticsEngine()

    if args.fix:
        healed_actions = engine.auto_heal(args.subsystem)
        if not args.json:
            if healed_actions:
                print(f"[AUTO-HEAL] Executed {len(healed_actions)} recovery actions:")  # noqa: T201
                for act in healed_actions:
                    print(f"  - {act}")  # noqa: T201
            else:
                print("[AUTO-HEAL] No issues requiring healing were found.")  # noqa: T201

    report = engine.diagnose_all(subsystem=args.subsystem)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        print(f"=== Unified System Diagnostics Report [{report.overall_status}] ===")  # noqa: T201
        print(  # noqa: T201
            f"Total Checks: {report.total_checks} | Healthy: {report.healthy_count} | "
            f"Warnings: {report.warning_count} | Critical: {report.critical_count}"
        )
        print("-" * 60)  # noqa: T201
        for check in report.checks:
            status_tag = f"[{check.severity}]"
            print(f"{status_tag:<12} {check.subsystem:<12} {check.name:<25}: {check.message}")  # noqa: T201
            if check.remediation_hint:
                print(f"             -> Hint: {check.remediation_hint}")  # noqa: T201

    return 1 if report.critical_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
