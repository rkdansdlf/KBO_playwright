"""CLI command for comprehensive platform diagnostics and automated self-healing."""

from __future__ import annotations

# ruff: noqa: T201
import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.diagnostics.engine import HEALABLE_SUBSYSTEMS, SystemDiagnosticsEngine

if TYPE_CHECKING:
    from src.diagnostics.dto import AutoHealResult


def _render_auto_heal(result: AutoHealResult) -> list[str]:
    """Render an auto-heal outcome without claiming more than was performed."""
    lines: list[str] = []
    if result.actions:
        lines.append(f"[AUTO-HEAL] Executed {len(result.actions)} recovery actions:")
        lines.extend(f"  - {action}" for action in result.actions)
    elif result.addressed:
        lines.append(f"[AUTO-HEAL] No repairs were needed for: {', '.join(result.addressed)}.")
    if result.unsupported:
        healable = ", ".join(sorted(item.value for item in HEALABLE_SUBSYSTEMS))
        lines.append(
            f"[AUTO-HEAL] No automated remediation exists for: {', '.join(result.unsupported)}. "
            f"Those subsystems were NOT healed (healable: {healable})."
        )
    return lines


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
        result = engine.auto_heal(args.subsystem)
        # In JSON mode stdout stays machine-parseable, so the heal outcome goes
        # to stderr rather than being dropped entirely.
        stream = sys.stderr if args.json else sys.stdout
        for line in _render_auto_heal(result):
            print(line, file=stream)

    report = engine.diagnose_all(subsystem=args.subsystem)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(f"=== Unified System Diagnostics Report [{report.overall_status}] ===")
        print(
            f"Total Checks: {report.total_checks} | Healthy: {report.healthy_count} | "
            f"Warnings: {report.warning_count} | Critical: {report.critical_count}"
        )
        print("-" * 60)
        for check in report.checks:
            status_tag = f"[{check.severity}]"
            print(f"{status_tag:<12} {check.subsystem:<12} {check.name:<25}: {check.message}")
            if check.remediation_hint:
                print(f"             -> Hint: {check.remediation_hint}")

    return 1 if report.critical_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
