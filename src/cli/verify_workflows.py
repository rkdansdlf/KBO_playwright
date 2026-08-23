"""CLI command to audit and verify GitHub Actions CI/CD workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.ci.verifier import WorkflowVerifier


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Audit and verify GitHub Actions workflows.")
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Target directory containing .github/workflows YAML files.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail (exit code 1) on any WARNING level issues.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output audit report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    verifier = WorkflowVerifier()
    report = verifier.verify_all_workflows(target_dir=args.dir)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        print(f"=== Workflow Integrity Audit ({report.total_workflows} workflows, {report.total_jobs} jobs) ===")  # noqa: T201
        print(  # noqa: T201
            f"Passed: {report.passed_workflows} | Failed: {report.failed_workflows} | Issues: {len(report.issues)}"
        )
        if report.issues:
            print("\nIssues:")  # noqa: T201
            for issue in report.issues:
                prefix = f"[{issue.severity}]"
                job_part = f" ({issue.job_id})" if issue.job_id else ""
                print(f"  {prefix} {issue.workflow_file}{job_part}: {issue.rule_name} - {issue.message}")  # noqa: T201

    has_errors = any(i.severity == "ERROR" for i in report.issues)
    has_warnings = any(i.severity == "WARN" for i in report.issues)

    if has_errors or (args.strict and has_warnings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
