"""CLI command for auditing and validating platform configuration and secrets."""

from __future__ import annotations

import argparse
import json
import sys

from src.config.dto import EnvironmentType
from src.config.manager import ConfigManager


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Validate platform configuration and credentials.")
    parser.add_argument(
        "--env",
        type=str,
        default="production",
        choices=["local", "development", "staging", "production", "ci"],
        help="Target environment to validate against (default: production).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail execution on warnings as well as missing keys.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output validation report as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    target_env = EnvironmentType(args.env)
    report = ConfigManager.validate_environment(target_env=target_env)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        status_tag = "[VALID]" if report.is_valid else "[INVALID]"
        print(f"=== Environment Configuration Audit {status_tag} ({target_env.value}) ===")  # noqa: T201
        print(f"Checked at: {report.checked_at}")  # noqa: T201
        print("-" * 60)  # noqa: T201

        if report.missing_required_keys:
            print("[CRITICAL] Missing Required Keys:")  # noqa: T201
            for k in report.missing_required_keys:
                print(f"  - {k}")  # noqa: T201

        if report.warnings:
            print("[WARNING] Warnings:")  # noqa: T201
            for w in report.warnings:
                print(f"  - {w}")  # noqa: T201

        if report.is_valid and not report.warnings:
            print("All required environment keys and settings are valid.")  # noqa: T201

    if not report.is_valid:
        return 1
    if args.strict and report.warnings:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
