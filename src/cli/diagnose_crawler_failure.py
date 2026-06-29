"""CLI for diagnosing crawler and pipeline failure logs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.monitoring.failure_diagnosis import diagnose_sources, render_diagnosis_text, report_to_json

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Diagnose crawler failure logs")

    parser.add_argument("logs", nargs="*", help="Log files to inspect. Reads stdin when omitted.")
    parser.add_argument("--json", action="store_true", help="Print JSON report")
    args = parser.parse_args(argv)

    sources = _read_sources(args.logs)
    report = diagnose_sources(sources)

    if args.json:
        print(report_to_json(report))
    else:
        print(render_diagnosis_text(report))
    return report.exit_code


def _read_sources(paths: Sequence[str]) -> dict[str, str]:
    if not paths:
        return {"stdin": sys.stdin.read()}
    return {path: Path(path).read_text(encoding="utf-8") for path in paths}


if __name__ == "__main__":
    raise SystemExit(main())
