"""Render the crawler adoption matrix, and fail if it has drifted from the code.

The matrix answers which crawlers have moved onto the shared transport, run
ledger, dead letter queue, and replay contract, and which have not. Most axes are
read out of each module's source, so the report cannot describe a crawler that
does not exist; the design axes are declared and are cross-checked against what
the code actually does.

Exit codes:
    0 -- matrix rendered, no drift
    1 -- drift found: the declared matrix disagrees with the source
    2 -- configuration or input error
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.crawlers.adoption_matrix import build_matrix, render_markdown


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the argument parser."""
    parser = argparse.ArgumentParser(
        prog="python3 -m src.cli.reports.crawler_adoption_matrix",
        description="Classify every crawler against the shared crawl contract.",
    )
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown", help="Output format.")
    parser.add_argument("--output", type=Path, default=None, help="Write the report to this path instead of stdout.")
    parser.add_argument(
        "--advisories-only",
        action="store_true",
        help="List only the advisory observations, one per line.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when advisories are present as well as on drift.",
    )
    return parser


def _render(matrix, args: argparse.Namespace) -> str:  # noqa: ANN001 - AdoptionMatrix
    if args.advisories_only:
        return "\n".join(matrix.advisories) or "No advisories."
    if args.format == "json":
        return json.dumps(matrix.to_dict(), indent=2, ensure_ascii=False)
    return render_markdown(matrix)


def main(argv: list[str] | None = None) -> int:
    """Run the report.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    args = build_arg_parser().parse_args(argv)
    matrix = build_matrix()
    report = _render(matrix, args)

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{report}\n", encoding="utf-8")
    else:
        sys.stdout.write(f"{report}\n")

    if matrix.drift:
        sys.stderr.write(f"adoption matrix drift: {len(matrix.drift)} problem(s)\n")
        for message in matrix.drift:
            sys.stderr.write(f"  - {message}\n")
        return 1
    if args.strict and matrix.advisories:
        sys.stderr.write(f"adoption matrix advisories: {len(matrix.advisories)}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
