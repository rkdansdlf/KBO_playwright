"""CLI for crawler selector stability checks."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from src.monitoring.crawler_selector_gate import load_selector_config, render_selector_summary, run_selector_gate


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run crawler selector stability checks")
    parser.add_argument("--config", required=True, help="Path to selector gate JSON config")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for selector_gate_report.json and Playwright artifacts",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    args = parser.parse_args(argv)

    targets = load_selector_config(args.config)
    summary = run_selector_gate(targets, output_dir=Path(args.output_dir) if args.output_dir else None)

    if args.json:
        print(json.dumps(summary.to_dict(), ensure_ascii=False))  # noqa: T201
    else:
        print(render_selector_summary(summary))  # noqa: T201
    return 0 if summary.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
