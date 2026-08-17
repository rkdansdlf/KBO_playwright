"""CLI for crawling historical seasons."""

from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    """Run historical season crawler CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.parse_args(argv)

    return 0


if __name__ == "__main__":
    sys.exit(main())
