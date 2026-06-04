from __future__ import annotations

import argparse
import logging
from typing import Sequence

from src.analyzers.data_summary import generate_report
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate KBO data summary report")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    parser.parse_args(argv)
    report = generate_report()
    print(report)


if __name__ == "__main__":
    main()
