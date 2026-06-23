from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from src.analyzers.data_summary import generate_report

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description="Generate KBO data summary report")


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    parser.parse_args(argv)
    report = generate_report()
    logger.info(report)


if __name__ == "__main__":
    main()
