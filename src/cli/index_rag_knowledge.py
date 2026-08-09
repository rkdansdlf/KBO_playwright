"""CLI command to index KBO notices and milestones into RAG knowledge chunks."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.db.engine import get_db_session
from src.services.rag_indexer import RagKnowledgeIndexer

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> None:
    """Run RAG knowledge indexing.

    Args:
        args: CLI arguments.

    """
    season = args.season or datetime.now(tz=KST).year

    with get_db_session() as session:
        indexer = RagKnowledgeIndexer(session)
        pr_count = indexer.index_press_releases()
        ms_count = indexer.index_milestones(season=season)
        logger.info("Indexed %d press release chunks and %d milestone chunks.", pr_count, ms_count)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Index KBO releases & milestones to RAG chunks")
    parser.add_argument("--season", type=int, default=None, help="Season year (default: current)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    run(args)


if __name__ == "__main__":  # pragma: no cover
    main()
