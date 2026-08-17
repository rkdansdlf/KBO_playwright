"""CLI to inventory and compare RAG chunk generation without altering state."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.cli.rag import build_rag_index
from src.db.engine import get_db_session, get_rag_index_session
from src.models.rag_chunk import RagChunk
from src.services.rag_corpus_inventory import CorpusInventory, failed_source_inventory, inventory_source_chunks
from src.services.rag_source_contract import SOURCE_PROFILES, required_sources_for_profile

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _collect_inventories(  # noqa: PLR0913
    source_session: Session,
    index_session: Session,
    sources: list[str],
    source_map: dict[str, Any],
    *,
    is_all: bool,
    season: int | None,
    limit: int | None,
) -> CorpusInventory:
    """Collect source inventory records."""
    existing_rows = index_session.execute(select(RagChunk)).scalars().all()

    inventories = []
    for source in sources:
        chunk_fn = source_map[source]
        chunk_iter = chunk_fn(source_session, season, limit)

        try:
            chunks = list(chunk_iter)
            inventory = inventory_source_chunks(source, chunks, existing_rows, complete_scope=True)
            inventories.append(inventory)
        except Exception as e:
            logger.exception("source inventory failed: %s", source)
            inventories.append(failed_source_inventory(source, e))

    return CorpusInventory(tuple(inventories), complete_scope=is_all and limit is None)


def _validate_corpus(corpus: CorpusInventory, required_sources: list[str]) -> int:
    """Validate inventory requirements and defects."""
    for req in required_sources:
        found = False
        for inv in corpus.sources:
            if inv.source == req:
                found = True
                if inv.chunks_generated == 0:
                    sys.stdout.write(f"required source produced no chunks: {req}\n")
                    return 1
        if not found:
            sys.stdout.write(f"required source produced no chunks: {req}\n")
            return 1

    if corpus.has_defects:
        sys.stdout.write("Error: validation error in inventory.\n")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run RAG corpus inventory."""
    parser = argparse.ArgumentParser(description="Inventory RAG chunk generation.")
    parser.add_argument("--source", type=str, default="all")
    parser.add_argument("--profile", choices=SOURCE_PROFILES, help="Source contract profile to validate")
    parser.add_argument("--season", type=int, help="Optional season passed to source iterators")
    parser.add_argument("--limit", type=int, help="Limit generated chunks; disables complete-scope delete census")
    parser.add_argument("--require-source", type=str, action="append", default=[])
    parser.add_argument("--output", type=str)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    source_map = getattr(build_rag_index, "_SOURCE_MAP", {})
    sources = list(source_map.keys()) if args.source == "all" else [args.source]

    try:
        with get_db_session() as source_session, get_rag_index_session() as index_session:
            corpus = _collect_inventories(
                source_session,
                index_session,
                sources,
                source_map,
                is_all=(args.source == "all"),
                season=args.season,
                limit=args.limit,
            )
    except Exception as e:
        logger.exception("schema unavailable")
        sys.stdout.write(f"schema unavailable: {e}\n")
        return 2

    required_sources = (*required_sources_for_profile(args.profile), *args.require_source)
    validation_result = _validate_corpus(corpus, list(dict.fromkeys(required_sources)))
    if validation_result != 0:
        return validation_result

    result = corpus.to_dict()
    if args.profile:
        result["profile"] = args.profile
    if args.json:
        sys.stdout.write(json.dumps(result, indent=2) + "\n")

    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
