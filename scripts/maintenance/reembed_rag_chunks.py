#!/usr/bin/env python3
"""Re-embed existing rag_chunks with the currently configured embedding model.

Use after switching EMBEDDING_MODEL (for example from
voyageai/voyage-4-lite to perplexity/pplx-embed-v1-4b) to regenerate the
SQLite embeddings without re-crawling any source content. pgvector should be
rebuilt afterwards with `python -m src.cli.build_rag_index --source all`, and
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, select

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk
from src.services.embedding_service import EmbeddingService

logger = logging.getLogger(__name__)


class ReembedReport:
    """Summary of a re-embedding run."""

    def __init__(self) -> None:
        self.candidates = 0
        self.embedded = 0
        self.skipped_zero = 0


def _is_zero_vector(embedding: list[float]) -> bool:
    return all(value == 0.0 for value in embedding)


def _build_query(source: str | None):
    conditions = [RagChunk.content != ""]
    if source:
        conditions.append(RagChunk.source_table == source)
    return select(RagChunk).where(and_(*conditions)).order_by(RagChunk.id)


def reembed_chunks(*, apply: bool, source: str | None, batch_size: int, limit: int | None) -> ReembedReport:
    """Re-embed SQLite rag_chunks using the current embedding model.

    Args:
        apply: Apply.
        source: Source.
        batch_size: Batch Size.
        limit: Limit.

    Returns:
        Report describing what was processed.

    """
    service = EmbeddingService()
    report = ReembedReport()
    model_name = service._model_name()
    logger.info("Re-embedding with model=%s (dry-run=%s)", model_name, not apply)

    with SessionLocal() as session:
        total_candidates = len(session.scalars(_build_query(source)).all())
        report.candidates = total_candidates
        logger.info("Found %d candidate rag_chunk rows", total_candidates)

        offset = 0
        while True:
            if limit is not None and report.embedded >= limit:
                break
            stmt = _build_query(source).offset(offset).limit(batch_size)
            rows = list(session.scalars(stmt).all())
            if not rows:
                break
            if not apply:
                report.embedded += len(rows)
                offset += batch_size
                continue

            embeddings = service.get_embeddings_batch([row.content for row in rows])
            for row, emb in zip(rows, embeddings, strict=False):
                if _is_zero_vector(emb):
                    report.skipped_zero += 1
                    continue
                row.embedding = emb
                report.embedded += 1
            session.commit()

            offset += batch_size

    logger.info(
        "Re-embedding done: candidates=%d, embedded=%d, skipped_zero_vector=%d",
        report.candidates,
        report.embedded,
        report.skipped_zero,
    )
    if not apply:
        logger.info("Dry-run: no rows were modified. Pass --apply to write embeddings.")
    return report


def parse_args() -> argparse.Namespace:
    """Parse command-line options for the re-embedding CLI."""
    parser = argparse.ArgumentParser(description="Re-embed existing rag_chunks with the current embedding model.")
    parser.add_argument("--apply", action="store_true", help="Write embeddings. Default is dry-run.")
    parser.add_argument("--source", type=str, default=None, help="Only re-embed rows of this source_table.")
    parser.add_argument("--batch-size", type=int, default=50, help="Embedding batch size.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of rows to process.")
    return parser.parse_args()


def main() -> None:
    """Run the rag_chunks re-embedding CLI."""
    load_dotenv()
    args = parse_args()
    reembed_chunks(
        apply=args.apply,
        source=args.source,
        batch_size=args.batch_size,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
