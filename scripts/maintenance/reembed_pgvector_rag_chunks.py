#!/usr/bin/env python3
"""Re-embed zero-vector rag_chunks rows in the pgvector database.

API failures (401/402) previously caused EmbeddingService to persist zero
vectors into pgvector `rag_chunks` (44,158 of 70,477 rows were affected).
This script finds rows whose embedding has norm 0 and re-embeds their content
with the currently configured model (perplexity/pplx-embed-v1-4b, 1536 dims).

The SQLite embedding_cache now skips zero vectors, so cached zeros do not
block regeneration. Dry-run by default; pass --apply to write.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.vector_engine import get_vector_session, is_pgvector_available
from src.models.rag_chunk_vector import RagChunkVector
from src.services.embedding_service import EmbeddingService

logger = logging.getLogger(__name__)


class ReembedPgvectorReport:
    """Summary of a pgvector zero-vector re-embedding run."""

    def __init__(self) -> None:
        self.candidates = 0
        self.embedded = 0
        self.skipped_zero = 0
        self.failed = 0


def _is_zero_vector(embedding: list[float] | None) -> bool:
    return bool(embedding) and all(value == 0.0 for value in embedding)


def _zero_norm_query() -> str:
    return "vector_norm(embedding) = 0"


def _process_batch(
    session: Session, service: EmbeddingService, rows: list[RagChunkVector], report: ReembedPgvectorReport
) -> None:
    """Embed one batch of zero-vector rows and update the report."""
    embeddings = service.get_embeddings_batch([row.content for row in rows])
    for row, emb in zip(rows, embeddings, strict=False):
        if _is_zero_vector(emb):
            report.skipped_zero += 1
            continue
        row.embedding = emb
        report.embedded += 1
    session.commit()


def reembed_pgvector_chunks(
    *, apply: bool, source: str | None, batch_size: int, limit: int | None
) -> ReembedPgvectorReport:
    """Re-embed zero-vector pgvector rag_chunks rows.

    Args:
        apply: Write embeddings (dry-run otherwise).
        source: Only process rows of this source_table.
        batch_size: Embedding batch size.
        limit: Maximum number of rows to process.

    Returns:
        Report describing what was processed.

    """
    report = ReembedPgvectorReport()
    if not is_pgvector_available():
        logger.warning("pgvector not available — nothing to re-embed")
        return report

    service = EmbeddingService()
    model_name = service._model_name()
    logger.info("Re-embedding zero-vector pgvector rows with model=%s (dry-run=%s)", model_name, not apply)

    with get_vector_session() as session:
        where = [text(_zero_norm_query())]
        if source:
            where.append(RagChunkVector.source_table == source)
        count_stmt = select(func.count(RagChunkVector.id)).where(*where)
        report.candidates = session.execute(count_stmt).scalar() or 0
        logger.info("Found %d zero-vector rag_chunks rows", report.candidates)
        if not apply:
            logger.info("Dry-run: %d rows would be re-embedded. Pass --apply to write embeddings.", report.candidates)
            return report

        processed = 0
        last_id: int | None = None
        while True:
            if limit is not None and processed >= limit:
                break
            stmt = select(RagChunkVector).where(*where).order_by(RagChunkVector.id).limit(batch_size)
            if last_id is not None:
                stmt = stmt.where(RagChunkVector.id > last_id)
            rows = list(session.scalars(stmt).all())
            if not rows:
                break

            _process_batch(session, service, rows, report)
            processed += len(rows)
            last_id = rows[-1].id
            logger.info("Processed %d/%d rows (embedded=%d)", processed, report.candidates, report.embedded)

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
    """Parse command-line options for the pgvector re-embedding CLI."""
    parser = argparse.ArgumentParser(description="Re-embed zero-vector pgvector rag_chunks with the current model.")
    parser.add_argument("--apply", action="store_true", help="Write embeddings. Default is dry-run.")
    parser.add_argument("--source", type=str, default=None, help="Only process rows of this source_table.")
    parser.add_argument("--batch-size", type=int, default=50, help="Embedding batch size.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of rows to process.")
    return parser.parse_args()


def main() -> None:
    """Run the pgvector rag_chunks re-embedding CLI."""
    load_dotenv()
    args = parse_args()
    reembed_pgvector_chunks(
        apply=args.apply,
        source=args.source,
        batch_size=args.batch_size,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
