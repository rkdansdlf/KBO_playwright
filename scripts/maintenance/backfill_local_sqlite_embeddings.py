"""Fill the local development database's RAG vectors, which were never written.

The dev database records "no vector" as the literal string ``null`` rather than
SQL NULL, so every reader that tests ``IS NOT NULL`` -- including the store
reconciliation this replaces a workaround for -- counts its 162 chunks as
embedded while retrieval finds nothing. The value is written here with the same
provider, model and dimension the operational index uses, so the two are
comparable.

This closes the *data* gap only. The local database still has no dense
retriever and no sparse terms, so a search against it returns nothing until
those exist; filling the column first keeps the two changes separate.

The embedding cache is deliberately not used: it lives in the operational
database, and a local backfill should not add rows there.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST, RAG_EMBEDDING_DIMENSION

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
BATCH_SIZE = 50
UNAPPLIED = ("null", "[]", "")


def needs_embedding(value: object) -> bool:
    """Return whether a stored vector column holds no vector.

    ``IS NULL`` is not enough: this database writes the four-character string
    ``null`` for "no vector", which every NULL test reads as present.
    """
    if value is None:
        return True
    return str(value).strip().lower() in UNAPPLIED


def vector_json(values: Sequence[float], *, dimension: int = RAG_EMBEDDING_DIMENSION) -> str:
    """Serialize a vector for the JSON column, refusing the unusable ones."""
    if len(values) != dimension:
        message = f"expected {dimension} dimensions, got {len(values)}"
        raise ValueError(message)
    if not any(value != 0.0 for value in values):
        message = "refusing to store a zero vector"
        raise ValueError(message)
    return json.dumps([float(value) for value in values])


def _load_pending(connection: sqlite3.Connection) -> list[tuple[int, str | None, str]]:
    """Return the rows whose vector column holds no vector."""
    rows = connection.execute("SELECT id, title, content, embedding_vector, embedding FROM rag_chunks").fetchall()
    return [
        (int(row[0]), row[1], str(row[2] or "")) for row in rows if needs_embedding(row[3]) or needs_embedding(row[4])
    ]


def _apply(connection: sqlite3.Connection, updates: list[tuple[str, str, int, str]]) -> None:
    """Write the vectors and their index timestamp."""
    connection.executemany(
        "UPDATE rag_chunks SET embedding_vector = ?, embedding = ?, indexed_at = ? WHERE id = ?",
        updates,
    )
    connection.commit()


def main(argv: list[str] | None = None) -> int:
    """Report the pending rows, or write their vectors with ``--apply``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database to fill.")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Chunks per embedding batch.")
    parser.add_argument("--apply", action="store_true", help="Write the vectors.")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.db.exists():
        sys.stderr.write(f"database not found: {args.db}\n")
        return 1

    connection = sqlite3.connect(args.db)
    try:
        pending = _load_pending(connection)
        logger.info("chunks without a usable vector: %d", len(pending))
        if not args.apply:
            sys.stderr.write(f"refusing write: {len(pending)} chunks pending; pass --apply\n")
            return 0

        from src.services.embedding_service import EmbeddingService

        service = EmbeddingService(cache_enabled=False)
        written = 0
        for start in range(0, len(pending), args.batch_size):
            batch = pending[start : start + args.batch_size]
            vectors = service.get_embeddings_batch([content for _, _, content in batch])
            if len(vectors) != len(batch):
                sys.stderr.write(f"provider returned {len(vectors)} vectors for {len(batch)} chunks\n")
                return 1
            stamp = datetime.now(KST).isoformat()
            updates: list[tuple[str, str, int, str]] = []
            for (chunk_id, _title, _content), values in zip(batch, vectors, strict=True):
                try:
                    literal = vector_json(values)
                except ValueError as error:
                    sys.stderr.write(f"chunk {chunk_id}: {error}\n")
                    return 1
                updates.append((literal, literal, stamp, chunk_id))
            _apply(connection, updates)
            written += len(updates)
            logger.info("embedded %d/%d", written, len(pending))

        remaining = len(_load_pending(connection))
        logger.info("done: %d written, %d still without a vector", written, remaining)
        return 0 if remaining == 0 else 1
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
