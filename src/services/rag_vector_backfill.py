"""Embed the identities the Oracle copy could not supply.

The Oracle store is the vector source, but it is not the whole corpus. The
sparse store keeps taking new rows after Oracle's last indexing run, so a small
set of identities exists only there -- with ``embedding_vector`` NULL, which is
exactly what the copy skipped. A finished copy is therefore still missing dense
coverage for those rows, and ``verify_target`` will not catch it: every row it
sees has a vector, it simply cannot know about rows that are absent.

They are embedded from their own content here, with the same provider, model,
dimension and fingerprint the rest of the index was built with, so the gap closes
without introducing a second set of conventions.

Two behaviours here are deliberate:

* Only ``ACTIVE`` rows are embedded. ``DELETED`` rows are excluded from
  retrieval by definition, so paying to vectorize them would buy nothing.
* A zero vector refuses the batch. The provider answers with 1536 zeros rather
  than raising when it fails, so without that guard a provider outage would be
  recorded as a successful backfill.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.constants import KST, RAG_EMBEDDING_DIMENSION
from src.services.rag_incremental_selection import embedding_fingerprint
from src.services.rag_index_identity import chunk_content_hash, current_index_version
from src.services.rag_vector_migration import (
    SourceChunk,
    apply_batch,
    chunk_from_row,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 50

_SOURCE_SQL = text(
    "SELECT id, season_year, season_id, league_type_code, team_id, player_id, "
    "source_table, source_row_id, title, content, content_hash, index_version, "
    "index_status, indexed_at, created_at, updated_at, meta, embedding_vector "
    "FROM rag_chunks WHERE index_status = 'ACTIVE' ORDER BY id"
)
_TARGET_KEYS_SQL = text("SELECT source_table, source_row_id FROM rag_chunks")


@dataclass
class BackfillReport:
    """Count what the gap backfill actually did."""

    source_rows: int = 0
    gap_rows: int = 0
    embedded: int = 0
    copied: int = 0
    failed: int = 0
    batches: int = 0
    failure_samples: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        """Return a one-line rendering for logs and CLI output."""
        return (
            f"source={self.source_rows} gap={self.gap_rows} embedded={self.embedded} "
            f"copied={self.copied} failed={self.failed} batches={self.batches}"
        )


@dataclass(frozen=True)
class _WriteContext:
    """Hold the collaborators every batch write needs."""

    session: Session
    embedding_service: object
    fingerprint: str
    report: BackfillReport
    on_progress: Callable[[BackfillReport], None] | None


class ZeroVectorError(RuntimeError):
    """State that the provider answered with an unusable vector."""


def load_target_keys(target_engine: Engine) -> set[str]:
    """Return the identity keys the vector store already holds."""
    with target_engine.connect() as connection:
        return {
            f"{source_table}:{source_row_id}"
            for source_table, source_row_id in connection.execute(_TARGET_KEYS_SQL).all()
        }


def iter_gap_chunks(source_engine: Engine, target_keys: set[str]) -> Iterator[SourceChunk]:
    """Yield ACTIVE source chunks whose identity the vector store does not hold."""
    with source_engine.connect() as connection:
        result = connection.execute(_SOURCE_SQL).mappings()
        for row in result:
            key = f"{row['source_table']}:{row['source_row_id']}"
            if key in target_keys:
                continue
            yield chunk_from_row(dict(row))


def require_usable_embedding(
    embeddings: Sequence[Sequence[float]],
    expected: int,
    *,
    dimension: int = RAG_EMBEDDING_DIMENSION,
) -> None:
    """Refuse a batch the provider could not really produce.

    Mirrors the builder's guard: a count mismatch means the provider dropped a
    row, and an all-zero vector is what this codebase gets back when the call
    fails. Either one written to the store would read as "indexed" while being
    unsearchable, which is the failure this backfill exists to avoid repeating.
    """
    if len(embeddings) != expected:
        message = f"provider returned {len(embeddings)} vectors for {expected} chunks"
        raise ZeroVectorError(message)
    for values in embeddings:
        if len(values) != dimension:
            message = f"provider returned {len(values)} dimensions, expected {dimension}"
            raise ZeroVectorError(message)
        if not any(value != 0.0 for value in values):
            message = "provider returned a zero vector"
            raise ZeroVectorError(message)


def _fingerprint(embedding_service: object) -> str:
    """Return the same fingerprint the builder persists."""
    model = getattr(embedding_service, "model_name", None)
    if callable(model):
        model = model()
    if not model:
        from src.services.embedding_service import DEFAULT_OPENROUTER_EMBEDDING_MODEL

        model = os.getenv("EMBEDDING_MODEL", DEFAULT_OPENROUTER_EMBEDDING_MODEL)
    dimension = getattr(embedding_service, "dimension", RAG_EMBEDDING_DIMENSION)
    return embedding_fingerprint(model, dimension, os.getenv("RAG_CHUNKING_VERSION", "rag-v1"))


def _embed_batch(
    chunks: Sequence[SourceChunk],
    embedding_service: object,
    fingerprint: str,
    *,
    dimension: int = RAG_EMBEDDING_DIMENSION,
) -> list[SourceChunk]:
    """Return the chunks with a validated embedding and builder-equivalent metadata.

    Only ``content`` is embedded: that is what ``build_rag_index`` sends, so a
    row embedded here lands in the same neighbourhood as the 222,987 rows the
    Oracle copy brought over.
    """
    texts = [chunk.content for chunk in chunks]
    embeddings = embedding_service.get_embeddings_batch(texts)  # type: ignore[attr-defined]
    require_usable_embedding(embeddings, len(chunks), dimension=dimension)
    version = current_index_version()
    now = datetime.now(KST)
    embedded: list[SourceChunk] = []
    for chunk, values in zip(chunks, embeddings, strict=True):
        meta = dict(chunk.meta)
        meta["embedding_fingerprint"] = fingerprint
        embedded.append(
            SourceChunk(
                source_id=chunk.source_id,
                season_year=chunk.season_year,
                season_id=chunk.season_id,
                league_type_code=chunk.league_type_code,
                team_id=chunk.team_id,
                player_id=chunk.player_id,
                source_table=chunk.source_table,
                source_row_id=chunk.source_row_id,
                title=chunk.title,
                content=chunk.content,
                content_hash=chunk_content_hash(chunk.title, chunk.content),
                index_version=version,
                index_status=chunk.index_status,
                indexed_at=now,
                created_at=chunk.created_at,
                updated_at=now,
                meta=meta,
                embedding=tuple(float(value) for value in values),
            )
        )
    return embedded


def backfill_gap(  # noqa: PLR0913 - both stores plus the resume and progress knobs are operator inputs
    source_engine: Engine,
    target_engine: Engine,
    embedding_service: object,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: int | None = None,
    on_progress: Callable[[BackfillReport], None] | None = None,
) -> BackfillReport:
    """Embed and upsert every source identity the vector store is missing."""
    report = BackfillReport()
    target_keys = load_target_keys(target_engine)
    logger.info("Vector store holds %d identities before the gap backfill", len(target_keys))
    fingerprint = _fingerprint(embedding_service)

    pending: list[SourceChunk] = []
    with Session(bind=target_engine) as target_session:
        context = _WriteContext(
            session=target_session,
            embedding_service=embedding_service,
            fingerprint=fingerprint,
            report=report,
            on_progress=on_progress,
        )
        for chunk in iter_gap_chunks(source_engine, target_keys):
            report.source_rows += 1
            report.gap_rows += 1
            pending.append(chunk)
            if len(pending) < batch_size:
                continue
            _flush(pending, context)
            pending = []
            if limit is not None and report.embedded >= limit:
                logger.info("Stopping at the --limit boundary after %s", report.summary)
                return report
        if pending:
            _flush(pending, context)
    return report


def _flush(pending: list[SourceChunk], context: _WriteContext) -> None:
    """Embed one batch and write it, isolating a provider failure from the rest."""
    report = context.report
    try:
        embedded = _embed_batch(pending, context.embedding_service, context.fingerprint)
    except ZeroVectorError:
        report.failed += len(pending)
        report.failure_samples.extend(chunk.key for chunk in pending[:5])
        logger.exception("Refusing to embed a batch of %d chunks", len(pending))
        return
    copied, failed = apply_batch(context.session, embedded)
    report.embedded += len(embedded)
    report.copied += copied
    report.failed += failed
    report.batches += 1
    logger.info("Backfilled %s", report.summary)
    if context.on_progress is not None:
        context.on_progress(report)
