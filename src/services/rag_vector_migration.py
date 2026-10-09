"""Copy Oracle dense vectors into a PostgreSQL pgvector store without re-embedding.

Oracle holds 222,987 RAG chunks in ``KBO_APP.RAG_CHUNKS`` with a native
``VECTOR`` column and an HNSW index on it; the pgvector store holds the same
223,114 identities but only 167 embeddings. Re-embedding would spend money to
reproduce bytes that already exist, and would leave the two stores holding
vectors that differ for no reason -- so the vectors are copied as they are.

Three details are worth stating up front, because each one silently corrupts a
destination when it is wrong:

* The source column is a native ``VECTOR``. The driver returns it as a float32
  ``array('f')``, which is serialized to the ``[0.1,...]`` text form pgvector
  accepts. A vector whose length is not the target dimension is rejected here
  rather than becoming a NULL embedding that reads as "indexed" while being
  unsearchable.
* ``DELETED`` rows are copied with their status intact. Retrieval already
  excludes them, and rewriting them as ``ACTIVE`` would resurrect tombstones.
* The source has no ``document_type``/``game_date`` columns -- those live inside
  the ``META`` JSON document -- while the pgvector schema wants them as real
  columns so retrieval can filter on them. They are promoted on the way in, and
  the whole META document is kept alongside.

Writes are batched and idempotent: the upsert keys on
``(source_table, source_row_id)``, so an interrupted run resumes by passing the
last committed Oracle ``id`` back as ``after_id`` and re-running never
duplicates.
"""

from __future__ import annotations

import json
import logging
from array import array
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

#: Dimension both stores agree on; ``embedding_service`` trims/pads to it.
EMBEDDING_DIMENSION = 1536
DEFAULT_BATCH_SIZE = 1000
SOURCE_TABLE = "KBO_APP.rag_chunks"

#: Meta keys promoted to first-class columns because retrieval filters on them.
PROMOTED_META_FIELDS = ("document_type", "source_url", "language", "game_date", "published_at")

_INSERT_COLUMNS = (
    "season_year",
    "season_id",
    "league_type_code",
    "team_id",
    "player_id",
    "source_table",
    "source_row_id",
    "title",
    "content",
    "document_type",
    "game_date",
    "published_at",
    "source_url",
    "language",
    "content_hash",
    "index_version",
    "index_status",
    "indexed_at",
    "meta",
    "embedding",
)
_UPDATABLE_COLUMNS = tuple(column for column in _INSERT_COLUMNS if column not in ("source_table", "source_row_id"))

# The identifiers are module constants, never caller input, so the statement is
# built once at import rather than interpolated per batch.
_UPSERT_SQL = text(
    f"INSERT INTO rag_chunks ({', '.join(_INSERT_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join(f':{column}' for column in _INSERT_COLUMNS)}) "
    f"ON CONFLICT (source_table, source_row_id) DO UPDATE SET "
    f"{', '.join(f'{column} = EXCLUDED.{column}' for column in _UPDATABLE_COLUMNS)}"
)

_SOURCE_SQL = text(
    "SELECT id, season_year, season_id, league_type_code, team_id, player_id, "
    "source_table, source_row_id, title, content, content_hash, index_version, "
    "index_status, indexed_at, created_at, updated_at, meta, embedding_vector "
    "FROM KBO_APP.rag_chunks "
    "WHERE id > :after_id AND embedding_vector IS NOT NULL "
    "ORDER BY id"
)

_ZERO_VECTOR_SQL = text(
    "SELECT COUNT(*) FROM rag_chunks WHERE embedding IS NOT NULL AND vector_dims(embedding) <> :dimension"
)


@dataclass(frozen=True)
class SourceChunk:
    """One Oracle RAG row already shaped for the pgvector destination."""

    source_id: int
    season_year: int | None
    season_id: int | None
    league_type_code: int | None
    team_id: str | None
    player_id: str | None
    source_table: str
    source_row_id: str
    title: str | None
    content: str
    content_hash: str | None
    index_version: str | None
    index_status: str | None
    indexed_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None
    meta: dict[str, Any]
    embedding: tuple[float, ...]

    @property
    def key(self) -> str:
        """Return the identity key used by the destination upsert."""
        return f"{self.source_table}:{self.source_row_id}"

    @property
    def document_type(self) -> str | None:
        """Return the promoted document type, absent for older rows."""
        return _meta_str(self.meta, "document_type")

    @property
    def language(self) -> str | None:
        """Return the promoted language tag."""
        return _meta_str(self.meta, "language")

    @property
    def source_url(self) -> str | None:
        """Return the promoted source URL, which most rows never carried."""
        return _meta_str(self.meta, "source_url")

    @property
    def game_date(self) -> date | None:
        """Return the promoted game date parsed from META."""
        return _meta_date(self.meta, "game_date")

    @property
    def published_at(self) -> datetime | None:
        """Return the promoted publication timestamp parsed from META."""
        return _meta_datetime(self.meta, "published_at")

    def as_bind_params(self) -> dict[str, Any]:
        """Return the destination bind parameters for this row."""
        return {
            "season_year": self.season_year,
            "season_id": self.season_id,
            "league_type_code": self.league_type_code,
            "team_id": self.team_id,
            "player_id": self.player_id,
            "source_table": self.source_table,
            "source_row_id": self.source_row_id,
            "title": self.title,
            "content": self.content,
            "document_type": self.document_type,
            "game_date": self.game_date,
            "published_at": self.published_at,
            "source_url": self.source_url,
            "language": self.language,
            "content_hash": self.content_hash,
            "index_version": self.index_version,
            "index_status": self.index_status,
            "indexed_at": self.indexed_at,
            "meta": json.dumps(self.meta, ensure_ascii=False),
            "embedding": vector_literal(self.embedding),
        }


@dataclass
class MigrationReport:
    """Count what a migration run actually did."""

    scanned: int = 0
    copied: int = 0
    failed: int = 0
    batches: int = 0
    last_source_id: int = 0
    failure_samples: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        """Return a one-line rendering for logs and CLI output."""
        return (
            f"scanned={self.scanned} copied={self.copied} failed={self.failed} "
            f"batches={self.batches} last_source_id={self.last_source_id}"
        )


@dataclass(frozen=True)
class TargetVerification:
    """Describe what the destination store holds after a migration."""

    rows: int
    with_embedding: int
    wrong_dimension: int
    zero_vectors: int

    @property
    def is_clean(self) -> bool:
        """Return whether every embedding is present, right-sized, and non-zero."""
        return self.wrong_dimension == 0 and self.zero_vectors == 0 and self.with_embedding == self.rows


def _meta_str(meta: Mapping[str, Any], key: str) -> str | None:
    """Return a meta value as text, treating blanks as absent."""
    value = meta.get(key)
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _meta_date(meta: Mapping[str, Any], key: str) -> date | None:
    """Return a meta value as a date, skipping values that will not parse."""
    raw = _meta_str(meta, key)
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        logger.debug("META.%s is not an ISO date: %r", key, raw)
        return None


def _meta_datetime(meta: Mapping[str, Any], key: str) -> datetime | None:
    """Return a meta value as a naive datetime, skipping values that will not parse."""
    raw = _meta_str(meta, key)
    if raw is None:
        return None
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=None)
    except ValueError:
        logger.debug("META.%s is not an ISO timestamp: %r", key, raw)
        return None


def _parse_meta(raw: object) -> dict[str, Any]:
    """Return the META document as a mapping, treating junk as an empty one."""
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    text_value = str(raw).strip()
    if not text_value:
        return {}
    try:
        parsed = json.loads(text_value)
    except json.JSONDecodeError:
        logger.warning("META is not valid JSON; copying the row without promoted fields")
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _coerce_embedding(raw: object) -> tuple[float, ...]:
    """Return the source vector as a float tuple from a driver array or text form."""
    if raw is None:
        return ()
    if isinstance(raw, str):
        stripped = raw.strip().strip("[]")
        if not stripped:
            return ()
        return tuple(float(part) for part in stripped.split(","))
    if isinstance(raw, array):
        return tuple(float(value) for value in raw)
    if isinstance(raw, (list, tuple)):
        return tuple(float(value) for value in raw)
    return ()


def vector_literal(values: Sequence[float], *, dimension: int = EMBEDDING_DIMENSION) -> str:
    """Return the pgvector text form, rejecting a vector of the wrong length.

    A wrong-length vector would fail at the database with an opaque type error
    halfway through a batch; refusing it here names the row that is wrong.
    """
    if len(values) != dimension:
        message = f"expected {dimension} dimensions, got {len(values)}"
        raise ValueError(message)
    return "[" + ",".join(repr(float(value)) for value in values) + "]"


def chunk_from_row(row: Mapping[str, Any]) -> SourceChunk:
    """Convert one Oracle row mapping into a :class:`SourceChunk`."""
    return SourceChunk(
        source_id=int(row["id"]),
        season_year=row.get("season_year"),
        season_id=row.get("season_id"),
        league_type_code=row.get("league_type_code"),
        team_id=row.get("team_id"),
        player_id=row.get("player_id"),
        source_table=str(row["source_table"]),
        source_row_id=str(row["source_row_id"]),
        title=row.get("title"),
        content=str(row.get("content") or ""),
        content_hash=row.get("content_hash"),
        index_version=row.get("index_version"),
        index_status=row.get("index_status"),
        indexed_at=row.get("indexed_at"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        meta=_parse_meta(row.get("meta")),
        embedding=_coerce_embedding(row.get("embedding_vector")),
    )


def iter_source_chunks(
    session: Session,
    *,
    after_id: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[list[SourceChunk]]:
    """Stream source rows in ``id`` order as batches, so memory stays bounded."""
    result = session.execute(_SOURCE_SQL, {"after_id": after_id}, execution_options={"stream_results": True}).mappings()
    for partition in result.partitions(batch_size):
        yield [chunk_from_row(row) for row in partition]


def apply_batch(session: Session, chunks: Sequence[SourceChunk]) -> tuple[int, int]:
    """Upsert one batch, isolating individual failures from the rest of it.

    A batch statement is all-or-nothing, so one bad row would roll back the
    999 good rows beside it. When the batch fails it is retried row by row:
    that costs a round trip only on the batches that actually contain a bad
    row, and it keeps one malformed chunk from stalling a 223k row migration.
    """
    if not chunks:
        return (0, 0)
    try:
        session.execute(_UPSERT_SQL, [chunk.as_bind_params() for chunk in chunks])
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        logger.warning("Batch upsert failed; retrying %d rows individually", len(chunks))
        return _apply_row_by_row(session, chunks)
    return (len(chunks), 0)


def _apply_row_by_row(session: Session, chunks: Sequence[SourceChunk]) -> tuple[int, int]:
    """Upsert rows one at a time so a single failure cannot roll back the batch."""
    copied = 0
    failed = 0
    for chunk in chunks:
        try:
            session.execute(_UPSERT_SQL, chunk.as_bind_params())
            session.commit()
        except SQLAlchemyError as error:
            session.rollback()
            failed += 1
            logger.warning("Row %s (%s) failed: %s", chunk.source_id, chunk.key, error)
        else:
            copied += 1
    return (copied, failed)


def migrate_vectors(  # noqa: PLR0913 - resume cursor and progress hook are both operator inputs
    source_engine: Engine,
    target_engine: Engine,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    after_id: int = 0,
    limit: int | None = None,
    on_progress: Callable[[MigrationReport], None] | None = None,
) -> MigrationReport:
    """Copy every source chunk that carries a vector into the target store.

    The target session is bound to the engine rather than to a connection inside
    an open transaction, so each batch's commit is a real commit. A session bound
    to an externally-managed connection joins that transaction instead, which
    would leave every batch invisible until the run ended -- and then an
    interrupted 223k row migration would have nothing to resume from.
    """
    report = MigrationReport(last_source_id=after_id)
    with source_engine.connect() as source_connection, Session(bind=target_engine) as target_session:
        source_session = Session(bind=source_connection)
        for batch in iter_source_chunks(source_session, after_id=after_id, batch_size=batch_size):
            copied, failed = apply_batch(target_session, batch)
            report.scanned += len(batch)
            report.copied += copied
            report.failed += failed
            report.batches += 1
            report.last_source_id = batch[-1].source_id
            if failed:
                report.failure_samples.extend(chunk.key for chunk in batch[: min(failed, 5)])
            logger.info("Migrated %s", report.summary)
            if on_progress is not None:
                on_progress(report)
            if limit is not None and report.scanned >= limit:
                logger.info("Stopping at the --limit boundary after %s", report.summary)
                break
    return report


def verify_target(
    target_engine: Engine,
    *,
    dimension: int = EMBEDDING_DIMENSION,
) -> TargetVerification:
    """Describe the destination store so a silent bad copy cannot pass as done."""
    with target_engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM rag_chunks")).scalar_one()
        with_embedding = connection.execute(text("SELECT COUNT(embedding) FROM rag_chunks")).scalar_one()
        wrong_dimension = connection.execute(_ZERO_VECTOR_SQL, {"dimension": dimension}).scalar_one()
        zero_vectors = connection.execute(
            text("SELECT COUNT(*) FROM rag_chunks WHERE embedding IS NOT NULL AND embedding <=> :zero < 1e-12"),
            {"zero": vector_literal([0.0] * dimension)},
        ).scalar_one()
    return TargetVerification(
        rows=rows,
        with_embedding=with_embedding,
        wrong_dimension=wrong_dimension,
        zero_vectors=zero_vectors,
    )
