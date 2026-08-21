"""Model representing a RAG text chunk with its metadata and vector embedding."""

from __future__ import annotations

import array
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Integer, String, Text, TypeDecorator, UniqueConstraint
from sqlalchemy.dialects.oracle import VECTOR, VectorStorageFormat, VectorStorageType
from sqlalchemy.orm import Mapped, mapped_column

from src.constants import RAG_EMBEDDING_DIMENSION

from .base import Base, TimestampMixin


class OracleVectorType(TypeDecorator[list[float] | None]):
    """Bind Python embedding lists as Oracle's native dense FLOAT32 vectors."""

    impl = VECTOR(
        RAG_EMBEDDING_DIMENSION,
        VectorStorageFormat.FLOAT32,
        VectorStorageType.DENSE,
    )
    cache_ok = True

    def process_bind_param(self, value: object, _dialect: object) -> object:
        """Convert list values to the array representation expected by oracledb."""
        if value is None or isinstance(value, array.array):
            return value
        if isinstance(value, (list, tuple)):
            return array.array("f", value)
        message = "Oracle VECTOR values must be lists, tuples, or array.array instances"
        raise TypeError(message)

    def process_result_value(self, value: object, _dialect: object) -> list[float] | None:
        """Return fetched Oracle vectors as ordinary Python lists."""
        if value is None:
            return None
        if isinstance(value, array.array):
            return list(value)
        return value  # type: ignore[return-value]


_ORACLE_EMBEDDING_TYPE = OracleVectorType()


class RagChunk(Base, TimestampMixin):
    """Represents an unstructured knowledge chunk (rules, news, historical logs).

    processed and embedded for semantic retrieval.

    """

    __tablename__ = "rag_chunks"
    __table_args__ = (UniqueConstraint("source_table", "source_row_id", name="uq_rag_chunks_source_identity"),)

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    season_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Season year if applicable")
    season_id: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Season meta table reference ID")
    league_type_code: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="League level code (e.g. KBO vs Futures)",
    )
    team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Associated team code")
    player_id: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Associated player person ID")
    source_table: Mapped[str] = mapped_column(
        Text().with_variant(String(100), "oracle"),
        nullable=False,
        comment="Source descriptor (e.g. naver_news, rulebook, namuwiki)",
    )
    source_row_id: Mapped[str] = mapped_column(
        Text().with_variant(String(1000), "oracle"),
        nullable=False,
        comment="Unique key or URL of the source article/document",
    )
    title: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Title of the article or section heading")
    content: Mapped[str] = mapped_column(Text, nullable=False, comment="Full text of the chunk")

    # Shared sparse/vector index identity and lifecycle state.
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    index_version: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    index_status: Mapped[str] = mapped_column(String(24), nullable=False, default="ACTIVE", server_default="ACTIVE")
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Keep the old JSON column untouched on Oracle; vector search uses the new VECTOR column.
    embedding: Mapped[Any | None] = mapped_column(
        "embedding_vector",
        JSON().with_variant(_ORACLE_EMBEDDING_TYPE, "oracle"),
        nullable=True,
        comment="Float embedding vector (Oracle VECTOR; JSON elsewhere)",
    )
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        nullable=True,
        default=dict,
        server_default="{}",
        comment="Additional metadata mappings",
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<RagChunk(id={self.id}, source='{self.source_table}', title='{self.title}')>"
