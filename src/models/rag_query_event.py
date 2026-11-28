"""Append-only query-event ledger for L3 RAG traffic measurement.

One row is one answered ``/api/v1/rag/ask`` request whose route had already
been decided. The raw question is **never** stored: only a sha256 over its
whitespace-normalised form and its character count. That is enough to count
distinct questions and to bound the sample, without turning an audit table into
a repository of user input (and, in practice, leaked API keys).

The classification is deliberately about *what was served*, not about what the
user intended. A table row arriving through semantic search is evidence that
SQL could have served it; it is not proof that the user wanted that row. The
read-only consumer prints that limitation alongside the numbers.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

#: One of these values is written to ``outcome`` for every row.
RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED = "structured_answered"
RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK = "structured_fallback"
RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS = "document_table_rows"
RAG_QUERY_OUTCOME_DOCUMENT_PROSE = "document_prose"
RAG_QUERY_OUTCOME_EMPTY = "empty"
RAG_QUERY_OUTCOME_UNKNOWN = "unknown"

RAG_QUERY_OUTCOMES = frozenset(
    {
        RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
        RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK,
        RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
        RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
        RAG_QUERY_OUTCOME_EMPTY,
        RAG_QUERY_OUTCOME_UNKNOWN,
    },
)


class RagQueryEvent(Base):
    """One routed RAG request, recorded without its raw query text."""

    __tablename__ = "rag_query_events"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=func.now(),
        server_default=func.now(),
    )
    query_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    query_chars: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    route: Mapped[str] = mapped_column(String(16), nullable=False)
    intent: Mapped[str | None] = mapped_column(String(32), nullable=True)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    source_table: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    structured_candidates: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fallback_used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("idx_rag_query_events_created_at", "created_at"),
        Index("idx_rag_query_events_outcome_created_at", "outcome", "created_at"),
    )
