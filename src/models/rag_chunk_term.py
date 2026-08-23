"""Sparse term postings for Oracle RAG chunk retrieval."""

from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RagChunkTerm(Base):
    """Store normalized token counts for one RAG chunk."""

    __tablename__ = "rag_chunk_terms"
    __table_args__ = (
        Index("idx_rag_chunk_terms_token_chunk", "token", "rag_chunk_id"),
        Index("idx_rag_chunk_terms_token_source", "token", "source_table", "rag_chunk_id"),
        Index("idx_rag_chunk_terms_game_date", "game_date", "token", "rag_chunk_id"),
        Index("idx_rag_chunk_terms_source_token", "source_table", "token", "rag_chunk_id"),
        Index("idx_rag_chunk_terms_source_date", "source_table", "game_date", "token", "rag_chunk_id"),
    )

    rag_chunk_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey("rag_chunks.id", ondelete="CASCADE"),
        primary_key=True,
    )
    source_table: Mapped[str] = mapped_column(String(100), nullable=False)
    token: Mapped[str] = mapped_column(String(128), primary_key=True)
    term_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    title_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    game_date: Mapped[str | None] = mapped_column(String(10), nullable=True)
