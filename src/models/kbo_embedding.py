"""
Embedding model for KBO data.
Stores vector representations of various entities (players, games, etc.) for RAG.
"""
from __future__ import annotations

from typing import Optional, Any, Dict
from sqlalchemy import Integer, String, Text, JSON, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin

class KBOEmbedding(Base, TimestampMixin):
    """
    Stores text embeddings for various source tables.
    Designed to be synced to a Supabase table with pgvector support.
    """
    __tablename__ = "embeddings"
    __table_args__ = (
        UniqueConstraint("table_name", "record_id", name="uq_embedding_source"),
        Index("idx_embedding_table", "table_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Source identification
    table_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="Source table name (e.g. 'players', 'game')")
    record_id: Mapped[str] = mapped_column(String(50), nullable=False, comment="Primary key of the source record")
    
    # Content
    content: Mapped[str] = mapped_column(Text, nullable=False, comment="Serialized natural language representation")
    
    # Vector data
    # In SQLite, we store as JSON or specific format. 
    # When syncing to Supabase, this will be cast to the 'vector' type.
    vector_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True, comment="Vector embedding (list of floats)")
    
    # Metadata for filtering (year, team, etc.)
    metadata_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True, comment="Additional context for filtering")
    
    def __repr__(self) -> str:
        return f"<KBOEmbedding({self.table_name}, {self.record_id})>"
