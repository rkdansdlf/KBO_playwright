"""Model representing a local cache for text embeddings to prevent duplicate API calls."""

from __future__ import annotations

from typing import Any

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class EmbeddingCache(Base, TimestampMixin):
    """Cache table mapping SHA-256 hashes of text content and model names.

    to their computed 256-dimensional float embedding vectors.

    """

    __tablename__ = "embedding_cache"

    text_hash: Mapped[str] = mapped_column(String(64), primary_key=True, comment="SHA-256 hash of cleaned text content")
    model_name: Mapped[str] = mapped_column(String(50), primary_key=True, comment="Name of the embedding model used")
    embedding: Mapped[Any] = mapped_column(
        JSON,
        nullable=False,
        comment="List of floats representing the embedding vector",
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<EmbeddingCache(model='{self.model_name}', hash='{self.text_hash[:8]}')>"
