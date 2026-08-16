"""Shared identity helpers for the sparse and vector RAG indexes."""

from __future__ import annotations

import hashlib
import os

DEFAULT_RAG_INDEX_VERSION = "rag-v1"
ACTIVE_INDEX_STATUS = "ACTIVE"
STALE_INDEX_STATUS = "STALE"
DELETED_INDEX_STATUS = "DELETED"
REINDEX_REQUIRED_STATUS = "REINDEX_REQUIRED"
RETRIEVABLE_INDEX_STATUSES = frozenset({"ACTIVE", "INDEXED"})


def current_index_version() -> str:
    """Return the configured RAG index version."""
    return os.getenv("RAG_INDEX_VERSION", DEFAULT_RAG_INDEX_VERSION)


def normalize_chunk_text(title: str | None, content: str) -> str:
    """Normalize title and content before hashing a canonical chunk."""
    return " ".join(f"{title or ''}\n{content}".split()).strip()


def chunk_content_hash(title: str | None, content: str) -> str:
    """Return the stable SHA-256 identity for a canonical chunk."""
    normalized = normalize_chunk_text(title, content)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
