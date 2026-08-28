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


def stable_award_source_row_id(
    year: int,
    award_type: str,
    category: str | None,
    player_name: str,
) -> str:
    """Build deterministic content-derived natural key for an award chunk."""
    clean_cat = (category or "").strip()
    category_part = f"_{clean_cat}" if clean_cat else ""
    return f"{year}_{award_type.strip()}{category_part}_{player_name.strip()}"


def stable_team_history_source_row_id(season: int, team_code: str) -> str:
    """Build deterministic natural key for a team history chunk."""
    return f"{season}_{team_code.strip()}"


def stable_milestone_source_row_id(
    season: int,
    player_id: int | str | None,
    category: str,
) -> str:
    """Build deterministic natural key for a player milestone chunk."""
    pid = str(player_id).strip() if player_id is not None else "UNKNOWN"
    return f"{season}_{pid}_{category.strip()}"


def stable_futures_source_row_id(game_id: str) -> str:
    """Build deterministic natural key for a Futures schedule chunk."""
    return game_id.strip()


def stable_splits_source_row_id(
    season: int,
    player_id: int | str | None,
    split_type: str,
    split_key: str,
) -> str:
    """Build deterministic natural key for a player situational split chunk."""
    pid = str(player_id).strip() if player_id is not None else "UNKNOWN"
    return f"{season}_{pid}_{split_type.strip()}_{split_key.strip()}"
