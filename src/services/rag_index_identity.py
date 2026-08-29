"""Shared identity helpers for the sparse and vector RAG indexes."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

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


def _identity_part(value: object | None) -> str:
    """Normalize one source identity component before composing a key."""
    return str(value).strip() if value is not None else ""


def _identity_digest(parts: tuple[str, ...]) -> str:
    """Return a collision-resistant digest for source identity components."""
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def stable_award_source_row_id(
    year: int,
    award_type: str,
    category: str | None,
    player_name: str,
) -> str:
    """Build deterministic content-derived natural key for an award chunk."""
    category_part = _identity_part(category) or "NONE"
    return f"{year}_{award_type.strip()}_{category_part}_{player_name.strip()}"


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


def stable_player_movement_source_row_id(
    movement_date: date | str,
    team_code: str,
    player_name: str,
    section: str,
) -> str:
    """Build a natural key from the player movement source unique tuple."""
    parts = tuple(_identity_part(value) for value in (movement_date, team_code, player_name, section))
    readable = "_".join(parts)
    return f"{readable}_{_identity_digest(parts)[:16]}"


@dataclass(frozen=True, slots=True)
class PbpSourceIdentity:
    """Source fields used to derive a stable play-by-play identity."""

    game_id: str
    source_row_index: int | None
    inning: int | None
    inning_half: str | None
    pitcher_name: str | None
    batter_name: str | None
    play_description: str | None
    event_type: str | None
    result: str | None
    source_name: str | None = None


def stable_pbp_source_row_id(identity: PbpSourceIdentity) -> str:
    """Build a stable PBP key using source position or content fallback."""
    clean_game_id = _identity_part(identity.game_id)
    if identity.source_row_index is not None:
        return f"{clean_game_id}_{identity.source_row_index}"
    parts = tuple(
        _identity_part(value)
        for value in (
            identity.game_id,
            identity.inning,
            identity.inning_half,
            identity.pitcher_name,
            identity.batter_name,
            identity.play_description,
            identity.event_type,
            identity.result,
            identity.source_name,
        )
    )
    return f"{clean_game_id}_content_{_identity_digest(parts)}"


def stable_highlight_source_row_id(
    game_id: str,
    highlight_type: str,
    event_seq: int | None,
    description: str | None = None,
) -> str:
    """Build a stable highlight key from its game event identity."""
    clean_game_id = _identity_part(game_id)
    clean_type = _identity_part(highlight_type)
    if event_seq is not None:
        return f"{clean_game_id}_{clean_type}_{event_seq}"
    parts = (clean_game_id, clean_type, _identity_part(description))
    return f"{clean_game_id}_{clean_type}_summary_{_identity_digest(parts)}"
