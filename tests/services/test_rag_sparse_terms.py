"""Tests for normalized Oracle sparse term postings."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
from src.services.rag_sparse_terms import build_term_rows, normalize_sparse_token, search_keywords


def test_search_keywords_preserves_existing_query_contract() -> None:
    """Remove Korean particles without changing query token case."""
    assert search_keywords("김도영의 2026시즌 KT와") == ["김도영", "2026시즌", "KT"]


def test_search_keywords_strips_attached_punctuation() -> None:
    """Match document-side tokenization when punctuation trails a keyword."""
    assert search_keywords("KBO 비디오 판독 신청 규정은?") == ["KBO", "비디오", "판독", "신청", "규정"]


def test_normalize_sparse_token_is_case_insensitive_and_particle_aware() -> None:
    """Normalize document terms with the same particle rules as queries."""
    assert normalize_sparse_token("OPS") == "ops"
    assert normalize_sparse_token("선수의") == "선수"
    assert normalize_sparse_token("a") is None


def test_build_term_rows_counts_title_and_game_date() -> None:
    """Build one row per term with title weighting metadata."""
    rows = build_term_rows(
        7,
        "김도영 선수",
        "김도영 선수 OPS OPS",
        {"game_date": "2026-08-21"},
        source_table="player_basic",
    )

    by_token = {row["token"]: row for row in rows}
    assert by_token["김도영"] == {
        "rag_chunk_id": 7,
        "source_table": "player_basic",
        "token": "김도영",
        "term_count": 2,
        "title_count": 1,
        "game_date": "2026-08-21",
    }
    assert by_token["ops"]["term_count"] == 2
