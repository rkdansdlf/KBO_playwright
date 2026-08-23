"""Shared token normalization and posting-row construction for sparse RAG search."""

from __future__ import annotations

import re
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

SEARCH_TOKEN_SUFFIXES = (
    "으로",
    "에서",
    "에게",
    "께서",
    "의",
    "은",
    "는",
    "이",
    "가",
    "을",
    "를",
    "와",
    "과",
    "도",
    "에",
)
_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
_MAX_TOKEN_LENGTH = 128


def _strip_particle(token: str) -> str:
    """Remove one trailing Korean particle using the query normalization contract."""
    for suffix in SEARCH_TOKEN_SUFFIXES:
        if len(token) > len(suffix) + 1 and token.endswith(suffix):
            return token[: -len(suffix)]
    return token


def search_keywords(query: str) -> list[str]:
    """Normalize whitespace-delimited query terms without changing their case."""
    keywords: list[str] = []
    for raw_keyword in query.split():
        for word_match in _TOKEN_RE.finditer(raw_keyword):
            keyword = _strip_particle(word_match.group().strip())
            if len(keyword) > 1:
                keywords.append(keyword)
    return keywords


def normalize_sparse_token(raw_token: str) -> str | None:
    """Normalize one document token for case-insensitive indexed lookup."""
    token = _strip_particle(raw_token.casefold())
    if len(token) <= 1 or len(token) > _MAX_TOKEN_LENGTH:
        return None
    return token


def tokenize_sparse_text(text: str | None) -> Counter[str]:
    """Return normalized token frequencies for one title or content value."""
    counts: Counter[str] = Counter()
    for raw_token in _TOKEN_RE.findall(text or ""):
        token = normalize_sparse_token(raw_token)
        if token:
            counts[token] += 1
    return counts


def _game_date(meta: Mapping[str, object] | None) -> str | None:
    """Return a stable date string copied from chunk metadata."""
    value = (meta or {}).get("game_date")
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[union-attr]
    return str(value)


def build_term_rows(
    rag_chunk_id: int,
    title: str | None,
    content: str,
    meta: Mapping[str, object] | None,
    *,
    source_table: str,
) -> list[dict[str, object]]:
    """Build one posting row per distinct normalized token in a RAG chunk."""
    title_counts = tokenize_sparse_text(title)
    content_counts = tokenize_sparse_text(content)
    game_date = _game_date(meta)
    return [
        {
            "rag_chunk_id": rag_chunk_id,
            "source_table": source_table,
            "token": token,
            "term_count": title_counts[token] + content_counts[token],
            "title_count": title_counts[token],
            "game_date": game_date,
        }
        for token in sorted(title_counts.keys() | content_counts.keys())
    ]
