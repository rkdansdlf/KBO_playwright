"""Physical indexes the RAG search paths require, and how to tell they are present.

The migration history records what was *applied*; it never records what *exists*.
On 2026-10-30 `053_rag_search_text_index.sql` was listed as applied since
2026-09-22 while its expression index was absent, so every BM25 candidate query
ran a sequential scan over 3.1 GB — 116 s for one keyword where 0.26 s was
available — for five weeks and produced no signal at all. Two tools could have
noticed and neither did: `apply_postgres_migrations --check` compares the history
table against the files on disk, and the index audit compares the sparse and
vector stores to each other. Nothing asked the database what it had built.

The same week, a second filter (`meta->>'game_date'`) turned out to have no index
either, which left `prod_pbp_028` collecting 325,953 candidates to return 9.

Matching is done on index *definitions*, not names: a renamed index that still
covers the expression satisfies the contract, and a same-named one that covers
something else does not.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

#: ``(label, tokens)`` — every token has to appear in one index definition for
#: that requirement to count as covered. Tokens are matched against normalized
#: ``pg_get_indexdef`` output, so they must survive case folding and whitespace
#: collapsing.
REQUIRED_SEARCH_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "lexical candidate index (RagSearchEngine._postgresql_candidates)",
        ("to_tsvector('simple'", "coalesce(title", "content"),
    ),
    (
        "game_date filter index (RagSearchEngine._apply_postgresql_filters)",
        ("(meta ->> 'game_date'",),
    ),
    (
        "dense vector index (VectorSearchRepository)",
        ("embedding vector_cosine_ops",),
    ),
    (
        "source identity index (ON CONFLICT upserts)",
        ("source_table", "source_row_id"),
    ),
)


def _normalize(definition: str) -> str:
    """Collapse whitespace and fold case so token matching survives formatting."""
    return " ".join(definition.split()).lower()


def missing_search_indexes(index_definitions: Iterable[tuple[str, str]]) -> list[str]:
    """Return the labels of required search expressions that no index covers.

    Args:
        index_definitions: ``(index_name, definition)`` pairs read from the live
            catalog. Callers pass only *valid* indexes — an INVALID index left by
            a failed ``CONCURRENTLY`` build is not coverage.

    Returns:
        The labels of the requirements nothing matched, in declaration order. An
        empty list means every requirement is covered.

    """
    normalized = [_normalize(definition) for _, definition in index_definitions]
    missing: list[str] = []
    for label, tokens in REQUIRED_SEARCH_INDEXES:
        if not any(all(token in definition for token in tokens) for definition in normalized):
            missing.append(label)
    return missing
