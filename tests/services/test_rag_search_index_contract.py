"""Tests for the required-search-index contract.

The definitions below are the live ``pg_get_indexdef`` output for the
``rag_chunks`` indexes, kept verbatim so the contract is checked against the
formatting PostgreSQL actually produces rather than a tidy approximation.
"""

from __future__ import annotations

from src.services.rag_search_index_contract import (
    REQUIRED_SEARCH_INDEXES,
    missing_search_indexes,
)

LEXICAL = (
    "idx_rag_chunks_search_tsvector",
    (
        "CREATE INDEX idx_rag_chunks_search_tsvector ON public.rag_chunks USING gin "
        "(to_tsvector('simple'::regconfig, ((COALESCE(title, ''::text) || ' '::text) || content)))"
    ),
)
GAME_DATE = (
    "idx_rag_chunks_meta_game_date",
    ("CREATE INDEX idx_rag_chunks_meta_game_date ON public.rag_chunks USING btree (((meta ->> 'game_date'::text)))"),
)
DENSE = (
    "idx_rag_chunks_embedding_ivfflat",
    (
        "CREATE INDEX idx_rag_chunks_embedding_ivfflat ON public.rag_chunks USING ivfflat "
        "(embedding vector_cosine_ops) WITH (lists='100')"
    ),
)
SOURCE = (
    "idx_rag_chunks_source",
    "CREATE UNIQUE INDEX idx_rag_chunks_source ON public.rag_chunks USING btree (source_table, source_row_id)",
)
PRODUCTION = [LEXICAL, GAME_DATE, DENSE, SOURCE]


def test_the_production_indexes_satisfy_the_contract() -> None:
    """Recognize the four indexes the live catalog carries."""
    assert missing_search_indexes(PRODUCTION) == []


def test_an_empty_catalog_names_every_requirement() -> None:
    """An empty catalog is an answer, not a measurement failure."""
    assert missing_search_indexes([]) == [label for label, _ in REQUIRED_SEARCH_INDEXES]


def test_a_renamed_index_still_satisfies_the_contract() -> None:
    """Coverage is the expression, not the name — 053's index was never built."""
    renamed = [("some_other_name", LEXICAL[1]), *PRODUCTION[1:]]
    assert missing_search_indexes(renamed) == []


def test_a_same_named_index_covering_something_else_does_not() -> None:
    """A name without the expression is not coverage — the trap that hid for weeks."""
    decoy = [(LEXICAL[0], "CREATE INDEX idx_rag_chunks_search_tsvector ON rag_chunks USING gin (content_tsv)")]
    missing = missing_search_indexes([*decoy, *PRODUCTION[1:]])
    assert missing == ["lexical candidate index (RagSearchEngine._postgresql_candidates)"]


def test_a_dropped_lexical_index_is_named() -> None:
    """Name the lexical requirement on its own when only it is absent."""
    missing = missing_search_indexes(PRODUCTION[1:])
    assert missing == ["lexical candidate index (RagSearchEngine._postgresql_candidates)"]


def test_a_dropped_game_date_index_is_named() -> None:
    """Name the game_date requirement on its own."""
    missing = missing_search_indexes([LEXICAL, DENSE, SOURCE])
    assert missing == ["game_date filter index (RagSearchEngine._apply_postgresql_filters)"]


def test_a_dropped_dense_index_is_named() -> None:
    """Name the dense vector requirement on its own."""
    missing = missing_search_indexes([LEXICAL, GAME_DATE, SOURCE])
    assert missing == ["dense vector index (VectorSearchRepository)"]


def test_a_dropped_source_identity_index_is_named() -> None:
    """Name the identity requirement on its own — ON CONFLICT depends on it."""
    missing = missing_search_indexes([LEXICAL, GAME_DATE, DENSE])
    assert missing == ["source identity index (ON CONFLICT upserts)"]


def test_whitespace_and_case_do_not_hide_an_index() -> None:
    """Normalize formatting so a reformatted definition is still recognized."""
    reformatted = (
        "catalog_restored_name",
        (
            "CREATE INDEX catalog_restored_name ON rag_chunks USING gin (\n"
            "    TO_TSVECTOR('simple'::regconfig,\n"
            "        COALESCE(Title, ''::text) || ' '::text || Content)\n"
            ")"
        ),
    )
    assert missing_search_indexes([reformatted, *PRODUCTION[1:]]) == []


def test_a_hnsw_vector_index_also_satisfies_the_dense_requirement() -> None:
    """Either ANN access method covers the dense requirement."""
    hnsw = (
        "idx_rag_chunks_embedding_hnsw",
        ("CREATE INDEX idx_rag_chunks_embedding_hnsw ON public.rag_chunks USING hnsw (embedding vector_cosine_ops)"),
    )
    assert missing_search_indexes([LEXICAL, GAME_DATE, hnsw, SOURCE]) == []


def test_a_lexical_index_without_the_title_is_not_coverage() -> None:
    """The search expression includes the title; a content-only index misses candidates."""
    content_only = (
        "idx_only_content",
        "CREATE INDEX idx_only_content ON public.rag_chunks USING gin (to_tsvector('simple'::regconfig, content))",
    )
    missing = missing_search_indexes([content_only, GAME_DATE, DENSE, SOURCE])
    assert missing == ["lexical candidate index (RagSearchEngine._postgresql_candidates)"]
