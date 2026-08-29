from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.cli import build_rag_index
from src.models.rankings import StatRanking


@pytest.mark.parametrize("season", [1988, 2025])
def test_game_season_filter_uses_regular_season_metadata(season) -> None:
    statement = str(build_rag_index._regular_season_ids(season))

    assert "kbo_seasons.season_year" in statement
    assert "kbo_seasons.league_type_code" in statement


@pytest.mark.parametrize(
    ("season_id", "expected"),
    [(None, None), (1988, 1988), (198800, 1988), (202500, 2025)],
)
def test_game_chunk_season_metadata_uses_season_year(season_id, expected) -> None:
    assert build_rag_index._season_year_from_id(season_id) == expected


def test_local_markdown_iterators_emit_the_three_vector_sources(tmp_path, monkeypatch):
    docs_root = tmp_path / "baseball"
    (docs_root / "kbo_rulebook" / "league_regulations").mkdir(parents=True)
    (docs_root / "glossary").mkdir()
    (docs_root / "kbo_knowledge").mkdir()
    (docs_root / "kbo_rulebook" / "league_regulations" / "rules.md").write_text(
        "# Rules\n\n## Article 1\n\nRegular season rule.",
        encoding="utf-8",
    )
    (docs_root / "glossary" / "terms.md").write_text(
        "# Terms\n\nBatting average means hits divided by at bats.",
        encoding="utf-8",
    )
    (docs_root / "kbo_knowledge" / "history.md").write_text(
        "# History\n\nKBO history and culture.",
        encoding="utf-8",
    )
    monkeypatch.setenv("KBO_MARKDOWN_DOCS_DIR", str(docs_root))

    rows = []
    for source in ("markdown_docs", "kbo_definitions", "kbo_regulations"):
        iterator = build_rag_index._SOURCE_MAP[source]
        rows.extend(iterator(None, None, None))

    assert {row["source_table"] for row in rows} == {
        "markdown_docs",
        "kbo_definitions",
        "kbo_regulations",
    }
    assert all(row["document_type"] == "markdown_doc" for row in rows)
    assert all(row["source_row_id"] for row in rows)
    assert "kbo_rulebook/league_regulations/rules.md_0" in {row["source_row_id"] for row in rows}


def test_local_markdown_iterator_honors_limit(tmp_path, monkeypatch):
    (tmp_path / "doc.md").write_text(
        "# Document\n\nFirst paragraph.\n\nSecond paragraph.",
        encoding="utf-8",
    )
    monkeypatch.setenv("KBO_MARKDOWN_DOCS_DIR", str(tmp_path))

    rows = list(build_rag_index._iter_markdown_chunks(None, None, 1))

    assert len(rows) == 1
    assert rows[0]["source_table"] == "markdown_docs"


def test_staging_rag_chunk_iterator_reembeds_content_without_reusing_vectors() -> None:
    """Read compatible staging schemas while leaving their old vectors behind."""
    session = MagicMock()
    session.execute.return_value.mappings.return_value = [
        {
            "id": 1,
            "source_table": "kbo_definitions",
            "source_row_id": "rules-1",
            "title": "규정",
            "content": "정규 시즌 규정 본문",
            "embedding": [0.1, 0.2],
            "metadata": {"document_type": "regulation"},
            "is_active": True,
        },
        {"id": 2, "content": "비활성", "is_active": False},
        {"id": 3, "content": "삭제됨", "index_status": "DELETED"},
    ]

    rows = list(build_rag_index._iter_staging_rag_chunks(session, None, None))

    assert len(rows) == 1
    assert rows[0]["content"] == "정규 시즌 규정 본문"
    assert rows[0]["document_type"] == "regulation"
    assert "embedding" not in rows[0]


def test_staging_rag_chunk_iterator_can_skip_populated_oracle_vectors() -> None:
    """Avoid materializing populated single-store Oracle vectors during resume."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "oracle"
    session.execute.return_value.mappings.return_value = []

    rows = list(build_rag_index._iter_staging_rag_chunks(session, None, None, skip_populated=True))

    assert rows == []
    statement = str(session.execute.call_args.args[0])
    assert "embedding_vector IS NULL" in statement


def test_prepare_staging_chunks_uses_populated_vector_filter() -> None:
    """Apply the single-store staging optimization through source preparation."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "oracle"
    session.execute.return_value.mappings.return_value = []

    prepared = build_rag_index._prepare_source_chunks(
        "staging_rag_chunks",
        build_rag_index._iter_staging_missing_rag_chunks,
        session,
        None,
        None,
    )

    assert list(prepared) == []
    assert "embedding_vector IS NULL" in str(session.execute.call_args.args[0])


def test_rankings_iterator_orders_ties_deterministically() -> None:
    """Include stable tie-break columns in ranking source queries."""
    query = MagicMock()
    query.order_by.return_value = query
    query.yield_per.return_value = []
    session = MagicMock()
    session.query.return_value = query

    list(build_rag_index._iter_rankings_chunks(session, None, None))

    order_columns = query.order_by.call_args.args
    assert order_columns[-3] is StatRanking.entity_label
    assert order_columns[-2] is StatRanking.entity_id
    assert order_columns[-1] is StatRanking.team_id


def test_deterministic_embedding_mode_is_available_for_staging() -> None:
    """Build the non-network provider used only for infrastructure acceptance."""
    service = build_rag_index._embedding_service("deterministic")

    assert service.dimension == 1536
    assert len(service.get_embedding("staging smoke")) == 1536


def test_process_source_persists_using_index_session(monkeypatch) -> None:
    index_session = MagicMock(name="index_session")
    embedding_service = MagicMock()
    embedding_service.get_embeddings_batch.return_value = [[0.1]]
    persisted = []

    monkeypatch.setattr(
        build_rag_index,
        "_persist_index_batch",
        lambda batch, session: persisted.append((batch, session)),
    )

    count = build_rag_index._process_source(
        "players",
        iter([{"source_table": "player_basic", "source_row_id": "1", "content": "player"}]),
        embedding_service,
        index_session,
        dry_run=False,
    )

    assert count == 1
    assert persisted[0][1] is index_session


def test_skip_existing_index_rows_filters_populated_identities(monkeypatch) -> None:
    """Resume a source build without re-writing already populated vectors."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = [("player_basic", "1")]
    chunks = iter(
        [
            {"source_table": "player_basic", "source_row_id": "1"},
            {"source_table": "player_basic", "source_row_id": "2"},
        ],
    )

    result = list(build_rag_index._skip_existing_index_rows(chunks, index_session))

    assert result == [{"source_table": "player_basic", "source_row_id": "2"}]


def test_skip_existing_index_rows_scopes_lookup_to_source_table() -> None:
    """Limit the populated-vector lookup to the source currently being built."""
    index_session = MagicMock()
    index_session.execute.return_value.all.return_value = []

    list(
        build_rag_index._skip_existing_index_rows(
            iter(()),
            index_session,
            source_table="player_basic",
        ),
    )

    statement = index_session.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert "player_basic" in compiled


def test_prepare_long_database_sources_releases_session() -> None:
    session = MagicMock(name="source_session")
    chunks = [{"source_table": "game_play_by_play", "source_row_id": "1"}]

    prepared = build_rag_index._prepare_source_chunks(
        "pbp",
        lambda _session, _season, _limit: iter(chunks),
        session,
        None,
        None,
    )

    assert list(prepared) == chunks
    session.close.assert_called_once_with()


def test_validate_embeddings_rejects_zero_vectors() -> None:
    with pytest.raises(RuntimeError, match="zero vector"):
        build_rag_index._validate_embeddings([[0.0, 0.0]], 1)


def test_validate_embeddings_rejects_incomplete_batches() -> None:
    with pytest.raises(RuntimeError, match="returned 1 vectors for 2 chunks"):
        build_rag_index._validate_embeddings([[0.1, 0.2]], 2)


def test_build_targets_redact_credentials_and_allow_dry_run(monkeypatch) -> None:
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector:secret@127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=True,
    )

    assert targets.display() == {
        "source_db": "oracle+oracledb://kbo_medium",
        "sparse_index_db": "oracle+oracledb://kbo_medium",
        "vector_db": "oracle+oracledb://kbo_medium",
        "target_environment": "staging",
        "write_enabled": False,
    }


def test_build_targets_reject_shared_write_target(monkeypatch) -> None:
    monkeypatch.setenv("RAG_INDEX_DB_URL", "oracle+oracledb://app:secret@kbo_medium")
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="source and sparse index targets must be different"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://other:password@kbo_medium",
            embedding_mode="configured",
            dry_run=False,
        )


def test_oracle_staging_build_allows_postgresql_index_targets(monkeypatch) -> None:
    """Allow Oracle source reads to publish into isolated PostgreSQL staging indexes."""
    monkeypatch.setenv("RAG_INDEX_DB_URL", "postgresql://127.0.0.1:5432/rag_sparse")
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.sparse_index_db == "postgresql://127.0.0.1:5432/rag_sparse"
    assert targets.vector_db == "postgresql://127.0.0.1:5432/rag_vector"


def test_oracle_build_uses_one_database_for_sparse_and_vector_targets(monkeypatch) -> None:
    """Allow Oracle AI Vector Search without PostgreSQL target variables."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "staging")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.sparse_index_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.vector_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.write_enabled is True


def test_postgresql_source_can_publish_to_oracle_target(monkeypatch) -> None:
    """Keep source reads separate from the Oracle single-store write target."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    targets = build_rag_index._resolve_build_targets(
        "postgresql://source:secret@127.0.0.1:5432/staging",
        target_db_url="oracle+oracledb://app:secret@kbo_medium",
        embedding_mode="configured",
        dry_run=False,
    )

    assert targets.source_db == "postgresql://source:secret@127.0.0.1:5432/staging"
    assert targets.sparse_index_db == "oracle+oracledb://app:secret@kbo_medium"
    assert targets.vector_db == "oracle+oracledb://app:secret@kbo_medium"


def test_oracle_production_build_requires_explicit_write_gate(monkeypatch) -> None:
    """Keep production Oracle writes behind a separate explicit safety flag."""
    for key in ("PGVECTOR_URL", "PGVECTOR_TEST_URL", "RAG_INDEX_DB_URL", "RAG_INDEX_ALLOW_PRODUCTION_WRITE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAG_TARGET_ENV", "production")
    monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")

    with pytest.raises(ValueError, match="RAG_INDEX_ALLOW_PRODUCTION_WRITE"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://app:secret@kbo_medium",
            embedding_mode="configured",
            dry_run=False,
        )


def test_deterministic_build_requires_staging_environment(monkeypatch) -> None:
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.delenv("RAG_TARGET_ENV", raising=False)

    with pytest.raises(ValueError, match="deterministic embedding requires RAG_TARGET_ENV=staging"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://kbo_medium",
            embedding_mode="deterministic",
            dry_run=True,
        )
