from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.cli import build_rag_index
from src.models.rankings import StatRanking


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
        "vector_db": "postgresql://127.0.0.1:5432/rag_vector",
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


def test_deterministic_build_requires_staging_environment(monkeypatch) -> None:
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://127.0.0.1:5432/rag_vector")
    monkeypatch.delenv("RAG_TARGET_ENV", raising=False)

    with pytest.raises(ValueError, match="deterministic embedding requires RAG_TARGET_ENV=staging"):
        build_rag_index._resolve_build_targets(
            "oracle+oracledb://kbo_medium",
            embedding_mode="deterministic",
            dry_run=True,
        )
