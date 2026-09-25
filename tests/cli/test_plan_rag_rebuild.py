"""Tests for the read-only RAG rebuild planner."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.cli.rag import build_rag_index
from src.cli.rag.plan_rag_rebuild import (
    PlannerSourceOptions,
    _plan_source,
    _summarize_decisions,
    _vector_context,
    main,
)
from src.services.rag_incremental_selection import RagCandidateDecision


def test_summarize_decisions_counts_candidates_and_blocks() -> None:
    decisions = iter(
        [
            RagCandidateDecision("game:1", True, "content_changed"),
            RagCandidateDecision("game:2", False, "healthy"),
            RagCandidateDecision("game:3", False, "terminal_status", blocked=True),
            RagCandidateDecision("game:4", False, "healthy", metadata_repair=True),
        ]
    )

    summary = _summarize_decisions(decisions, sample_limit=1)

    assert summary["generated_chunks"] == 4
    assert summary["candidate_count"] == 1
    assert summary["blocked_count"] == 1
    assert summary["metadata_gap_count"] == 1
    assert summary["candidate_keys"] == ["game:1"]


def test_plan_source_uses_sparse_only_state_when_vector_unknown(monkeypatch) -> None:
    monkeypatch.setattr(build_rag_index, "load_incremental_index_states", lambda _session, _table: {})
    monkeypatch.setattr(build_rag_index, "load_incremental_vector_states", lambda _session, _table: {})
    chunks = iter(
        [
            {"source_table": "player_basic", "source_row_id": "1", "title": "선수", "content": "본문"},
        ]
    )

    result = _plan_source(
        "players",
        chunks,
        MagicMock(),
        None,
        PlannerSourceOptions(require_vector=False, require_primary_embedding=False, sample_limit=20),
    )

    assert result["generated_chunks"] == 1
    assert result["candidate_count"] == 1
    assert result["candidate_keys"] == ["player_basic:1"]


def test_vector_context_unknown_does_not_require_backend() -> None:
    context, state, require_vector, require_primary = _vector_context(required=False)

    assert state == "UNKNOWN"
    assert require_vector is False
    assert require_primary is False
    with context as session:
        assert session is None


def test_vector_context_required_fails_closed_without_backend(monkeypatch) -> None:
    monkeypatch.setattr("src.db.vector_engine.is_oracle_vector_backend", lambda: False)
    monkeypatch.setattr("src.db.vector_engine.is_pgvector_available", lambda: False)

    with pytest.raises(RuntimeError, match="vector state required"):
        _vector_context(required=True)


def test_planner_cli_rejects_negative_sample(capsys) -> None:
    exit_code = main(["--sample", "-1"])

    assert exit_code == 2
    assert "sample" in capsys.readouterr().err
