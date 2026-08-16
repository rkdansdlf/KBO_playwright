"""Tests for the RAG evaluation corpus bootstrap CLI."""

from __future__ import annotations

from src.cli.bootstrap_rag_eval_corpus import main
from src.cli import evaluate_rag_retrieval


def test_bootstrap_defaults_to_validation_only(capsys) -> None:
    """Validate the corpus without requiring a database or writing rows."""
    assert main(["--json"]) == 0

    output = capsys.readouterr().out
    assert '"document_count": 12' in output
    assert '"query_count": 100' in output
    assert '"valid": true' in output


def test_bootstrap_apply_requires_explicit_write_guard(monkeypatch, capsys) -> None:
    """Prevent accidental writes when a test database opt-in is absent."""
    monkeypatch.delenv("RAG_EVAL_ALLOW_WRITE", raising=False)

    assert main(["--apply", "--json"]) == 2

    assert "RAG_EVAL_ALLOW_WRITE=1" in capsys.readouterr().out


def test_retrieval_evaluation_can_require_indexed_golden_ids(monkeypatch, capsys) -> None:
    """Treat missing corpus references as an infrastructure/data setup failure."""
    monkeypatch.setattr(evaluate_rag_retrieval, "_missing_corpus_ids", lambda _queries, require_vector: ["missing:1"])

    assert (
        evaluate_rag_retrieval.main(["--dataset", "tests/fixtures/rag_corpus/golden_queries.json", "--require-corpus"])
        == 2
    )
    assert "golden IDs missing" in capsys.readouterr().err
