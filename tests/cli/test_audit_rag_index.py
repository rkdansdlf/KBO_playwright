"""Tests for the read-only RAG index audit CLI contract."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.audit_rag_index import main
from src.services.rag_index_consistency import IndexConsistencyFinding, IndexConsistencyReport


def test_audit_returns_infrastructure_code_when_pgvector_is_unavailable(capsys) -> None:
    """Return exit code 2 when the vector dependency cannot be reached."""
    with patch("src.cli.audit_rag_index.is_pgvector_available", return_value=False):
        assert main(["--json"]) == 2
    assert "pgvector is unavailable" in capsys.readouterr().out


def test_audit_returns_zero_for_consistent_indexes(capsys) -> None:
    """Return exit code 0 for an empty but successfully queried pair of indexes."""
    report = IndexConsistencyReport(0, 0, (), total_keys=0)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--json"]) == 0
    assert '"consistent": true' in capsys.readouterr().out


def test_audit_returns_one_for_findings(capsys) -> None:
    """Return exit code 1 when both indexes are reachable but inconsistent."""
    report = IndexConsistencyReport(1, 0, (IndexConsistencyFinding("game:g1", "MISSING_IN_VECTOR"),), total_keys=1)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--json"]) == 1
    assert '"consistent": false' in capsys.readouterr().out


def test_audit_require_nonempty_rejects_empty_indexes(capsys) -> None:
    """Require an actual corpus for the production acceptance gate."""
    report = IndexConsistencyReport(0, 0, (), total_keys=0)
    with (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--require-nonempty", "--json"]) == 1
    assert "RAG index is empty" in capsys.readouterr().out


def _oracle_audit_context(report):
    return (
        patch("src.cli.audit_rag_index.is_pgvector_available", return_value=True),
        patch("src.cli.audit_rag_index.is_oracle_vector_backend", return_value=True),
        patch("src.cli.audit_rag_index.get_rag_index_session"),
        patch("src.cli.audit_rag_index.audit_single_store_session", return_value=report),
    )


def test_audit_reports_postings_coverage_without_failing_by_default(capsys) -> None:
    """Report the sparse postings gap even when the default gate stays green."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=7):
            assert main(["--json"]) == 0
    out = capsys.readouterr().out
    assert '"postings_missing": 7' in out
    assert '"consistent": true' in out


def test_audit_require_postings_fails_on_coverage_gap(capsys) -> None:
    """Fail the audit when retrievable chunks lack sparse postings."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=7):
            assert main(["--require-postings", "--json"]) == 1
    out = capsys.readouterr().out
    assert "missing sparse postings" in out
    assert '"consistent": false' in out


def test_audit_require_postings_passes_when_fully_covered(capsys) -> None:
    """Pass the postings gate when every retrievable chunk is indexed."""
    report = IndexConsistencyReport(1, 1, (), total_keys=1)
    patches = _oracle_audit_context(report)
    with patches[0], patches[1], patches[2] as primary, patches[3]:
        primary.return_value.__enter__.return_value = MagicMock()
        with patch("src.cli.audit_rag_index._count_missing_postings", return_value=0):
            assert main(["--require-postings", "--json"]) == 0
    assert '"postings_missing": 0' in capsys.readouterr().out
