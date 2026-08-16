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
        patch("src.cli.audit_rag_index.get_db_session") as primary,
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
        patch("src.cli.audit_rag_index.get_db_session") as primary,
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
        patch("src.cli.audit_rag_index.get_db_session") as primary,
        patch("src.cli.audit_rag_index.get_vector_session") as vector,
        patch("src.cli.audit_rag_index.audit_index_sessions", return_value=report),
    ):
        primary.return_value.__enter__.return_value = MagicMock()
        vector.return_value.__enter__.return_value = MagicMock()
        assert main(["--require-nonempty", "--json"]) == 1
    assert "RAG index is empty" in capsys.readouterr().out
