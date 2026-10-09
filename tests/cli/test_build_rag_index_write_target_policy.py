"""Tests for which write targets a RAG build will accept.

The vector store moved from Oracle to a pgvector database on the Windows host.
The guard that decides this predates that move and only ever admitted
``staging`` for a non-Oracle target -- so a production pgvector build was
refused by a rule written when "production" meant "Oracle". These pin the new
shape without weakening the part that matters: a production write still needs
its own explicit opt-in, for pgvector exactly as for Oracle.
"""

from __future__ import annotations

import pytest
from src.cli import build_rag_index

SOURCE = "postgresql://reader@db.internal/kbo"
SPARSE = "postgresql://indexer@db.internal/bega_prod"
VECTOR = "postgresql://writer@db.internal/kbo_rag"
ORACLE_TARGET = "oracle+oracledb://ADMIN@etyqpnpj0l1ep777_medium"

GUARD_VARS = ("RAG_INDEX_ALLOW_WRITE", "RAG_INDEX_ALLOW_PRODUCTION_WRITE", "PGVECTOR_URL", "PGVECTOR_TEST_URL")


@pytest.fixture(autouse=True)
def _clear_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test from "no guard is set" so each one states its own."""
    for name in GUARD_VARS:
        monkeypatch.delenv(name, raising=False)


def _errors(target_url: str, target_environment: str, *, sparse: str | None = SPARSE) -> list[str]:
    """Run the write-target guard for a non-dry-run build."""
    return build_rag_index._write_target_errors(SOURCE, target_url, sparse, VECTOR, target_environment)


class TestPgvectorProductionIsAllowed:
    """Pin that the vector store's new home can be written in production."""

    def test_both_guards_allow_a_production_build(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Accept production for a PostgreSQL target once both opt-ins are set."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
        assert _errors(VECTOR, "production") == []

    def test_production_still_needs_its_own_opt_in(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Refuse production with only the generic write guard set."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        errors = _errors(VECTOR, "production")
        assert any("RAG_INDEX_ALLOW_PRODUCTION_WRITE" in error for error in errors)

    def test_staging_needs_only_the_write_guard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep staging as cheap to enter as it was."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        assert _errors(VECTOR, "staging") == []


class TestOtherRefusalsAreUnchanged:
    """Pin the rules that were not the problem and must not be relaxed."""

    def test_an_unknown_environment_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Refuse anything that is not staging or production."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
        errors = _errors(VECTOR, "local")
        assert any("RAG_TARGET_ENV" in error for error in errors)

    def test_a_missing_sparse_target_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Refuse a build that would write somewhere it did not name."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        errors = _errors(VECTOR, "staging", sparse=None)
        assert any("RAG_INDEX_DB_URL" in error for error in errors)

    def test_oracle_production_still_needs_its_opt_in(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep the Oracle path's own production guard intact."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        errors = build_rag_index._write_target_errors(SOURCE, ORACLE_TARGET, None, None, "production")
        assert any("RAG_INDEX_ALLOW_PRODUCTION_WRITE" in error for error in errors)

    def test_oracle_production_passes_with_both_guards(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Accept the Oracle shape the old rule was written for."""
        monkeypatch.setenv("RAG_INDEX_ALLOW_WRITE", "1")
        monkeypatch.setenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE", "1")
        assert build_rag_index._write_target_errors(SOURCE, ORACLE_TARGET, None, None, "production") == []
