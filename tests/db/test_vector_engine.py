"""Unit tests for src/db/vector_engine.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.db import vector_engine
from src.db.vector_engine import (
    VectorBase,
    _create_vector_engine,
    get_vector_session,
    init_vector_db,
    is_pgvector_available,
)


def test_create_vector_engine_without_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None when PGVECTOR_URL is empty."""
    monkeypatch.setattr(vector_engine, "PGVECTOR_URL", "")
    assert _create_vector_engine() is None


def test_create_vector_engine_with_invalid_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None and handle exception when engine creation fails."""
    monkeypatch.setattr(vector_engine, "PGVECTOR_URL", "invalid://url")
    with patch("src.db.vector_engine.create_engine", side_effect=SQLAlchemyError("Connection failed")):
        assert _create_vector_engine() is None


def test_create_vector_engine_survives_a_missing_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    """A driver that is not installed disables the store instead of killing the import.

    The URL outlives the dependency it names: this repository dropped psycopg2,
    but ``PGVECTOR_URL`` may still say ``postgresql+psycopg2://``. That makes
    ``create_engine`` raise ``ModuleNotFoundError`` out of ``import_dbapi``
    before any connection is attempted -- not a ``SQLAlchemyError``, so it used
    to escape the handler and take the whole module down. Every caller that
    guards with ``is_pgvector_available()`` never got the chance to ask, and the
    failure surfaced as an ``ImportError`` from an unrelated import line.
    """
    monkeypatch.setattr(vector_engine, "PGVECTOR_URL", "postgresql+psycopg2://u:p@h/db")
    with patch(
        "src.db.vector_engine.create_engine",
        side_effect=ModuleNotFoundError("No module named 'psycopg2'"),
    ):
        assert _create_vector_engine() is None


def test_is_pgvector_available_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return False when VectorEngine is None."""
    monkeypatch.setattr(vector_engine, "VectorEngine", None)
    assert is_pgvector_available() is False


def test_is_pgvector_available_when_connection_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return False when database connection check fails."""
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = SQLAlchemyError("DB error")
    monkeypatch.setattr(vector_engine, "VectorEngine", mock_engine)
    assert is_pgvector_available() is False


def test_is_pgvector_available_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return True when database connection succeeds."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    monkeypatch.setattr(vector_engine, "VectorEngine", mock_engine)
    assert is_pgvector_available() is True


def test_get_vector_session_raises_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise RuntimeError when VectorSessionLocal is None."""
    monkeypatch.setattr(vector_engine, "VectorSessionLocal", None)
    with pytest.raises(RuntimeError, match="pgvector DB를 사용할 수 없습니다"):
        with get_vector_session():
            pass


def test_get_vector_session_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Yield session and commit on success."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    monkeypatch.setattr(vector_engine, "VectorSessionLocal", mock_session_class)

    with get_vector_session() as session:
        assert session == mock_session

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def test_get_vector_session_rollback_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rollback session and re-raise exception on error."""
    mock_session_class = MagicMock()
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session
    monkeypatch.setattr(vector_engine, "VectorSessionLocal", mock_session_class)

    with pytest.raises(SQLAlchemyError), get_vector_session():
        raise SQLAlchemyError("Session query error")

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_init_vector_db_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Do nothing when VectorEngine is None."""
    monkeypatch.setattr(vector_engine, "VectorEngine", None)
    init_vector_db()  # Should complete without error


def test_init_vector_db_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Create extension and tables when VectorEngine is valid."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    monkeypatch.setattr(vector_engine, "VectorEngine", mock_engine)

    with patch.object(VectorBase.metadata, "create_all") as mock_create_all:
        init_vector_db()
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_create_all.assert_called_once_with(bind=mock_engine)


class _UrlOnlyEngine:
    """Minimal engine double: only ``url`` is read by the identity comparison."""

    def __init__(self, url: str) -> None:
        from sqlalchemy.engine import make_url

        self.url = make_url(url)


def _colocated(vector_url: str, primary_url: str) -> bool:
    import src.db.engine as engine_module

    with (
        patch.object(vector_engine, "VectorEngine", _UrlOnlyEngine(vector_url)),
        patch.object(engine_module, "Engine", _UrlOnlyEngine(primary_url)),
    ):
        return vector_engine.shares_operational_database()


def test_no_vector_engine_never_shares_the_operational_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """An absent vector store cannot be colocated with anything."""
    monkeypatch.setattr(vector_engine, "VectorEngine", None)
    assert vector_engine.shares_operational_database() is False


def test_the_same_host_port_and_database_is_one_store() -> None:
    """Both indexes live in one ``rag_chunks`` table, so one scan must serve the audit."""
    assert (
        _colocated("postgresql://u:p@100.81.73.13:5432/bega_prod", "postgresql://other:pw@100.81.73.13:5432/bega_prod")
        is True
    )


def test_driver_differences_do_not_split_one_store() -> None:
    """psycopg2 vs psycopg is a client choice, not a different database."""
    assert _colocated("postgresql+psycopg2://u:p@h:5432/bega_prod", "postgresql://u:p@h:5432/bega_prod") is True


def test_a_different_database_is_not_the_same_store() -> None:
    """``bega_prod`` and ``bega_dev`` on one host are still two stores."""
    assert _colocated("postgresql://u:p@h:5432/bega_dev", "postgresql://u:p@h:5432/bega_prod") is False


def test_a_different_port_is_not_the_same_store() -> None:
    """The retired 5435 snapshot is a different store even with the same database name."""
    assert _colocated("postgresql://u:p@h:5435/bega_prod", "postgresql://u:p@h:5432/bega_prod") is False
