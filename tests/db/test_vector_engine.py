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
