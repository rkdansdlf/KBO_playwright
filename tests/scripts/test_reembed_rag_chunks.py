from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.maintenance.reembed_rag_chunks import reembed_chunks


def _make_session_with_rows(rows: list, extra_calls: list) -> MagicMock:
    session = MagicMock()
    session.scalars.return_value.all.side_effect = [rows, *extra_calls]
    return session


def test_dry_run_does_not_call_api_or_write() -> None:
    row = MagicMock()
    row.content = "hello"
    row.embedding = None
    session = _make_session_with_rows([row], [[row], []])

    mock_service = MagicMock()
    with (
        patch("scripts.maintenance.reembed_rag_chunks.SessionLocal") as session_local,
        patch("scripts.maintenance.reembed_rag_chunks.EmbeddingService", return_value=mock_service),
    ):
        session_local.return_value.__enter__.return_value = session
        report = reembed_chunks(apply=False, source=None, batch_size=50, limit=None)

    assert report.candidates == 1
    assert report.embedded == 1
    mock_service.get_embeddings_batch.assert_not_called()
    assert row.embedding is None


def test_apply_writes_embeddings() -> None:
    row = MagicMock()
    row.content = "hello"
    row.embedding = None
    session = _make_session_with_rows([row], [[row], []])

    mock_service = MagicMock()
    mock_service.get_embeddings_batch.return_value = [[0.1] * 256]
    with (
        patch("scripts.maintenance.reembed_rag_chunks.SessionLocal") as session_local,
        patch("scripts.maintenance.reembed_rag_chunks.EmbeddingService", return_value=mock_service),
    ):
        session_local.return_value.__enter__.return_value = session
        report = reembed_chunks(apply=True, source=None, batch_size=50, limit=None)

    assert report.embedded == 1
    assert row.embedding == [0.1] * 256
    mock_service.get_embeddings_batch.assert_called_once_with(["hello"])


def test_zero_vector_fallback_is_skipped() -> None:
    row = MagicMock()
    row.content = "hello"
    row.embedding = None
    session = _make_session_with_rows([row], [[row], []])

    mock_service = MagicMock()
    mock_service.get_embeddings_batch.return_value = [[0.0] * 256]
    with (
        patch("scripts.maintenance.reembed_rag_chunks.SessionLocal") as session_local,
        patch("scripts.maintenance.reembed_rag_chunks.EmbeddingService", return_value=mock_service),
    ):
        session_local.return_value.__enter__.return_value = session
        report = reembed_chunks(apply=True, source=None, batch_size=50, limit=None)

    assert report.embedded == 0
    assert report.skipped_zero == 1
    assert row.embedding is None


def test_limit_stops_at_batch_granularity() -> None:
    rows = [MagicMock() for _ in range(5)]
    session = _make_session_with_rows(rows, [rows, rows])

    mock_service = MagicMock()
    mock_service.get_embeddings_batch.return_value = [[0.1] * 256] * 2
    with (
        patch("scripts.maintenance.reembed_rag_chunks.SessionLocal") as session_local,
        patch("scripts.maintenance.reembed_rag_chunks.EmbeddingService", return_value=mock_service),
    ):
        session_local.return_value.__enter__.return_value = session
        report = reembed_chunks(apply=True, source=None, batch_size=2, limit=3)

    assert report.embedded == 4
    assert mock_service.get_embeddings_batch.call_count == 2
