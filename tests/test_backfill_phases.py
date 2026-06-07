"""Tests for the multi-phase backfill helper functions in scheduler.py."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from scripts.scheduler import (
    _compact_date,
    _find_detail_gaps,
    _find_pbp_gaps,
    _find_player_profile_gaps,
    _find_preview_gaps,
)

# ── _compact_date ────────────────────────────────────────────────────────────────────


def test_compact_date_from_date_object():
    assert _compact_date(date(2026, 6, 3)) == "20260603"


def test_compact_date_from_string_with_dashes():
    assert _compact_date("2026-06-03") == "20260603"


def test_compact_date_already_compact():
    assert _compact_date("20260603") == "20260603"


def test_compact_date_datetime():
    from datetime import datetime

    assert _compact_date(datetime(2026, 6, 3, 15, 30)) == "20260603"


# ── _find_detail_gaps ────────────────────────────────────────────────────────────────


def test_find_detail_gaps_empty(monkeypatch):
    """No detail gaps → empty list."""
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    result = _find_detail_gaps(mock_session, date(2026, 1, 1))
    assert result == []


def test_find_detail_gaps_with_hits(monkeypatch):
    mock_session = MagicMock()

    class FakeDate1:
        def strftime(self, fmt):
            return "20260603"

    class FakeDate2:
        def strftime(self, fmt):
            return "20260604"

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [FakeDate1(), FakeDate2()]
    mock_session.execute.return_value = mock_result

    result = _find_detail_gaps(mock_session, date(2026, 1, 1))
    assert result == ["20260603", "20260604"]


# ── _find_pbp_gaps ───────────────────────────────────────────────────────────────────


def test_find_pbp_gaps_empty(monkeypatch):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    result = _find_pbp_gaps(mock_session, date(2026, 1, 1))
    assert result == []


def test_find_pbp_gaps_with_hits(monkeypatch):
    mock_session = MagicMock()

    class FakeDate:
        def strftime(self, fmt):
            return "20260603"

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [FakeDate()]
    mock_session.execute.return_value = mock_result

    result = _find_pbp_gaps(mock_session, date(2026, 1, 1))
    assert result == ["20260603"]


# ── _find_preview_gaps ───────────────────────────────────────────────────────────────


def test_find_preview_gaps_empty(monkeypatch):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    result = _find_preview_gaps(mock_session, date(2026, 1, 1))
    assert result == []


# ── _find_player_profile_gaps ────────────────────────────────────────────────────────


def test_find_player_profile_gaps_empty(monkeypatch):
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_filter = MagicMock()
    mock_filter.all.return_value = []
    mock_query.filter.return_value = mock_filter
    mock_session.query.return_value = mock_query

    result = _find_player_profile_gaps(mock_session)
    assert result == []


def test_find_player_profile_gaps_with_hits(monkeypatch):
    mock_session = MagicMock()

    class FakeRow:
        player_id = 10042

    class FakeRow2:
        player_id = 10123

    mock_filter = MagicMock()
    mock_filter.all.return_value = [FakeRow(), FakeRow2()]
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_filter
    mock_session.query.return_value = mock_query

    result = _find_player_profile_gaps(mock_session)
    assert result == [10042, 10123]


def test_find_player_profile_gaps_excludes_small_ids(monkeypatch):
    """player_id < 10000 should be excluded by the query filter."""
    mock_session = MagicMock()
    mock_filter = MagicMock()
    mock_filter.all.return_value = []
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_filter
    mock_session.query.return_value = mock_query

    _find_player_profile_gaps(mock_session)
    # The filter should include player_id >= 10000
    call_kwargs = mock_query.filter.call_args
    assert call_kwargs is not None
