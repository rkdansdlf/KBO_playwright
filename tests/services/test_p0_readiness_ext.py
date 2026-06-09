from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services.p0_readiness import (
    _broadcast_missing_reason,
    _coerce_date,
    _coverage,
    _date_key,
    _has_text,
    _is_cancelled_or_postponed,
    _meta_has_stadium,
    _meta_has_start_time,
    _score_present,
    _status,
    format_p0_readiness_summary,
    normalize_yyyymmdd,
)


class TestNormalizeYyyymmdd:
    def test_from_date(self):
        assert normalize_yyyymmdd(date(2024, 3, 15)) == "20240315"

    def test_from_datetime(self):
        assert normalize_yyyymmdd(datetime(2024, 3, 15, 12, 0)) == "20240315"

    def test_from_string(self):
        assert normalize_yyyymmdd("20240315") == "20240315"

    def test_from_dashed_string(self):
        assert normalize_yyyymmdd("2024-03-15") == "20240315"

    def test_none_returns_today(self):
        with patch("src.services.p0_readiness.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20240601"
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = normalize_yyyymmdd(None)
            assert result == "20240601"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            normalize_yyyymmdd("invalid")


class TestHelpers:
    def test_has_text(self):
        assert _has_text("abc") is True
        assert _has_text("") is False
        assert _has_text(None) is False
        assert _has_text("  ") is False

    def test_status_uppercases(self):
        assert _status("completed") == "COMPLETED"

    def test_coerce_date_various(self):
        assert _coerce_date(date(2024, 3, 15)) == date(2024, 3, 15)
        assert _coerce_date(datetime(2024, 3, 15, 12, 0)) == date(2024, 3, 15)
        assert _coerce_date("2024-03-15") == date(2024, 3, 15)
        assert _coerce_date("20240315") == date(2024, 3, 15)
        assert _coerce_date("") is None
        assert _coerce_date(None) is None

    def test_date_key(self):
        assert _date_key(date(2024, 3, 15)) == "20240315"
        assert _date_key(datetime(2024, 3, 15, 12, 0)) == "20240315"
        assert _date_key("20240315") == "20240315"

    def test_coverage(self):
        assert _coverage(5, 10) == 50.0
        assert _coverage(0, 10) == 0.0
        assert _coverage(10, 0) == 100.0

    def test_score_present(self):
        class Game:
            home_score = 3
            away_score = 2
        assert _score_present(Game()) is True
        g2 = Game()
        g2.home_score = None
        assert _score_present(g2) is False

    def test_is_cancelled_or_postponed(self):
        class Game:
            game_status = "CANCELLED"
        assert _is_cancelled_or_postponed(Game()) is True
        g2 = Game()
        g2.game_status = "COMPLETED"
        assert _is_cancelled_or_postponed(g2) is False

    def test_meta_has_start_time(self):
        meta = MagicMock()
        meta.start_time = None
        assert _meta_has_start_time(meta) is False
        meta.start_time = datetime(2024, 3, 15, 14, 0)
        assert _meta_has_start_time(meta) is True
        assert _meta_has_start_time(None) is False

    def test_meta_has_stadium(self):
        class Game:
            stadium = "Jamsil"
        meta = MagicMock()
        meta.stadium_name = None
        meta.stadium_code = None
        assert _meta_has_stadium(Game(), meta) is True
        g2 = Game()
        g2.stadium = None
        assert _meta_has_stadium(g2, None) is False

    def test_broadcast_missing_reason_future(self):
        class Game:
            game_status = "SCHEDULED"
            game_date = date(2024, 6, 1)
        assert _broadcast_missing_reason(Game(), date(2024, 5, 30)) == "broadcast_not_announced"

    def test_broadcast_missing_reason_past(self):
        class Game:
            game_status = "COMPLETED"
            game_date = date(2024, 5, 1)
        assert _broadcast_missing_reason(Game(), date(2024, 6, 1)) == "broadcast_source_unavailable"


class TestFormatP0ReadinessSummary:
    def test_none_input(self):
        assert format_p0_readiness_summary(None) == "p0=unavailable"

    def test_empty_dict(self):
        result = format_p0_readiness_summary({})
        assert "p0_ok=False" in result

    def test_with_data(self):
        readiness = {
            "summary": {"ok": True, "failure_count": 0, "critical_failure_count": 0},
            "schedule": {"games": 3},
            "pregame": {"starters_complete": 2, "games": 3},
            "postgame": {"boxscore_detail_complete": 1, "games": 1},
            "relay": {"with_events_or_pbp": 1, "games": 1},
        }
        result = format_p0_readiness_summary(readiness)
        assert "p0_ok=True" in result
        assert "schedule_games=3" in result
