from __future__ import annotations

from datetime import date, datetime

import pytest

from src.services.postgame_reconciliation_service import (
    GameScoreStatusSnapshot,
    PostgameReconciliationChange,
    _display,
    _format_game_date,
    _normalize_range,
    _parse_yyyymmdd,
    _score,
    format_reconciliation_report,
    write_reconciliation_csv,
)


class TestParseYyyymmdd:
    def test_parses_correctly(self):
        assert _parse_yyyymmdd("20240315") == date(2024, 3, 15)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_yyyymmdd("notadate")


class TestNormalizeRange:
    def test_already_ordered(self):
        s, e = _normalize_range("20240301", "20240315")
        assert s == "20240301"
        assert e == "20240315"

    def test_reversed_order(self):
        s, e = _normalize_range("20240315", "20240301")
        assert s == "20240301"
        assert e == "20240315"

    def test_same_date(self):
        s, e = _normalize_range("20240301", "20240301")
        assert s == e


class TestDisplay:
    def test_none_returns_dash(self):
        assert _display(None) == "-"

    def test_value_returns_str(self):
        assert _display(3) == "3"
        assert _display("hello") == "hello"


class TestScore:
    def test_both_none(self):
        assert _score(None, None) == "---"

    def test_with_values(self):
        assert _score(3, 2) == "3-2"

    def test_one_none(self):
        assert _score(3, None) == "3--"


class TestFormatGameDate:
    def test_datetime_object(self):
        d = datetime(2024, 3, 15, 12, 0)
        assert _format_game_date(d, fallback_game_id="x") == "20240315"

    def test_date_object(self):
        d = date(2024, 3, 15)
        assert _format_game_date(d, fallback_game_id="x") == "20240315"

    def test_yyyymmdd_string(self):
        assert _format_game_date("20240315", fallback_game_id="x") == "20240315"

    def test_none_falls_back(self):
        assert _format_game_date(None, fallback_game_id="20240315LG0") == "20240315"


class TestGameScoreStatusSnapshot:
    def test_score_tuple_property(self):
        snap = GameScoreStatusSnapshot(
            game_id="G1",
            game_date="20240315",
            game_status="completed",
            away_score=3,
            home_score=5,
        )
        assert snap.score_tuple == (3, 5)

    def test_score_tuple_none(self):
        snap = GameScoreStatusSnapshot(
            game_id="G1",
            game_date="20240315",
            game_status="scheduled",
            away_score=None,
            home_score=None,
        )
        assert snap.score_tuple == (None, None)


class TestPostgameReconciliationChange:
    def test_status_changed_true(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="started",
            after_status="completed",
            before_away_score=None,
            before_home_score=None,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.status_changed is True

    def test_status_changed_false(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="completed",
            after_status="completed",
            before_away_score=3,
            before_home_score=5,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.status_changed is False

    def test_score_changed_true(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="started",
            after_status="completed",
            before_away_score=None,
            before_home_score=None,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.score_changed is True


class TestFormatReconciliationReport:
    def test_no_changes(self):
        result = format_reconciliation_report([])
        assert "No status or score changes" in result

    def test_with_changes(self):
        changes = [
            PostgameReconciliationChange(
                game_id="G1",
                game_date="20240315",
                before_status="started",
                after_status="completed",
                before_away_score=None,
                before_home_score=None,
                after_away_score=3,
                after_home_score=5,
                detail_status="saved",
            )
        ]
        result = format_reconciliation_report(changes)
        assert "G1" in result
        assert "started -> completed" in result
        assert "-- -> 3-5" in result


class TestWriteReconciliationCsv:
    def test_writes_file(self, tmp_path):
        changes = [
            PostgameReconciliationChange(
                game_id="G1",
                game_date="20240315",
                before_status="started",
                after_status="completed",
                before_away_score=None,
                before_home_score=None,
                after_away_score=3,
                after_home_score=5,
                detail_status="saved",
            )
        ]
        out_path = tmp_path / "report.csv"
        result = write_reconciliation_csv(changes, out_path)
        assert result.exists()
        content = result.read_text(encoding="utf-8")
        assert "G1" in content
        assert "game_id" in content
