from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest

from src.cli.regenerate_game_stories import (
    _append_missing_game_rows,
    _build_story_report_row,
    _default_backup_path,
    _default_report_path,
    _game_batches,
    _mark_story_oci_status,
    _parse_date,
    _season_filters,
    _short_hash,
    _skipped_story_row,
)


class TestShortHash:
    def test_returns_hash_for_string(self) -> None:
        result = _short_hash("hello")
        assert len(result) == 12
        assert isinstance(result, str)

    def test_none_returns_empty(self) -> None:
        assert _short_hash(None) == ""

    def test_deterministic(self) -> None:
        assert _short_hash("test") == _short_hash("test")

    def test_different_inputs_different_hashes(self) -> None:
        assert _short_hash("abc") != _short_hash("xyz")


class TestParseDate:
    def test_valid_date_string(self) -> None:
        result = _parse_date("20260101")
        assert result == date(2026, 1, 1)

    def test_another_date(self) -> None:
        result = _parse_date("20251225")
        assert result == date(2025, 12, 25)


class TestDefaultReportPath:
    def test_returns_path_in_data_reports(self) -> None:
        result = _default_report_path()
        assert "data/reports" in str(result)
        assert result.suffix == ".csv"
        assert "game_story_regen_report" in str(result)


class TestDefaultBackupPath:
    def test_returns_path_in_data_recovery(self) -> None:
        result = _default_backup_path()
        assert "data/recovery" in str(result)
        assert result.suffix == ".csv"
        assert "game_story_regen_backup" in str(result)


class TestSeasonFilters:
    def test_single_season(self) -> None:
        filters = _season_filters([2026])
        assert len(filters) == 1

    def test_multiple_seasons(self) -> None:
        filters = _season_filters([2025, 2026])
        assert len(filters) == 2

    def test_empty(self) -> None:
        filters = _season_filters([])
        assert filters == []


class TestGameBatches:
    def test_empty_list(self) -> None:
        batches = list(_game_batches([]))
        assert batches == []

    def test_single_batch(self) -> None:
        games = list(range(10))
        batches = list(_game_batches(games, batch_size=100))
        assert len(batches) == 1
        assert len(batches[0]) == 10

    def test_multiple_batches(self) -> None:
        games = list(range(10))
        batches = list(_game_batches(games, batch_size=3))
        assert len(batches) == 4
        assert len(batches[0]) == 3
        assert len(batches[-1]) == 1


class TestAppendMissingGameRows:
    def test_no_missing(self) -> None:
        rows: list = []
        _append_missing_game_rows(rows, ["g1", "g2"], {"g1": object(), "g2": object()})
        assert len(rows) == 0

    def test_some_missing(self) -> None:
        rows: list = []
        _append_missing_game_rows(rows, ["g1", "g2", "g3"], {"g1": object()})
        assert len(rows) == 2
        assert all(r.game_id in {"g2", "g3"} for r in rows)
        assert all(r.status == "SKIPPED_GAME_NOT_FOUND" for r in rows)


class TestSkippedStoryRow:
    def test_with_date(self) -> None:
        @dataclass
        class MockGame:
            game_id: str
            game_date: date
            game_status: str

        game = MockGame(game_id="20260101LGSS0", game_date=date(2026, 1, 1), game_status="CANCELED")
        row = _skipped_story_row(game)
        assert row.game_id == "20260101LGSS0"
        assert row.game_date == "20260101"
        assert row.status == "SKIPPED_NOT_COMPLETED"
        assert "CANCELED" in row.message

    def test_without_date(self) -> None:
        @dataclass
        class MockGame:
            game_id: str
            game_date: date | None
            game_status: str

        game = MockGame(game_id="g1", game_date=None, game_status="SCHEDULED")
        row = _skipped_story_row(game)
        assert row.game_date == ""


class TestBuildStoryReportRow:
    def test_basic_row(self) -> None:
        @dataclass
        class MockGame:
            game_id: str
            game_date: date

        game = MockGame(game_id="20260101LGSS0", game_date=date(2026, 1, 1))
        story_data = {"timeline": [1, 2, 3]}
        row = _build_story_report_row(game, '{"old": true}', story_data, '{"new": true}')
        assert row.game_id == "20260101LGSS0"
        assert row.game_date == "20260101"
        assert row.changed is True
        assert row.timeline_events == 3

    def test_no_change(self) -> None:
        @dataclass
        class MockGame:
            game_id: str
            game_date: date

        game = MockGame(game_id="g1", game_date=date(2026, 1, 1))
        story_data = {"timeline": []}
        row = _build_story_report_row(game, '{"same": true}', story_data, '{"same": true}')
        assert row.changed is False
        assert row.timeline_events == 0

    def test_warnings(self) -> None:
        @dataclass
        class MockGame:
            game_id: str
            game_date: date

        game = MockGame(game_id="g1", game_date=date(2026, 1, 1))
        story_data = {"source": {"warnings": ["warn1", "warn2"]}, "timeline": []}
        row = _build_story_report_row(game, None, story_data, "{}")
        assert "warn1" in row.warnings
        assert "warn2" in row.warnings


class TestMarkStoryOciStatus:
    def test_not_apply_marks_skipped(self) -> None:
        rows: list = []
        for i in range(3):

            @dataclass
            class Row:
                status: str = ""
                oci_status: str = ""

            row = Row()
            rows.append(row)
        _mark_story_oci_status(rows, apply=False, oci_url="http://example.com")
        assert all(r.oci_status == "skipped_dry_run" for r in rows)

    def test_apply_no_oci_url(self) -> None:
        rows: list = []
        for status in ("APPLIED", "UNCHANGED", "FAILED", ""):

            @dataclass
            class Row:
                status: str
                oci_status: str = ""

            rows.append(Row(status=status))
        _mark_story_oci_status(rows, apply=True, oci_url=None)
        assert rows[0].oci_status == "skipped_missing_oci_url"
        assert rows[1].oci_status == "skipped_missing_oci_url"
        assert rows[2].oci_status == ""
        assert rows[3].oci_status == ""
