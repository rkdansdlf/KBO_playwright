from __future__ import annotations

from src.cli.rebuild_relay_events import (
    _batter_from_description,
    _result_from_description,
    _dedupe_game_ids,
    RebuildReportRow,
)


class TestBatterFromDescription:
    def test_valid(self):
        assert _batter_from_description("김선수: 홈런") == "김선수"

    def test_no_colon(self):
        assert _batter_from_description("홈런") is None

    def test_empty(self):
        assert _batter_from_description("") is None

    def test_none(self):
        assert _batter_from_description(None) is None

    def test_whitespace_batter(self):
        assert _batter_from_description("  : 홈런") is None


class TestResultFromDescription:
    def test_valid(self):
        assert _result_from_description("김선수: 홈런") == "HR"

    def test_no_colon(self):
        assert _result_from_description("홈런") is None

    def test_empty(self):
        assert _result_from_description("") is None


class TestDedupeGameIds:
    def test_dedup(self):
        result = _dedupe_game_ids(["g1", "g2", "g1", "g3"])
        assert result == ["g1", "g2", "g3"]

    def test_empty(self):
        assert _dedupe_game_ids([]) == []

    def test_with_empty_strings(self):
        result = _dedupe_game_ids(["g1", "", "g2", ""])
        assert result == ["g1", "g2"]


class TestRebuildReportRow:
    def test_defaults(self):
        row = RebuildReportRow(
            game_id="20260412LGSS0",
            status="READY",
            old_rows=50,
            new_rows=45,
        )
        assert row.game_id == "20260412LGSS0"
        assert row.status == "READY"
        assert row.old_rows == 50
        assert row.new_rows == 45
        assert row.notes == ""
        assert row.backup_path == ""


class TestMain:
    def test_main_dry_run_empty(self):
        from unittest.mock import patch
        from src.cli.rebuild_relay_events import main

        with patch("src.cli.rebuild_relay_events.rebuild_relay_events") as mock_rebuild:
            exit_code = main(["--dry-run", "--season", "2026"])
            assert exit_code == 0
            assert mock_rebuild.call_count == 1
            options = mock_rebuild.call_args[0][0]
            assert options.apply is False
            assert options.seasons == [2026]
