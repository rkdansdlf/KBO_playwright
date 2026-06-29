from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

import pytest

from src.services.relay_recovery_service import (
    GameStateInput,
    RelayRecoveryTarget,
    RelaySaveCounts,
    _classify_relay_failure,
    _coerce_int,
    _dedupe,
    _join_notes,
    _last_event_score,
    _manifest_base_dir,
    _should_mark_source_unavailable,
    load_game_ids_from_file,
    parse_source_order,
)


class TestParseSourceOrder:
    def test_none_returns_none(self):
        assert parse_source_order(None) is None

    def test_empty_string_returns_none(self):
        assert parse_source_order("") is None

    def test_parses_comma_separated(self):
        assert parse_source_order("naver, kbo, import") == ["naver", "kbo", "import"]

    def test_single_value(self):
        assert parse_source_order("naver") == ["naver"]

    def test_strips_whitespace(self):
        assert parse_source_order("  naver , kbo  ") == ["naver", "kbo"]


class TestLoadGameIdsFromFile:
    def test_none_path_returns_empty(self):
        assert load_game_ids_from_file(None) == []

    def test_nonexistent_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_game_ids_from_file("/nonexistent/file.txt")

    def test_reads_game_ids_ignoring_comments(self):
        with NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("# comment\n20240315LGSS0\n20240315SS0\n")
            path = f.name
        try:
            result = load_game_ids_from_file(path)
            assert result == ["20240315LGSS0", "20240315SS0"]
        finally:
            Path(path).unlink(missing_ok=True)

    def test_deduplicates(self):
        with NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("G1\nG1\nG2\n")
            path = f.name
        try:
            result = load_game_ids_from_file(path)
            assert result == ["G1", "G2"]
        finally:
            Path(path).unlink(missing_ok=True)

    def test_skips_header_line(self):
        with NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("game_id\nG1\nG2\n")
            path = f.name
        try:
            result = load_game_ids_from_file(path)
            assert "game_id" not in result
        finally:
            Path(path).unlink(missing_ok=True)


class TestDedupe:
    def test_empty(self):
        assert _dedupe([]) == []

    def test_deduplicates(self):
        assert _dedupe(["a", "b", "a", "c"]) == ["a", "b", "c"]

    def test_skips_empty(self):
        assert _dedupe(["a", "", "b"]) == ["a", "b"]


class TestClassifyRelayFailure:
    def test_match_failed(self):
        assert _classify_relay_failure("invalid_relay_match") == "relay_match_failed"
        assert _classify_relay_failure("match error") == "relay_match_failed"

    def test_api_failed(self):
        assert _classify_relay_failure("relay_api_error") == "relay_api_failed"
        assert _classify_relay_failure("timeout") == "relay_api_failed"

    def test_empty(self):
        assert _classify_relay_failure("other error") == "relay_empty"
        assert _classify_relay_failure(None) == "relay_empty"


class TestShouldMarkSourceUnavailable:
    def test_not_legacy_bucket_returns_false(self):
        assert _should_mark_source_unavailable("2024_regular", [], MagicMock()) is False

    def test_modern_year_returns_false(self):
        assert _should_mark_source_unavailable("2020_legacy", [], MagicMock()) is False

    def test_old_legacy_without_transient_tokens(self):
        mock_result = MagicMock()
        mock_result.notes = "no data found"
        assert _should_mark_source_unavailable("2009_legacy", [], mock_result) is True

    def test_old_legacy_with_transient_token(self):
        mock_result = MagicMock()
        mock_result.notes = "timeout error"
        assert _should_mark_source_unavailable("2009_legacy", [], mock_result) is False


class TestLastEventScore:
    def test_returns_last_score(self):
        events = [
            {"away_score": 0, "home_score": 0},
            {"away_score": 1, "home_score": 2},
            {"away_score": 3, "home_score": 4},
        ]
        assert _last_event_score(events) == (3, 4)

    def test_none_if_no_scores(self):
        assert _last_event_score([{"no_score": True}]) is None

    def test_skips_none_scores(self):
        events = [
            {"away_score": None, "home_score": None},
            {"away_score": 1, "home_score": 2},
        ]
        assert _last_event_score(events) == (1, 2)


class TestCoerceInt:
    def test_none_returns_none(self):
        assert _coerce_int(None) is None

    def test_empty_string_returns_none(self):
        assert _coerce_int("") is None

    def test_valid_int(self):
        assert _coerce_int(42) == 42

    def test_valid_string(self):
        assert _coerce_int("42") == 42

    def test_invalid_returns_none(self):
        assert _coerce_int("abc") is None


class TestJoinNotes:
    def test_all_none(self):
        assert _join_notes(None, None) is None

    def test_some_none(self):
        assert _join_notes("a", None) == "a"

    def test_multiple_notes(self):
        assert _join_notes("a", "b") == "a;b"

    def test_empty_strings(self):
        assert _join_notes("", "a") == "a"


class TestManifestBaseDir:
    def test_from_path(self):
        p = Path("/some/dir/file.csv")
        assert _manifest_base_dir(p) == Path("/some/dir")

    def test_from_string(self):
        result = _manifest_base_dir("/some/dir/file.csv")
        assert str(result).endswith("/some/dir")

    def test_empty_string_fallback(self):
        result = _manifest_base_dir("")
        assert result is not None


class TestRelayRecoveryTarget:
    def test_from_game_state_with_events(self):
        target = RelayRecoveryTarget.from_game_state(
            state=GameStateInput(
                game_id="20240315LGSS0",
                league_type_name="Regular",
                bucket_id=None,
                has_events=True,
                has_event_state=True,
                has_pbp=False,
            ),
        )
        assert target.game_id == "20240315LGSS0"
        assert target.needs_event_recovery is False
        assert target.needs_pbp_recovery is True

    def test_from_game_state_fully_recovered(self):
        target = RelayRecoveryTarget.from_game_state(
            state=GameStateInput(
                game_id="20240315LGSS0",
                league_type_name="Regular",
                bucket_id="2024_regular",
                has_events=True,
                has_event_state=True,
                has_pbp=True,
            ),
        )
        assert target.needs_event_recovery is False
        assert target.needs_pbp_recovery is False


class TestRelaySaveCounts:
    def test_defaults(self):
        counts = RelaySaveCounts(saved_rows=0)
        assert counts.saved_rows == 0
        assert counts.saved_event_rows == 0
        assert counts.saved_pbp_rows == 0

    def test_with_rows(self):
        counts = RelaySaveCounts(saved_rows=10, saved_event_rows=5, saved_pbp_rows=5)
        assert counts.saved_rows == 10
        assert counts.saved_event_rows == 5
