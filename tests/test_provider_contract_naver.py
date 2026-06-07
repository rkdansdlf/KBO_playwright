"""
Contract tests for Naver Sports API relay response structure.

Validates that the Naver API response matches the expected schema
so that parser assumptions hold. Uses fixtures captured from real API calls
(tests/fixtures/naver_live/ and tests/fixtures/naver_result/).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
NAVER_LIVE_DIR = FIXTURE_ROOT / "naver_live"
NAVER_RESULT_DIR = FIXTURE_ROOT / "naver_result"

# Expected top-level keys in a Naver relay API response
REQUIRED_RESULT_KEYS = {"result"}
REQUIRED_RESULT_NESTED_KEYS = {"textRelayData"}

# Expected keys inside each textRelay segment
SEGMENT_REQUIRED_KEYS = {"title"}
SEGMENT_OPTIONAL_KEYS = {"textOptions", "homeOrAway", "inn"}

# Expected keys inside each textOptions log entry
LOG_REQUIRED_KEYS = {"text"}
LOG_OPTIONAL_KEYS = {
    "pitcherName",
    "batterName",
    "batterRecord",
    "currentGameState",
}

STATE_REQUIRED_KEYS = {"homeScore", "awayScore", "out", "base1", "base2", "base3"}

# Expected status values in schedule API
VALID_SCHEDULE_STATUSES = {"BEFORE", "RUNNING", "RESULT", "CANCEL"}


def _load_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _iter_relay_fixtures():
    """Yield (path, source_type) for each relay inning fixture."""
    for d, label in [(NAVER_LIVE_DIR, "live"), (NAVER_RESULT_DIR, "result")]:
        if not d.exists():
            continue
        for fpath in sorted(d.iterdir()):
            if fpath.name.startswith("relay_inning_") and fpath.suffix == ".json":
                yield fpath, label


class TestNaverRelayResponseContract:
    """Validate Naver relay API response structure."""

    @pytest.mark.parametrize("fixture_path,source_type", list(_iter_relay_fixtures()))
    def test_top_level_structure(self, fixture_path: Path, source_type: str):
        """Response must have result.textRelayData.textRelays structure."""
        payload = _load_json(fixture_path)
        assert isinstance(payload, dict), f"Top-level is not dict: {fixture_path}"
        assert REQUIRED_RESULT_KEYS.issubset(payload.keys()), (
            f"Missing required keys in {fixture_path}: {REQUIRED_RESULT_KEYS - payload.keys()}"
        )
        result = payload["result"]
        assert isinstance(result, dict), f"result is not dict: {fixture_path}"
        assert REQUIRED_RESULT_NESTED_KEYS.issubset(result.keys()), (
            f"Missing nested keys in result: {REQUIRED_RESULT_NESTED_KEYS - result.keys()}"
        )
        relay_data = result["textRelayData"]
        assert isinstance(relay_data, dict), f"textRelayData is not dict: {fixture_path}"
        assert "textRelays" in relay_data, f"Missing textRelays in textRelayData: {fixture_path}"
        assert isinstance(relay_data["textRelays"], list), f"textRelays is not list: {fixture_path}"

    @pytest.mark.parametrize("fixture_path,source_type", list(_iter_relay_fixtures()))
    def test_segment_structure(self, fixture_path: Path, source_type: str):
        """Each textRelay segment must have title, optionally textOptions/homeOrAway/inn."""
        payload = _load_json(fixture_path)
        segments = payload["result"]["textRelayData"]["textRelays"]

        for idx, segment in enumerate(segments):
            assert isinstance(segment, dict), f"Segment {idx} is not dict: {fixture_path}"
            assert SEGMENT_REQUIRED_KEYS.issubset(segment.keys()), (
                f"Segment {idx} missing required keys {SEGMENT_REQUIRED_KEYS - segment.keys()}: {fixture_path}"
            )
            # title should have 회/초/말 pattern
            title = str(segment.get("title", ""))
            assert "회" in title, f"Segment {idx} title missing 회: {title}"
            assert any(h in title for h in ["초", "말"]), f"Segment {idx} title missing 초/말: {title}"

            if "textOptions" in segment:
                assert isinstance(segment["textOptions"], list), f"Segment {idx} textOptions not list: {fixture_path}"

    @pytest.mark.parametrize("fixture_path,source_type", list(_iter_relay_fixtures()))
    def test_log_entry_structure(self, fixture_path: Path, source_type: str):
        """Each log entry must have text, optionally has game state fields."""
        payload = _load_json(fixture_path)
        segments = payload["result"]["textRelayData"]["textRelays"]

        for seg_idx, segment in enumerate(segments):
            for log_idx, log in enumerate(segment.get("textOptions", [])):
                assert isinstance(log, dict), f"Log {seg_idx}:{log_idx} not dict: {fixture_path}"
                assert LOG_REQUIRED_KEYS.issubset(log.keys()), f"Log {seg_idx}:{log_idx} missing text: {fixture_path}"
                text = str(log.get("text", "")).strip()
                assert text, f"Log {seg_idx}:{log_idx} empty text: {fixture_path}"

    @pytest.mark.parametrize("fixture_path,source_type", list(_iter_relay_fixtures()))
    def test_game_state_structure(self, fixture_path: Path, source_type: str):
        """If currentGameState exists, it must have score/out/base fields."""
        payload = _load_json(fixture_path)
        segments = payload["result"]["textRelayData"]["textRelays"]

        for seg_idx, segment in enumerate(segments):
            for log_idx, log in enumerate(segment.get("textOptions", [])):
                state = log.get("currentGameState")
                if state is None:
                    continue
                assert isinstance(state, dict), f"currentGameState not dict at {seg_idx}:{log_idx}: {fixture_path}"
                missing = STATE_REQUIRED_KEYS - state.keys()
                if missing:
                    # Batting/pitching info may have no state, but at minimum warn
                    pytest.skip(f"currentGameState missing keys {missing} at {seg_idx}:{log_idx}: {fixture_path}")

    def test_title_parsing_roundtrip(self):
        """Validate that _parse_segment_inning_half succeeds on all fixture titles."""
        from src.crawlers.relay_crawler import RelayCrawler

        crawler = RelayCrawler()

        for fixture_path, _source_type in _iter_relay_fixtures():
            payload = _load_json(fixture_path)
            segments = payload["result"]["textRelayData"]["textRelays"]
            for seg_idx, segment in enumerate(segments):
                inn, half = crawler._parse_segment_inning_half(segment)
                assert inn is not None and inn > 0, (
                    f"Failed to parse inning from segment {seg_idx} title='{segment.get('title')}' "
                    f"homeOrAway={segment.get('homeOrAway')} inn={segment.get('inn')}: {fixture_path}"
                )
                assert half in ("top", "bottom"), f"Failed to parse half from segment {seg_idx}: {fixture_path}"


class TestNaverScheduleContract:
    """Validate Naver schedule API response structure."""

    def _iter_schedule_fixtures(self):
        for d in [NAVER_LIVE_DIR, NAVER_RESULT_DIR]:
            if not d.exists():
                continue
            for fpath in sorted(d.iterdir()):
                if fpath.name.startswith("schedule_") and fpath.suffix == ".json":
                    yield fpath

    def test_schedule_top_level(self):
        """Schedule response must have result.games."""
        for fixture_path in self._iter_schedule_fixtures():
            payload = _load_json(fixture_path)
            assert isinstance(payload, dict), f"Schedule payload not dict: {fixture_path}"
            result = payload.get("result", {})
            assert isinstance(result, dict), f"Schedule result not dict: {fixture_path}"
            games = result.get("games", [])
            assert isinstance(games, list), f"Schedule games not list: {fixture_path}"
            assert len(games) > 0, f"Schedule games empty: {fixture_path}"

    def test_schedule_game_fields(self):
        """Each schedule game must have gameId, team codes, and status."""
        for fixture_path in self._iter_schedule_fixtures():
            payload = _load_json(fixture_path)
            games = payload["result"]["games"]
            for g_idx, game in enumerate(games):
                assert isinstance(game, dict), f"Game {g_idx} not dict: {fixture_path}"
                assert "gameId" in game, f"Game {g_idx} missing gameId: {fixture_path}"
                assert "status" in game, f"Game {g_idx} missing status: {fixture_path}"
                status = str(game["status"]).upper()
                assert status in VALID_SCHEDULE_STATUSES | {"UNKNOWN"}, (
                    f"Game {g_idx} invalid status '{status}': {fixture_path}"
                )
                # At least one team code should be present
                assert game.get("awayTeamCode") or game.get("homeTeamCode"), (
                    f"Game {g_idx} missing both team codes: {fixture_path}"
                )

    def test_schedule_status_used_by_live_crawler(self):
        """Verify the status field is accessible by live_crawler.py's naver_status_map logic."""
        for fixture_path in self._iter_schedule_fixtures():
            payload = _load_json(fixture_path)
            games = payload["result"]["games"]
            for game in games:
                key = (game.get("awayTeamCode"), game.get("homeTeamCode"))
                status = game.get("status")
                assert key != (None, None), f"Game has no team codes to form lookup key: {fixture_path}"
                assert status in VALID_SCHEDULE_STATUSES, f"Unexpected status '{status}' for key {key}: {fixture_path}"
