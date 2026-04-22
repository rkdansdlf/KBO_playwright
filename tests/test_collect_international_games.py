from __future__ import annotations

from datetime import date

from scripts.crawling import collect_international_games
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED


def _game(status: str = "End") -> dict:
    return {
        "game_id": "20241113AUJP0",
        "season_id": 202490,
        "game_date": date(2024, 11, 13),
        "game_time": "19:30",
        "home_team": "JP",
        "away_team": "AU",
        "stadium": "Tokyo Dome",
        "status": status,
        "away_score": 3,
        "home_score": 9,
        "series_id": 90,
    }


def test_international_snapshot_payload_marks_completed_scores():
    payload, status = collect_international_games._to_snapshot_payload(_game("End"))

    assert status == GAME_STATUS_COMPLETED
    assert payload["game_id"] == "20241113AUJP0"
    assert payload["season_id"] == 202490
    assert payload["metadata"]["source"] == "international_schedule"
    assert payload["teams"]["away"]["code"] == "AU"
    assert payload["teams"]["away"]["score"] == 3
    assert payload["teams"]["home"]["score"] == 9


def test_international_snapshot_payload_omits_scheduled_scores():
    payload, status = collect_international_games._to_snapshot_payload(_game("Scheduled"))

    assert status == GAME_STATUS_SCHEDULED
    assert payload["teams"]["away"]["score"] is None
    assert payload["teams"]["home"]["score"] is None


def test_save_games_uses_shared_snapshot_persistence(monkeypatch):
    calls = []

    def _fake_save_game_snapshot(payload, *, status):
        calls.append((payload, status))
        return True

    monkeypatch.setattr(collect_international_games, "save_game_snapshot", _fake_save_game_snapshot)

    saved = collect_international_games.save_games([_game("End"), _game("Scheduled")])

    assert saved == 2
    assert [status for _, status in calls] == [GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED]
    assert calls[0][0]["teams"]["home"]["score"] == 9
    assert calls[1][0]["teams"]["home"]["score"] is None
