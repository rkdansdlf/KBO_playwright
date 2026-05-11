from __future__ import annotations

from datetime import date

import src.services.schedule_collection_service as service
from src.utils.schedule_validation import is_detail_candidate_game


def _schedule_game(game_id: str) -> dict:
    return {
        "game_id": game_id,
        "game_date": game_id[:8],
        "away_team_code": "LG",
        "home_team_code": "SS",
        "season_year": int(game_id[:4]),
        "season_type": "regular",
        "game_status": "SCHEDULED",
        "stadium": "잠실",
    }


def test_save_schedule_games_counts_saved_and_failed(monkeypatch):
    calls = []

    def _fake_save(game, **_kwargs):
        calls.append(game["game_id"])
        return game["game_id"] != "20250402LGSS0"

    warnings = []
    monkeypatch.setattr(service, "save_schedule_game", _fake_save)

    result = service.save_schedule_games(
        [
            _schedule_game("20250401LGSS0"),
            _schedule_game("20250402LGSS0"),
            _schedule_game("20250403LGSS0"),
        ],
        log=warnings.append,
    )

    assert calls == ["20250401LGSS0", "20250402LGSS0", "20250403LGSS0"]
    assert result.discovered == 3
    assert result.saved == 2
    assert result.failed == 1
    assert result.filtered == 0
    assert [game["game_id"] for game in result.saved_games] == ["20250401LGSS0", "20250403LGSS0"]
    assert [game["game_id"] for game in result.failed_games] == ["20250402LGSS0"]
    assert warnings == [
        "[WARN] Failed to save schedule game: 20250402LGSS0",
        "[WRITE-SUMMARY] run=schedule_collection games=0 field_updates=0 field_duplicates=0 "
        "dataset_replacements=0 dataset_duplicates=0",
    ]


def test_save_schedule_games_filters_invalid_payloads_before_save(monkeypatch):
    calls = []

    def _fake_save(game, **_kwargs):
        calls.append(game["game_id"])
        return True

    warnings = []
    monkeypatch.setattr(service, "save_schedule_game", _fake_save)

    invalid_game = _schedule_game("20250404LGSS0")
    invalid_game.pop("stadium")

    result = service.save_schedule_games(
        [_schedule_game("20250401LGSS0"), invalid_game],
        log=warnings.append,
    )

    assert calls == ["20250401LGSS0"]
    assert result.saved == 1
    assert result.failed == 1
    assert result.filtered == 1
    assert result.filtered_games[0]["failure_reason"] == "missing_stadium"
    assert warnings[0] == "[WARN] Filtered schedule game: 20250404LGSS0 reason=missing_stadium"


def test_is_detail_candidate_skips_future_and_cancelled_games():
    assert is_detail_candidate_game(
        _schedule_game("20250401LGSS0"),
        today=date(2025, 4, 2),
    )

    future_game = _schedule_game("20250403LGSS0")
    assert not is_detail_candidate_game(future_game, today=date(2025, 4, 2))

    cancelled_game = _schedule_game("20250401LGSS0")
    cancelled_game["game_status"] = "CANCELLED"
    assert not is_detail_candidate_game(cancelled_game, today=date(2025, 4, 2))
