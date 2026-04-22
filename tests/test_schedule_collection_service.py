from __future__ import annotations

import src.services.schedule_collection_service as service


def test_save_schedule_games_counts_saved_and_failed(monkeypatch):
    calls = []

    def _fake_save(game):
        calls.append(game["game_id"])
        return game["game_id"] != "fail"

    warnings = []
    monkeypatch.setattr(service, "save_schedule_game", _fake_save)

    result = service.save_schedule_games(
        [{"game_id": "ok-1"}, {"game_id": "fail"}, {"game_id": "ok-2"}],
        log=warnings.append,
    )

    assert calls == ["ok-1", "fail", "ok-2"]
    assert result.discovered == 3
    assert result.saved == 2
    assert result.failed == 1
    assert [game["game_id"] for game in result.saved_games] == ["ok-1", "ok-2"]
    assert [game["game_id"] for game in result.failed_games] == ["fail"]
    assert warnings == ["[WARN] Failed to save schedule game: fail"]
