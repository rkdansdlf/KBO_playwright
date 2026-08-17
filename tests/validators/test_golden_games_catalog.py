"""Tests for Golden Game Catalog Integrity and Coverage."""

from __future__ import annotations

import json
from pathlib import Path

CATALOG_PATH = Path(__file__).parent.parent / "fixtures" / "golden_games" / "golden_game_catalog.json"


def test_golden_games_catalog_structure() -> None:
    assert CATALOG_PATH.exists()
    data = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    assert "games" in data
    assert len(data["games"]) >= 8

    categories = {g["category"] for g in data["games"]}
    assert "standard_9inn" in categories
    assert "extra_innings_tie" in categories
    assert "shortened_rain_cold" in categories
    assert "doubleheader_1" in categories
    assert "suspended_postseason" in categories
    assert "mass_substitutions" in categories

    for game in data["games"]:
        assert "game_id" in game
        assert "season" in game
        assert "final_score" in game
        assert len(game["final_score"]) == 2
