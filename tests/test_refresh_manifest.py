from __future__ import annotations

import json

from src.utils.refresh_manifest import write_refresh_manifest


def test_write_refresh_manifest_adds_optional_stability_without_dropping_existing_fields(tmp_path):
    stability = {
        "detail": {"failure_counts": {"incomplete_detail": 1}},
        "relay": {"target_count": 1},
        "oci": {"skip_counts": {"skipped_empty_relay": 1}},
    }

    path = write_refresh_manifest(
        phase="postgame_finalize",
        target_date="20250101",
        game_ids=["20250101LGSS0", "20250101LGSS0"],
        datasets=["game", "game_events", "game_summary"],
        derived_refresh=["standings"],
        output_dir=tmp_path,
        stability=stability,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["phase"] == "postgame_finalize"
    assert payload["target_date"] == "20250101"
    assert payload["game_ids"] == ["20250101LGSS0"]
    assert payload["datasets"] == ["game", "game_events", "game_summary"]
    assert payload["derived_refresh"] == ["standings"]
    assert payload["topics"] == ["coach_review", "leaderboard", "search_rag"]
    assert payload["stability"] == stability


def test_write_refresh_manifest_omits_stability_when_not_provided(tmp_path):
    path = write_refresh_manifest(
        phase="pregame",
        target_date="20250102",
        game_ids=[],
        datasets=["game_metadata"],
        output_dir=tmp_path,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert "stability" not in payload
