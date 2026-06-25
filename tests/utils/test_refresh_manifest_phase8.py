from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.refresh_manifest import (
    RefreshManifestSpec,
    infer_topics,
    write_refresh_manifest,
)


class TestInferTopics:
    def test_game_only(self):
        result = infer_topics(datasets=["game"])
        assert "coach_review" in result

    def test_game_events_only(self):
        result = infer_topics(datasets=["game_events"])
        assert "coach_review" in result

    def test_game_and_game_events(self):
        result = infer_topics(datasets=["game", "game_events"])
        assert "coach_review" in result

    def test_game_metadata_only(self):
        result = infer_topics(datasets=["game_metadata"])
        assert "coach_matchup" in result

    def test_game_lineups_only(self):
        result = infer_topics(datasets=["game_lineups"])
        assert "coach_matchup" in result

    def test_game_summary_only(self):
        result = infer_topics(datasets=["game_summary"])
        assert "search_rag" in result

    def test_game_play_by_play_only(self):
        result = infer_topics(datasets=["game_play_by_play"])
        assert "search_rag" in result

    def test_standings_derived(self):
        result = infer_topics(derived_refresh=["standings"])
        assert "leaderboard" in result

    def test_matchups_derived(self):
        result = infer_topics(derived_refresh=["matchups"])
        assert "leaderboard" in result

    def test_stat_rankings_derived(self):
        result = infer_topics(derived_refresh=["stat_rankings"])
        assert "leaderboard" in result

    def test_empty_datasets(self):
        result = infer_topics()
        assert result == []

    def test_empty_strings_filtered(self):
        result = infer_topics(datasets=[""])
        assert result == []

    def test_sorted_output(self):
        result = infer_topics(datasets=["game", "game_metadata", "game_summary"])
        assert result == sorted(result)

    def test_no_duplicates(self):
        result = infer_topics(datasets=["game", "game", "game_events"])
        assert len(result) == len(set(result))

    def test_combined_datasets(self):
        result = infer_topics(
            datasets=["game", "game_metadata", "game_summary"],
            derived_refresh=["standings"],
        )
        assert "coach_review" in result
        assert "coach_matchup" in result
        assert "search_rag" in result
        assert "leaderboard" in result


class TestRefreshManifestSpec:
    def test_default_values(self):
        spec = RefreshManifestSpec(
            phase="finalize",
            target_date="2026-06-25",
            game_ids=["20260625LGSS0"],
            datasets=["game"],
        )
        assert spec.derived_refresh is None
        assert spec.topics is None
        assert spec.output_dir is None
        assert spec.stability is None

    def test_frozen(self):
        spec = RefreshManifestSpec(
            phase="finalize",
            target_date="2026-06-25",
            game_ids=["20260625LGSS0"],
            datasets=["game"],
        )
        with pytest.raises(AttributeError):
            spec.phase = "other"


class TestWriteRefreshManifest:
    def test_write_with_spec(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="finalize",
            target_date="2026-06-25",
            game_ids=["20260625LGSS0", "20260625KTNC0"],
            datasets=["game", "game_events"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        assert result.exists()
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert payload["phase"] == "finalize"
        assert payload["target_date"] == "2026-06-25"
        assert len(payload["game_ids"]) == 2

    def test_write_with_kwargs(self, tmp_path):
        result = write_refresh_manifest(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        assert result.exists()

    def test_both_spec_and_kwargs_raises(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        with pytest.raises(TypeError, match="Pass either RefreshManifestSpec or keyword"):
            write_refresh_manifest(spec, phase="other")

    def test_deduplicates_game_ids(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1", "GAME1", "GAME2"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert len(payload["game_ids"]) == 2

    def test_filters_empty_game_ids(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1", "", "GAME2"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert "" not in payload["game_ids"]

    def test_deduplicates_datasets(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game", "game", "events"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert len(payload["datasets"]) == 2

    def test_includes_topics(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert "topics" in payload
        assert "coach_review" in payload["topics"]

    def test_custom_topics(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            topics=["custom_topic"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert payload["topics"] == ["custom_topic"]

    def test_includes_generated_at(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert "generated_at" in payload

    def test_stability_included_when_provided(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            stability={"window_minutes": 30},
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert payload["stability"] == {"window_minutes": 30}

    def test_stability_omitted_when_none(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert "stability" not in payload

    def test_creates_output_dir(self, tmp_path):
        new_dir = tmp_path / "new_subdir"
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=new_dir,
        )
        result = write_refresh_manifest(spec)
        assert new_dir.exists()
        assert result.exists()

    def test_filename_contains_phase(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="finalize",
            target_date="2026-06-25",
            game_ids=["GAME1"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        assert "finalize" in result.name

    def test_empty_game_ids(self, tmp_path):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="2026-06-25",
            game_ids=[],
            datasets=["game"],
            output_dir=tmp_path,
        )
        result = write_refresh_manifest(spec)
        payload = json.loads(result.read_text(encoding="utf-8"))
        assert payload["game_ids"] == []
