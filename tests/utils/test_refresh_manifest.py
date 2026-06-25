from __future__ import annotations

from src.utils.refresh_manifest import (
    RefreshManifestSpec,
    infer_topics,
)


class TestInferTopics:
    def test_empty(self):
        result = infer_topics()
        assert result == []

    def test_coach_review_from_game(self):
        result = infer_topics(datasets=["game"])
        assert "coach_review" in result

    def test_coach_review_from_game_events(self):
        result = infer_topics(datasets=["game", "game_events"])
        assert "coach_review" in result

    def test_coach_matchup_from_metadata(self):
        result = infer_topics(datasets=["game_metadata"])
        assert "coach_matchup" in result

    def test_search_rag_from_summary(self):
        result = infer_topics(datasets=["game_summary"])
        assert "search_rag" in result

    def test_leaderboard_from_standings(self):
        result = infer_topics(derived_refresh=["standings"])
        assert "leaderboard" in result

    def test_multiple_topics(self):
        result = infer_topics(
            datasets=["game", "game_metadata"],
            derived_refresh=["standings"],
        )
        assert "coach_review" in result
        assert "coach_matchup" in result
        assert "leaderboard" in result

    def test_sorted_output(self):
        result = infer_topics(
            datasets=["game_summary", "game"],
            derived_refresh=["matchups"],
        )
        assert result == sorted(result)


class TestRefreshManifestSpec:
    def test_creation(self):
        spec = RefreshManifestSpec(
            phase="finalize",
            target_date="20260625",
            datasets=["game", "game_events"],
            game_ids=["20260625LGSS0"],
        )
        assert spec.phase == "finalize"
        assert spec.target_date == "20260625"

    def test_optional_fields(self):
        spec = RefreshManifestSpec(
            phase="test",
            target_date="20260101",
            datasets=[],
            game_ids=[],
        )
        assert spec.derived_refresh is None
        assert spec.topics is None
