import json

from src.utils.refresh_manifest import infer_topics, write_refresh_manifest


class TestInferTopics:
    def test_coach_review_topic(self):
        topics = infer_topics(datasets=["game", "game_events", "game_summary"])
        assert "coach_review" in topics

    def test_coach_matchup_topic(self):
        topics = infer_topics(datasets=["game_metadata", "game_lineups"])
        assert "coach_matchup" in topics

    def test_search_rag_topic(self):
        topics = infer_topics(datasets=["game_summary", "game_play_by_play"])
        assert "search_rag" in topics

    def test_leaderboard_topic(self):
        topics = infer_topics(derived_refresh=["standings", "matchups"])
        assert "leaderboard" in topics

    def test_no_datasets_returns_empty(self):
        topics = infer_topics()
        assert topics == []

    def test_derived_refresh_triggers_leaderboard(self):
        topics = infer_topics(datasets=["game"], derived_refresh=["stat_rankings"])
        assert "leaderboard" in topics
        assert "coach_review" in topics


class TestWriteRefreshManifest:
    def test_writes_minimal_manifest(self, tmp_path):
        path = write_refresh_manifest(
            phase="pregame",
            target_date="20250101",
            game_ids=["20250101LGSS0"],
            datasets=["game_metadata"],
            output_dir=tmp_path,
        )
        assert path.exists()
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["phase"] == "pregame"
        assert payload["target_date"] == "20250101"
        assert payload["game_ids"] == ["20250101LGSS0"]
        assert "topics" in payload
        assert "generated_at" in payload

    def test_deduplicates_game_ids(self, tmp_path):
        path = write_refresh_manifest(
            phase="postgame",
            target_date="20250101",
            game_ids=["20250101LGSS0", "20250101LGSS0", "20250102LGSS0"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["game_ids"] == ["20250101LGSS0", "20250102LGSS0"]

    def test_includes_stability(self, tmp_path):
        stability = {"detail": {"failure_counts": {"incomplete_detail": 1}}}
        path = write_refresh_manifest(
            phase="finalize",
            target_date="20250101",
            game_ids=[],
            datasets=[],
            output_dir=tmp_path,
            stability=stability,
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["stability"] == stability

    def test_filters_empty_game_ids(self, tmp_path):
        path = write_refresh_manifest(
            phase="test",
            target_date="20250101",
            game_ids=["", "20250101LGSS0"],
            datasets=["game"],
            output_dir=tmp_path,
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert "" not in payload["game_ids"]

    def test_csv_filename_format(self, tmp_path):
        path = write_refresh_manifest(
            phase="postgame_finalize",
            target_date="20250101",
            game_ids=[],
            datasets=[],
            output_dir=tmp_path,
        )
        assert "_postgame_finalize.json" in path.name
