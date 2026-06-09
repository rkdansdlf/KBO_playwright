from unittest.mock import patch, MagicMock

from src.cli.diagnose_coach_pitching import main


class TestDiagnoseCoachPitching:
    def test_no_date_or_game_id(self):
        try:
            main([])
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_with_date(self):
        with patch("src.cli.diagnose_coach_pitching.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250101"])
            assert result == 0

    def test_with_game_id(self):
        with patch("src.cli.diagnose_coach_pitching.SessionLocal") as mock_sf, \
             patch("src.cli.diagnose_coach_pitching.ContextAggregator") as mock_agg_cls:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_agg = MagicMock()
            mock_agg_cls.return_value = mock_agg
            mock_agg.diagnose_completed_game_coach_pitching.return_value = {
                "game_id": "20250401LGSS0",
                "raw_tables": {"game_pitching_rows": 0, "starter_rows": 0, "bullpen_rows": 0, "player_id_missing_rows": 0},
                "repository": {"starter_rows": 0, "bullpen_rows": 0, "season_pitching_matches": 0, "unmatched_season_stats": []},
                "final_payload": {"review_summary_found": False, "review_summary_rows": 0, "pitching_breakdown_found": False, "starter_rows": 0, "bullpen_rows": 0},
                "drop_stage": "raw",
            }
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0
