from __future__ import annotations

from src.crawlers.fielding_stats_crawler import build_fielding_crawl_summary


class TestBuildFieldingCrawlSummary:
    def test_all_valid(self):
        records = [
            {
                "player_id": 1,
                "player_name": "A",
                "season": 2023,
                "team_code": "LG",
                "games": 120,
                "putouts": 100,
                "assists": 50,
                "errors": 3,
            },
            {
                "player_id": 2,
                "player_name": "B",
                "season": 2023,
                "team_code": "SS",
                "games": 110,
                "putouts": 80,
                "assists": 40,
                "errors": 2,
            },
        ]
        summary, valid = build_fielding_crawl_summary(records)
        assert summary["processed_rows"] == 2
        assert summary["valid_rows"] >= 0
        assert summary["filtered_rows"] == summary["processed_rows"] - summary["valid_rows"]

    def test_empty_list(self):
        summary, valid = build_fielding_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert summary["valid_rows"] == 0
        assert len(valid) == 0

    def test_summary_structure(self):
        records = [
            {
                "player_id": 1,
                "player_name": "A",
                "season": 2023,
                "team_code": "LG",
                "games": 120,
                "fielding_avg": 0.980,
            },
        ]
        summary, _ = build_fielding_crawl_summary(records)
        assert "processed_rows" in summary
        assert "valid_rows" in summary
        assert "filtered_rows" in summary
        assert "failure_counts" in summary
