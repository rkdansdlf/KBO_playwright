from src.crawlers.fielding_stats_crawler import build_fielding_crawl_summary


class TestBuildFieldingCrawlSummary:
    def test_valid_records(self):
        records = [
            {
                "player_id": 1,
                "player_name": "A",
                "team_id": "LG",
                "position_id": "SS",
                "year": 2025,
                "errors": 0,
                "games": 10,
            },
            {
                "player_id": 2,
                "player_name": "B",
                "team_id": "SS",
                "position_id": "2B",
                "year": 2025,
                "errors": 0,
                "games": 10,
            },
        ]
        summary, valid = build_fielding_crawl_summary(records)
        assert summary["processed_rows"] == 2
        assert len(valid) == 2

    def test_empty_records(self):
        summary, valid = build_fielding_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert valid == []

    def test_summary_keys(self):
        records = [
            {
                "player_id": 1,
                "player_name": "A",
                "team_id": "LG",
                "position_id": "C",
                "year": 2025,
                "errors": 1,
                "games": 10,
            }
        ]
        summary, _ = build_fielding_crawl_summary(records)
        assert "valid_rows" in summary
        assert "filtered_rows" in summary
        assert "failure_counts" in summary
