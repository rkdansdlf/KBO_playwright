from src.crawlers.fielding_stats_crawler import build_fielding_crawl_summary


def test_fielding_crawl_summary_filters_invalid_rows():
    summary, valid_rows = build_fielding_crawl_summary(
        [
            {
                "player_id": "3001",
                "player_name": "오지환",
                "year": "2025",
                "team_id": "LG",
                "position_id": "SS",
                "games": 10,
                "errors": 1,
            },
            {
                "player_id": None,
                "player_name": "ID없음",
                "year": 2025,
                "team_id": "LG",
                "position_id": "SS",
                "games": 1,
            },
            {
                "player_id": 3002,
                "player_name": "팀없음",
                "year": 2025,
                "team_id": "",
                "position_id": "SS",
                "games": 1,
            },
            {
                "player_id": 3003,
                "player_name": "기록없음",
                "year": 2025,
                "team_id": "LG",
                "position_id": "SS",
            },
        ]
    )

    assert summary == {
        "processed_rows": 4,
        "valid_rows": 1,
        "filtered_rows": 3,
        "failure_counts": {
            "invalid_player_id": 1,
            "missing_team_id": 1,
            "empty_core_stats": 1,
        },
    }
    assert valid_rows[0]["player_id"] == 3001
    assert valid_rows[0]["year"] == 2025
