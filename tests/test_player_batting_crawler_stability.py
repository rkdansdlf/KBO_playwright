from src.crawlers.player_batting_all_series_crawler import build_batting_crawl_summary


def test_batting_crawl_summary_filters_basic2_only_rows():
    summary, valid_rows = build_batting_crawl_summary(
        [
            {
                "player_id": 1001,
                "player_name": "홍길동",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "LG",
                "games": 10,
                "hits": 5,
            },
            {
                "player_id": 1002,
                "player_name": "Basic2전용",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "LG",
                "walks": 10,
                "obp": 0.4,
            },
        ],
    )

    assert summary == {
        "processed_rows": 2,
        "valid_rows": 1,
        "filtered_rows": 1,
        "failure_counts": {"empty_core_stats": 1},
    }
    assert [row["player_id"] for row in valid_rows] == [1001]
