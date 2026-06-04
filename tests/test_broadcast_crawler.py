from __future__ import annotations

from src.crawlers.broadcast_crawler import BroadcastCrawler


def test_broadcast_crawler_builds_kbo_legacy_game_ids_for_modern_team_codes():
    crawler = BroadcastCrawler()

    rows = crawler._normalize_game_ids(
        [
            {
                "game_date": "20260531",
                "away_team_code": "KIA",
                "home_team_code": "LG",
                "broadcaster": "M-T",
                "channel_name": "M-T",
                "source": "KBO",
            },
            {
                "game_date": "20260530",
                "away_team_code": "DB",
                "home_team_code": "SS",
                "broadcaster": "S-T",
                "channel_name": "S-T",
                "source": "KBO",
            },
            {
                "game_date": "20260530",
                "away_team_code": "SSG",
                "home_team_code": "HH",
                "broadcaster": "RADIO_TEST",
                "channel_name": "TEST (라디오)",
                "source": "KBO",
            },
            {
                "game_date": "20260530",
                "away_team_code": "KT",
                "home_team_code": "KH",
                "broadcaster": "SPOTV",
                "channel_name": "SPOTV",
                "source": "KBO",
            },
        ],
        2026,
    )

    assert [row["game_id"] for row in rows] == [
        "20260531HTLG0",
        "20260530OBSS0",
        "20260530SKHH0",
        "20260530KTWO0",
    ]
