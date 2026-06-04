from __future__ import annotations

from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler


def test_roster_transaction_crawler_dedupes_payload_before_save():
    crawler = RosterTransactionCrawler()
    rows = [
        {"dedupe_key": "2026-05-30_LG_권우준_registered", "player_name": "권우준", "player_id": 56149},
        {"dedupe_key": "2026-05-30_LG_권우준_registered", "player_name": "권우준", "player_id": 56149},
        {"dedupe_key": "2026-05-30_LG_김현수_registered", "player_name": "김현수", "player_id": 76290},
    ]

    assert crawler._dedupe_transactions(rows) == [rows[0], rows[2]]
