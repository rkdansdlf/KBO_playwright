from __future__ import annotations

import asyncio

from src.crawlers.game_detail_crawler import GameDetailCrawler


def test_extract_hitters_uses_roster_map_for_missing_player_id():
    crawler = GameDetailCrawler()

    base_rows = [
        {
            "playerName": "홍길동",
            "playerId": None,
            "uniformNo": "7",
            "cells": {
                "타순": "1",
                "POS": "유격수",
                "타수": "4",
                "안타": "1",
            },
        }
    ]

    async def fake_extract_table_rows(_page, selector: str):
        if selector == "#tblAwayHitter1":
            return base_rows
        return []

    crawler._extract_table_rows = fake_extract_table_rows  # type: ignore[method-assign]

    hitters = asyncio.run(
        crawler._extract_hitters(
            page=None,
            team_side="away",
            team_code="LG",
            season_year=2025,
            roster_map={"홍길동": [{"id": "12345", "uniform": "7"}]},
        )
    )

    assert len(hitters) == 1
    assert hitters[0]["player_id"] == "12345"
