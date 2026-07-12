from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.player_list_crawler import PlayerListCrawler, _is_pitcher, _row_to_dict
from src.crawlers.player_search_crawler import PlayerRow


class TestIsPitcher:
    def test_korean_pitcher(self):
        assert _is_pitcher("투수")

    def test_english_pitcher(self):
        assert _is_pitcher("P")
        assert _is_pitcher("SP")
        assert _is_pitcher("RP")
        assert _is_pitcher("CP")

    def test_not_a_pitcher(self):
        assert not _is_pitcher("내야수")
        assert not _is_pitcher("외야수")
        assert not _is_pitcher("포수")

    def test_edge_cases(self):
        assert not _is_pitcher(None)
        assert not _is_pitcher("")
        assert not _is_pitcher("  ")

    def test_partial_match(self):
        assert _is_pitcher("투수 (P)")
        assert _is_pitcher("내야수투수")  # two-way player


class TestRowToDict:
    def test_active_hitter(self):
        row = PlayerRow(
            player_id=12345,
            uniform_no="7",
            name="홍길동",
            team="LG",
            position="내야수",
            birth_date="1990-05-15",
            height_cm=180,
            weight_kg=80,
            career="LG 트윈스",
        )
        result = _row_to_dict(row)
        assert result["player_id"] == "12345"
        assert result["player_name"] == "홍길동"
        assert result["uniform_no"] == "7"
        assert result["team"] == "LG"
        assert result["position"] == "내야수"
        assert result["birth_date"] == "1990-05-15"
        assert result["height_cm"] == 180
        assert result["weight_kg"] == 80
        assert result["career"] == "LG 트윈스"
        assert result["status"] == "active"
        assert result["staff_role"] is None
        assert result["status_source"] == "heuristic"

    def test_active_pitcher(self):
        row = PlayerRow(
            player_id=67890,
            uniform_no="1",
            name="김철수",
            team="두산",
            position="투수",
            birth_date="1992-08-20",
            height_cm=185,
            weight_kg=90,
            career="두산 베어스",
        )
        result = _row_to_dict(row)
        assert result["status"] == "active"
        assert result["staff_role"] is None

    def test_retired_player(self):
        row = PlayerRow(
            player_id=99999,
            uniform_no=None,
            name="은퇴선수",
            team="",
            position="",
            birth_date=None,
            height_cm=None,
            weight_kg=None,
            career=None,
        )
        result = _row_to_dict(row)
        assert result["status"] == "retired"

    def test_manager_staff(self):
        row = PlayerRow(
            player_id=88888,
            uniform_no=None,
            name="김감독",
            team="LG",
            position="감독",
            birth_date="1960-01-01",
            height_cm=None,
            weight_kg=None,
            career=None,
        )
        result = _row_to_dict(row)
        assert result["status"] == "staff"
        assert result["staff_role"] == "manager"

    def test_coach_staff(self):
        row = PlayerRow(
            player_id=77777,
            uniform_no=None,
            name="이코치",
            team="NC",
            position="코치",
            birth_date="1970-03-15",
            height_cm=None,
            weight_kg=None,
            career=None,
        )
        result = _row_to_dict(row)
        assert result["status"] == "staff"
        assert result["staff_role"] == "coach"


class TestPlayerListCrawler:
    @pytest.mark.asyncio
    async def test_crawl_all_players_forwards_options_and_groups_categories(self):
        rows = [
            PlayerRow(1, "7", "타자", "LG", "내야수", None, None, None, None),
            PlayerRow(2, "1", "투수", "LG", "P", None, None, None, None),
            PlayerRow(3, None, "은퇴", "", "", None, None, None, None),
            PlayerRow(4, None, "코치", "LG", "코치", None, None, None, None),
        ]
        crawler = PlayerListCrawler(request_delay=0.2, headless=False, max_pages=3)

        with patch(
            "src.crawlers.player_list_crawler.crawl_all_players",
            new=AsyncMock(return_value=rows),
        ) as crawl_all:
            result = await crawler.crawl_all_players(2025)

        crawl_all.assert_awaited_once_with(max_pages=3, headless=False, request_delay=0.2)
        assert result["total"] == 4
        assert result["active_total"] == 2
        assert result["retired_total"] == 1
        assert result["staff_total"] == 1
        assert result["pitchers"][0]["player_name"] == "투수"
        assert result["hitters"][0]["player_name"] == "타자"
        assert result["staff"][0]["staff_role"] == "COACH"
