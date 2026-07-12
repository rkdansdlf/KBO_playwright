from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import src.crawlers.player_search_crawler as player_search
from src.crawlers.player_search_crawler import PlayerRow, PlayerSearchCrawler


def _row(player_id: int = 1, name: str = "홍길동") -> PlayerRow:
    return PlayerRow(player_id, "1", name, "LG", "투수", "2000.01.01", 180, 80, "고교")


def _pool_with_page(page):
    pool = MagicMock()
    pool.start = AsyncMock()
    pool.close = AsyncMock()
    pool.acquire = AsyncMock(return_value=page)
    pool.release = AsyncMock()
    return pool


def _search_page():
    page = MagicMock()
    input_ = MagicMock()
    input_.fill = AsyncMock()
    button = MagicMock()
    button.click = AsyncMock()
    page.locator.side_effect = lambda selector: {
        player_search.SEARCH_INPUT: input_,
        player_search.SEARCH_BTN: button,
    }[selector]
    page.wait_for_selector = AsyncMock()
    return page, input_, button


class TestSearchPlayer:
    def test_returns_serialized_rows_and_releases_injected_pool(self, monkeypatch):
        page, input_, button = _search_page()
        pool = _pool_with_page(page)
        crawler = PlayerSearchCrawler(pool=pool)
        crawler._navigate_search_page = AsyncMock(return_value=(True, "ok"))
        monkeypatch.setattr(crawler, "_paginate_current_tab", AsyncMock(return_value=[_row()]))

        result = asyncio.run(crawler.search_player(" 홍길동! "))

        assert result[0]["player_id"] == 1
        input_.fill.assert_awaited_once_with("홍길동")
        button.click.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_awaited()

    def test_returns_empty_for_unsearchable_name_without_acquiring_page(self):
        pool = _pool_with_page(MagicMock())
        crawler = PlayerSearchCrawler(pool=pool)

        assert asyncio.run(crawler.search_player("!!!")) == []
        pool.acquire.assert_not_awaited()

    def test_closes_owned_pool_when_navigation_is_rejected(self, monkeypatch):
        page, _, _ = _search_page()
        pool = _pool_with_page(page)
        crawler = PlayerSearchCrawler()
        crawler._navigate_search_page = AsyncMock(return_value=(False, "blocked"))
        monkeypatch.setattr(player_search, "AsyncPlaywrightPool", MagicMock(return_value=pool))

        assert asyncio.run(crawler.search_player("홍길동")) == []
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_awaited_once()

    def test_returns_empty_when_result_table_times_out(self, monkeypatch):
        page, _, _ = _search_page()
        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        pool = _pool_with_page(page)
        crawler = PlayerSearchCrawler(pool=pool)
        crawler._navigate_search_page = AsyncMock(return_value=(True, "ok"))
        monkeypatch.setattr(crawler, "_paginate_current_tab", AsyncMock())

        assert asyncio.run(crawler.search_player("홍길동")) == []
        crawler._paginate_current_tab.assert_not_awaited()


class TestCrawlAllPlayers:
    def test_merges_single_tab_when_initial_links_are_missing(self, monkeypatch):
        page, _, _ = _search_page()
        pool = _pool_with_page(page)
        crawler = PlayerSearchCrawler(pool=pool)
        crawler._navigate_search_page = AsyncMock(return_value=(True, "ok"))
        crawler._list_initial_links = AsyncMock(return_value=[])

        async def merge(_page, rows, _seen, _limit):
            rows.append(_row())
            return False

        monkeypatch.setattr(crawler, "_merge_rows", merge)

        assert asyncio.run(crawler.crawl_all_players()) == [_row()]
        pool.release.assert_awaited_once_with(page)

    def test_stops_when_navigation_fails(self):
        pool = _pool_with_page(MagicMock())
        crawler = PlayerSearchCrawler(pool=pool)
        crawler._navigate_search_page = AsyncMock(return_value=(False, "blocked"))

        assert asyncio.run(crawler.crawl_all_players()) == []
        pool.release.assert_awaited_once()

    def test_merge_rows_deduplicates_and_honors_limit(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        monkeypatch.setattr(crawler, "_paginate_current_tab", AsyncMock(return_value=[_row(1), _row(2), _row(2)]))
        collected = []

        done = asyncio.run(crawler._merge_rows(MagicMock(), collected, set(), limit=2))

        assert done is True
        assert [row.player_id for row in collected] == [1, 2]


class TestPaginationHelpers:
    def test_paginate_stops_when_pager_is_missing(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        page = MagicMock()
        pager = MagicMock()
        pager.count = AsyncMock(return_value=0)
        page.locator.return_value.last = pager

        async def add_rows(_page, rows, _seen):
            rows.append(_row())

        monkeypatch.setattr(crawler, "_add_current_page_rows", add_rows)

        assert asyncio.run(crawler._paginate_current_tab(page)) == [_row()]

    def test_add_current_page_rows_records_duplicate_ids(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        monkeypatch.setattr(crawler, "_collect_page_rows", AsyncMock(return_value=[_row(), _row()]))
        collected = []

        asyncio.run(crawler._add_current_page_rows(MagicMock(), collected, set()))

        assert collected == [_row()]
        assert crawler.get_failure_summary() == {"duplicate_player_id": 1}

    def test_current_pager_index_uses_active_numeric_button(self):
        nums = MagicMock()
        first = MagicMock()
        first.get_attribute = AsyncMock(return_value="")
        second = MagicMock()
        second.get_attribute = AsyncMock(return_value="on")
        nums.nth.side_effect = [first, second]

        assert asyncio.run(PlayerSearchCrawler._current_pager_index(nums, 2)) == 1

    def test_visits_remaining_anchor_pages_and_reports_movement(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        page = MagicMock()
        first = MagicMock()
        first.evaluate = AsyncMock(return_value="A")
        second = MagicMock()
        second.evaluate = AsyncMock(return_value="SPAN")
        nums = MagicMock()
        nums.nth.side_effect = [first, second]
        page.locator.return_value.last.locator.return_value.filter.return_value = nums
        click = AsyncMock(return_value=True)
        monkeypatch.setattr(crawler, "_click_pager_target", click)

        moved = asyncio.run(crawler._visit_remaining_numeric_pages(page, 0, 3, [], set()))

        assert moved is True
        click.assert_awaited_once()

    def test_click_target_waits_then_adds_new_rows(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        monkeypatch.setattr(crawler, "_get_hfpage_value", AsyncMock(return_value="1"))
        monkeypatch.setattr(crawler, "_get_first_player_name", AsyncMock(return_value="홍길동"))
        monkeypatch.setattr(crawler, "_trigger_postback", AsyncMock(return_value=True))
        wait = AsyncMock()
        add = AsyncMock()
        monkeypatch.setattr(crawler, "_wait_after_nav", wait)
        monkeypatch.setattr(crawler, "_add_current_page_rows", add)

        assert asyncio.run(crawler._click_pager_target(MagicMock(), MagicMock(), [], set())) is True
        wait.assert_awaited_once()
        add.assert_awaited_once()

    def test_next_pager_block_records_failed_navigation(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        page = MagicMock()
        next_button = MagicMock()
        next_button.count = AsyncMock(return_value=1)
        next_button.evaluate = AsyncMock(return_value="A")
        page.locator.return_value.last.locator.return_value.first = next_button
        monkeypatch.setattr(crawler, "_click_pager_target", AsyncMock(return_value=False))

        assert asyncio.run(crawler._visit_next_pager_block(page, [], set())) == (False, True)
        assert crawler.get_failure_summary() == {"pagination_failed": 1}


class TestPageAndPostbackHelpers:
    def test_collect_rows_retries_destroyed_execution_context(self, monkeypatch):
        crawler = PlayerSearchCrawler()
        payload = [
            {"cells": ["1", "홍길동", "LG", "투수", "2000.01.01", "180cm/80kg", "고교"], "linkHref": "?playerId=1"}
        ]
        page = MagicMock()
        page.evaluate = AsyncMock(side_effect=[RuntimeError("Execution context was destroyed"), payload])
        monkeypatch.setattr(player_search.asyncio, "sleep", AsyncMock())

        assert asyncio.run(crawler._collect_page_rows(page)) == [_row()]
        assert page.evaluate.await_count == 2

    def test_first_player_name_returns_empty_when_locator_times_out(self):
        crawler = PlayerSearchCrawler()
        page = MagicMock()
        page.locator.return_value.first.locator.return_value.nth.return_value.inner_text = AsyncMock(
            side_effect=TimeoutError()
        )

        assert asyncio.run(crawler._get_first_player_name(page)) == ""

    def test_trigger_postback_uses_manual_evaluation_for_javascript_link(self):
        crawler = PlayerSearchCrawler()
        anchor = MagicMock()
        anchor.get_attribute = AsyncMock(return_value="javascript:__doPostBack('pager', '2')")
        page = MagicMock()
        page.evaluate = AsyncMock()
        page.wait_for_load_state = AsyncMock()

        assert asyncio.run(crawler._trigger_postback(page, anchor)) is True
        page.evaluate.assert_awaited_once()
        anchor.click.assert_not_called()

    def test_wait_after_navigation_uses_hidden_page_value(self, monkeypatch):
        crawler = PlayerSearchCrawler(request_delay=0.2)
        page = MagicMock()
        page.wait_for_function = AsyncMock()
        sleep = AsyncMock()
        monkeypatch.setattr(player_search.asyncio, "sleep", sleep)

        asyncio.run(crawler._wait_after_nav(page, "1", "ignored"))

        page.wait_for_function.assert_awaited_once()
        sleep.assert_awaited_once_with(0.2)

    def test_list_initial_links_filters_non_initial_labels(self):
        crawler = PlayerSearchCrawler()
        links = MagicMock()
        links.count = AsyncMock(return_value=3)
        korean = MagicMock()
        korean.inner_text = AsyncMock(return_value="가")
        latin = MagicMock()
        latin.inner_text = AsyncMock(return_value="A")
        numeric = MagicMock()
        numeric.inner_text = AsyncMock(return_value="1")
        links.nth.side_effect = lambda index: [korean, latin, numeric][index]
        page = MagicMock()
        page.locator.return_value = links

        assert asyncio.run(crawler._list_initial_links(page)) == [korean, latin]


class TestCliOrchestration:
    def test_main_stops_when_player_crawl_is_empty(self, monkeypatch):
        monkeypatch.setattr(
            player_search, "_parse_crawl_args", lambda: SimpleNamespace(max_pages=None, save=True, sync_oci=None)
        )
        monkeypatch.setattr(player_search, "_crawl_players", AsyncMock(return_value=[]))

        assert asyncio.run(player_search.main()) is None

    def test_main_initializes_and_saves_when_requested(self, monkeypatch):
        args = SimpleNamespace(max_pages=1, save=True, sync_oci=False)
        init_db = MagicMock()
        row = _row()
        monkeypatch.setattr(player_search, "_parse_crawl_args", lambda: args)
        monkeypatch.setattr(player_search, "_crawl_players", AsyncMock(return_value=[row]))
        monkeypatch.setattr(player_search, "_crawl_and_save", AsyncMock())
        monkeypatch.setattr("src.db.engine.init_db", init_db)

        assert asyncio.run(player_search.main()) is None
        init_db.assert_called_once()
        player_search._crawl_and_save.assert_awaited_once()
