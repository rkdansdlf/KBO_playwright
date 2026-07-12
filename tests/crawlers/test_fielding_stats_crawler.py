from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.crawlers.fielding_stats_crawler import (
    FieldingCrawlContext,
    _crawl_catcher_fielding_details,
    _crawl_team_fielding_basic,
    _go_to_page,
    _get_team_list,
    _init_fielding_page,
    _parse_catcher_detail_row,
    _parse_fielding_row,
    build_fielding_crawl_summary,
    crawl_all_fielding_stats,
    save_fielding_stats,
)


class _Node:
    def __init__(self, text: str = "", *, href: str | None = None, children: dict[str, object] | None = None) -> None:
        self.text = text
        self.href = href
        self.children = children or {}
        self.clicked = False

    def click(self) -> None:
        self.clicked = True

    def get_attribute(self, name: str) -> str | None:
        return self.href if name in {"href", "value"} else None

    def inner_text(self) -> str:
        return self.text

    def query_selector(self, selector: str) -> object | None:
        return self.children.get(selector)

    def query_selector_all(self, selector: str) -> list[object]:
        return self.children.get(selector, [])  # type: ignore[return-value]


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
            },
        ]
        summary, _ = build_fielding_crawl_summary(records)
        assert "valid_rows" in summary
        assert "filtered_rows" in summary
        assert "failure_counts" in summary


class TestFieldingParser:
    def test_get_team_list_filters_empty_values(self):
        options = [
            _Node("전체", href=""),
            _Node("LG", href="LG"),
            _Node("두산", href="OB"),
        ]
        dropdown = _Node(children={"option": options})
        page = MagicMock()
        page.query_selector.return_value = dropdown

        teams = _get_team_list(page)

        assert teams == [("LG", "LG"), ("OB", "두산")]

    def test_parse_fielding_row_builds_normalized_record(self):
        cells = [_Node(str(index)) for index in range(13)]
        cells[1] = _Node("홍길동", children={"a": _Node(href="/Player/Detail.aspx?playerId=123&foo=bar")})
        cells[2] = _Node("LG")
        cells[3] = _Node("유격수")
        cells[4] = _Node("10")
        cells[5] = _Node("8")
        cells[6] = _Node("9.2")
        cells[7] = _Node("1")
        cells[8] = _Node("2")
        cells[9] = _Node("20")
        cells[10] = _Node("30")
        cells[11] = _Node("4")
        cells[12] = _Node("0.980")
        row = _Node(children={"td": cells})
        records = {}

        with patch("src.crawlers.fielding_stats_crawler.resolve_team_code", return_value="LG"):
            _parse_fielding_row(row, 2025, {"유격수": "SS"}, records)

        assert records == {
            ("123", "LG", "SS"): {
                "player_id": "123",
                "player_name": "홍길동",
                "team_id": "LG",
                "year": 2025,
                "position_id": "SS",
                "games": 10,
                "games_started": 8,
                "innings": 9.2,
                "errors": 1,
                "pickoffs": 2,
                "putouts": 20,
                "assists": 30,
                "double_plays": 4,
                "fielding_pct": 0.98,
                "source": "CRAWLER",
            },
        }

    def test_parse_catcher_detail_enriches_existing_record(self):
        cells = [_Node("") for _ in range(17)]
        cells[1] = _Node("포수", children={"a": _Node(href="?playerId=123")})
        cells[2] = _Node("LG")
        cells[13] = _Node("2")
        cells[14] = _Node("10")
        cells[15] = _Node("4")
        cells[16] = _Node("0.286")
        records = {("123", "LG", "C"): {"player_id": "123"}}

        with patch("src.crawlers.fielding_stats_crawler.resolve_team_code", return_value="LG"):
            _parse_catcher_detail_row(_Node(children={"td": cells}), 2025, records)

        assert records[("123", "LG", "C")].update is not None
        assert records[("123", "LG", "C")]["passed_balls"] == 2
        assert records[("123", "LG", "C")]["cs_pct"] == 0.286


class TestFieldingCrawlerFlow:
    def test_init_page_selects_requested_season(self):
        page = MagicMock()
        page.query_selector.return_value = MagicMock()
        policy = MagicMock()

        initialized = _init_fielding_page(page, "https://example.test/fielding", 2025, policy)

        assert initialized is True
        page.select_option.assert_called_once_with(
            "select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason",
            "2025",
        )
        assert policy.delay.call_count == 2

    def test_crawl_team_processes_all_page_rows(self):
        page = MagicMock()
        policy = MagicMock()
        row = MagicMock()
        tbody = MagicMock()
        tbody.query_selector_all.return_value = [row]
        table = MagicMock()
        table.query_selector.return_value = tbody
        page.query_selector.return_value = table
        records = {}
        ctx = FieldingCrawlContext(page, "LG", "LG", 2025, {}, records, policy)

        with patch("src.crawlers.fielding_stats_crawler._parse_fielding_row") as parse_row:
            _crawl_team_fielding_basic(ctx)

        parse_row.assert_called_once_with(row, 2025, {}, records)

    def test_go_to_page_clicks_matching_pagination_link(self):
        link = _Node("2")
        paging = _Node(children={"a": [link]})
        page = MagicMock()
        page.query_selector.return_value = paging
        page.expect_response.return_value.__enter__.return_value = None
        policy = MagicMock()

        _go_to_page(page, 2, policy)

        assert link.clicked is True
        page.wait_for_load_state.assert_called_once()
        policy.delay.assert_called_once()

    def test_crawl_catcher_details_parses_current_page(self):
        page = MagicMock()
        row = MagicMock()
        tbody = MagicMock()
        tbody.query_selector_all.return_value = [row]
        table = MagicMock()
        table.query_selector.return_value = tbody
        page.query_selector.return_value = table
        page.evaluate.return_value = ""
        page.expect_response.return_value.__enter__.return_value = None
        records = {}

        with patch("src.crawlers.fielding_stats_crawler._parse_catcher_detail_row") as parse_row:
            _crawl_catcher_fielding_details(page, "https://example.test/fielding", 2025, records, MagicMock())

        parse_row.assert_called_once_with(row, 2025, records)

    def test_crawl_all_fielding_stats_orchestrates_team_and_catcher_collection(self):
        page = MagicMock()
        browser = MagicMock()
        context = MagicMock()
        context.new_page.return_value = page
        browser.new_context.return_value = context
        playwright = MagicMock()
        playwright.chromium.launch.return_value = browser
        manager = MagicMock()
        manager.__enter__.return_value = playwright
        manager.__exit__.return_value = False

        def collect_team(ctx):
            ctx.fielding_data_map[("123", "LG", "SS")] = {"player_id": "123", "team_id": "LG"}

        with (
            patch("src.crawlers.fielding_stats_crawler.sync_playwright", return_value=manager),
            patch("src.crawlers.fielding_stats_crawler.install_sync_resource_blocking"),
            patch("src.crawlers.fielding_stats_crawler._init_fielding_page", return_value=True),
            patch("src.crawlers.fielding_stats_crawler._get_team_list", return_value=[("LG", "LG")]),
            patch("src.crawlers.fielding_stats_crawler._crawl_team_fielding_basic", side_effect=collect_team),
            patch("src.crawlers.fielding_stats_crawler._crawl_catcher_fielding_details") as catcher,
            patch(
                "src.crawlers.fielding_stats_crawler.build_fielding_crawl_summary",
                return_value=({}, [{"player_id": "123", "team_id": "LG"}]),
            ),
        ):
            records = crawl_all_fielding_stats(2025)

        assert records == [{"player_id": "123", "team_id": "LG"}]
        catcher.assert_called_once()
        browser.close.assert_called_once()

    def test_save_fielding_stats_writes_valid_records_and_skips_missing_ids(self):
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value = cursor
        records = [
            {
                "player_id": "123",
                "player_name": "홍길동",
                "team_id": "LG",
                "position_id": "SS",
                "games": 10,
                "games_started": 8,
                "innings": 9.2,
                "putouts": 20,
                "assists": 30,
                "errors": 1,
                "double_plays": 4,
                "fielding_pct": 0.98,
            },
            {"player_id": None},
        ]

        with (
            patch("src.crawlers.fielding_stats_crawler.crawl_all_fielding_stats", return_value=records),
            patch("src.crawlers.fielding_stats_crawler.sqlite3.connect", return_value=connection),
        ):
            save_fielding_stats(2025, "test.db")

        cursor.execute.assert_called_once()
        connection.commit.assert_called_once()
        connection.close.assert_called_once()

    def test_save_fielding_stats_skips_database_connection_when_no_records(self):
        with (
            patch("src.crawlers.fielding_stats_crawler.crawl_all_fielding_stats", return_value=[]),
            patch("src.crawlers.fielding_stats_crawler.sqlite3.connect") as connect,
        ):
            save_fielding_stats(2025)

        connect.assert_not_called()
