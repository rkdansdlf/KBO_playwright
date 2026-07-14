from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

import src.crawlers.text_relay_crawler as text_relay_crawler
from src.crawlers.text_relay_crawler import (
    RelayCrawlResult,
    RelayRow,
    TextRelayCrawler,
    crawl_text_relay,
    crawl_text_relays,
)


class FakeRelayPage:
    """Mock Playwright page for testing TextRelayCrawler."""

    def __init__(self, spans: list[dict[str, str]] | None = None) -> None:
        self.spans = spans or []
        self.url = "https://www.koreabaseball.com/Game/LiveText.aspx"

    async def evaluate(self, _script: str) -> list[dict[str, str]]:
        return self.spans

    async def content(self) -> str:
        return "<html><body>Test</body></html>"


class TestRelayRow:
    def test_to_dict_defaults(self):
        row = RelayRow()
        result = row.to_dict()
        assert result["이닝"] == 0
        assert result["타석번호"] == 0
        assert result["투수명"] == ""
        assert result["결과"] == ""

    def test_to_dict_populated(self):
        row = RelayRow(
            inning=3,
            inning_half="초",
            at_bat_num=1,
            pitcher_name="류현진",
            batter_name="김하성",
            pitch_type="슬라이더",
            pitch_speed="145",
            result="스트라이크",
            balls=2,
            strikes=1,
            outs=1,
            runners="1루",
            description="타자 김하성 : 스트라이크",
        )
        result = row.to_dict()
        assert result["이닝"] == 3
        assert result["이닝_초말"] == "초"
        assert result["타석번호"] == 1
        assert result["투수명"] == "류현진"
        assert result["타자명"] == "김하성"
        assert result["구종"] == "슬라이더"
        assert result["구속"] == "145"
        assert result["결과"] == "스트라이크"
        assert result["볼"] == 2
        assert result["스트라이크"] == 1
        assert result["아웃"] == 1
        assert result["주자"] == "1루"
        assert result["상세"] == "타자 김하성 : 스트라이크"


class TestRelayCrawlResult:
    def test_to_dataframe_empty(self):
        result = RelayCrawlResult(game_id="20260412SKLG0", game_date="20260412")
        df = result.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "이닝" in df.columns
        assert "타자명" in df.columns

    def test_to_dataframe_with_rows(self):
        result = RelayCrawlResult(
            game_id="20260412SKLG0",
            game_date="20260412",
            rows=[
                RelayRow(inning=1, batter_name="김하성", result="안타"),
                RelayRow(inning=1, batter_name="박병호", result="홈런"),
            ],
        )
        df = result.to_dataframe()
        assert len(df) == 2
        assert df.iloc[0]["타자명"] == "김하성"
        assert df.iloc[1]["결과"] == "홈런"

    def test_save_csv(self, tmp_path):
        result = RelayCrawlResult(
            game_id="20260412SKLG0",
            game_date="20260412",
            rows=[RelayRow(inning=1, batter_name="김하성", result="안타")],
        )
        output_path = result.save_csv(str(tmp_path))
        assert output_path.exists()
        assert output_path.name == "20260412SKLG0_text_relay.csv"

        df = pd.read_csv(output_path, encoding="utf-8-sig")
        assert len(df) == 1
        assert df.iloc[0]["타자명"] == "김하성"


class TestTextRelayCrawlerParseInningHeader:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_top_of_inning(self):
        result = self.crawler._parse_inning_header("1회초 삼성 공격", "blue")
        assert result == (1, "초")

    def test_bottom_of_inning(self):
        result = self.crawler._parse_inning_header("3회말 LG 공격", "blue")
        assert result == (3, "말")

    def test_no_blue_class(self):
        result = self.crawler._parse_inning_header("1회초", "red")
        assert result is None

    def test_no_inning_text(self):
        result = self.crawler._parse_inning_header("Some text", "blue")
        assert result is None

    def test_extra_inning(self):
        result = self.crawler._parse_inning_header("12회초 두산 공격", "blue")
        assert result == (12, "초")


class TestTextRelayCrawlerIsEventText:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_normal_event(self):
        assert self.crawler._is_event_text("타자 김하성 : 안타", "normaiflTxt") is True

    def test_red_event(self):
        assert self.crawler._is_event_text("홈런", "red") is True

    def test_game_prep_skipped(self):
        assert self.crawler._is_event_text("경기 준비중", "normaiflTxt") is False

    def test_game_start_skipped(self):
        assert self.crawler._is_event_text("경기 시작", "normaiflTxt") is False

    def test_wrong_class(self):
        assert self.crawler._is_event_text("Some text", "blue") is False


class TestTextRelayCrawlerExtractPitchInfo:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_slider_with_speed(self):
        pitch_type, pitch_speed = self.crawler._extract_pitch_info("145km/h 슬라이더")
        assert pitch_type == "슬라이더"
        assert pitch_speed == "145"

    def test_no_pitch_info(self):
        pitch_type, pitch_speed = self.crawler._extract_pitch_info("안타")
        assert pitch_type == ""
        assert pitch_speed == ""

    def test_curveball(self):
        pitch_type, pitch_speed = self.crawler._extract_pitch_info("130km/h 커브")
        assert pitch_type == "커브"
        assert pitch_speed == "130"


class TestTextRelayCrawlerExtractPlayerNames:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_batter_name(self):
        pitcher, batter = self.crawler._extract_player_names("타자 김하성 : 안타")
        assert batter == "김하성"
        assert pitcher == ""

    def test_pitcher_name(self):
        pitcher, batter = self.crawler._extract_player_names("투수 류현진")
        assert pitcher == "류현진"
        assert batter == ""

    def test_both_names(self):
        pitcher, batter = self.crawler._extract_player_names("투수 류현진, 타자 김하성 : 안타")
        assert pitcher == "류현진"
        assert batter == "김하성"

    def test_no_names(self):
        pitcher, batter = self.crawler._extract_player_names("안타")
        assert pitcher == ""
        assert batter == ""


class TestTextRelayCrawlerExtractResult:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_strike(self):
        assert self.crawler._extract_result("스트라이크") == "스트라이크"

    def test_ball(self):
        assert self.crawler._extract_result("볼") == "볼"

    def test_hit(self):
        assert self.crawler._extract_result("안타") == "안타"

    def test_home_run(self):
        assert self.crawler._extract_result("홈런") == "홈런"

    def test_strikeout(self):
        assert self.crawler._extract_result("삼진") == "삼진"

    def test_no_keyword(self):
        assert self.crawler._extract_result("Some result") == "Some result"


class TestTextRelayCrawlerExtractPitchCount:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_count_extraction(self):
        balls, strikes = self.crawler._extract_pitch_count("2볼 1스트라이크")
        assert balls == 2
        assert strikes == 1

    def test_no_count(self):
        balls, strikes = self.crawler._extract_pitch_count("안타")
        assert balls == 0
        assert strikes == 0


class TestTextRelayCrawlerExtractRunners:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_loaded_bases(self):
        assert self.crawler._extract_runners("만루") == "만루"

    def test_first_base(self):
        assert self.crawler._extract_runners("1루") == "1루"

    def test_first_and_second(self):
        assert self.crawler._extract_runners("1,2루") == "1루,2루"

    def test_no_runners(self):
        assert self.crawler._extract_runners("안타") == ""


class TestTextRelayCrawlerParseRelaySpans:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_empty_spans(self):
        rows = self.crawler._parse_relay_spans([])
        assert rows == []

    def test_single_inning(self):
        raw_spans = [
            {"text": "1회초 삼성 공격", "class": "blue"},
            {"text": "타자 김하성 : 안타", "class": "normaiflTxt"},
            {"text": "타자 박병호 : 홈런", "class": "red"},
        ]
        rows = self.crawler._parse_relay_spans(raw_spans)
        assert len(rows) == 2
        assert rows[0].inning == 1
        assert rows[0].inning_half == "초"
        assert rows[0].at_bat_num == 1
        assert rows[0].batter_name == "김하성"
        assert rows[0].result == "안타"
        assert rows[1].at_bat_num == 2
        assert rows[1].batter_name == "박병호"
        assert rows[1].result == "홈런"

    def test_multiple_innings(self):
        raw_spans = [
            {"text": "1회초 삼성 공격", "class": "blue"},
            {"text": "타자 김하성 : 안타", "class": "normaiflTxt"},
            {"text": "1회말 LG 공격", "class": "blue"},
            {"text": "타자 이정후 : 삼진", "class": "normaiflTxt"},
            {"text": "2회초 삼성 공격", "class": "blue"},
            {"text": "타자 박병호 : 홈런", "class": "red"},
        ]
        rows = self.crawler._parse_relay_spans(raw_spans)
        assert len(rows) == 3
        assert rows[0].inning == 1
        assert rows[0].inning_half == "초"
        assert rows[1].inning == 1
        assert rows[1].inning_half == "말"
        assert rows[2].inning == 2
        assert rows[2].inning_half == "초"

    def test_noise_filtered(self):
        raw_spans = [
            {"text": "1회초 삼성 공격", "class": "blue"},
            {"text": "경기 시작", "class": "normaiflTxt"},
            {"text": "===================", "class": ""},
            {"text": "타자 김하성 : 안타", "class": "normaiflTxt"},
        ]
        rows = self.crawler._parse_relay_spans(raw_spans)
        assert len(rows) == 1
        assert rows[0].batter_name == "김하성"


class TestTextRelayCrawlerExtractRelayRows:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    async def test_extracts_rows(self):
        page = FakeRelayPage(
            [
                {"text": "1회초 삼성 공격", "class": "blue"},
                {"text": "타자 김하성 : 안타", "class": "normaiflTxt"},
            ],
        )
        rows = await self.crawler._extract_relay_rows(page)  # type: ignore[arg-type]
        assert len(rows) == 1
        assert rows[0].batter_name == "김하성"

    async def test_empty_page(self):
        page = FakeRelayPage([])
        rows = await self.crawler._extract_relay_rows(page)  # type: ignore[arg-type]
        assert rows == []

    async def test_reverse_chronological(self):
        page = FakeRelayPage(
            [
                {"text": "타자 박병호 : 아웃", "class": "normaiflTxt"},
                {"text": "타자 김하성 : 안타", "class": "normaiflTxt"},
                {"text": "1회초 삼성 공격", "class": "blue"},
            ],
        )
        rows = await self.crawler._extract_relay_rows(page)  # type: ignore[arg-type]
        assert len(rows) == 2
        assert rows[0].batter_name == "김하성"
        assert rows[1].batter_name == "박병호"


class TestTextRelayCrawlerIsAuthRedirect:
    def setup_method(self):
        self.crawler = TextRelayCrawler()

    def test_error_page(self):
        class FakePage:
            url = "https://www.koreabaseball.com/Error.html"

        assert self.crawler._is_auth_redirect(FakePage()) is True  # type: ignore[arg-type]

    def test_login_page(self):
        class FakePage:
            url = "https://www.koreabaseball.com/Login.aspx"

        assert self.crawler._is_auth_redirect(FakePage()) is True  # type: ignore[arg-type]

    def test_normal_page(self):
        class FakePage:
            url = "https://www.koreabaseball.com/Game/LiveText.aspx"

        assert self.crawler._is_auth_redirect(FakePage()) is False  # type: ignore[arg-type]


class TestTextRelayCrawlerOrchestration:
    def test_prepare_stops_when_compliance_rejects_url(self, monkeypatch):
        crawler = TextRelayCrawler()
        page = MagicMock()
        monkeypatch.setattr(text_relay_crawler.compliance, "is_allowed", AsyncMock(return_value=False))

        result = asyncio.run(crawler._prepare_live_text_page(page, "20260412", "https://example.test/relay"))

        assert result is False
        page.goto.assert_not_called()

    def test_prepare_warms_up_scoreboard_before_relay_navigation(self, monkeypatch):
        crawler = TextRelayCrawler()
        crawler.policy.delay_async = AsyncMock()
        page = MagicMock()
        page.goto = AsyncMock()
        monkeypatch.setattr(text_relay_crawler.compliance, "is_allowed", AsyncMock(return_value=True))
        monkeypatch.setattr(text_relay_crawler.asyncio, "sleep", AsyncMock())

        result = asyncio.run(crawler._prepare_live_text_page(page, "20260412", "https://example.test/relay"))

        assert result is True
        assert page.goto.await_count == 2
        assert page.goto.await_args_list[0].args[0].endswith("gameDate=20260412")
        assert page.goto.await_args_list[1].kwargs["referer"].endswith("gameDate=20260412")

    def test_wait_marks_no_data_page_as_empty(self):
        crawler = TextRelayCrawler()
        page = MagicMock()
        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        page.content = AsyncMock(return_value="데이터가 없습니다")

        assert asyncio.run(crawler._wait_for_relay_container(page, "game")) is False
        assert crawler.last_failure_reason == "empty"

    def test_wait_allows_non_empty_page_after_selector_timeout(self):
        crawler = TextRelayCrawler()
        page = MagicMock()
        page.wait_for_selector = AsyncMock(side_effect=TimeoutError())
        page.content = AsyncMock(return_value="temporary markup")

        assert asyncio.run(crawler._wait_for_relay_container(page, "game")) is True

    def test_extract_returns_empty_rows_when_evaluation_fails(self):
        crawler = TextRelayCrawler()
        page = MagicMock()
        page.evaluate = AsyncMock(side_effect=RuntimeError("page closed"))

        assert asyncio.run(crawler._extract_relay_rows(page)) == []

    def test_crawl_success_saves_and_releases_injected_pool(self, tmp_path):
        pool = MagicMock()
        page = MagicMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        crawler = TextRelayCrawler(pool=pool, output_dir=str(tmp_path))
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._wait_for_relay_container = AsyncMock(return_value=True)
        crawler._extract_relay_rows = AsyncMock(return_value=[RelayRow(inning=1, batter_name="김하성", result="안타")])
        crawler._is_auth_redirect = MagicMock(return_value=False)

        result = asyncio.run(crawler.crawl_game_relay("20260412SKLG0", save=True))

        assert result.status == "success"
        assert result.rows[0].batter_name == "김하성"
        assert (tmp_path / "20260412SKLG0_text_relay.csv").exists()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_awaited()

    def test_crawl_creates_and_closes_pool_when_compliance_blocks_page(self, monkeypatch):
        pool = MagicMock()
        page = MagicMock()
        pool.start = AsyncMock()
        pool.close = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        monkeypatch.setattr(text_relay_crawler, "AsyncPlaywrightPool", MagicMock(return_value=pool))
        crawler = TextRelayCrawler()
        crawler._prepare_live_text_page = AsyncMock(return_value=False)

        result = asyncio.run(crawler.crawl_game_relay("20260412SKLG0"))

        assert result.status == "blocked"
        assert result.error_message == "compliance_blocked"
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_awaited_once()

    def test_crawl_retries_after_auth_redirect(self):
        pool = MagicMock()
        initial_page = MagicMock()
        retry_page = MagicMock()
        pool.acquire = AsyncMock(side_effect=[initial_page, retry_page])
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        pool.start = AsyncMock()
        crawler = TextRelayCrawler(pool=pool)
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._wait_for_relay_container = AsyncMock(return_value=True)
        crawler._extract_relay_rows = AsyncMock(return_value=[RelayRow(result="안타")])
        crawler._is_auth_redirect = MagicMock(side_effect=[True, False])

        result = asyncio.run(crawler.crawl_game_relay("20260412SKLG0"))

        assert result.status == "success"
        assert pool.acquire.await_count == 2
        pool.close.assert_awaited_once()
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(retry_page)

    def test_crawl_marks_empty_when_no_relay_container_exists(self):
        pool = MagicMock()
        page = MagicMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        crawler = TextRelayCrawler(pool=pool)
        crawler._prepare_live_text_page = AsyncMock(return_value=True)
        crawler._wait_for_relay_container = AsyncMock(return_value=False)
        crawler._is_auth_redirect = MagicMock(return_value=False)

        result = asyncio.run(crawler.crawl_game_relay("20260412SKLG0"))

        assert result.status == "empty"
        assert result.error_message == "no_data"

    def test_crawl_marks_pool_errors(self):
        pool = MagicMock()
        pool.acquire = AsyncMock(side_effect=RuntimeError("pool unavailable"))
        crawler = TextRelayCrawler(pool=pool)

        result = asyncio.run(crawler.crawl_game_relay("20260412SKLG0"))

        assert result.status == "error"
        assert result.error_message == "crawl_exception"
        assert crawler.last_failure_reason == "error"

    def test_crawl_games_returns_each_game_result(self):
        crawler = TextRelayCrawler()
        crawler.crawl_game_relay = AsyncMock(
            side_effect=[
                RelayCrawlResult("G1", "20260412", status="success"),
                RelayCrawlResult("G2", "20260412", status="empty"),
            ],
        )

        results = asyncio.run(crawler.crawl_games(["G1", "G2"], save=True))

        assert [result.status for result in results] == ["success", "empty"]
        assert crawler.crawl_game_relay.await_args_list[0].kwargs["save"] is True

    def test_convenience_functions_return_successful_data_and_paths(self, tmp_path):
        successful = RelayCrawlResult(
            "G1",
            "20260412",
            rows=[RelayRow(inning=1, batter_name="김하성", result="안타")],
            status="success",
        )
        failed = RelayCrawlResult("G2", "20260412", status="empty", error_message="no_data")
        successful.save_csv = MagicMock(return_value=tmp_path / "G1_text_relay.csv")

        with patch("src.crawlers.text_relay_crawler.TextRelayCrawler") as crawler_class:
            crawler = crawler_class.return_value
            crawler.crawl_game_relay = AsyncMock(side_effect=[successful, failed])
            crawler.crawl_games = AsyncMock(return_value=[successful, failed])

            dataframe = asyncio.run(crawl_text_relay("G1", output_dir=str(tmp_path)))
            no_dataframe = asyncio.run(crawl_text_relay("G2", output_dir=str(tmp_path)))
            saved_paths = asyncio.run(crawl_text_relays(["G1", "G2"], output_dir=str(tmp_path)))

        assert dataframe is not None
        assert dataframe.iloc[0]["타자명"] == "김하성"
        assert no_dataframe is None
        assert saved_paths == [str(tmp_path / "G1_text_relay.csv")]
        successful.save_csv.assert_called_once_with(str(tmp_path))
