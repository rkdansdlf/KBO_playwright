"""Unit tests for src.crawlers.award_crawler.

Covers wikipedia table rendering/parsing (MVP style + positional),
yagoonara parsing, award-label mapping, dedup, save idempotency and
crawl/resilience paths. No network calls except the integration test.

"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from bs4 import BeautifulSoup

from src.crawlers.award_crawler import (
    AwardCrawler,
    AwardRecord,
    _extract_year,
    _split_pairs,
)


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


MVP_HTML = """
<table class="wikitable">
<tr><th>연도</th><th>수상자</th><th>소속 구단</th><th>포지션</th><th>수상 성적</th><th>득표</th></tr>
<tr><td>1982</td><td>최동원</td><td>OB 베어스</td><td>투수</td><td>다승 1위</td><td></td></tr>
<tr><td>1984</td><td>(故)유두열</td><td>청보 핀토스</td><td>외야수</td><td></td><td></td></tr>
<tr><td>1987</td><td>장효조</td><td>해태 타이거즈</td><td>삼성 라이온즈</td><td>타격 1위</td><td></td></tr>
</table>
"""

ROOKIE_HTML = """
<table class="wikitable">
  <tr><th>연도</th><th>수상자</th><th>소속 구단</th></tr>
  <tr><td>1990</td><td>박충식</td><td>쌍방울 레이더스</td></tr>
  <tr><td>2005</td><td>오태민</td><td>삼성 라이온즈</td></tr>
</table>
"""

ROWSPAN_CHAIN_HTML = """
<table class="wikitable">
  <tr><th>연도</th><th>수상자</th><th>소속 구단</th><th>포지션</th><th>수상 성적</th><th>득표</th></tr>
  <tr><td>1988</td><td>김성한</td><td rowspan="3">해태 타이거즈</td><td>1루수</td><td>성적88</td><td>비고88</td></tr>
  <tr><td>1989</td><td rowspan="2">선동열</td><td rowspan="2">투수</td><td>성적89</td><td>비고89</td></tr>
  <tr><td>1990</td><td>성적90</td><td>비고90</td></tr>
  <tr><td>2001</td><td rowspan="3">이승엽</td><td rowspan="4">삼성 라이온즈</td><td rowspan="3">1루수</td><td>성적01</td><td>비고01</td></tr>
  <tr><td>2002</td><td>성적02</td><td>비고02</td></tr>
  <tr><td>2003</td><td>성적03</td><td>비고03</td></tr>
  <tr><td>2004</td><td>배영수</td><td>투수</td><td>성적04</td><td>비고04</td></tr>
</table>
"""

GG_HTML = """
<table class="wikitable">
  <tr><th>연도</th><th>1루수</th><th>외야수</th><th>투수</th></tr>
  <tr><td rowspan="2">1998</td><td>이승엽(삼성)</td><td>박재홍(현대), 전준호(현대)</td><td>-</td></tr>
  <tr><td>김기태(쌍방울)</td><td>김선민(OB)</td><td>—</td></tr>
  <tr><td>1999</td><td>이승엽(삼성)</td><td>이병현(LG)</td><td>정민철(한화)</td></tr>
</table>
"""

DEFENSE_HTML = """
<table class="wikitable">
  <tr><th>연도</th><th>포수</th><th>유격수</th></tr>
  <tr><td>2023</td><td>박동원(LG)</td><td>-</td></tr>
  <tr><td>2024</td><td>박동원(LG)</td><td>박찬호(KIA)</td></tr>
</table>
"""

YAGOO_HTML = """
<html><body>
<h2>2024 시즌 수상 내역</h2>
<table>
  <tr><th>수상</th><th>선수</th><th>팀</th><th>포지션</th></tr>
  <tr><td>올스타 MVP</td><td>김광현 영상</td><td>SSG</td><td>투수</td></tr>
  <tr><td>한국시리즈 MVP</td><td>김재환</td><td>두산</td><td>외야수</td></tr>
  <tr><td>골든글러브</td><td>김도영</td><td>KIA</td><td>3루수</td></tr>
  <tr><td>신인상</td><td>홍원빈</td><td>한화</td><td>내야수</td></tr>
  <tr><td>부문별 시상</td><td>아무개</td><td>LG</td><td>-</td></tr>
</table>
</body></html>
"""


class TestExtractYear:
    def test_four_digit_year(self) -> None:
        assert _extract_year("1982") == 1982

    def test_year_inside_text(self) -> None:
        assert _extract_year("2024시즌 수상") == 2024

    def test_no_year_returns_none(self) -> None:
        assert _extract_year("수상 내역") is None


class TestSplitPairs:
    def test_single_pair(self) -> None:
        assert _split_pairs("이승엽(삼성)") == [("이승엽", "삼성")]

    def test_multiple_pairs_in_cell(self) -> None:
        assert _split_pairs("박재홍(현대), 전준호(현대)") == [("박재홍", "현대"), ("전준호", "현대")]

    def test_plain_text_returns_self(self) -> None:
        assert _split_pairs("이승엽") == [("이승엽", "")]

    def test_empty_cell(self) -> None:
        assert _split_pairs("") == []

    def test_team_with_parenthesis_kept_inside(self) -> None:
        assert _split_pairs("김도영(KIA)") == [("김도영", "KIA")]


class TestRenderTable:
    def test_renders_headers_and_rows(self) -> None:
        headers, rows = AwardCrawler._render_table(_soup(GG_HTML).find("table"))
        assert headers == ["연도", "1루수", "외야수", "투수"]
        assert rows[0][0] == "1998"
        assert rows[2][1] == "이승엽(삼성)"

    def test_rowspan_propagates_year(self) -> None:
        _, rows = AwardCrawler._render_table(_soup(GG_HTML).find("table"))
        assert rows[1][0] == "1998"

    def test_multi_pair_cell_kept_whole(self) -> None:
        _, rows = AwardCrawler._render_table(_soup(GG_HTML).find("table"))
        assert rows[0][2] == "박재홍(현대), 전준호(현대)"

    def test_empty_header_row_falls_back_to_coln(self) -> None:
        html = "<table><tr></tr><tr><td>a</td><td>b</td></tr></table>"
        headers, rows = AwardCrawler._render_table(_soup(html).find("table"))
        assert headers == ["col0", "col1"]
        assert rows == [["a", "b"]]

    def test_no_rows_returns_empty(self) -> None:
        assert AwardCrawler._render_table(_soup("<table></table>").find("table")) == ([], [])

    def test_short_rows_padded(self) -> None:
        html = "<table><tr><th>a</th><th>b</th></tr><tr><td>x</td></tr></table>"
        _, rows = AwardCrawler._render_table(_soup(html).find("table"))
        assert rows == [["x", ""]]

    def test_column_index_substring_match(self) -> None:
        headers = ["연도", "수상자", "소속 구단"]
        assert AwardCrawler._column_index(headers, "소속", "팀") == 2
        assert AwardCrawler._column_index(headers, "포지션") == -1

    def test_chained_rowspans_fill_every_covered_row(self) -> None:
        _, rows = AwardCrawler._render_table(_soup(ROWSPAN_CHAIN_HTML).find("table"))
        assert rows == [
            ["1988", "김성한", "해태 타이거즈", "1루수", "성적88", "비고88"],
            ["1989", "선동열", "해태 타이거즈", "투수", "성적89", "비고89"],
            ["1990", "선동열", "해태 타이거즈", "투수", "성적90", "비고90"],
            ["2001", "이승엽", "삼성 라이온즈", "1루수", "성적01", "비고01"],
            ["2002", "이승엽", "삼성 라이온즈", "1루수", "성적02", "비고02"],
            ["2003", "이승엽", "삼성 라이온즈", "1루수", "성적03", "비고03"],
            ["2004", "배영수", "삼성 라이온즈", "투수", "성적04", "비고04"],
        ]

    def test_chained_rowspans_last_covered_row_keeps_cells(self) -> None:
        _, rows = AwardCrawler._render_table(_soup(ROWSPAN_CHAIN_HTML).find("table"))
        rowspan3_team = rows[2]
        assert rowspan3_team[2] == "해태 타이거즈"
        rowspan4_team = rows[6]
        assert rowspan4_team[2] == "삼성 라이온즈"
        assert rowspan4_team[1] == "배영수"
        assert rowspan4_team[3] == "투수"


class TestParseMvpStyleWiki:
    def test_parses_mvp_rows_with_categories(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(MVP_HTML), "MVP")
        assert len(records) == 3
        first, _, third = records
        assert first.year == 1982
        assert first.player_name == "최동원"
        assert first.team_name == "OB 베어스"
        assert first.category == "투수"
        assert first.award_type == "MVP"
        assert third.year == 1987
        assert third.player_name == "장효조"
        assert third.team_name == "해태 타이거즈"

    def test_1987_position_column_kept_as_category_source_faithful(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(MVP_HTML), "MVP")
        assert records[2].category == "삼성 라이온즈"

    def test_deceased_prefix_stripped(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(MVP_HTML), "MVP")
        assert records[1].player_name == "유두열"

    def test_no_position_column_yields_null_category(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(ROOKIE_HTML), "신인상")
        assert len(records) == 2
        assert records[0].category is None
        assert records[1].player_name == "오태민"

    def test_missing_player_cell_skipped(self) -> None:
        html = """
        <table class="wikitable">
          <tr><th>연도</th><th>수상자</th></tr>
          <tr><td>1990</td><td></td></tr>
          <tr><td>1991</td><td>김선우</td></tr>
        </table>
        """
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(html), "MVP")
        assert len(records) == 1
        assert records[0].player_name == "김선우"

    def test_no_wikitable_returns_empty(self) -> None:
        assert AwardCrawler()._parse_mvp_style_wiki(_soup("<html></html>"), "MVP") == []

    def test_year_column_missing_falls_back_to_first_column(self) -> None:
        html = """<table class="wikitable">
          <tr><th>시즌</th><th>수상자</th></tr>
          <tr><td>2001</td><td>이승엽</td></tr>
        </table>"""
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(html), "MVP")
        assert records[0].year == 2001

    def test_chained_rowspans_keep_player_and_team_columns(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(ROWSPAN_CHAIN_HTML), "MVP")
        by_year = {r.year: r for r in records}
        assert by_year[1989].player_name == "선동열"
        assert by_year[1989].team_name == "해태 타이거즈"
        assert by_year[1990].player_name == "선동열"
        assert by_year[1990].team_name == "해태 타이거즈"
        assert by_year[1990].category == "투수"
        assert by_year[2003].player_name == "이승엽"
        assert by_year[2003].team_name == "삼성 라이온즈"
        assert by_year[2004].player_name == "배영수"
        assert by_year[2004].team_name == "삼성 라이온즈"
        assert by_year[2004].category == "투수"

    def test_chained_rowspans_repeat_player_into_covered_years(self) -> None:
        records = AwardCrawler()._parse_mvp_style_wiki(_soup(ROWSPAN_CHAIN_HTML), "MVP")
        years = {r.year for r in records}
        assert years == {1988, 1989, 1990, 2001, 2002, 2003, 2004}
        rookie_years = {r.year for r in records if r.player_name == "선동열"}
        assert rookie_years == {1989, 1990}
        triple_years = {r.year for r in records if r.player_name == "이승엽"}
        assert triple_years == {2001, 2002, 2003}


class TestParsePositionalWiki:
    def test_pairs_in_multiple_columns(self) -> None:
        records = AwardCrawler()._parse_positional_wiki(_soup(GG_HTML), "골든글러브")
        assert len(records) == 8

    def test_year_rowspan_applies_to_second_row(self) -> None:
        records = AwardCrawler()._parse_positional_wiki(_soup(GG_HTML), "골든글러브")
        second_row_records = [r for r in records if r.player_name == "김기태"]
        assert len(second_row_records) == 1
        assert second_row_records[0].year == 1998

    def test_dashes_skipped(self) -> None:
        records = AwardCrawler()._parse_positional_wiki(_soup(DEFENSE_HTML), "수비상")
        assert ("2023", "포수", "박동원") in {(str(r.year), r.category, r.player_name) for r in records}
        assert len([r for r in records if r.year == 2023]) == 1

    def test_category_from_header_label(self) -> None:
        records = AwardCrawler()._parse_positional_wiki(_soup(DEFENSE_HTML), "수비상")
        assert all(r.category == "유격수" for r in records if r.player_name == "박찬호")

    def test_no_year_row_skipped(self) -> None:
        html = """<table class="wikitable">
          <tr><th>연도</th><th>1루수</th></tr>
          <tr><td>수상자 없음</td><td>이승엽(삼성)</td></tr>
        </table>"""
        assert AwardCrawler()._parse_positional_wiki(_soup(html), "골든글러브") == []

    def test_coln_fallback_headers_ignored(self) -> None:
        html = "<table><tr></tr><tr><td>1998</td><td>이승엽(삼성)</td></tr></table>"
        assert AwardCrawler()._parse_positional_wiki(_soup(html), "골든글러브") == []

    def test_parse_wiki_soup_routes_positional_types(self) -> None:
        crawler = AwardCrawler()
        assert len(crawler._parse_wiki_soup(_soup(DEFENSE_HTML), "수비상")) == 3
        assert len(crawler._parse_wiki_soup(_soup(ROOKIE_HTML), "신인상")) == 2


class TestParseYagoonara:
    def test_detects_year_from_preceding_heading(self) -> None:
        soup = _soup(YAGOO_HTML)
        table = soup.find("table")
        assert AwardCrawler._detect_yagoonara_year(table) == 2024

    def test_detect_year_stops_at_h2_boundary(self) -> None:
        html = """<div>2023년 기록 백과</div><h2>수상 내역</h2><table></table>"""
        soup = BeautifulSoup(html, "html.parser")
        # year above the h2 must not be reached
        assert AwardCrawler._detect_yagoonara_year(soup.find("table")) is None

    def test_detect_year_returns_none_when_no_heading(self) -> None:
        html = "<div>값</div><table></table>"
        soup = BeautifulSoup(html, "html.parser")
        # find_previous walks past div; no year anywhere -> None
        assert AwardCrawler._detect_yagoonara_year(soup.find("table")) is None

    def test_parse_yagoonara_full(self) -> None:
        records = AwardCrawler()._parse_yagoonara(BeautifulSoup(YAGOO_HTML, "html.parser"))
        assert len(records) == 4
        by_player = {r.player_name: r for r in records}
        assert by_player["김광현"].award_type == "올스타전MVP"
        assert by_player["김광현"].team_name == "SSG 랜더스"
        assert by_player["김재환"].award_type == "한국시리즈MVP"
        assert by_player["김재환"].team_name == "두산 베어스"
        assert by_player["김도영"].award_type == "골든글러브"
        assert by_player["김도영"].category == "3루수"
        assert by_player["김도영"].team_name == "KIA 타이거즈"
        assert by_player["홍원빈"].award_type == "신인상"
        assert by_player["홍원빈"].team_name == "한화 이글스"
        assert all(r.year == 2024 for r in records)

    def test_video_suffix_stripped_and_unknown_skipped(self) -> None:
        records = AwardCrawler()._parse_yagoonara(BeautifulSoup(YAGOO_HTML, "html.parser"))
        assert "김광현" in {r.player_name for r in records}
        assert all("영상" not in r.player_name for r in records)
        assert all(r.player_name != "누군가" for r in records)

    def test_unrelated_table_ignored(self) -> None:
        html = "<table><tr><th>이름</th><th>값</th></tr><tr><td>a</td><td>1</td></tr></table>"
        assert AwardCrawler()._parse_yagoonara(BeautifulSoup(html, "html.parser")) == []

    def test_short_rows_skipped(self) -> None:
        html = """
        <h2>2024 시즌</h2>
        <table>
          <tr><th>수상</th><th>선수</th><th>팀</th><th>포지션</th></tr>
          <tr><td>MVP</td><td></td><td>LG</td><td>투수</td></tr>
          <tr><td>MVP</td><td>김동주</td><td>LG</td><td>투수</td></tr>
        </table>
        """
        records = AwardCrawler()._parse_yagoonara(BeautifulSoup(html, "html.parser"))
        assert len(records) == 1
        assert records[0].player_name == "김동주"


class TestMapYagoonaraAward:
    @pytest.mark.parametrize(
        ("label", "position", "expected"),
        [
            ("골든글러브", "1루수", ("골든글러브", "1루수")),
            ("골든글러브", "", ("골든글러브", None)),
            ("수비상", "외야수", ("수비상", "외야수")),
            ("올스타 MVP", "-", ("올스타전MVP", None)),
            ("한국시리즈 MVP", "외야수", ("한국시리즈MVP", None)),
            ("MVP", "-", ("MVP", None)),
            ("신인상", "-", ("신인상", None)),
            ("부문별 시상", "-", (None, None)),
        ],
    )
    def test_mapping(self, label: str, position: str, expected: tuple[str | None, str | None]) -> None:
        assert AwardCrawler._map_yagoonara_award(label, position) == expected


class TestDedup:
    def test_duplicates_removed(self) -> None:
        dups = [
            AwardRecord(2024, "MVP", None, "김영웅", "LG 트윈스"),
            AwardRecord(2024, "MVP", None, "김영웅", "LG 트윈스"),
            AwardRecord(2024, "MVP", "투수", "김영웅", "LG 트윈스"),
            AwardRecord(2023, "MVP", None, "이정후", "키움 히어로즈"),
        ]
        result = AwardCrawler()._dedup(dups)
        assert len(result) == 3

    def test_empty_player_skipped(self) -> None:
        records = [AwardRecord(2024, "MVP", None, "", "LG 트윈스")]
        assert AwardCrawler()._dedup(records) == []


class TestWikiFetchSnapshot:
    @pytest.mark.asyncio
    async def test_wiki_fetch_stores_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = AwardCrawler()
        crawler._fetch = AsyncMock(return_value=('{"parse":{"text":{"*":"<p>hi</p>"}}}', 200))
        soup = await crawler._fetch_wiki_page("KBO MVP")
        assert soup is not None
        assert len(crawler._raw_snapshots) == 1
        assert crawler._raw_snapshots[0]["source_key"] == "kbo_awards_wikipedia"
        assert crawler._raw_snapshots[0]["status_code"] == 200
        assert "page=KBO MVP" in crawler._raw_snapshots[0]["url"]

    def test_parse_status_is_attached_to_matching_snapshot(self) -> None:
        crawler = AwardCrawler()
        crawler._raw_snapshots = [
            {
                "url": "https://example.com/awards",
                "html": "<html></html>",
                "source_key": "kbo_awards_yagoonara",
            },
        ]

        crawler._mark_snapshot_parse_status(
            "kbo_awards_yagoonara",
            "https://example.com/awards",
            parsed_records=12,
        )

        assert crawler._raw_snapshots[0]["parse_status"] == "done"
        assert crawler._raw_snapshots[0]["parser_version"] == "award-crawler-v1"
        assert crawler._raw_snapshots[0]["capture_metadata"] == {"parsed_records": 12}

    @pytest.mark.asyncio
    async def test_yagoonara_fetch_stores_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = AwardCrawler()
        crawler._fetch = AsyncMock(return_value=("<html></html>", 200))
        await crawler._fetch_yagoonara()
        assert len(crawler._raw_snapshots) == 1
        assert crawler._raw_snapshots[0]["source_key"] == "kbo_awards_yagoonara"
        assert crawler._raw_snapshots[0]["status_code"] == 200


class TestSave:
    def _records(self) -> list[AwardRecord]:
        return [
            AwardRecord(2024, "MVP", None, "김영지", "LG 트윈스"),
            AwardRecord(2024, "MVP", None, "김영지", "LG 트윈스"),
        ]

    def _fake_repo(self, existing_ids: set[tuple[int, str]]) -> type:
        class FakeRepo:
            def __init__(self, session: object) -> None:
                self.calls: list[dict] = []
                self.session = session

            def save_award(self, award_data: dict) -> object:
                self.calls.append(award_data)
                if (award_data["year"], award_data["player_name"]) in existing_ids:
                    return SimpleNamespace(id=7)
                return SimpleNamespace(id=None)

        return FakeRepo

    @pytest.mark.asyncio
    async def test_save_counts_new_and_existing(self) -> None:
        crawler = AwardCrawler()
        session = MagicMock()
        session_cm = MagicMock()
        session_cm.__enter__.return_value = session
        session_cm.__exit__.return_value = False
        existing_ids = {(2024, "김영지")}
        fake = self._fake_repo(existing_ids)

        with (
            patch("src.crawlers.award_crawler.SessionLocal", return_value=session_cm),
            patch("src.crawlers.award_crawler.AwardRepository", side_effect=fake),
        ):
            records = [
                AwardRecord(2024, "MVP", None, "김영지", "LG 트윈스"),
                AwardRecord(2024, "MVP", None, "박찬호", "한화 이글스"),
            ]
            saved, skipped = await crawler.save(records)

        assert saved == 1
        assert skipped == 1
        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_rolls_back_on_error(self) -> None:
        session = MagicMock()
        session_cm = MagicMock()
        session_cm.__enter__.return_value = session
        session_cm.__exit__.return_value = False

        class ExplodingRepo:
            def __init__(self, session: object) -> None:
                pass

            def save_award(self, award_data: dict) -> object:
                raise RuntimeError("db down")

        with (
            patch("src.crawlers.award_crawler.SessionLocal", return_value=session_cm),
            patch("src.crawlers.award_crawler.AwardRepository", ExplodingRepo),
        ):
            saved, skipped = await AwardCrawler().save([AwardRecord(2024, "MVP", None, "김영지", "LG 트윈스")])

        assert (saved, skipped) == (0, 0)
        session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_persists_raw_snapshots_and_clears(self) -> None:
        crawler = AwardCrawler()
        crawler._raw_snapshots = [
            {"url": "https://example.com/a", "html": "<p>a</p>", "source_key": "kbo_awards_wikipedia"},
            {"url": "https://example.com/b", "html": "<p>b</p>", "source_key": "kbo_awards_yagoonara"},
        ]
        session = MagicMock()
        session_cm = MagicMock()
        session_cm.__enter__.return_value = session
        session_cm.__exit__.return_value = False

        with (
            patch("src.crawlers.award_crawler.SessionLocal", return_value=session_cm),
            patch("src.crawlers.award_crawler.AwardRepository", self._fake_repo(set())),
            patch("src.crawlers.award_crawler.save_raw_snapshots", return_value=2),
        ):
            saved, skipped = await crawler.save(self._records())

        assert (saved, skipped) == (2, 0)
        assert crawler._raw_snapshots == []


class TestCrawlOrchestration:
    def _crawler(self) -> AwardCrawler:
        crawler = AwardCrawler()
        crawler.policy = SimpleNamespace(delay_async=AsyncMock())
        return crawler

    def _soups(self) -> dict[str, BeautifulSoup]:
        return {
            "KBO MVP": _soup(MVP_HTML),
            "KBO 신인상": _soup(ROOKIE_HTML),
            "KBO 골든글러브": _soup(GG_HTML),
            "KBO 수비상": _soup(DEFENSE_HTML),
        }

    @pytest.mark.asyncio
    async def test_crawl_all_sources_and_delays(self) -> None:
        crawler = self._crawler()
        soups = self._soups()
        crawler._fetch_wiki_page = AsyncMock(side_effect=lambda title: soups[title])
        crawler._fetch_yagoonara = AsyncMock(return_value=BeautifulSoup(YAGOO_HTML, "html.parser"))

        records = await crawler.crawl()

        assert len(records) == 3 + 2 + 8 + 3 + 4
        assert crawler.policy.delay_async.await_count == 5
        assert {r.award_type for r in records} == {
            "MVP",
            "신인상",
            "골든글러브",
            "수비상",
            "올스타전MVP",
            "한국시리즈MVP",
        }
        assert len(crawler.source_runs) == 5
        assert sum(run.parsed_records for run in crawler.source_runs) == len(records)

    @pytest.mark.asyncio
    async def test_wiki_failure_skipped_other_sources_continue(self) -> None:
        crawler = self._crawler()
        soups = self._soups()

        async def fake_fetch(title: str) -> BeautifulSoup:
            if title == "KBO 신인상":
                raise ValueError("boom")
            return soups[title]

        crawler._fetch_wiki_page = AsyncMock(side_effect=fake_fetch)
        crawler._fetch_yagoonara = AsyncMock(return_value=BeautifulSoup(YAGOO_HTML, "html.parser"))

        records = await crawler.crawl()

        assert len(records) == 3 + 8 + 3 + 4
        assert crawler.policy.delay_async.await_count == 5
        assert any(run.error for run in crawler.source_runs)

    @pytest.mark.asyncio
    async def test_yagoonara_failure_warns_and_keeps_wiki(self) -> None:
        crawler = self._crawler()
        soups = self._soups()
        crawler._fetch_wiki_page = AsyncMock(side_effect=lambda title: soups[title])
        crawler._fetch_yagoonara = AsyncMock(side_effect=httpx.ConnectError("offline"))

        records = await crawler.crawl()

        assert len(records) == 3 + 2 + 8 + 3

    @pytest.mark.asyncio
    async def test_crawl_type_filter_limits_wiki_and_yagoonara(self) -> None:
        crawler = self._crawler()
        soups = self._soups()
        crawler._fetch_wiki_page = AsyncMock(side_effect=lambda title: soups[title])
        crawler._fetch_yagoonara = AsyncMock(return_value=BeautifulSoup(YAGOO_HTML, "html.parser"))

        records = await crawler.crawl(types={"MVP"})

        assert crawler._fetch_wiki_page.await_count == 1
        assert len(records) == 3
        assert all(r.award_type == "MVP" for r in records)
        assert crawler.policy.delay_async.await_count == 2

    @pytest.mark.asyncio
    async def test_run_passes_types_to_crawl(self) -> None:
        crawler = self._crawler()
        crawler.crawl = AsyncMock(return_value=[])  # type: ignore[method-assign]
        await crawler.run(save=False, types={"MVP"})
        crawler.crawl.assert_awaited_once_with(types={"MVP"})

    @pytest.mark.asyncio
    async def test_run_dry_run_returns_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = self._crawler()
        crawler._fetch_wiki_page = AsyncMock(side_effect=lambda title: _soup("<html></html>"))
        crawler._fetch_yagoonara = AsyncMock(side_effect=httpx.ConnectError("offline"))
        count = await crawler.run(save=False)
        assert count == 0

    @pytest.mark.asyncio
    async def test_run_with_save_calls_save(self, monkeypatch: pytest.MonkeyPatch) -> None:
        crawler = self._crawler()
        crawler.crawl = AsyncMock(return_value=[AwardRecord(2024, "MVP", None, "김영지", "LG 트윈스")])  # type: ignore[method-assign]
        crawler.save = AsyncMock(return_value=(1, 0))  # type: ignore[method-assign]
        count = await crawler.run(save=True)
        assert count == 1
        crawler.save.assert_awaited_once()


@pytest.mark.integration
class TestLiveAwardCrawler:
    @pytest.mark.asyncio
    async def test_live_crawl_counts(self) -> None:
        crawler = AwardCrawler()
        try:
            records = await crawler.crawl()
        except httpx.HTTPError as err:
            pytest.skip(f"Live external award source unavailable: {err}")
        finally:
            await crawler.close()
        if not records:
            pytest.skip("Live awards crawl returned empty (offline/firewall)")
        assert len(records) >= 380
        by_type = {r.award_type for r in records}
        assert {"MVP", "신인상", "골든글러브", "수비상"} <= by_type
        years = {r.year for r in records}
        assert 1982 in years
        assert max(years) >= 2024
