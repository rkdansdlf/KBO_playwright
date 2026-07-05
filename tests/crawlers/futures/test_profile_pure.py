"""Pure parser tests for futures profile crawler."""

from bs4 import BeautifulSoup

from src.crawlers.futures.profile import FuturesProfileCrawler


def test_parse_table_with_bs4_caption_headers_and_rows():
    soup = BeautifulSoup(
        """
        <table summary="타자 기록">
          <caption>퓨처스 타자</caption>
          <thead><tr><th>연도</th><th>팀</th><th>타율</th></tr></thead>
          <tbody>
            <tr><td>2026</td><td>두산</td><td>.333</td></tr>
            <tr><td></td><td></td><td></td></tr>
          </tbody>
        </table>
        """,
        "lxml",
    )

    parsed = FuturesProfileCrawler(request_delay=0)._parse_table_with_bs4(soup.table)

    assert parsed == {
        "caption": "퓨처스 타자",
        "summary": "타자 기록",
        "headers": ["연도", "팀", "타율"],
        "rows": [["2026", "두산", ".333"]],
    }


def test_parse_table_with_bs4_promotes_first_row_to_headers():
    soup = BeautifulSoup(
        """
        <table>
          <tr><th>연도</th><th>팀</th></tr>
          <tr><td>2025</td><td>한화</td></tr>
        </table>
        """,
        "lxml",
    )

    parsed = FuturesProfileCrawler(request_delay=0)._parse_table_with_bs4(soup.table)

    assert parsed is not None
    assert parsed["headers"] == ["연도", "팀"]
    assert parsed["rows"] == [["2025", "한화"]]


def test_parse_table_with_bs4_returns_none_for_empty_table():
    soup = BeautifulSoup("<table><tbody><tr><td></td></tr></tbody></table>", "lxml")

    parsed = FuturesProfileCrawler(request_delay=0)._parse_table_with_bs4(soup.table)

    assert parsed is None


def test_extract_known_futures_tables_marks_hitter_and_pitcher():
    soup = BeautifulSoup(
        """
        <table id="tblHitterRecord"><thead><tr><th>연도</th></tr></thead><tbody><tr><td>2026</td></tr></tbody></table>
        <table id="tblPitcherRecord"><thead><tr><th>연도</th></tr></thead><tbody><tr><td>2025</td></tr></tbody></table>
        """,
        "lxml",
    )

    tables = FuturesProfileCrawler(request_delay=0)._extract_known_futures_tables(soup)

    assert [table["_table_type"] for table in tables] == ["HITTER", "PITCHER"]
    assert tables[0]["rows"] == [["2026"]]
    assert tables[1]["rows"] == [["2025"]]


def test_extract_fallback_futures_tables_from_futures_divs():
    soup = BeautifulSoup(
        """
        <div id="PlayerFuturesStats">
          <table><thead><tr><th>구분</th></tr></thead><tbody><tr><td>퓨처스</td></tr></tbody></table>
        </div>
        <div id="RegularStats">
          <table><thead><tr><th>구분</th></tr></thead><tbody><tr><td>정규</td></tr></tbody></table>
        </div>
        """,
        "lxml",
    )

    tables = FuturesProfileCrawler(request_delay=0)._extract_fallback_futures_tables(soup)

    assert len(tables) == 1
    assert tables[0]["rows"] == [["퓨처스"]]
