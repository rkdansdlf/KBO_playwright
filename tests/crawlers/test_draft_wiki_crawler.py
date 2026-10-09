"""Unit tests for the Wikipedia draft crawler (offline fixtures)."""

from __future__ import annotations

from src.crawlers.draft_wiki_crawler import (
    DraftWikiCrawler,
    _is_first_pick_table,
    _is_round_table,
    _round_order_even_reversed,
    parse_draft_cell,
)

HEADERS = ["순위", "한화", "삼성"]


def test_parse_draft_cell_full() -> None:
    """Cell with school, position, and fee parses into fields."""
    parsed = parse_draft_cell("황준서(장충고,투수)(계약금:3억 5천만원)")
    assert parsed is not None
    assert parsed["player_name"] == "황준서"
    assert parsed["school"] == "장충고"
    assert parsed["position"] == "투수"
    assert parsed["sign_fee"] == "3억 5천만원"


def test_parse_draft_cell_empty() -> None:
    """Placeholder cells return None."""
    assert parse_draft_cell("-") is None
    assert parse_draft_cell("") is None


def test_round_order_snake() -> None:
    """Even rounds reverse the column order."""
    assert _round_order_even_reversed(1, 3) == [0, 1, 2]
    assert _round_order_even_reversed(2, 3) == [2, 1, 0]


def test_table_detection() -> None:
    """Round tables and 1차 tables are distinguished."""
    wide = ["순위", "한화", "삼성", "롯데", "두산", "NC"]
    assert _is_round_table(wide, [["1", "a", "b", "c", "d", "e"]])
    assert not _is_round_table(["구단", "학교"], [["x"]])
    assert _is_first_pick_table(["구단", "학교", "포지션"])
    assert not _is_first_pick_table(wide)


def test_parse_season_records_resolves_teams() -> None:
    """Records carry season/type/round/pick and resolved team codes."""
    crawler = DraftWikiCrawler()
    rows = [["1", "김선수(서울고,투수)(계약금:1억원)", "이선수(부산고,포수)"]]
    records = crawler.parse_season_records(
        2024, HEADERS, rows, team_resolver=lambda name, season: {"한화": "HH", "삼성": "SS"}.get(name)
    )
    assert len(records) == 2
    assert records[0]["team_code"] == "HH"
    assert records[0]["round_num"] == 1
    assert records[0]["pick_seq"] == 1
    assert records[1]["pick_seq"] == 2
    assert records[0]["draft_type"] == "2차"


def test_parse_first_pick_records() -> None:
    """1차 rows map team/school/position/name/fee columns."""
    crawler = DraftWikiCrawler()
    rows = [["삼성 라이온즈", "경북고", "투수", "박선수", "3억원"]]
    records = crawler.parse_first_pick_records(2024, rows)
    assert len(records) == 1
    assert records[0]["draft_type"] == "1차"
    assert records[0]["player_name"] == "박선수"
    assert records[0]["school"] == "경북고"
