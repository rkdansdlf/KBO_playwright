
from src.parsers.retired_player_parser import (
    _apply_stat,
    _clean_header,
    _cleanup_consumed,
    _merge_extra_stats,
    _select_tables,
    _table_to_dicts,
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)
from src.utils.type_helpers import safe_int_or_none


class TestCleaners:
    def test_clean_header(self):
        assert _clean_header("  연도 ") == "연도"
        assert _clean_header(" 출루율 ") == "출루율"
        assert _clean_header("") == ""
        assert _clean_header(None) == ""


class TestTableToDicts:
    def test_normal_table(self):
        table = {
            "headers": ["연도", "팀명", "경기"],
            "rows": [
                ["2020", "두산", "100"],
                ["2021", "두산", "120"],
            ],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["연도", "팀명", "경기"]
        assert len(rows) == 2
        assert rows[0]["연도"] == "2020"
        assert rows[0]["경기"] == "100"

    def test_headers_from_first_row(self):
        table = {
            "rows": [
                ["연도", "팀명", "경기"],
                ["2020", "두산", "100"],
            ],
        }
        headers, rows = _table_to_dicts(table)
        assert headers == ["연도", "팀명", "경기"]
        assert len(rows) == 1

    def test_mismatched_lengths_skipped(self):
        table = {
            "headers": ["연도", "팀명"],
            "rows": [["2020", "두산", "100"]],
        }
        _, rows = _table_to_dicts(table)
        assert len(rows) == 0

    def test_empty_table(self):
        table = {"headers": [], "rows": []}
        headers, rows = _table_to_dicts(table)
        assert headers == []
        assert rows == []


class TestSelectTables:
    def test_base_table_selected(self):
        tables = [
            {"headers": ["연도", "안타", "타수"], "rows": [["2020", "100", "300"]]},
        ]
        base, adv = _select_tables(tables)
        assert len(base) == 1
        assert len(adv) == 0

    def test_adv_table_selected(self):
        tables = [
            {"headers": ["연도", "출루율", "OPS"], "rows": [["2020", "0.400", "0.900"]]},
        ]
        base, adv = _select_tables(tables)
        assert len(base) == 0
        assert len(adv) == 1

    def test_both_base_and_adv(self):
        tables = [
            {"headers": ["연도", "안타", "타수", "출루율", "OPS"], "rows": [["2020", "100", "300", "0.400", "0.900"]]},
        ]
        base, adv = _select_tables(tables)
        assert len(base) == 1
        assert len(adv) == 1

    def test_empty_tables(self):
        base, adv = _select_tables([])
        assert base == []
        assert adv == []

    def test_table_type_marker(self):
        tables = [
            {"_table_type": "HITTER", "headers": ["연도", "some_col"], "rows": [["2020", "val"]]},
        ]
        base, adv = _select_tables(tables)
        assert len(base) == 1


class TestApplyStat:
    def test_applies_stat(self):
        record = {}
        row = {"안타": "100"}
        _apply_stat(record, row, ("안타", "H"), "hits", safe_int_or_none)
        assert record["hits"] == 100

    def test_first_matching_key_wins(self):
        record = {}
        row = {"H": "50", "안타": "100"}
        _apply_stat(record, row, ("안타", "H"), "hits", safe_int_or_none)
        assert record["hits"] == 100  # first key wins

    def test_no_match(self):
        record = {}
        row = {"타율": "0.300"}
        _apply_stat(record, row, ("H", "안타"), "hits", safe_int_or_none)
        assert "hits" not in record


class TestHitterTables:
    def test_basic_hitter(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타", "홈런", "타율"],
                "rows": [["2020", "두산", "100", "350", "120", "15", "0.300"]],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 1
        r = records[0]
        assert r["season"] == 2020
        assert r["games"] == 100
        assert r["at_bats"] == 350
        assert r["hits"] == 120
        assert r["home_runs"] == 15
        assert r["avg"] == 0.3

    def test_hitter_excludes_summary_rows(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타"],
                "rows": [
                    ["2020", "두산", "100", "350", "120"],
                    ["통산", "두산", "500", "1750", "600"],
                    ["합계", "", "500", "1750", "600"],
                ],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 1
        assert records[0]["season"] == 2020

    def test_hitter_excludes_out_of_range_seasons(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타"],
                "rows": [
                    ["1970", "두산", "100", "350", "120"],
                    ["2050", "두산", "100", "350", "120"],
                ],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 0

    def test_hitter_with_advanced_stats(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "안타", "타수", "타율"],
                "rows": [["2020", "두산", "100", "120", "350", "0.343"]],
            },
            {
                "headers": ["연도", "팀명", "출루율", "장타율", "OPS"],
                "rows": [["2020", "두산", "0.400", "0.550", "0.950"]],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 1
        assert records[0]["obp"] == 0.4
        assert records[0]["slg"] == 0.55
        assert records[0]["ops"] == 0.95

    def test_hitter_games_guard_excludes_over_165(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타"],
                "rows": [["2020", "두산", "200", "700", "250"]],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 0

    def test_hitter_hr_guard_excludes_over_65(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "홈런", "타수", "안타"],
                "rows": [["2020", "두산", "100", "70", "350", "120"]],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 0

    def test_hitter_extra_stats(self):
        tables = [
            {
                "headers": ["연도", "팀명", "경기", "타수", "안타", "BB%"],
                "rows": [["2020", "두산", "100", "350", "120", "12.5"]],
            },
        ]
        records = parse_retired_hitter_tables(tables)
        assert len(records) == 1
        assert records[0]["extra_stats"] is not None
        assert records[0]["extra_stats"].get("BB%") == "12.5"

    def test_hitter_empty_tables(self):
        assert parse_retired_hitter_tables([]) == []


class TestPitcherTable:
    def test_basic_pitcher(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패", "세", "이닝", "평균자책", "WHIP"],
            "rows": [["2021", "삼성", "45", "5", "3", "20", "60.2", "2.50", "1.10"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 1
        r = records[0]
        assert r["season"] == 2021
        assert r["games"] == 45
        assert r["wins"] == 5
        assert r["losses"] == 3
        assert r["saves"] == 20
        assert r["innings_outs"] == 182
        assert r["era"] == 2.5
        assert r["whip"] == 1.1

    def test_pitcher_excludes_summary_rows(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패"],
            "rows": [
                ["2021", "삼성", "45", "5", "3"],
                ["통산", "삼성", "200", "30", "20"],
            ],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 1

    def test_pitcher_excludes_out_of_range_seasons(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패"],
            "rows": [["1970", "삼성", "45", "5", "3"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 0

    def test_pitcher_games_guard(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패"],
            "rows": [["2021", "삼성", "200", "5", "3"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 0

    def test_pitcher_wins_guard(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패"],
            "rows": [["2021", "삼성", "45", "40", "3"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 0

    def test_pitcher_advanced_stats(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "K/9", "BB/9", "FIP", "K/BB"],
            "rows": [["2021", "삼성", "45", "5", "9.5", "2.5", "3.20", "3.80"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 1
        assert records[0]["k_per_nine"] == 9.5
        assert records[0]["bb_per_nine"] == 2.5
        assert records[0]["fip"] == 3.2
        assert records[0]["kbb"] == 3.8

    def test_pitcher_empty_table(self):
        table = {"headers": [], "rows": []}
        assert parse_retired_pitcher_table(table) == []

    def test_pitcher_extra_stats(self):
        table = {
            "headers": ["연도", "팀명", "경기", "승", "패", "K/BB", "BB%"],
            "rows": [["2021", "삼성", "45", "5", "3", "3.5", "8.0"]],
        }
        records = parse_retired_pitcher_table(table)
        assert records[0]["extra_stats"] is not None
        assert records[0]["extra_stats"].get("BB%") == "8.0"

    def test_pitcher_hold_and_blk(self):
        table = {
            "headers": ["연도", "팀명", "경기", "홀드", "폭투", "보크"],
            "rows": [["2021", "삼성", "30", "10", "3", "1"]],
        }
        records = parse_retired_pitcher_table(table)
        assert len(records) == 1
        assert records[0]["holds"] == 10
        assert records[0]["wild_pitches"] == 3
        assert records[0]["balks"] == 1


class TestMergeCleanup:
    def test_merge_extra_stats(self):
        record = {"_consumed_keys": {"연도", "팀명"}}
        row = {"연도": "2020", "팀명": "두산", "BB%": "12.5"}
        _merge_extra_stats(record, row, record["_consumed_keys"])
        assert record["extra_stats"]["BB%"] == "12.5"
        assert "연도" not in record["extra_stats"]

    def test_cleanup_consumed_key(self):
        record = {"_consumed_keys": {"연도"}, "extra_stats": {"BB%": "12.5"}}
        _cleanup_consumed(record)
        assert "_consumed_keys" not in record

    def test_cleanup_empty_extra_stats(self):
        record = {"extra_stats": {}}
        _cleanup_consumed(record)
        assert record["extra_stats"] is None
