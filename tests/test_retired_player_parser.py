from src.parsers.retired_player_parser import (
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)


def test_parse_retired_hitter_tables_basic():
    tables = [
        {
            "headers": ["연도", "팀명", "경기", "타수", "안타", "홈런", "타율"],
            "rows": [
                ["2020", "두산", "100", "350", "120", "15", "0.300"],
                ["통산", "두산", "100", "350", "120", "15", "0.300"],
            ],
        },
        {
            "headers": ["연도", "팀명", "출루율", "장타율", "OPS", "ISO"],
            "rows": [["2020", "두산", "0.400", "0.500", "0.900", "0.200"]],
        },
    ]

    records = parse_retired_hitter_tables(tables)
    assert len(records) == 1
    record = records[0]
    assert record["season"] == 2020
    assert record["team_code"] == "OB"
    assert record["games"] == 100
    assert record["at_bats"] == 350
    assert record["hits"] == 120
    assert record["home_runs"] == 15
    assert abs(record["avg"] - 0.3) < 1e-6
    assert abs(record["obp"] - 0.4) < 1e-6
    assert abs(record["ops"] - 0.9) < 1e-6
    assert record["source"] == "PROFILE"


def test_parse_retired_pitcher_table_basic():
    table = {
        "headers": ["연도", "팀명", "경기", "선발", "승", "패", "세", "홀드", "이닝", "평균자책", "WHIP"],
        "rows": [
            ["2021", "삼성", "45", "10", "5", "3", "20", "15", "60.2", "2.50", "1.10"],
            ["합계", "삼성", "45", "10", "5", "3", "20", "15", "60.2", "2.50", "1.10"],
        ],
    }

    records = parse_retired_pitcher_table(table)
    assert len(records) == 1
    record = records[0]
    assert record["season"] == 2021
    assert record["team_code"] == "SS"
    assert record["games"] == 45
    assert record["games_started"] == 10
    assert record["wins"] == 5
    assert record["saves"] == 20
    assert record["holds"] == 15
    assert record["innings_outs"] == 60 * 3 + 2
    assert abs(record["era"] - 2.5) < 1e-6
    assert abs(record["whip"] - 1.1) < 1e-6
    assert record["source"] == "PROFILE"
