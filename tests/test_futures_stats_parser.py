from src.parsers.futures_stats_parser import parse_futures_tables


def test_parse_futures_tables_hitter_and_pitcher():
    tables = [
        {
            "caption": "퓨처스 타자 기본",
            "headers": ["연도", "팀명", "경기", "타수", "안타", "홈런", "타율"],
            "rows": [
                ["2023", "한화", "80", "250", "70", "8", "0.280"],
                ["통산", "한화", "80", "250", "70", "8", "0.280"],
            ],
        },
        {
            "caption": "퓨처스 타자 고급",
            "headers": ["연도", "팀명", "출루율", "장타율", "OPS"],
            "rows": [["2023", "한화", "0.360", "0.420", "0.780"]],
        },
        {
            "caption": "퓨처스 투수",
            "headers": ["연도", "팀명", "경기", "승", "세", "이닝", "ERA", "WHIP"],
            "rows": [
                ["2022", "두산", "30", "5", "10", "45.1", "2.10", "1.05"],
                ["합계", "두산", "30", "5", "10", "45.1", "2.10", "1.05"],
            ],
        },
    ]

    result = parse_futures_tables(tables)

    batting = result["batting"]
    assert len(batting) == 1
    assert batting[0]["season"] == 2023
    assert batting[0]["league"] == "FUTURES"
    assert batting[0]["level"] == "KBO2"
    assert batting[0]["hits"] == 70
    assert abs(batting[0]["avg"] - 0.28) < 1e-6
    assert batting[0]["source"] == "PROFILE"

    pitching = result["pitching"]
    assert len(pitching) == 1
    assert pitching[0]["season"] == 2022
    assert pitching[0]["league"] == "FUTURES"
    assert pitching[0]["level"] == "KBO2"
    assert pitching[0]["wins"] == 5
    assert abs(pitching[0]["era"] - 2.10) < 1e-6
    assert pitching[0]["source"] == "PROFILE"
