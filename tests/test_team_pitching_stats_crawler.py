import pathlib

import pytest

pytest.importorskip("bs4")

from src.crawlers.team_pitching_stats_crawler import parse_team_pitching_html

pytestmark = pytest.mark.integration

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "html" / "team_pitching_2023.html"


def test_team_pitching_parser_extracts_rows():
    html = FIXTURE.read_text(encoding="utf-8")
    mapping = {"LG": "LG", "삼성": "SS"}

    rows = parse_team_pitching_html(html, season=2023, league="REGULAR", team_mapping=mapping)

    assert len(rows) == 2
    lg = rows[0]
    assert lg["team_id"] == "LG"
    assert lg["wins"] == 86
    assert pytest.approx(lg["innings_pitched"], rel=1e-4) == (1268 + (2.0 / 3.0))
    assert pytest.approx(lg["era"], rel=1e-4) == 3.61
    assert pytest.approx(lg["whip"], rel=1e-4) == 1.27

    ss = rows[1]
    assert ss["team_id"] == "SS"
    assert ss["losses"] == 68
    assert pytest.approx(ss["avg_against"], rel=1e-4) == 0.267
