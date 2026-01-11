import pathlib

import pytest

pytest.importorskip("bs4")

from src.crawlers.team_batting_stats_crawler import parse_team_batting_html

pytestmark = pytest.mark.integration

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "html" / "team_batting_2023.html"


def test_team_batting_parser_extracts_rows():
    html = FIXTURE.read_text(encoding="utf-8")
    mapping = {"LG": "LG", "삼성": "SS"}

    rows = parse_team_batting_html(html, season=2023, league="REGULAR", team_mapping=mapping)

    assert len(rows) == 2
    first = rows[0]
    assert first["team_id"] == "LG"
    assert first["games"] == 144
    assert first["plate_appearances"] == 5699
    assert pytest.approx(first["avg"], rel=1e-4) == 0.287
    assert pytest.approx(first["ops"], rel=1e-4) == 0.779

    samsung = rows[1]
    assert samsung["team_id"] == "SS"
    assert samsung["home_runs"] == 130
    assert samsung["stolen_bases"] == 150
