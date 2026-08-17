from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from src.crawlers.external_stats_crawler import ExternalStatsCrawler
from src.sources.stats.base import ExternalStatsAccessError
from src.sources.stats.fangraphs import FanGraphsKboAdapter
from src.sources.stats.statiz import StatizKboAdapter
from src.utils.request_policy import RequestPolicy, RequestPolicyConfig

FANGRAPHS_BATTING_HTML = """
<table>
  <thead><tr><th>Name</th><th>Team</th><th>G</th><th>PA</th><th>HR</th><th>BB%</th><th>wRC+</th><th>WAR</th></tr></thead>
  <tbody>
    <tr><td><a href="/players/kim?playerid=123">Kim Hitter 김타자</a></td><td>Bears (KBO)</td><td>100</td><td>420</td><td>25</td><td>12.5%</td><td>145</td><td>4.2</td></tr>
  </tbody>
</table>
"""

FANGRAPHS_PITCHING_HTML = """
<table>
  <thead><tr><th>Name</th><th>Team</th><th>W</th><th>IP</th><th>ERA</th><th>FIP</th><th>WAR</th></tr></thead>
  <tbody>
    <tr><td><a href="/players/lee?playerid=456">Lee Pitcher 김투수</a></td><td>Twins (KBO)</td><td>10</td><td>120.2</td><td>3.10</td><td>3.40</td><td>3.1</td></tr>
  </tbody>
</table>
"""

STATIZ_BATTING_HTML = """
<table class="table_type03">
  <thead><tr><th>순위</th><th>선수</th><th>팀</th><th>타석</th><th>타율</th><th>WAR</th></tr></thead>
  <tbody>
    <tr><td>1</td><td><a href="/player/?m=playerinfo&p_no=789">이타자</a></td><td>LG</td><td>400</td><td>0.321</td><td>5.0</td></tr>
  </tbody>
</table>
"""

FANGRAPHS_API_JSON = """
[
  {
    "Name": "<a href='statss.aspx?playerid=123'>Kim Hitter 김타자</a>",
    "Team": "Bears (KBO)",
    "G": 119.0,
    "PA": 486.0,
    "HR": 35.0,
    "AVG": 0.235849056,
    "wOBA": 0.360,
    "wRC+": 125.0,
    "WAR": 3.4,
    "Season": 2025,
    "KName": "김타자",
    "playerids": 123
  }
]
"""


def test_fangraphs_batting_parser_normalizes_metrics_and_identity() -> None:
    adapter = FanGraphsKboAdapter()

    records = adapter.parse_html(
        FANGRAPHS_BATTING_HTML,
        2025,
        "batting",
        adapter.build_url(2025, "batting"),
    )

    assert len(records) == 1
    record = records[0]
    assert record.player_name == "김타자"
    assert record.external_player_id == "123"
    assert record.team_code == "DB"
    assert record.metrics == {
        "games": 100,
        "plate_appearances": 420,
        "home_runs": 25,
        "bb_pct": 12.5,
        "wrc_plus": 145,
        "war": 4.2,
    }
    assert record.metric_metadata["parser_version"] == "fangraphs-kbo-v1"


def test_fangraphs_pitching_parser_converts_displayed_innings_to_outs() -> None:
    adapter = FanGraphsKboAdapter()

    record = adapter.parse_html(
        FANGRAPHS_PITCHING_HTML,
        2025,
        "pitching",
        adapter.build_url(2025, "pitching"),
    )[0]

    assert record.external_player_id == "456"
    assert record.player_name == "김투수"
    assert record.team_code == "LG"
    assert record.metrics["innings_outs"] == 362
    assert record.metrics["innings_pitched"] == pytest.approx(120.6666667)
    assert record.metrics["war_pitch"] == 3.1


def test_fangraphs_api_parser_uses_requested_season_and_provider_id() -> None:
    adapter = FanGraphsKboAdapter()

    records = adapter.parse_html(
        FANGRAPHS_API_JSON,
        2025,
        "batting",
        adapter.build_url(2025, "batting"),
    )

    assert len(records) == 1
    assert records[0].external_player_id == "123"
    assert records[0].metrics["wrc_plus"] == 125
    assert "player_name" not in records[0].metrics
    assert "team_name" not in records[0].metrics
    assert records[0].metric_metadata["parser_version"] == "fangraphs-kbo-v1"


def test_statiz_parser_accepts_korean_headers() -> None:
    adapter = StatizKboAdapter()

    records = adapter.parse_html(
        STATIZ_BATTING_HTML,
        2025,
        "batting",
        adapter.build_url(2025, "batting"),
    )

    assert len(records) == 1
    assert records[0].external_player_id == "789"
    assert records[0].team_code == "LG"
    assert records[0].metrics["plate_appearances"] == 400
    assert records[0].metrics["avg"] == 0.321


def test_statiz_login_page_is_not_treated_as_empty_success() -> None:
    adapter = StatizKboAdapter()

    with pytest.raises(ExternalStatsAccessError, match="authenticated"):
        adapter.parse_html(
            "<script>alert('로그인 후 이용 가능합니다.')</script>",
            2025,
            "batting",
            adapter.build_url(2025, "batting"),
        )


def test_build_urls_include_season_and_stat_type() -> None:
    batting_url = FanGraphsKboAdapter().build_url(2024, "batting")
    pitching_url = FanGraphsKboAdapter().build_url(2024, "pitching")
    assert "/api/leaders/international/kbo/data" in batting_url
    assert "stats=bat" in batting_url
    assert "qual=0" in batting_url
    assert "seasonstart=2024" in batting_url
    assert "stats=pit" in pitching_url
    assert "year=2024" in StatizKboAdapter().build_url(2024, "batting")
    assert "m2=2" in StatizKboAdapter().build_url(2024, "pitching")


@pytest.mark.asyncio
async def test_crawler_stops_on_http_403_without_browser_fallback() -> None:
    request = httpx.Request("GET", "https://www.fangraphs.com/test")
    response = httpx.Response(403, request=request)
    client = AsyncMock()
    client.get.return_value = response
    policy = RequestPolicy(
        RequestPolicyConfig(
            min_delay=0,
            max_delay=0,
            max_retries=1,
            retry_exceptions=(httpx.TimeoutException,),
        ),
    )
    crawler = ExternalStatsCrawler(
        adapters={"fangraphs": FanGraphsKboAdapter()},
        client=client,
        policy=policy,
    )

    result = await crawler.crawl(2025, providers=["fangraphs"], stat_types=["batting"])

    assert result.records == []
    assert "HTTP 403" in result.failures[0]
    client.get.assert_awaited_once()
