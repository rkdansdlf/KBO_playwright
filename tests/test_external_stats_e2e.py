"""SQLite acceptance tests for the external statistics pipeline."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crawlers.external_stats_crawler import ExternalStatsCrawler
from src.models.external_season_stat import ExternalSeasonStat
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.repositories.external_season_stats_repository import ExternalSeasonStatsRepository
from src.sources.stats.base import ExternalStatRecord
from src.sources.stats.fangraphs import FanGraphsKboAdapter
from src.utils.request_policy import RequestPolicy, RequestPolicyConfig

pytestmark = pytest.mark.integration

FANGRAPHS_FIXTURE_JSON = """
[
  {
    "Name": "<a href='statss.aspx?playerid=123'>Kim Hitter 김타자</a>",
    "Team": "Bears (KBO)",
    "G": 119.0,
    "PA": 486.0,
    "HR": 35.0,
    "wRC+": 125.0,
    "WAR": 3.4,
    "Season": 2025,
    "KName": "김타자",
    "playerids": 123
  }
]
"""


def _session():
    engine = create_engine("sqlite:///:memory:")
    PlayerBasic.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    PlayerSeasonPitching.__table__.create(engine)
    ExternalSeasonStat.__table__.create(engine)
    return sessionmaker(bind=engine)()


@pytest.mark.asyncio
async def test_external_stats_round_trip_is_idempotent_and_preserves_lineage() -> None:
    """Persist, resolve, project, and repeat one provider payload without duplication."""
    adapter = FanGraphsKboAdapter()
    request = httpx.Request("GET", adapter.build_url(2025, "batting"))
    response = httpx.Response(
        200,
        request=request,
        content=FANGRAPHS_FIXTURE_JSON.encode("utf-8"),
        headers={"content-type": "application/json"},
    )
    client = AsyncMock()
    client.get.return_value = response
    policy = RequestPolicy(
        RequestPolicyConfig(
            min_delay=0,
            max_delay=0,
            max_retries=0,
            retry_exceptions=(httpx.TimeoutException,),
        ),
    )
    crawler = ExternalStatsCrawler(
        adapters={"fangraphs": adapter},
        client=client,
        policy=policy,
    )
    result = await crawler.crawl(2025, providers=("fangraphs",), stat_types=("batting",))
    await crawler.close()

    assert len(result.records) == 1
    assert len(result.pages) == 1
    assert result.failures == []

    session = _session()
    session.add(PlayerBasic(player_id=1, name="김타자", team="두산"))
    session.add(
        PlayerSeasonBatting(
            player_id=1,
            season=2025,
            league="REGULAR",
            level="KBO1",
            team_code="DB",
            extra_stats={"official_metric": 7},
        ),
    )
    session.commit()

    repository = ExternalSeasonStatsRepository(session)
    first = repository.save_records(
        result.records,
        content_hashes={result.pages[0].source_key: result.pages[0].content_hash},
    )
    projection = repository.project(provider="fangraphs", season=2025, stat_type="batting")
    session.commit()

    second = repository.save_records(
        result.records,
        content_hashes={result.pages[0].source_key: result.pages[0].content_hash},
    )
    session.commit()

    row = session.query(ExternalSeasonStat).one()
    target = session.query(PlayerSeasonBatting).one()
    assert first.resolved == 1
    assert second.resolved == 1
    assert projection.projected == 1
    assert row.player_id == 1
    assert row.source_url == result.pages[0].url
    assert row.content_hash == result.pages[0].content_hash
    assert row.parser_version == "fangraphs-kbo-v1"
    assert row.metrics["wrc_plus"] == 125
    assert target.extra_stats["official_metric"] == 7
    assert target.extra_stats["external_sources"]["fangraphs"]["metrics"]["war"] == 3.4
    assert session.query(ExternalSeasonStat).count() == 1


def test_external_stats_ambiguous_player_is_not_auto_resolved() -> None:
    """Multiple plausible canonical players must remain unresolved."""
    session = _session()
    session.add_all(
        [
            PlayerBasic(player_id=10, name="동명이인", team="두산"),
            PlayerBasic(player_id=11, name="동명이인", team="두산"),
            PlayerSeasonBatting(
                player_id=10,
                season=2025,
                league="REGULAR",
                level="KBO1",
                team_code="DB",
            ),
            PlayerSeasonBatting(
                player_id=11,
                season=2025,
                league="REGULAR",
                level="KBO1",
                team_code="DB",
            ),
        ],
    )
    session.commit()

    record = ExternalStatRecord(
        provider="fangraphs",
        source_key="fangraphs_kbo_batting",
        stat_type="batting",
        season=2025,
        player_name="동명이인",
        team_name="두산",
        team_code="DB",
        external_player_id="ambiguous-1",
        metrics={"war": 1.2},
        source_url="https://example.test/stats",
        metric_metadata={"parser_version": "test-v1"},
    )

    report = ExternalSeasonStatsRepository(session).save_records([record])
    row = session.query(ExternalSeasonStat).one()

    assert report.resolved == 0
    assert report.unresolved_player == 1
    assert row.player_id is None
    assert row.resolution_status == "unresolved_player"
    assert session.query(PlayerBasic).count() == 2
