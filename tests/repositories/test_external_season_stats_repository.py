from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.external_season_stat import ExternalSeasonStat
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.repositories.external_season_stats_repository import (
    ExternalSeasonStatsRepository,
    overlay_external_metrics,
)
from src.sources.stats.base import ExternalStatRecord


def _record(
    *,
    stat_type: str = "batting",
    player_name: str = "홍타자",
    team_name: str | None = "LG",
    metrics: dict[str, int | float] | None = None,
) -> ExternalStatRecord:
    return ExternalStatRecord(
        provider="fangraphs",
        source_key=f"fangraphs_kbo_{stat_type}",
        stat_type=stat_type,
        season=2025,
        player_name=player_name,
        team_name=team_name,
        team_code="LG" if team_name else None,
        external_player_id="123" if team_name else None,
        metrics=metrics or {"woba": 0.4, "war": 3.2},
        source_url="https://example.test/stats",
        metric_metadata={"parser_version": "test-v1"},
    )


def _session():
    engine = create_engine("sqlite:///:memory:")
    PlayerBasic.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    PlayerSeasonPitching.__table__.create(engine)
    ExternalSeasonStat.__table__.create(engine)
    return sessionmaker(bind=engine)()


def test_save_records_resolves_and_upserts_provider_rows() -> None:
    session = _session()
    session.add(PlayerBasic(player_id=1, name="홍타자"))
    session.commit()
    repository = ExternalSeasonStatsRepository(session)

    with patch.object(repository, "_resolve_record_player", return_value=(1, "resolved", None)):
        first = repository.save_records([_record()], content_hashes={"fangraphs_kbo_batting": "abc"})
        second = repository.save_records(
            [_record(metrics={"woba": 0.42, "war": 3.8})],
            content_hashes={"fangraphs_kbo_batting": "def"},
        )
    session.commit()

    row = session.query(ExternalSeasonStat).one()
    assert first.as_dict() == {
        "attempted": 1,
        "saved": 1,
        "resolved": 1,
        "unresolved_team": 0,
        "unresolved_player": 0,
    }
    assert second.saved == 1
    assert row.metrics["woba"] == 0.42
    assert row.content_hash == "def"
    assert row.player_id == 1


def test_project_preserves_existing_extra_stats_and_namespaces_provider_values() -> None:
    session = _session()
    session.add(PlayerBasic(player_id=1, name="홍타자"))
    session.add(
        PlayerSeasonBatting(
            player_id=1,
            season=2025,
            league="REGULAR",
            level="KBO1",
            team_code="LG",
            extra_stats={"woba": 0.39, "internal_metric": 7},
        ),
    )
    session.commit()
    repository = ExternalSeasonStatsRepository(session)

    with patch.object(repository, "_resolve_record_player", return_value=(1, "resolved", None)):
        repository.save_records([_record()])
    report = repository.project(provider="fangraphs", season=2025, stat_type="batting")
    session.commit()

    target = session.query(PlayerSeasonBatting).one()
    assert report.projected == 1
    assert target.extra_stats["woba"] == 0.39
    assert target.extra_stats["internal_metric"] == 7
    assert target.extra_stats["external_sources"]["fangraphs"]["metrics"]["war"] == 3.2


def test_unmapped_team_is_saved_without_auto_registering_player() -> None:
    session = _session()
    repository = ExternalSeasonStatsRepository(session)

    report = repository.save_records([_record(team_name=None)])
    session.commit()

    row = session.query(ExternalSeasonStat).one()
    assert report.unresolved_team == 1
    assert row.player_id is None
    assert row.resolution_status == "unresolved_team"


def test_overlay_external_metrics_is_explicit_and_read_only() -> None:
    session = _session()
    row = ExternalSeasonStat(
        source_record_key="a" * 64,
        provider="fangraphs",
        source_key="fangraphs_kbo_batting",
        stat_type="batting",
        season=2025,
        league="REGULAR",
        level="KBO1",
        player_id=1,
        player_name="홍타자",
        team_code="LG",
        metrics={"wrc_plus": 140},
        source_url="https://example.test",
        fetched_at=datetime(2025, 1, 1),
        parser_version="test-v1",
        resolution_status="resolved",
    )
    session.add(row)
    session.commit()

    rows = [{"player_id": 1, "team_code": "LG", "extra_stats": {"wrc_plus": 100}}]
    overlaid = overlay_external_metrics(
        session,
        rows,
        provider="fangraphs",
        season=2025,
        stat_type="batting",
    )

    assert rows[0]["extra_stats"]["wrc_plus"] == 100
    assert overlaid[0]["extra_stats"]["wrc_plus"] == 140
