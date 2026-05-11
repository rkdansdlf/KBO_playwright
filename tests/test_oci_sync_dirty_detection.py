from __future__ import annotations

from datetime import date, datetime, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.sync.oci_sync import (
    OCISync,
    build_game_sync_eligibility,
    _dedupe_records_for_conflict_keys,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
    filter_publishable_game_ids,
)


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
        GamePlayByPlay.__table__,
        GameIdAlias.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_game(
    SessionLocal,
    game_id: str,
    *,
    game_updated_at: datetime,
    start_time: time = time(18, 30),
    metadata_updated_at: datetime | None = None,
    lineup_count: int = 0,
    lineup_updated_at: datetime | None = None,
):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="LIVE",
                away_score=1,
                home_score=0,
                away_pitcher="임찬규",
                home_pitcher="원태인",
                created_at=game_updated_at,
                updated_at=game_updated_at,
            )
        )
        session.add(
            GameMetadata(
                game_id=game_id,
                start_time=start_time,
                created_at=metadata_updated_at or game_updated_at,
                updated_at=metadata_updated_at or game_updated_at,
            )
        )
        for idx in range(1, lineup_count + 1):
            session.add(
                GameLineup(
                    game_id=game_id,
                    team_side="away",
                    team_code="LG",
                    player_id=1000 + idx,
                    player_name=f"타자{idx}",
                    batting_order=idx,
                    appearance_seq=idx,
                    standard_position="CF",
                    created_at=lineup_updated_at or game_updated_at,
                    updated_at=lineup_updated_at or game_updated_at,
                )
            )
        session.commit()


def test_detect_dirty_game_ids_when_child_row_count_differs():
    local_factory = _build_session_factory()
    remote_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    _seed_game(local_factory, "20250401LGSS0", game_updated_at=stamp, lineup_count=1, lineup_updated_at=stamp)
    _seed_game(remote_factory, "20250401LGSS0", game_updated_at=stamp, lineup_count=0)

    with local_factory() as local_session, remote_factory() as remote_session:
        dirty = detect_dirty_game_ids(local_session, remote_session)

    assert dirty == ["20250401LGSS0"]


def test_detect_dirty_game_ids_uses_local_newer_updated_at_but_not_remote_newer():
    local_factory = _build_session_factory()
    remote_factory = _build_session_factory()
    older = datetime(2025, 4, 1, 18, 0, 0)
    newer = datetime(2025, 4, 1, 18, 5, 0)

    _seed_game(
        local_factory,
        "20250402LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=newer,
    )
    _seed_game(
        remote_factory,
        "20250402LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=older,
    )
    _seed_game(
        local_factory,
        "20250403LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=older,
    )
    _seed_game(
        remote_factory,
        "20250403LGSS0",
        game_updated_at=newer,
        lineup_count=1,
        lineup_updated_at=newer,
    )

    with local_factory() as local_session, remote_factory() as remote_session:
        dirty = detect_dirty_game_ids(local_session, remote_session)

    assert dirty == ["20250402LGSS0"]


def test_filter_publishable_game_ids_excludes_schedule_only_parent_rows():
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add(
            Game(
                game_id="20250404LGSS0",
                game_date=date(2025, 4, 4),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            Game(
                game_id="20250405LGSS0",
                game_date=date(2025, 4, 5),
                away_team="LG",
                home_team="SS",
                game_status="CANCELLED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            Game(
                game_id="20250406LGSS0",
                game_date=date(2025, 4, 6),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GameLineup(
                game_id="20250406LGSS0",
                team_side="away",
                team_code="LG",
                player_name="타자1",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

    with local_factory() as session:
        publishable = filter_publishable_game_ids(
            session,
            ["20250404LGSS0", "20250405LGSS0", "20250406LGSS0"],
        )

    assert publishable == ["20250405LGSS0", "20250406LGSS0"]


def test_build_game_sync_eligibility_splits_detail_and_relay_targets():
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add_all(
            [
                Game(
                    game_id="20250407LGSS0",
                    game_date=date(2025, 4, 7),
                    away_team="LG",
                    home_team="SS",
                    game_status="SCHEDULED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250408LGSS0",
                    game_date=date(2025, 4, 8),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250409LGSS0",
                    game_date=date(2025, 4, 9),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250410LGSS0",
                    game_date=date(2025, 4, 10),
                    away_team="LG",
                    home_team="SS",
                    game_status="CANCELLED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
            ]
        )
        for side, player_id in (("away", 1001), ("home", 1002)):
            session.add(
                GameBattingStat(
                    game_id="20250408LGSS0",
                    team_side=side,
                    player_id=player_id,
                    player_name=f"{side} batter",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
            session.add(
                GamePitchingStat(
                    game_id="20250408LGSS0",
                    team_side=side,
                    player_id=player_id + 100,
                    player_name=f"{side} pitcher",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
        session.add(
            GameBattingStat(
                game_id="20250409LGSS0",
                team_side="away",
                player_name="away only",
                appearance_seq=1,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GamePlayByPlay(
                game_id="20250409LGSS0",
                inning=1,
                inning_half="top",
                play_description="타자A : 안타",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

    with local_factory() as session:
        eligibility = build_game_sync_eligibility(
            session,
            [
                "20250407LGSS0",
                "20250408LGSS0",
                "20250409LGSS0",
                "20250410LGSS0",
            ],
        )

    assert eligibility.parent_game_ids == ["20250408LGSS0", "20250409LGSS0", "20250410LGSS0"]
    assert eligibility.detail_game_ids == ["20250408LGSS0"]
    assert eligibility.relay_game_ids == ["20250409LGSS0"]
    assert eligibility.skipped_schedule_only == ["20250407LGSS0"]
    assert eligibility.skipped_incomplete_detail == ["20250409LGSS0"]
    assert eligibility.skipped_empty_relay == ["20250408LGSS0"]
    assert eligibility.skipped_cancelled == ["20250410LGSS0"]


def test_sync_game_details_filters_child_datasets_by_eligibility(monkeypatch):
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add_all(
            [
                Game(
                    game_id="20250411LGSS0",
                    game_date=date(2025, 4, 11),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250412LGSS0",
                    game_date=date(2025, 4, 12),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
            ]
        )
        for side, player_id in (("away", 1101), ("home", 1102)):
            session.add(
                GameBattingStat(
                    game_id="20250411LGSS0",
                    team_side=side,
                    player_id=player_id,
                    player_name=f"{side} batter",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
            session.add(
                GamePitchingStat(
                    game_id="20250411LGSS0",
                    team_side=side,
                    player_id=player_id + 100,
                    player_name=f"{side} pitcher",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
        session.add(
            GameBattingStat(
                game_id="20250412LGSS0",
                team_side="away",
                player_name="away only",
                appearance_seq=1,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GamePlayByPlay(
                game_id="20250412LGSS0",
                inning=1,
                inning_half="top",
                play_description="타자A : 안타",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session

        monkeypatch.setattr(
            OCISync,
            "get_unsynced_or_modified_game_ids",
            lambda _self: ["20250411LGSS0", "20250412LGSS0"],
        )
        monkeypatch.setattr(
            OCISync,
            "sync_games",
            lambda _self, filters=None, **_kwargs: session.query(Game).filter(*(filters or [])).count(),
        )

        def _count_table(_self, model, _conflict_keys, filters=None, **_kwargs):
            return session.query(model).filter(*(filters or [])).count()

        monkeypatch.setattr(OCISync, "_sync_simple_table", _count_table)
        monkeypatch.setattr(
            OCISync,
            "_sync_game_play_by_play",
            lambda _self, filters=None: session.query(GamePlayByPlay).filter(*(filters or [])).count(),
        )
        monkeypatch.setattr(
            OCISync,
            "_sync_game_summary_rows",
            lambda _self, filters=None, **_kwargs: session.query(GameSummary).filter(*(filters or [])).count(),
        )

        results = syncer.sync_game_details(unsynced_only=True)

    assert results["games"] == 2
    assert results["batting_stats"] == 2
    assert results["pitching_stats"] == 2
    assert results["play_by_play"] == 1
    assert results["skipped_incomplete_detail"] == 1
    assert results["skipped_empty_relay"] == 1


def test_filter_game_ids_by_year_preserves_only_requested_year():
    game_ids = ["20240401LGSS0", "20250401LGSS0", "20250402LGSS0", "20260401LGSS0"]

    assert filter_game_ids_by_year(game_ids, 2025) == ["20250401LGSS0", "20250402LGSS0"]
    assert filter_game_ids_by_year(game_ids, None) == game_ids


def test_dedupe_records_for_conflict_keys_preserves_null_key_rows():
    records = [
        {"game_id": "20260426KTSK0", "player_id": None, "appearance_seq": 1, "team_side": "away"},
        {"game_id": "20260426KTSK0", "player_id": None, "appearance_seq": 1, "team_side": "home"},
        {"game_id": "20260426KTSK0", "player_id": 50859, "appearance_seq": 1, "team_side": "away"},
        {"game_id": "20260426KTSK0", "player_id": 50859, "appearance_seq": 1, "team_side": "away"},
    ]

    deduped = _dedupe_records_for_conflict_keys(
        records,
        ["game_id", "player_id", "appearance_seq"],
    )

    assert deduped == records[:3]
