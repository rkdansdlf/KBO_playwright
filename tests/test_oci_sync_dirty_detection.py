from __future__ import annotations

from datetime import date, datetime, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.sync.oci_sync import detect_dirty_game_ids, filter_game_ids_by_year, filter_publishable_game_ids


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


def test_filter_game_ids_by_year_preserves_only_requested_year():
    game_ids = ["20240401LGSS0", "20250401LGSS0", "20250402LGSS0", "20260401LGSS0"]

    assert filter_game_ids_by_year(game_ids, 2025) == ["20250401LGSS0", "20250402LGSS0"]
    assert filter_game_ids_by_year(game_ids, None) == game_ids
