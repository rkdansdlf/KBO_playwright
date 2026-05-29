from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import src.repositories.player_season_pitching_repository as pitching_repo
import src.repositories.safe_batting_repository as batting_repo
import src.repositories.team_stats_repository as team_stats_repo
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonFielding, PlayerSeasonPitching
from src.models.team import Team
from src.repositories.player_stats_repository import PlayerSeasonFieldingRepository


def _build_session_factory(tmp_path, name):
    engine = create_engine(f"sqlite:///{tmp_path / name}")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        PlayerSeasonFielding.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine), engine


def test_save_batting_stats_filters_invalid_and_basic2_only_payloads(monkeypatch, tmp_path):
    SessionLocal, engine = _build_session_factory(tmp_path, "batting.db")
    monkeypatch.setattr(batting_repo, "SessionLocal", SessionLocal)
    monkeypatch.setattr(batting_repo, "get_database_type", lambda: "sqlite")

    saved = batting_repo.save_batting_stats_safe(
        [
            {
                "player_id": 1001,
                "player_name": "홍길동",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "LG",
                "games": 10,
                "hits": 5,
            },
            {
                "player_id": 1002,
                "player_name": "Basic2전용",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "LG",
                "walks": 10,
                "obp": 0.4,
            },
            {
                "player_id": 1003,
                "player_name": "Unknown Player",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "LG",
                "games": 1,
            },
        ]
    )

    assert saved == 1
    assert batting_repo.get_last_filter_counts() == {
        "empty_core_stats": 1,
        "unknown_player_name": 1,
    }
    with SessionLocal() as session:
        rows = session.execute(select(PlayerSeasonBatting)).scalars().all()

    assert len(rows) == 1
    assert rows[0].player_id == 1001


def test_save_pitching_stats_filters_invalid_and_basic2_only_payloads(monkeypatch, tmp_path):
    SessionLocal, engine = _build_session_factory(tmp_path, "pitching.db")
    monkeypatch.setattr(pitching_repo, "SessionLocal", SessionLocal)
    monkeypatch.setattr(pitching_repo, "get_database_type", lambda: "sqlite")

    saved = pitching_repo.save_pitching_stats_to_db(
        [
            {
                "player_id": 2001,
                "player_name": "원태인",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "SS",
                "games": 10,
                "innings_outs": 90,
            },
            {
                "player_id": 2002,
                "player_name": "Basic2전용",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "SS",
                "extra_stats": {"metrics": {"np": 100}},
            },
            {
                "player_id": 2003,
                "player_name": "숫자오류",
                "season": 2025,
                "league": "REGULAR",
                "team_code": "SS",
                "games": "bad",
            },
        ]
    )

    assert saved == 1
    assert pitching_repo.get_last_filter_counts() == {
        "empty_core_stats": 1,
        "invalid_numeric_stat": 1,
    }
    with SessionLocal() as session:
        rows = session.execute(select(PlayerSeasonPitching)).scalars().all()

    assert len(rows) == 1
    assert rows[0].player_id == 2001


def test_fielding_repository_filters_invalid_payloads(monkeypatch, tmp_path):
    SessionLocal, engine = _build_session_factory(tmp_path, "fielding.db")
    monkeypatch.setattr(team_stats_repo, "SessionLocal", SessionLocal)
    monkeypatch.setattr(team_stats_repo, "get_database_type", lambda: "sqlite")

    repo = PlayerSeasonFieldingRepository()
    saved = repo.upsert_many(
        [
            {
                "player_id": 3001,
                "year": 2025,
                "team_id": "LG",
                "position_id": "SS",
                "games": 10,
                "errors": 1,
            },
            {
                "player_id": 3002,
                "year": 2025,
                "team_id": "LG",
                "position_id": "SS",
            },
            {
                "player_id": 3003,
                "year": 2025,
                "team_id": "",
                "position_id": "SS",
                "games": 1,
            },
        ]
    )

    assert saved == 1
    assert dict(repo.last_filter_counts) == {
        "empty_core_stats": 1,
        "missing_team_id": 1,
    }

    with SessionLocal() as session:
        rows = session.execute(select(PlayerSeasonFielding)).scalars().all()

    assert len(rows) == 1
    assert rows[0].player_id == 3001
