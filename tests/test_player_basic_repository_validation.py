from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import src.repositories.player_basic_repository as module
from src.models.player import PlayerBasic
from src.repositories.player_basic_repository import PlayerBasicRepository


def _build_repo(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'players.db'}")
    PlayerBasic.__table__.create(engine)
    monkeypatch.setattr(module, "Engine", engine)
    monkeypatch.setattr(module, "SessionLocal", sessionmaker(bind=engine))
    return PlayerBasicRepository(), engine


def test_upsert_players_filters_invalid_payloads_before_save(monkeypatch, tmp_path):
    repo, engine = _build_repo(monkeypatch, tmp_path)

    saved = repo.upsert_players(
        [
            {"player_id": None, "name": "홍길동"},
            {"player_id": 1001, "name": ""},
            {"player_id": 1002, "name": "Unknown Player"},
            {"player_id": 1004, "name": "Unknown 1004"},
            {"player_id": 1003, "name": "정상선수", "team": "LG", "position": "투수"},
        ]
    )

    assert saved == 1
    assert dict(repo.last_filter_counts) == {
        "invalid_player_id": 1,
        "missing_player_name": 1,
        "unknown_player_name": 2,
    }

    with sessionmaker(bind=engine)() as session:
        rows = session.execute(select(PlayerBasic)).scalars().all()

    assert len(rows) == 1
    assert rows[0].player_id == 1003
    assert rows[0].name == "정상선수"


def test_upsert_players_deduplicates_by_player_id_with_last_valid_row(monkeypatch, tmp_path):
    repo, engine = _build_repo(monkeypatch, tmp_path)

    saved = repo.upsert_players(
        [
            {"player_id": "1001", "name": "이전이름", "team": "LG"},
            {"player_id": 1001, "name": "현재이름", "team": "KT"},
        ]
    )

    assert saved == 1
    assert dict(repo.last_filter_counts) == {}

    with sessionmaker(bind=engine)() as session:
        player = session.get(PlayerBasic, 1001)

    assert player.name == "현재이름"
    assert player.team == "KT"


def test_upsert_players_accepts_legacy_player_name_key(monkeypatch, tmp_path):
    repo, engine = _build_repo(monkeypatch, tmp_path)

    saved = repo.upsert_players([{"player_id": 2001, "player_name": "레거시선수"}])

    assert saved == 1
    with sessionmaker(bind=engine)() as session:
        player = session.get(PlayerBasic, 2001)

    assert player.name == "레거시선수"
