from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_repository as game_repository
from src.models.base import Base
from src.models.game import Game
from src.models.season import KboSeason


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_save_schedule_game_preserves_existing_season_id_when_mapping_missing(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                season_id=225,
                home_team="SS",
                away_team="LG",
            )
        )
        session.commit()

    saved = game_repository.save_schedule_game(
        {
            "game_id": "20250401LGSS0",
            "game_date": "20250401",
            "home_team_code": "SS",
            "away_team_code": "LG",
            "season_year": 2099,  # no kbo_seasons mapping
            "season_type": "regular",
        }
    )
    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()
        assert game.season_id == 225


def test_save_schedule_game_uses_official_kbo_season_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        # two mappings for same year/type -> official(min season_id) should be selected
        session.add(
            KboSeason(
                season_id=251,
                season_year=2025,
                league_type_code=0,
                league_type_name="정규시즌",
            )
        )
        session.add(
            KboSeason(
                season_id=250,
                season_year=2025,
                league_type_code=0,
                league_type_name="정규시즌",
            )
        )
        session.commit()

    saved = game_repository.save_schedule_game(
        {
            "game_id": "20250402LGSS0",
            "game_date": "20250402",
            "home_team_code": "SS",
            "away_team_code": "LG",
            "season_year": 2025,
            "season_type": "regular",
        }
    )
    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250402LGSS0").one()
        assert game.season_id == 250
