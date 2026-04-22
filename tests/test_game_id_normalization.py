from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_repository as game_repository
from src.models.game import Game, GameIdAlias
from src.utils.team_codes import normalize_kbo_game_id


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameIdAlias.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_normalize_kbo_game_id_splits_mixed_length_team_codes():
    assert normalize_kbo_game_id("20260319LGSSG0") == "20260319LGSK0"
    assert normalize_kbo_game_id("20260319SSGLG0") == "20260319SKLG0"
    assert normalize_kbo_game_id("20260401LGKIA0") == "20260401LGHT0"
    assert normalize_kbo_game_id("20260401KIALG0") == "20260401HTLG0"
    assert normalize_kbo_game_id("20260401DBKH0") == "20260401OBWO0"
    assert normalize_kbo_game_id("20260401KHDB0") == "20260401WOOB0"
    assert normalize_kbo_game_id("20260401LGSS0") == "20260401LGSS0"
    assert normalize_kbo_game_id("20260401OBWO0") == "20260401OBWO0"


def test_save_schedule_game_records_alias_for_modern_source_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)

    saved = game_repository.save_schedule_game(
        {
            "game_id": "20260319LGSSG0",
            "game_date": "20260319",
            "home_team_code": "SSG",
            "away_team_code": "LG",
            "season_year": 2026,
            "season_type": "regular",
        }
    )

    assert saved is True
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20260319LGSK0").one()
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "20260319LGSSG0").one()
        assert game.game_date == date(2026, 3, 19)
        assert game.home_team == "SSG"
        assert game.away_team == "LG"
        assert alias.canonical_game_id == "20260319LGSK0"


def test_resolve_canonical_game_id_uses_alias_table(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(Game(game_id="20260319LGSK0", game_date=date(2026, 3, 19)))
        session.add(
            GameIdAlias(
                alias_game_id="20260319LGSSG0",
                canonical_game_id="20260319LGSK0",
                source="test",
                reason="alias",
            )
        )
        session.commit()

    assert game_repository.resolve_canonical_game_id("20260319LGSSG0") == "20260319LGSK0"
    assert game_repository.resolve_canonical_game_id("20260401LGSSG0") == "20260401LGSK0"
