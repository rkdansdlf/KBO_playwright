from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_relay as game_relay_module
import src.repositories.game_repository as game_repository
import src.repositories.game_save as game_save_module
from src.models.game import Game, GameIdAlias
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment
from src.utils.team_history import canonical_code_for_team_code, franchise_id_for_team_code


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


def test_hd_team_code_resolves_to_hyundai_hu():
    assert resolve_team_code("HD", 2001) == "HU"
    assert team_code_from_game_id_segment("HD", 2001) == "HU"
    assert franchise_id_for_team_code("HD", 2001) == 6


def test_historical_team_code_boundaries_resolve_by_season():
    assert resolve_team_code("청보", 1985) == "CB"
    assert team_code_from_game_id_segment("SM", 1985) == "CB"
    assert resolve_team_code("빙그레", 1993) == "BE"
    assert resolve_team_code("한화", 1994) == "HH"
    assert team_code_from_game_id_segment("WO", 2008) == "WO"
    assert team_code_from_game_id_segment("WO", 2010) == "NX"
    assert team_code_from_game_id_segment("WO", 2024) == "KH"


def test_historical_franchise_split_identity():
    assert franchise_id_for_team_code("HU", 2001) == 6
    assert canonical_code_for_team_code("HU", 2001) == "HU"
    assert franchise_id_for_team_code("WO", 2008) == 11
    assert canonical_code_for_team_code("WO", 2008) == "KH"
    assert franchise_id_for_team_code("SL", 1999) == 12
    assert canonical_code_for_team_code("SL", 1999) == "SL"
    assert franchise_id_for_team_code("SSG", 1999) == 12
    assert canonical_code_for_team_code("SSG", 1999) == "SL"
    assert franchise_id_for_team_code("SK", 2000) == 8
    assert franchise_id_for_team_code("SSG", 2024) == 8


def test_save_schedule_game_records_alias_for_modern_source_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)

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


def test_save_schedule_game_records_alias_for_legacy_source_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)

    saved = game_repository.save_schedule_game(
        {
            "game_id": "20010509SSGLT1",
            "game_date": "20010509",
            "away_team_code": "SSG",
            "home_team_code": "LT",
            "doubleheader_no": 1,
            "season_year": 2001,
            "season_type": "regular",
        }
    )

    assert saved is True
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20010509SKLT1").one()
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "20010509SSGLT1").one()
        assert game.game_date == date(2001, 5, 9)
        assert game.home_team == "LT"
        assert game.away_team == "SSG"
        assert alias.canonical_game_id == "20010509SKLT1"


def test_save_schedule_game_uses_payload_teams_for_malformed_source_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)

    saved = game_repository.save_schedule_game(
        {
            "game_id": "20260403SSSGT0",
            "game_date": "20260403",
            "away_team_code": "SS",
            "home_team_code": "KT",
            "season_year": 2026,
            "season_type": "regular",
        }
    )

    assert saved is True
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20260403SSKT0").one()
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "20260403SSSGT0").one()
        assert game.game_date == date(2026, 4, 3)
        assert game.away_team == "SS"
        assert game.home_team == "KT"
        assert alias.canonical_game_id == "20260403SSKT0"


def test_resolve_canonical_game_id_uses_alias_table(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)

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
