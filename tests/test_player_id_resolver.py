from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.services.player_id_resolver import PlayerIdResolver


def _build_resolver_session():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    uniform_no TEXT,
                    team TEXT,
                    career TEXT,
                    status TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    player_id INTEGER NOT NULL,
                    season INTEGER NOT NULL,
                    team_code TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    player_id INTEGER NOT NULL,
                    season INTEGER NOT NULL,
                    team_code TEXT
                )
                """
            )
        )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_resolver_does_not_register_unknown_when_season_candidates_are_ambiguous():
    session = _build_resolver_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team)
                VALUES
                    (50996, '박시원', 'NC'),
                    (55121, '박시원', 'NC')
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting (player_id, season, team_code)
                VALUES
                    (50996, 2026, 'NC'),
                    (55121, 2026, 'NC')
                """
            )
        )
        session.commit()

        resolver = PlayerIdResolver(session)

        assert resolver.resolve_id("박시원", "NC", 2026) is None
        assert session.execute(text("SELECT COUNT(*) FROM player_basic WHERE player_id >= 900000")).scalar() == 0
    finally:
        session.close()


def test_preloaded_resolver_keeps_same_team_same_name_candidates_ambiguous():
    session = _build_resolver_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team)
                VALUES
                    (76100, '이병규', 'LG'),
                    (97109, '이병규', 'LG')
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting (player_id, season, team_code)
                VALUES
                    (76100, 2010, 'LG'),
                    (97109, 2010, 'LG')
                """
            )
        )
        session.commit()

        resolver = PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(2010)

        assert resolver.resolve_id("이병규", "LG", 2010) is None
    finally:
        session.close()


def test_strict_resolver_does_not_use_global_unique_name_or_register_unknown():
    session = _build_resolver_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team)
                VALUES (77777, '전역선수', 'LG')
                """
            )
        )
        session.commit()

        resolver = PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )

        assert resolver.resolve_id("전역선수", "SS", 2026) is None
        assert session.execute(text("SELECT COUNT(*) FROM player_basic WHERE player_id >= 900000")).scalar() == 0
    finally:
        session.close()


def test_resolver_does_not_auto_register_unresolved_player_by_default():
    session = _build_resolver_session()
    try:
        resolver = PlayerIdResolver(session)

        assert resolver.resolve_id("없는선수", "NC", 2026) is None
        assert session.execute(text("SELECT COUNT(*) FROM player_basic WHERE player_id >= 900000")).scalar() == 0
    finally:
        session.close()


def test_resolver_does_not_auto_register_without_team_context():
    session = _build_resolver_session()
    try:
        resolver = PlayerIdResolver(session)

        assert resolver.resolve_id("무소속", None, 2026) is None
        assert session.execute(text("SELECT COUNT(*) FROM player_basic WHERE player_id >= 900000")).scalar() == 0
    finally:
        session.close()


def test_resolver_reuses_existing_unknown_exact_key():
    session = _build_resolver_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team, uniform_no, status)
                VALUES
                    (900123, '새선수', 'LG', '17', 'Unknown/Local'),
                    (900456, '새선수', 'LG', '17', 'Unknown/Local')
                """
            )
        )
        session.commit()

        resolver = PlayerIdResolver(session)

        assert resolver.resolve_id("새선수", "LG", 2026, uniform_no="17") == 900123
        assert session.execute(text("SELECT COUNT(*) FROM player_basic WHERE name = '새선수'")).scalar() == 2
    finally:
        session.close()
