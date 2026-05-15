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
                    career TEXT
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
