from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.validators.season_team_code import audit_season_team_codes


def test_audit_classifies_archive_all_star_and_unresolved_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    player_id INTEGER,
                    season INTEGER,
                    source TEXT,
                    team_code TEXT
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    player_id INTEGER,
                    season INTEGER,
                    source TEXT,
                    team_code TEXT
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE player_game_batting (
                    player_id INTEGER,
                    game_id TEXT,
                    team_code TEXT
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE player_game_pitching (
                    player_id INTEGER,
                    game_id TEXT,
                    team_code TEXT
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                INSERT INTO player_season_batting (player_id, season, source, team_code)
                VALUES
                    (1, 1982, 'OFFICIAL_ARCHIVE', NULL),
                    (2, 2023, 'AGGREGATED', NULL),
                    (3, 2023, 'AGGREGATED', NULL)
                """,
            ),
        )
        connection.execute(
            text(
                """
                INSERT INTO player_season_pitching (player_id, season, source, team_code)
                VALUES
                    (4, 1982, 'OFFICIAL_ARCHIVE', NULL),
                    (5, 2022, 'AGGREGATED', NULL),
                    (6, 2023, 'AGGREGATED', NULL)
                """,
            ),
        )
        connection.execute(
            text(
                """
                INSERT INTO player_game_batting (player_id, game_id, team_code)
                VALUES (2, '20230715EAWE0', 'EA'), (3, '20230715LGSS0', 'LG')
                """,
            ),
        )
        connection.execute(
            text(
                """
                INSERT INTO player_game_pitching (player_id, game_id, team_code)
                VALUES (5, '20220716WEEA0', 'WE'), (6, '20230715LGSS0', 'SS')
                """,
            ),
        )

    with Session(engine) as session:
        audit = audit_season_team_codes(session)

    assert audit.batting_missing == 3
    assert audit.batting_archive == 1
    assert audit.batting_all_star == 1
    assert audit.batting_unresolved == 1
    assert audit.pitching_missing == 3
    assert audit.pitching_archive == 1
    assert audit.pitching_all_star == 1
    assert audit.pitching_unresolved == 1
    assert audit.total_unresolved == 2
