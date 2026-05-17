from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError

from src.models.game import Game, GameEvent, GameSummary
from src.models.player import PlayerBasic
from src.models.team import Team, TeamDailyRoster


def _engine_with_foreign_keys():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _sqlite_foreign_keys(dbapi_con, _):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def test_game_delete_cascades_to_game_children():
    engine = _engine_with_foreign_keys()
    for table in (PlayerBasic.__table__, Game.__table__, GameEvent.__table__, GameSummary.__table__):
        table.create(bind=engine)

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO player_basic (player_id, name) VALUES (1001, '홍길동'), (2001, '투수')")
        )
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, away_team, home_team)
                VALUES ('20250401LGSS0', :game_date, 'LG', 'SS')
                """
            ),
            {"game_date": date(2025, 4, 1)},
        )
        conn.execute(
            text(
                """
                INSERT INTO game_events (game_id, event_seq, batter_id, pitcher_id)
                VALUES ('20250401LGSS0', 1, 1001, 2001)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_summary (game_id, summary_type, player_id)
                VALUES ('20250401LGSS0', '결승타', 1001)
                """
            )
        )

        conn.execute(text("DELETE FROM game WHERE game_id = '20250401LGSS0'"))

        assert conn.execute(text("SELECT COUNT(*) FROM game_events")).scalar() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM game_summary")).scalar() == 0


def test_player_and_team_deletes_are_restricted_when_referenced():
    engine = _engine_with_foreign_keys()
    for table in (Team.__table__, PlayerBasic.__table__, Game.__table__, GameSummary.__table__, TeamDailyRoster.__table__):
        table.create(bind=engine)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO teams (team_id, team_name, team_short_name, city, is_active)
                VALUES ('LG', 'LG 트윈스', 'LG', '서울', 1)
                """
            )
        )
        conn.execute(text("INSERT INTO player_basic (player_id, name) VALUES (1001, '홍길동')"))
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, away_team, home_team)
                VALUES ('20250401LGSS0', :game_date, 'LG', 'SS')
                """
            ),
            {"game_date": date(2025, 4, 1)},
        )
        conn.execute(
            text(
                """
                INSERT INTO game_summary (game_id, summary_type, player_id)
                VALUES ('20250401LGSS0', '결승타', 1001)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO team_daily_roster
                    (roster_date, team_code, player_id, player_basic_id, person_type, player_name, position)
                VALUES
                    (:roster_date, 'LG', 1001, 1001, 'player', '홍길동', '외야수')
                """
            ),
            {"roster_date": date(2025, 4, 1)},
        )

        with pytest.raises(IntegrityError):
            conn.execute(text("DELETE FROM player_basic WHERE player_id = 1001"))

        with pytest.raises(IntegrityError):
            conn.execute(text("DELETE FROM teams WHERE team_id = 'LG'"))
