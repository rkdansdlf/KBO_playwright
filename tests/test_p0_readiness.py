from __future__ import annotations

import json
from datetime import date, time

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import src.cli.check_data_status as check_data_status
from src.models.broadcast import GameBroadcast
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
from src.models.roster_transaction import RosterTransaction
from src.models.source_registry import DataSource
from src.models.team import Team, TeamDailyRoster
from src.services.p0_readiness import P0ReadinessOptions, build_p0_readiness


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        DataSource.__table__,
        Game.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GamePlayByPlay.__table__,
        GameSummary.__table__,
        GameBroadcast.__table__,
        TeamDailyRoster.__table__,
        RosterTransaction.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _build_legacy_game_session_factory():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    id INTEGER PRIMARY KEY,
                    game_id VARCHAR NOT NULL,
                    game_date DATE,
                    stadium VARCHAR,
                    home_team VARCHAR,
                    away_team VARCHAR,
                    home_score INTEGER,
                    away_score INTEGER,
                    away_pitcher VARCHAR,
                    home_pitcher VARCHAR,
                    game_status VARCHAR
                )
                """,
            ),
        )
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        DataSource.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GamePlayByPlay.__table__,
        GameSummary.__table__,
        GameBroadcast.__table__,
        TeamDailyRoster.__table__,
        RosterTransaction.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _add_metadata(session, game_id: str, stadium: str = "잠실"):
    session.add(
        GameMetadata(
            game_id=game_id,
            stadium_name=stadium,
            start_time=time(18, 30),
        ),
    )


def _add_broadcast(session, game_id: str):
    session.add(GameBroadcast(game_id=game_id, broadcaster="SPOTV", channel_name="SPOTV", source="KBO"))


def _add_roster(session, roster_date: date):
    session.add(
        TeamDailyRoster(
            roster_date=roster_date,
            team_code="LG",
            player_id=1,
            player_name="홍길동",
            position="투수",
        ),
    )


def _add_completed_detail(session, game_id: str):
    session.add_all(
        [
            GameInningScore(game_id=game_id, team_side="away", team_code="LG", inning=1, runs=1),
            GameInningScore(game_id=game_id, team_side="home", team_code="SS", inning=1, runs=0),
            GameBattingStat(game_id=game_id, team_side="away", team_code="LG", player_name="타자A", appearance_seq=1),
            GameBattingStat(game_id=game_id, team_side="home", team_code="SS", player_name="타자B", appearance_seq=1),
            GamePitchingStat(
                game_id=game_id,
                team_side="away",
                team_code="LG",
                player_name="투수A",
                appearance_seq=1,
                wins=1,
                is_starting=True,
            ),
            GamePitchingStat(
                game_id=game_id,
                team_side="home",
                team_code="SS",
                player_name="투수B",
                appearance_seq=1,
                losses=1,
                is_starting=True,
            ),
            GameEvent(game_id=game_id, event_seq=1, inning=1, inning_half="top", description="안타"),
            GamePlayByPlay(game_id=game_id, inning=1, inning_half="top", play_description="안타"),
        ],
    )


def test_build_p0_readiness_reports_clean_operational_window():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        completed_id = "20250101LGSS0"
        live_id = "20250102HHKT0"
        scheduled_id = "20250103NCLT0"
        session.add_all(
            [
                Game(
                    game_id=completed_id,
                    game_date=date(2025, 1, 1),
                    away_team="LG",
                    home_team="SS",
                    away_score=4,
                    home_score=2,
                    game_status="COMPLETED",
                ),
                Game(
                    game_id=live_id,
                    game_date=date(2025, 1, 2),
                    away_team="HH",
                    home_team="KT",
                    away_score=1,
                    home_score=1,
                    game_status="LIVE",
                    game_lifecycle_state="running",
                ),
                Game(
                    game_id=scheduled_id,
                    game_date=date(2025, 1, 3),
                    away_team="NC",
                    home_team="LT",
                    away_pitcher="선발A",
                    home_pitcher="선발B",
                    game_status="SCHEDULED",
                ),
            ],
        )
        for game_id in (completed_id, live_id, scheduled_id):
            _add_metadata(session, game_id)
            _add_broadcast(session, game_id)
        _add_completed_detail(session, completed_id)
        session.add_all(
            [
                GameEvent(game_id=live_id, event_seq=1, inning=5, inning_half="bottom", description="볼넷"),
                GamePlayByPlay(game_id=live_id, inning=5, inning_half="bottom", play_description="볼넷"),
                GameLineup(
                    game_id=scheduled_id,
                    team_side="away",
                    team_code="NC",
                    player_name="타자A",
                    batting_order=1,
                    position="CF",
                    is_starter=True,
                    appearance_seq=1,
                ),
                GameLineup(
                    game_id=scheduled_id,
                    team_side="home",
                    team_code="LT",
                    player_name="타자B",
                    batting_order=1,
                    position="CF",
                    is_starter=True,
                    appearance_seq=1,
                ),
                GameSummary(game_id=scheduled_id, summary_type="프리뷰", detail_text="{}"),
            ],
        )
        _add_roster(session, date(2025, 1, 1))
        _add_roster(session, date(2025, 1, 2))
        session.commit()

        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(target_date="20250102", lookback_days=1, lookahead_days=1),
        )

    assert readiness["summary"]["ok"] is True
    assert readiness["summary"]["critical_failure_count"] == 0
    assert readiness["schedule"]["games"] == 3
    assert readiness["pregame"]["starters_complete"] == 1
    assert readiness["live"]["with_relay"] == 1
    assert readiness["postgame"]["boxscore_detail_complete"] == 1
    assert readiness["relay"]["with_events_or_pbp"] == 2
    assert readiness["roster"]["daily_roster_dates"] == 2
    assert readiness["broadcast"]["with_broadcast"] == 3


def test_build_p0_readiness_reports_dataset_failures():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        game_id = "20250101LGSS0"
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 1, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
            ),
        )
        session.commit()

        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(target_date="20250101", lookback_days=0, lookahead_days=0),
        )

    failures = {(failure["dataset"], failure["game_id"], failure["reason"]) for failure in readiness["failures"]}
    assert ("postgame", game_id, "missing_final_score") in failures
    assert ("postgame", game_id, "missing_boxscore_detail") in failures
    assert ("relay", game_id, "missing_relay") in failures
    assert ("broadcast", game_id, "broadcast_source_unavailable") in failures
    assert readiness["broadcast"]["skip_counts"] == {"broadcast_source_unavailable": 1}
    assert readiness["broadcast"]["skip_game_ids"] == {"broadcast_source_unavailable": [game_id]}
    assert readiness["summary"]["ok"] is False
    assert readiness["summary"]["critical_failure_count"] >= 3


def test_build_p0_readiness_marks_future_broadcast_as_not_announced():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        game_id = "20250102LGSS0"
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 1, 2),
                away_team="LG",
                home_team="SS",
                away_pitcher="선발A",
                home_pitcher="선발B",
                game_status="SCHEDULED",
            ),
        )
        _add_metadata(session, game_id)
        session.commit()

        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(target_date="20250101", lookback_days=0, lookahead_days=1),
        )

    assert readiness["broadcast"]["skip_counts"] == {"broadcast_not_announced": 1}
    assert readiness["broadcast"]["skip_game_ids"] == {"broadcast_not_announced": [game_id]}
    assert any(failure["reason"] == "broadcast_not_announced" for failure in readiness["failures"])


def test_build_p0_readiness_handles_legacy_game_schema_without_lifecycle_column():
    SessionLocal = _build_legacy_game_session_factory()
    with SessionLocal() as session:
        session.execute(
            text(
                """
                INSERT INTO game (
                    game_id, game_date, stadium, away_team, home_team,
                    away_pitcher, home_pitcher, game_status
                )
                VALUES (
                    '20250103NCLT0', '2025-01-03', '사직', 'NC', 'LT',
                    '선발A', '선발B', 'SCHEDULED'
                )
                """,
            ),
        )
        _add_metadata(session, "20250103NCLT0", stadium="사직")
        _add_broadcast(session, "20250103NCLT0")
        _add_roster(session, date(2025, 1, 3))
        session.commit()

        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(target_date="20250103", lookback_days=0, lookahead_days=0),
        )

    assert readiness["schedule"]["games"] == 1
    assert readiness["schedule"]["with_stadium"] == 1
    assert readiness["summary"]["critical_failure_count"] == 0


def test_check_data_status_p0_json_output(monkeypatch, capsys):
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250103NCLT0",
                game_date=date(2025, 1, 3),
                away_team="NC",
                home_team="LT",
                game_status="SCHEDULED",
            ),
        )
        session.commit()

    monkeypatch.setattr(check_data_status, "SessionLocal", SessionLocal)
    check_data_status.main(["--p0", "--date", "20250103", "--lookahead-days", "0", "--json"])

    payload = json.loads(capsys.readouterr().out)
    readiness = payload["p0_readiness"]
    assert readiness["target_date"] == "20250103"
    assert readiness["pregame"]["games"] == 1
    assert any(failure["reason"] == "missing_starter" for failure in readiness["failures"])
