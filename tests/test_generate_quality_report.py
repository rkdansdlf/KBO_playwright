from __future__ import annotations

from datetime import date

from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

import src.cli.generate_quality_report as generate_quality_report
from src.models.game import Game, GamePlayByPlay
from src.models.player import PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily


class _ReportTestBase(DeclarativeBase):
    pass


class _PlayerBasicWithoutCreatedAt(_ReportTestBase):
    __tablename__ = "player_basic"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


def _build_session_factory(*, player_created_at_column: bool):
    engine = create_engine("sqlite:///:memory:")
    KboSeason.__table__.create(bind=engine)
    Game.__table__.create(bind=engine)
    if player_created_at_column:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_at DATETIME
                )
                """
            )
    else:
        _PlayerBasicWithoutCreatedAt.__table__.create(bind=engine)
    GamePlayByPlay.__table__.create(bind=engine)
    TeamStandingsDaily.__table__.create(bind=engine)
    PlayerSeasonBatting.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_scheduled_game(session, game_id: str = "20260516LGSS0") -> None:
    session.add(
        Game(
            game_id=game_id,
            game_date=date(2026, 5, 16),
            away_team="LG",
            home_team="SS",
            game_status="SCHEDULED",
        )
    )


def _seed_regular_season(session, season_id: int = 20260) -> None:
    session.add(
        KboSeason(
            season_id=season_id,
            season_year=2026,
            league_type_code=0,
            league_type_name="regular",
        )
    )


def _seed_completed_game(
    session,
    game_id: str,
    game_date: date,
    *,
    away_team: str = "LG",
    home_team: str = "SS",
    away_score: int | None = 3,
    home_score: int | None = 2,
    season_id: int = 20260,
) -> None:
    session.add(
        Game(
            game_id=game_id,
            game_date=game_date,
            away_team=away_team,
            home_team=home_team,
            away_score=away_score,
            home_score=home_score,
            season_id=season_id,
            game_status="COMPLETED",
        )
    )


def _standings_row(team_code: str, **overrides):
    values = {
        "standings_date": date(2026, 5, 16),
        "team_code": team_code,
        "games_played": 1,
        "wins": 0,
        "losses": 0,
        "draws": 0,
        "win_pct": 0.0,
        "games_behind": 0.0,
        "current_streak": 0,
        "runs_scored": 0,
        "runs_allowed": 0,
        "run_differential": 0,
    }
    values.update(overrides)
    return TeamStandingsDaily(**values)


def _base_report_metrics(**overrides):
    metrics = {
        "date": "20260516",
        "status_counts": {},
        "detail_integrity": [],
        "new_players": [],
        "relay_integrity": {"ok": True},
        "standings_integrity": {"ok": True},
        "total_games": 0,
        "completed_count": 0,
    }
    metrics.update(overrides)
    return metrics


def test_daily_metrics_computes_new_players_when_created_at_exists_but_model_lacks_attr(monkeypatch):
    SessionLocal = _build_session_factory(player_created_at_column=True)
    monkeypatch.setattr(generate_quality_report, "PlayerBasic", _PlayerBasicWithoutCreatedAt)

    with SessionLocal() as session:
        _seed_scheduled_game(session)
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, created_at)
                VALUES
                    (1001, 'Rookie One', '2026-05-16 04:10:00'),
                    (1002, 'Old Player', '2026-05-15 23:59:59')
                """
            )
        )
        session.commit()

        metrics = generate_quality_report.get_daily_metrics(session, "20260516")

    assert metrics["new_players"] == [{"id": 1001, "name": "Rookie One"}]


def test_daily_metrics_omits_new_players_when_created_at_is_unavailable(monkeypatch):
    SessionLocal = _build_session_factory(player_created_at_column=False)
    monkeypatch.setattr(generate_quality_report, "PlayerBasic", _PlayerBasicWithoutCreatedAt)

    with SessionLocal() as session:
        _seed_scheduled_game(session)
        session.add(_PlayerBasicWithoutCreatedAt(player_id=1001, name="Rookie One"))
        session.commit()

        metrics = generate_quality_report.get_daily_metrics(session, "20260516")

    assert metrics["new_players"] == []
    message = generate_quality_report.format_telegram_report(
        metrics,
        {"ok": True, "batting": {}, "pitching": {}},
    )
    assert "New Players" not in message


def test_relay_integrity_reports_recent_and_current_season_missing_pbp():
    SessionLocal = _build_session_factory(player_created_at_column=True)

    with SessionLocal() as session:
        _seed_regular_season(session)
        _seed_completed_game(session, "20260516LGSS0", date(2026, 5, 16))
        _seed_completed_game(session, "20260401KTWO0", date(2026, 4, 1), away_team="KT", home_team="WO")
        _seed_completed_game(session, "20260515NCHH0", date(2026, 5, 15), away_team="NC", home_team="HH")
        session.add(GamePlayByPlay(game_id="20260515NCHH0", inning=1, inning_half="top"))
        session.commit()

        relay = generate_quality_report.get_relay_integrity_metrics(session, date(2026, 5, 16))

    assert relay["ok"] is False
    assert relay["recent_missing_count"] == 1
    assert relay["current_season_missing_count"] == 2
    assert relay["missing_game_ids"] == ["20260401KTWO0", "20260516LGSS0"]

    message = generate_quality_report.format_telegram_report(
        _base_report_metrics(relay_integrity=relay),
        {"ok": True, "batting": {}, "pitching": {}},
    )
    assert "PBP" in message
    assert "20260516LGSS0" in message


def test_standings_integrity_passes_when_snapshot_matches_game_rollup():
    SessionLocal = _build_session_factory(player_created_at_column=True)

    with SessionLocal() as session:
        _seed_regular_season(session)
        _seed_completed_game(session, "20260516LGSS0", date(2026, 5, 16))
        session.add_all(
            [
                _standings_row(
                    "LG",
                    wins=1,
                    runs_scored=3,
                    runs_allowed=2,
                    run_differential=1,
                    win_pct=1.0,
                ),
                _standings_row(
                    "SS",
                    losses=1,
                    runs_scored=2,
                    runs_allowed=3,
                    run_differential=-1,
                ),
            ]
        )
        session.commit()

        result = generate_quality_report.validate_standings_integrity(session, date(2026, 5, 16))

    assert result["ok"] is True
    assert result["mismatches"] == []
    assert result["missing_score_games"] == []


def test_standings_integrity_reports_value_mismatch_in_message():
    SessionLocal = _build_session_factory(player_created_at_column=True)

    with SessionLocal() as session:
        _seed_regular_season(session)
        _seed_completed_game(session, "20260516LGSS0", date(2026, 5, 16))
        session.add_all(
            [
                _standings_row("LG", wins=0, losses=1, runs_scored=3, runs_allowed=2),
                _standings_row("SS", losses=1, runs_scored=2, runs_allowed=3),
            ]
        )
        session.commit()

        result = generate_quality_report.validate_standings_integrity(session, date(2026, 5, 16))

    assert result["ok"] is False
    assert result["mismatches"][0]["team_code"] == "LG"
    assert result["mismatches"][0]["issue"] == "value_mismatch"
    assert "wins" in result["mismatches"][0]["differences"]

    message = generate_quality_report.format_telegram_report(
        _base_report_metrics(standings_integrity=result),
        {"ok": True, "batting": {}, "pitching": {}},
    )
    assert "Standings" in message
    assert "LG: value_mismatch" in message


def test_standings_integrity_reports_completed_games_missing_scores():
    SessionLocal = _build_session_factory(player_created_at_column=True)

    with SessionLocal() as session:
        _seed_regular_season(session)
        _seed_completed_game(
            session,
            "20260516LGSS0",
            date(2026, 5, 16),
            away_score=None,
            home_score=None,
        )
        session.commit()

        result = generate_quality_report.validate_standings_integrity(session, date(2026, 5, 16))

    assert result["ok"] is False
    assert result["missing_score_games"] == ["20260516LGSS0"]


def test_report_issue_detection_includes_relay_and_standings_integrity():
    gate_result = {"ok": True, "batting": {}, "pitching": {}}

    assert generate_quality_report._has_report_issues(_base_report_metrics(), gate_result) is False
    assert (
        generate_quality_report._has_report_issues(
            _base_report_metrics(relay_integrity={"ok": False}),
            gate_result,
        )
        is True
    )
    assert (
        generate_quality_report._has_report_issues(
            _base_report_metrics(standings_integrity={"ok": False}),
            gate_result,
        )
        is True
    )


def test_team_stats_integrity_all_ok():
    gate = {
        "team_batting": {"ok": True, "mismatches": [], "checked_players": 10},
        "team_pitching": {"ok": True, "mismatches": [], "checked_players": 10},
    }
    result = generate_quality_report.get_team_stats_integrity(gate)
    assert result["ok"] is True
    assert result["total_mismatches"] == 0
    assert result["batting_checked"] == 10
    assert result["pitching_checked"] == 10


def test_team_stats_integrity_batting_mismatch():
    gate = {
        "team_batting": {
            "ok": False,
            "mismatches": [{"team_id": "SSG", "issue": "games mismatch"}],
            "checked_players": 10,
        },
        "team_pitching": {"ok": True, "mismatches": [], "checked_players": 10},
    }
    result = generate_quality_report.get_team_stats_integrity(gate)
    assert result["ok"] is False
    assert result["total_mismatches"] == 1
    assert result["batting_ok"] is False
    assert result["pitching_ok"] is True


def test_team_stats_integrity_pitching_mismatch():
    gate = {
        "team_batting": {"ok": True, "mismatches": [], "checked_players": 10},
        "team_pitching": {
            "ok": False,
            "mismatches": [{"team_id": "LG", "issue": "era mismatch"}],
            "checked_players": 10,
        },
    }
    result = generate_quality_report.get_team_stats_integrity(gate)
    assert result["ok"] is False
    assert result["total_mismatches"] == 1
    assert result["batting_ok"] is True
    assert result["pitching_ok"] is False


def test_team_stats_integrity_both_mismatch():
    gate = {
        "team_batting": {
            "ok": False,
            "mismatches": [{"team_id": "SSG", "issue": "games mismatch"}],
            "checked_players": 10,
        },
        "team_pitching": {
            "ok": False,
            "mismatches": [{"team_id": "LG", "issue": "era mismatch"}],
            "checked_players": 10,
        },
    }
    result = generate_quality_report.get_team_stats_integrity(gate)
    assert result["ok"] is False
    assert result["total_mismatches"] == 2
    assert result["batting_ok"] is False
    assert result["pitching_ok"] is False


def test_team_stats_integrity_empty_gate():
    result = generate_quality_report.get_team_stats_integrity({})
    assert result["ok"] is True
    assert result["total_mismatches"] == 0
    assert result["batting_checked"] == 0
    assert result["pitching_checked"] == 0
