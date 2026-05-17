from __future__ import annotations

from datetime import date

from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

import src.cli.generate_quality_report as generate_quality_report
from src.models.game import Game


class _ReportTestBase(DeclarativeBase):
    pass


class _PlayerBasicWithoutCreatedAt(_ReportTestBase):
    __tablename__ = "player_basic"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


def _build_session_factory(*, player_created_at_column: bool):
    engine = create_engine("sqlite:///:memory:")
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
