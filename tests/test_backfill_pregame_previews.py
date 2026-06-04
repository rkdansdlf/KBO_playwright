from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.cli.backfill_pregame_previews as backfill_pregame_previews
from src.models.game import Game, GameSummary
from src.models.player import PlayerBasic


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        PlayerBasic.__table__,
        Game.__table__,
        GameSummary.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_backfill_selects_date_when_preview_json_missing_starters(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(backfill_pregame_previews, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260602HHOB0",
                game_date=date(2026, 6, 2),
                away_team="HH",
                home_team="DB",
                away_pitcher="문동주",
                home_pitcher="곽빈",
                game_status="SCHEDULED",
            )
        )
        session.flush()
        session.add(
            GameSummary(
                game_id="20260602HHOB0",
                summary_type="프리뷰",
                detail_text=json.dumps(
                    {
                        "game_id": "20260602HHOB0",
                        "away_starter": "",
                        "home_starter": "",
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

    targets = backfill_pregame_previews.find_missing_pregame_dates(
        start_date="20260602",
        end_date="20260602",
    )

    assert targets == [
        backfill_pregame_previews.PregameBackfillDate(
            target_date="20260602",
            scheduled_total=1,
            starters_complete=1,
            preview_rows=1,
            preview_missing_starters=1,
        )
    ]


def test_backfill_skips_complete_preview_json(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(backfill_pregame_previews, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260602HHOB0",
                game_date=date(2026, 6, 2),
                away_team="HH",
                home_team="DB",
                away_pitcher="문동주",
                home_pitcher="곽빈",
                game_status="SCHEDULED",
            )
        )
        session.flush()
        session.add(
            GameSummary(
                game_id="20260602HHOB0",
                summary_type="프리뷰",
                detail_text=json.dumps(
                    {
                        "game_id": "20260602HHOB0",
                        "away_starter": "문동주",
                        "home_starter": "곽빈",
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

    assert (
        backfill_pregame_previews.find_missing_pregame_dates(
            start_date="20260602",
            end_date="20260602",
        )
        == []
    )
