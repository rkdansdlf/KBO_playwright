from __future__ import annotations

import asyncio
import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.cli.daily_story_batch as story_batch
from src.models.game import Game, GameEvent, GameSummary, GameValidationMetrics
from src.services.game_story_builder import STORY_SUMMARY_TYPE
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
        GameValidationMetrics.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_story_batch_saves_completed_games_only(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(story_batch, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        story_batch,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 2, "updated": 0, "status_counts": {}},
    )
    monkeypatch.setattr(story_batch, "write_refresh_manifest", lambda **_kwargs: tmp_path / "manifest.json")

    with SessionLocal() as session:
        session.add_all(
            [
                Game(
                    game_id="20250405HHSS0",
                    game_date=date(2025, 4, 5),
                    away_team="HH",
                    home_team="SS",
                    away_score=7,
                    home_score=6,
                    game_status=GAME_STATUS_COMPLETED,
                ),
                Game(
                    game_id="20250405LGOB0",
                    game_date=date(2025, 4, 5),
                    away_team="LG",
                    home_team="OB",
                    game_status=GAME_STATUS_SCHEDULED,
                ),
            ]
        )
        session.add(
            GameEvent(
                game_id="20250405HHSS0",
                event_seq=1,
                inning=9,
                inning_half="TOP",
                description="우익수 뒤 홈런 (홈런거리:120M)",
                event_type="HIT",
                result_code="HR",
                rbi=1,
                batter_name="문현빈",
                pitcher_name="김재윤",
                wpa=0.5,
                away_score=7,
                home_score=6,
            )
        )
        session.add(GameValidationMetrics(game_id="20250405HHSS0", validation_status="verified"))
        session.commit()

    saved_ids = asyncio.run(story_batch.run_story_batch("20250405", sync_to_oci=False))

    assert saved_ids == ["20250405HHSS0"]
    with SessionLocal() as session:
        summaries = session.query(GameSummary).order_by(GameSummary.game_id.asc()).all()
        assert len(summaries) == 1
        assert summaries[0].summary_type == STORY_SUMMARY_TYPE
        payload = json.loads(summaries[0].detail_text)
        assert payload["game_id"] == "20250405HHSS0"
        assert payload["timeline"][0]["description"] == "우익수 뒤 홈런 (홈런거리:120M)"


def test_story_batch_allows_wpa_events_when_validation_metrics_missing(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(story_batch, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        story_batch,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 1, "updated": 0, "status_counts": {}},
    )
    monkeypatch.setattr(story_batch, "write_refresh_manifest", lambda **_kwargs: tmp_path / "manifest.json")

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250406HHSS0",
                game_date=date(2025, 4, 6),
                away_team="HH",
                home_team="SS",
                away_score=7,
                home_score=6,
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(
            GameEvent(
                game_id="20250406HHSS0",
                event_seq=1,
                inning=9,
                inning_half="TOP",
                description="우전 안타",
                event_type="HIT",
                batter_name="문현빈",
                wpa=0.5,
                away_score=7,
                home_score=6,
            )
        )
        session.commit()

    saved_ids = asyncio.run(story_batch.run_story_batch("20250406", sync_to_oci=False))

    assert saved_ids == ["20250406HHSS0"]
