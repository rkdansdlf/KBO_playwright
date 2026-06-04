from __future__ import annotations

import asyncio
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.cli.daily_review_batch as review_batch
import src.repositories.game_relay as game_relay_module
import src.repositories.game_save as game_save_module
import src.repositories.game_status as game_status_module
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
    GameValidationMetrics,
)
from src.models.player import PlayerBasic
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameMetadata.__table__,
        GameEvent.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameSummary.__table__,
        GameValidationMetrics.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class _FakeContextAggregator:
    def __init__(self, session):
        self.session = session

    def get_crucial_moments(self, *_args, **_kwargs):
        return [{"inning": 9, "description": "go-ahead run"}]

    def get_completed_game_pitching_breakdown(self, *_args, **_kwargs):
        return {"away": [], "home": []}

    def get_recent_player_movements(self, *_args, **_kwargs):
        return []

    def get_daily_roster_changes(self, *_args, **_kwargs):
        return []


def test_review_batch_refreshes_stale_scheduled_completed_game(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(review_batch, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_status_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(review_batch, "ContextAggregator", _FakeContextAggregator)
    monkeypatch.setattr(review_batch, "write_refresh_manifest", lambda **_kwargs: tmp_path / "manifest.json")

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20240516LGSS0",
                game_date=date(2024, 5, 16),
                away_team="LG",
                home_team="SS",
                away_score=3,
                home_score=5,
                game_status=GAME_STATUS_SCHEDULED,
            )
        )
        session.add(
            GameBattingStat(
                game_id="20240516LGSS0",
                team_side="away",
                player_name="Batter",
                appearance_seq=1,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20240516LGSS0",
                team_side="home",
                player_name="Pitcher",
                appearance_seq=1,
            )
        )
        session.add(GameValidationMetrics(game_id="20240516LGSS0", validation_status="verified"))
        session.commit()

    saved_ids = asyncio.run(review_batch.run_review_batch("20240516", sync_to_oci=False))

    assert saved_ids == ["20240516LGSS0"]
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20240516LGSS0").one()
        summary = (
            session.query(GameSummary)
            .filter(
                GameSummary.game_id == "20240516LGSS0",
                GameSummary.summary_type == review_batch.REVIEW_SUMMARY_TYPE,
            )
            .one()
        )
        assert game.game_status == GAME_STATUS_COMPLETED
        assert "go-ahead run" in summary.detail_text


def test_review_batch_allows_wpa_events_when_validation_metrics_missing(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(review_batch, "SessionLocal", SessionLocal)
    monkeypatch.setattr(review_batch, "ContextAggregator", _FakeContextAggregator)
    monkeypatch.setattr(review_batch, "write_refresh_manifest", lambda **_kwargs: tmp_path / "manifest.json")
    monkeypatch.setattr(
        review_batch,
        "refresh_game_status_for_date",
        lambda *_args, **_kwargs: {"total": 1, "updated": 0, "status_counts": {}},
    )

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20240517LGSS0",
                game_date=date(2024, 5, 17),
                away_team="LG",
                home_team="SS",
                away_score=3,
                home_score=5,
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(GameEvent(game_id="20240517LGSS0", event_seq=1, wpa=0.42))
        session.commit()

    saved_ids = asyncio.run(review_batch.run_review_batch("20240517", sync_to_oci=False))

    assert saved_ids == ["20240517LGSS0"]
