from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_relay as game_relay_module
import src.repositories.game_save as game_save_module
import src.services.postgame_reconciliation_service as service
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerBasic
from src.services.postgame_reconciliation_service import ReconciliationRequest
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_SCHEDULED


import pytest

pytestmark = pytest.mark.integration


class _FakeDetailCrawler:
    pass


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_find_postgame_reconciliation_targets_live_and_completed_missing_scores(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add_all(
            [
                Game(
                    game_id="20260424LGOB0",
                    game_date=date(2026, 4, 24),
                    game_status=GAME_STATUS_LIVE,
                ),
                Game(
                    game_id="20260424HTNC0",
                    game_date=date(2026, 4, 24),
                    game_status=GAME_STATUS_COMPLETED,
                    away_score=None,
                    home_score=2,
                ),
                Game(
                    game_id="20260424SSKT0",
                    game_date=date(2026, 4, 24),
                    game_status=GAME_STATUS_COMPLETED,
                    away_score=3,
                    home_score=2,
                ),
                Game(
                    game_id="20260425SSKT0",
                    game_date=date(2026, 4, 25),
                    game_status=GAME_STATUS_SCHEDULED,
                ),
            ]
        )
        session.commit()

    targets = service.find_postgame_reconciliation_targets("20260424", "20260424")

    assert [target.game_id for target in targets] == ["20260424HTNC0", "20260424LGOB0"]


def test_find_postgame_reconciliation_targets_can_force_include_game_id(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260424SSKT0",
                game_date=date(2026, 4, 24),
                game_status=GAME_STATUS_COMPLETED,
                away_score=3,
                home_score=2,
            )
        )
        session.commit()

    targets = service.find_postgame_reconciliation_targets(
        "20260424",
        "20260424",
        extra_game_ids=["20260424SSKT0"],
    )

    assert [target.game_id for target in targets] == ["20260424SSKT0"]


def test_reconcile_postgame_range_reports_status_and_score_changes(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(service, "repair_game_parent_from_existing_children", lambda _game_id: True)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260424LGOB0",
                game_date=date(2026, 4, 24),
                game_status=GAME_STATUS_LIVE,
                away_score=None,
                home_score=None,
            )
        )
        session.commit()

    async def _fake_collect(games, *, detail_crawler, config=None, **_kwargs):
        assert detail_crawler.__class__ is _FakeDetailCrawler
        assert config is not None
        assert config.force is True
        assert config.concurrency == 1
        game_list = list(games)
        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == "20260424LGOB0").one()
            game.game_status = GAME_STATUS_COMPLETED
            game.away_score = 4
            game.home_score = 1
            session.commit()
        return SimpleNamespace(
            items={
                "20260424LGOB0": SimpleNamespace(
                    detail_saved=True,
                    detail_status="saved",
                    failure_reason=None,
                )
            },
            processed_game_ids=[target.game_id for target in game_list],
        )

    monkeypatch.setattr(service, "crawl_and_save_game_details", _fake_collect)

    result = asyncio.run(
        service.reconcile_postgame_range(
            ReconciliationRequest(
                "20260424", "20260424", detail_crawler=_FakeDetailCrawler(), log=lambda _message: None
            )
        )
    )

    assert result.candidates == 1
    assert result.changed_game_ids == ["20260424LGOB0"]
    change = result.changes[0]
    assert change.before_status == GAME_STATUS_LIVE
    assert change.after_status == GAME_STATUS_COMPLETED
    assert (change.before_away_score, change.before_home_score) == (None, None)
    assert (change.after_away_score, change.after_home_score) == (4, 1)
    assert "20260424LGOB0" in service.format_reconciliation_report(result.changes)


def test_reconcile_postgame_range_marks_cancelled_miss(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260424LGOB0",
                game_date=date(2026, 4, 24),
                game_status=GAME_STATUS_LIVE,
            )
        )
        session.commit()

    async def _fake_collect(games, **_kwargs):
        return SimpleNamespace(
            items={
                "20260424LGOB0": SimpleNamespace(
                    detail_saved=False,
                    detail_status="crawl_failed",
                    failure_reason="cancelled",
                )
            }
        )

    def _fake_update_status(game_id: str, status: str):
        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).one()
            game.game_status = status
            session.commit()
        return True

    monkeypatch.setattr(service, "crawl_and_save_game_details", _fake_collect)
    monkeypatch.setattr(service, "update_game_status", _fake_update_status)

    result = asyncio.run(
        service.reconcile_postgame_range(
            ReconciliationRequest(
                "20260424", "20260424", detail_crawler=_FakeDetailCrawler(), log=lambda _message: None
            )
        )
    )

    assert result.changed_game_ids == ["20260424LGOB0"]
    assert result.changes[0].after_status == "CANCELLED"
    assert result.changes[0].failure_reason == "cancelled"


def test_reconcile_postgame_range_preserves_existing_cancelled_detail_miss(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    repair_calls = []
    status_updates = []

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260527SSLG0",
                game_date=date(2026, 5, 27),
                game_status=GAME_STATUS_CANCELLED,
                away_score=3,
                home_score=5,
            )
        )
        session.add(
            GameBattingStat(
                game_id="20260527SSLG0",
                team_side="away",
                team_code="SS",
                player_id=1001,
                player_name="Stale Batter",
                appearance_seq=1,
            )
        )
        session.commit()

    async def _fake_collect(games, **_kwargs):
        assert [target.game_id for target in games] == ["20260527SSLG0"]
        return SimpleNamespace(
            items={
                "20260527SSLG0": SimpleNamespace(
                    detail_saved=False,
                    detail_status="crawl_failed",
                    failure_reason="no_detail_payload",
                )
            }
        )

    monkeypatch.setattr(service, "crawl_and_save_game_details", _fake_collect)
    monkeypatch.setattr(
        service,
        "repair_game_parent_from_existing_children",
        lambda game_id: repair_calls.append(game_id) or True,
    )
    monkeypatch.setattr(service, "update_game_status", lambda game_id, status: status_updates.append((game_id, status)))

    result = asyncio.run(
        service.reconcile_postgame_range(
            ReconciliationRequest(
                "20260527",
                "20260527",
                detail_crawler=_FakeDetailCrawler(),
                extra_game_ids=["20260527SSLG0"],
                log=lambda _message: None,
            )
        )
    )

    assert result.candidates == 1
    assert result.changed_game_ids == []
    assert repair_calls == []
    assert status_updates == []
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20260527SSLG0").one()
        assert game.game_status == GAME_STATUS_CANCELLED
        assert (game.away_score, game.home_score) == (3, 5)
