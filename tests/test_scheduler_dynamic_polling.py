from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import scripts.scheduler as scheduler
from src.models.game import Game, GameMetadata
from src.models.player import PlayerBasic

KST = ZoneInfo("Asia/Seoul")


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    # Create required tables
    for table in (
        Game.__table__,
        GameMetadata.__table__,
        PlayerBasic.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_get_live_poll_interval_seconds_no_games(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(scheduler, "SessionLocal", SessionLocal)

    # No games in DB -> should return 1800
    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 1800


def test_get_live_poll_interval_seconds_all_terminal(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(scheduler, "SessionLocal", SessionLocal)

    now_kst = datetime.now(KST)
    today_date = now_kst.date()

    with SessionLocal() as session:
        # Game 1: COMPLETED, updated_at 30 minutes ago
        session.add(
            Game(
                game_id="20260607TTXX0",
                game_date=today_date,
                game_status="COMPLETED",
                updated_at=(now_kst - timedelta(minutes=30)).replace(tzinfo=None),
            )
        )
        # Game 2: CANCELLED, updated_at 40 minutes ago
        session.add(
            Game(
                game_id="20260607TTXX1",
                game_date=today_date,
                game_status="CANCELLED",
                updated_at=(now_kst - timedelta(minutes=40)).replace(tzinfo=None),
            )
        )
        session.commit()

    # Long after last game -> should return 1800
    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 1800

    # Test cooldown: Game 1 finished 5 minutes ago
    with SessionLocal() as session:
        g = session.query(Game).filter(Game.game_id == "20260607TTXX0").first()
        g.updated_at = (now_kst - timedelta(minutes=5)).replace(tzinfo=None)
        session.commit()

    # Within cooldown period -> should return 60
    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 60


def test_get_live_poll_interval_seconds_active_games(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(scheduler, "SessionLocal", SessionLocal)

    now_kst = datetime.now(KST)
    today_date = now_kst.date()

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260607LIVE0",
                game_date=today_date,
                game_status="LIVE",
                updated_at=now_kst.replace(tzinfo=None),
            )
        )
        session.commit()

    # Active game -> should return 10
    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 10

    # Suspended game -> should return 60
    with SessionLocal() as session:
        g = session.query(Game).filter(Game.game_id == "20260607LIVE0").first()
        g.game_status = "SUSPENDED"
        session.commit()

    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 60


def test_get_live_poll_interval_seconds_scheduled_games(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(scheduler, "SessionLocal", SessionLocal)

    now_kst = datetime.now(KST)
    today_date = now_kst.date()

    # 1. Earliest start time is 2 hours from now -> should return 120
    start_time_1 = (now_kst + timedelta(hours=2)).time()
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260607SCHED0",
                game_date=today_date,
                game_status="SCHEDULED",
                updated_at=now_kst.replace(tzinfo=None),
            )
        )
        session.add(GameMetadata(game_id="20260607SCHED0", start_time=start_time_1))
        session.commit()

    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 120

    # 2. Earliest start time is 10 minutes from now (pregame) -> should return 30
    start_time_2 = (now_kst + timedelta(minutes=10)).time()
    with SessionLocal() as session:
        meta = session.query(GameMetadata).filter(GameMetadata.game_id == "20260607SCHED0").first()
        meta.start_time = start_time_2
        session.commit()

    interval = scheduler._get_live_poll_interval_seconds()
    assert interval == 30


def test_crawl_live_refresh_fast_exit(monkeypatch):
    calls = []

    # Mock dynamic interval helper to return 10s
    monkeypatch.setattr(scheduler, "_get_live_poll_interval_seconds", lambda: 10)
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)

    # Mock crawler cycle function
    async def fake_run_live_crawler_cycle(**kw):
        calls.append("executed")
        return {"active": True}

    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", fake_run_live_crawler_cycle)
    monkeypatch.setattr(scheduler, "_live_refresh_max_games_per_cycle", lambda: 1)

    # Reset globals
    scheduler.LAST_LIVE_RUN_TIME = None
    scheduler.LAST_LIVE_POLL_INTERVAL = None

    # First run: LAST_LIVE_RUN_TIME is None -> should run
    scheduler.crawl_live_refresh()
    assert calls == ["executed"]
    assert scheduler.LAST_LIVE_RUN_TIME is not None
    last_run = scheduler.LAST_LIVE_RUN_TIME

    # Second run: executed immediately (0s elapsed < 10s interval) -> should skip (exit early)
    calls.clear()
    scheduler.crawl_live_refresh()
    assert calls == []
    assert last_run == scheduler.LAST_LIVE_RUN_TIME

    # Simulate elapsed time of 15 seconds by manually setting LAST_LIVE_RUN_TIME back
    scheduler.LAST_LIVE_RUN_TIME = last_run - timedelta(seconds=15)
    scheduler.crawl_live_refresh()
    assert calls == ["executed"]
    assert last_run < scheduler.LAST_LIVE_RUN_TIME


def test_crawl_live_refresh_skips_when_live_lock_busy(monkeypatch):
    calls = []

    async def fake_run_live_crawler_cycle(**kw):
        calls.append(kw)
        return {"active": True}

    monkeypatch.setattr(scheduler, "_get_live_poll_interval_seconds", lambda: 0)
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", fake_run_live_crawler_cycle)
    monkeypatch.setattr(scheduler, "_live_refresh_max_games_per_cycle", lambda: 1)
    scheduler.LAST_LIVE_RUN_TIME = None
    scheduler.LAST_LIVE_POLL_INTERVAL = None

    assert scheduler.LIVE_LOCK.acquire(blocking=False)
    try:
        scheduler.crawl_live_refresh()
    finally:
        scheduler.LIVE_LOCK.release()

    assert calls == []
