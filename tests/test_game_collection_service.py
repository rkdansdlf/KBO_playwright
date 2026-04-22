from __future__ import annotations

import asyncio
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.services.game_collection_service as service
from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat, GamePlayByPlay
from src.models.player import PlayerBasic


class _FakeDetailCrawler:
    def __init__(self):
        self.calls = []

    async def crawl_games(self, games, concurrency=None, lightweight=False):
        self.calls.append(
            {
                "games": list(games),
                "concurrency": concurrency,
                "lightweight": lightweight,
            }
        )
        return [{"game_id": game["game_id"], "game_date": game["game_date"]} for game in games]


class _FakeRelayCrawler:
    def __init__(self):
        self.calls = []

    async def crawl_game_events(self, game_id: str):
        self.calls.append(game_id)
        return {"events": [{"event_seq": 1, "inning": 1, "inning_half": "top", "description": game_id}]}


class _FailingDetailCrawler:
    async def crawl_games(self, games, concurrency=None, lightweight=False):
        return []

    def get_last_failure_reason(self, game_id: str):
        return "missing"


class _EmptyHittersDetailCrawler:
    async def crawl_games(self, games, concurrency=None, lightweight=False):
        return [
            {
                "game_id": game["game_id"],
                "game_date": game["game_date"],
                "hitters": {"away": [], "home": []},
            }
            for game in games
        ]


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GamePlayByPlay.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_existing_game(SessionLocal, game_id: str):
    with SessionLocal() as session:
        session.add(Game(game_id=game_id, game_date=date(2025, 4, 1), away_team="LG", home_team="SS"))
        session.add(
            GameBattingStat(
                game_id=game_id,
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="Existing Batter",
                appearance_seq=1,
            )
        )
        session.add(
            GamePitchingStat(
                game_id=game_id,
                team_side="home",
                team_code="SS",
                player_id=2001,
                player_name="Existing Pitcher",
                appearance_seq=1,
            )
        )
        session.add(
            GameEvent(
                game_id=game_id,
                event_seq=1,
                inning=1,
                inning_half="top",
                description="existing",
            )
        )
        session.commit()


def test_crawl_and_save_game_details_skips_existing_detail_and_relay(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_existing_game(SessionLocal, "20250401LGSS0")

    saved_details = []
    saved_relays = []
    monkeypatch.setattr(service, "save_game_detail", lambda payload: saved_details.append(payload["game_id"]) or True)
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, events: saved_relays.append((game_id, len(events))) or len(events),
    )

    detail_crawler = _FakeDetailCrawler()
    relay_crawler = _FakeRelayCrawler()
    result = asyncio.run(
        service.crawl_and_save_game_details(
            [
                {"game_id": "20250401LGSS0", "game_date": "2025-04-01"},
                {"game_id": "20250402LGSS0", "game_date": "2025-04-02"},
            ],
            detail_crawler=detail_crawler,
            relay_crawler=relay_crawler,
            force=False,
            concurrency=2,
            log=lambda _message: None,
        )
    )

    assert detail_crawler.calls == [
        {
            "games": [{"game_id": "20250402LGSS0", "game_date": "20250402"}],
            "concurrency": 2,
            "lightweight": False,
        }
    ]
    assert relay_crawler.calls == ["20250402LGSS0"]
    assert saved_details == ["20250402LGSS0"]
    assert saved_relays == [("20250402LGSS0", 1)]
    assert result.detail_saved == 1
    assert result.detail_skipped_existing == 1
    assert result.relay_saved_games == 1
    assert result.relay_skipped_existing == 1


def test_crawl_and_save_game_details_force_recrawls_existing(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_existing_game(SessionLocal, "20250401LGSS0")

    saved_details = []
    saved_relays = []
    monkeypatch.setattr(service, "save_game_detail", lambda payload: saved_details.append(payload["game_id"]) or True)
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, events: saved_relays.append((game_id, len(events))) or len(events),
    )

    detail_crawler = _FakeDetailCrawler()
    relay_crawler = _FakeRelayCrawler()
    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250401LGSS0", "game_date": "20250401"}],
            detail_crawler=detail_crawler,
            relay_crawler=relay_crawler,
            force=True,
            log=lambda _message: None,
        )
    )

    assert detail_crawler.calls[0]["games"] == [{"game_id": "20250401LGSS0", "game_date": "20250401"}]
    assert relay_crawler.calls == ["20250401LGSS0"]
    assert saved_details == ["20250401LGSS0"]
    assert saved_relays == [("20250401LGSS0", 1)]
    assert result.detail_skipped_existing == 0
    assert result.relay_skipped_existing == 0


def test_crawl_and_save_game_details_does_not_fetch_relay_when_detail_fails(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(service, "save_game_detail", lambda payload: saved_details.append(payload["game_id"]) or True)

    relay_crawler = _FakeRelayCrawler()
    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250403LGSS0", "game_date": "20250403"}],
            detail_crawler=_FailingDetailCrawler(),
            relay_crawler=relay_crawler,
            log=lambda _message: None,
        )
    )

    item = result.items["20250403LGSS0"]
    assert saved_details == []
    assert relay_crawler.calls == []
    assert item.detail_status == "crawl_failed"
    assert item.relay_status == "skipped_no_detail"
    assert item.failure_reason == "missing"


def test_crawl_and_save_game_details_can_filter_payloads_before_save(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(service, "save_game_detail", lambda payload: saved_details.append(payload["game_id"]) or True)

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250404LGSS0", "game_date": "20250404"}],
            detail_crawler=_EmptyHittersDetailCrawler(),
            force=True,
            should_save_detail=lambda payload: bool(payload.get("hitters", {}).get("away")),
            log=lambda _message: None,
        )
    )

    item = result.items["20250404LGSS0"]
    assert saved_details == []
    assert result.detail_saved == 0
    assert result.detail_failed == 1
    assert item.detail_status == "filtered"
    assert item.failure_reason == "detail_payload_filtered"
