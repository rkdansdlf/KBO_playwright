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
        return [_valid_detail_payload(game["game_id"], game["game_date"]) for game in games]


class _FakeRelayCrawler:
    def __init__(self):
        self.calls = []

    async def crawl_game_events(self, game_id: str):
        self.calls.append(game_id)
        return {"events": [{"event_seq": 1, "inning": 1, "inning_half": "top", "description": game_id}]}


class _RawRelayCrawler:
    async def crawl_game_events(self, game_id: str):
        return {
            "events": [],
            "raw_pbp_rows": [
                {"inning": 1, "inning_half": "top", "play_description": f"{game_id} raw"},
            ],
        }


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
                "pitchers": {
                    "away": [{"player_name": "Away Pitcher"}],
                    "home": [{"player_name": "Home Pitcher"}],
                },
            }
            for game in games
        ]


def _valid_detail_payload(game_id: str, game_date: str):
    return {
        "game_id": game_id,
        "game_date": game_date,
        "hitters": {
            "away": [{"player_name": "Away Hitter"}],
            "home": [{"player_name": "Home Hitter"}],
        },
        "pitchers": {
            "away": [{"player_name": "Away Pitcher"}],
            "home": [{"player_name": "Home Pitcher"}],
        },
    }


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
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, events, **_kwargs: saved_relays.append((game_id, len(events))) or len(events),
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
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, events, **_kwargs: saved_relays.append((game_id, len(events))) or len(events),
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


def test_crawl_and_save_game_details_processes_every_detail_target_in_batch(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [
                {"game_id": "20250401LGSS0", "game_date": "20250401"},
                {"game_id": "20250402KTHH0", "game_date": "20250402"},
            ],
            detail_crawler=_FakeDetailCrawler(),
            force=True,
            log=lambda _message: None,
        )
    )

    assert saved_details == ["20250401LGSS0", "20250402KTHH0"]
    assert result.detail_saved == 2
    assert result.detail_failed == 0
    assert result.processed_game_ids == ["20250401LGSS0", "20250402KTHH0"]
    assert result.items["20250401LGSS0"].detail_status == "saved"
    assert result.items["20250402KTHH0"].detail_status == "saved"


def test_crawl_and_save_game_details_records_mixed_batch_failure_and_success(monkeypatch):
    class OneMissingDetailCrawler:
        async def crawl_games(self, games, concurrency=None, lightweight=False):
            return [_valid_detail_payload("20250402KTHH0", "20250402")]

        def get_last_failure_reason(self, game_id: str):
            return "missing" if game_id == "20250401LGSS0" else None

    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [
                {"game_id": "20250401LGSS0", "game_date": "20250401"},
                {"game_id": "20250402KTHH0", "game_date": "20250402"},
            ],
            detail_crawler=OneMissingDetailCrawler(),
            force=True,
            log=lambda _message: None,
        )
    )

    assert saved_details == ["20250402KTHH0"]
    assert result.detail_saved == 1
    assert result.detail_failed == 1
    assert result.items["20250401LGSS0"].detail_status == "crawl_failed"
    assert result.items["20250401LGSS0"].failure_reason == "missing"
    assert result.items["20250402KTHH0"].detail_status == "saved"


def test_crawl_and_save_game_details_saves_raw_pbp_without_events(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    monkeypatch.setattr(service, "save_game_detail", lambda payload, **_kwargs: True)
    saved_relays = []

    def _save_relay(game_id, events, **kwargs):
        saved_relays.append((game_id, list(events or []), list(kwargs.get("raw_pbp_rows") or [])))
        return len(kwargs.get("raw_pbp_rows") or [])

    monkeypatch.setattr(service, "save_relay_data", _save_relay)

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250405LGSS0", "game_date": "20250405"}],
            detail_crawler=_FakeDetailCrawler(),
            relay_crawler=_RawRelayCrawler(),
            log=lambda _message: None,
        )
    )

    assert saved_relays == [
        (
            "20250405LGSS0",
            [],
            [{"inning": 1, "inning_half": "top", "play_description": "20250405LGSS0 raw"}],
        )
    ]
    assert result.relay_saved_games == 1
    assert result.relay_rows_saved == 1


def test_crawl_and_save_game_details_does_not_fetch_relay_when_detail_fails(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )

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
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250404LGSS0", "game_date": "20250404"}],
            detail_crawler=_FakeDetailCrawler(),
            force=True,
            should_save_detail=lambda payload: False,
            log=lambda _message: None,
        )
    )

    item = result.items["20250404LGSS0"]
    assert saved_details == []
    assert result.detail_saved == 0
    assert result.detail_failed == 1
    assert item.detail_status == "filtered"
    assert item.failure_reason == "filtered"


def test_crawl_and_save_game_details_marks_save_failed_reason_when_save_returns_false(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    monkeypatch.setattr(service, "save_game_detail", lambda payload, **_kwargs: False)
    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250408LGSS0", "game_date": "20250408"}],
            detail_crawler=_FakeDetailCrawler(),
            force=True,
            log=lambda _message: None,
        )
    )

    item = result.items["20250408LGSS0"]
    assert result.detail_saved == 0
    assert result.detail_failed == 1
    assert item.detail_status == "save_failed"
    assert item.failure_reason == "save_failed"


def test_normalize_detail_failure_reason_maps_internal_codes():
    assert service._normalize_detail_failure_reason("detail_payload_filtered", default="fallback") == "filtered"
    assert service._normalize_detail_failure_reason("detail_save_failed", default="fallback") == "save_failed"
    assert service._normalize_detail_failure_reason("no_detail_payload", default="fallback") == "no_detail_payload"
    assert service._normalize_detail_failure_reason(None, default="fallback") == "fallback"


def test_crawl_and_save_game_details_filters_incomplete_payload_by_default(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)

    saved_details = []
    monkeypatch.setattr(
        service,
        "save_game_detail",
        lambda payload, **_kwargs: saved_details.append(payload["game_id"]) or True,
    )

    result = asyncio.run(
        service.crawl_and_save_game_details(
            [{"game_id": "20250407LGSS0", "game_date": "20250407"}],
            detail_crawler=_EmptyHittersDetailCrawler(),
            force=True,
            log=lambda _message: None,
        )
    )

    item = result.items["20250407LGSS0"]
    assert saved_details == []
    assert result.detail_saved == 0
    assert result.detail_failed == 1
    assert item.detail_status == "filtered"
    assert item.failure_reason == "incomplete_detail"


class TestBuildGameIdRange:
    def test_with_month_returns_correct_range(self):
        start, end = service.build_game_id_range(2025, 5)
        assert start == "20250501"
        assert end == "20250601"

    def test_december_wraps_to_next_year(self):
        start, end = service.build_game_id_range(2025, 12)
        assert start == "20251201"
        assert end == "20260101"

    def test_without_month_returns_full_year(self):
        start, end = service.build_game_id_range(2025, None)
        assert start == "20250101"
        assert end == "20260101"


class TestNormalizeGameTargets:
    def test_from_dicts(self):
        targets = service.normalize_game_targets(
            [
                {"game_id": "20250401LGSS0", "game_date": "20250401"},
                {"game_id": "20250402LGSS0", "game_date": "20250402"},
            ]
        )
        assert len(targets) == 2
        assert targets[0].game_id == "20250401LGSS0"
        assert targets[0].game_date == "20250401"

    def test_deduplicates_by_game_id(self):
        targets = service.normalize_game_targets(
            [
                {"game_id": "20250401LGSS0", "game_date": "20250401"},
                {"game_id": "20250401LGSS0", "game_date": "20250401"},
            ]
        )
        assert len(targets) == 1

    def test_skips_empty_game_id(self):
        targets = service.normalize_game_targets(
            [
                {"game_id": "", "game_date": "20250401"},
                {"game_id": None, "game_date": "20250401"},
            ]
        )
        assert len(targets) == 0

    def test_from_objects_with_attributes(self):
        class FakeGame:
            game_id = "20250401LGSS0"
            game_date = "20250401"

        targets = service.normalize_game_targets([FakeGame()])
        assert len(targets) == 1
        assert targets[0].game_id == "20250401LGSS0"


class TestFormatGameDate:
    def test_from_datetime(self):
        from datetime import datetime

        result = service._format_game_date(datetime(2025, 4, 1), fallback_game_id="20250401LGSS0")
        assert result == "20250401"

    def test_from_date(self):
        result = service._format_game_date(date(2025, 4, 1), fallback_game_id="20250401LGSS0")
        assert result == "20250401"

    def test_from_dashed_string(self):
        result = service._format_game_date("2025-04-01", fallback_game_id="20250401LGSS0")
        assert result == "20250401"

    def test_fallback_to_game_id_prefix(self):
        result = service._format_game_date("", fallback_game_id="20250401LGSS0")
        assert result == "20250401"


class TestHasRequiredDetailRows:
    def test_full_box_returns_true(self):
        payload = {
            "hitters": {"away": [{"player_name": "A"}], "home": [{"player_name": "B"}]},
            "pitchers": {"away": [{"player_name": "C"}], "home": [{"player_name": "D"}]},
        }
        assert service._has_required_detail_rows(payload) is True

    def test_partial_with_teams_and_score_returns_true(self):
        payload = {
            "teams": {"away": {"code": "LG"}, "home": {"code": "SS"}},
            "metadata": {"stadium": "잠실"},
            "hitters": {},
            "pitchers": {},
        }
        assert service._has_required_detail_rows(payload) is True

    def test_no_teams_returns_false(self):
        payload = {"hitters": {}, "pitchers": {}, "metadata": {"stadium": "잠실"}}
        assert service._has_required_detail_rows(payload) is False

    def test_no_score_and_no_metadata_returns_false(self):
        payload = {
            "teams": {"away": {"code": "LG"}, "home": {"code": "SS"}},
        }
        assert service._has_required_detail_rows(payload) is False

    def test_empty_payload_returns_false(self):
        assert service._has_required_detail_rows({}) is False
