from __future__ import annotations

import asyncio
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_repository as game_repository
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.sources.relay.base import NormalizedRelayResult, read_manifest_entries
from src.sources.relay.importer import ImportRelayAdapter
from src.sources.relay.kbo import KboRelayAdapter
from src.sources.relay.orchestrator import RelayRecoveryOrchestrator


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GamePlayByPlay.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_game(SessionLocal, game_id: str):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 4, 1),
                home_team="LG",
                away_team="SS",
            )
        )
        session.commit()


def _sample_event(**overrides):
    event = {
        "event_seq": 1,
        "inning": 1,
        "inning_half": "top",
        "outs": 0,
        "batter_name": "타자A",
        "pitcher_name": "투수B",
        "description": "타자A : 좌전 안타",
        "event_type": "batting",
        "result_code": "안타",
        "rbi": 0,
        "bases_before": "---",
        "bases_after": "1--",
        "wpa": 0.1234,
        "win_expectancy_before": 0.5,
        "win_expectancy_after": 0.6234,
        "score_diff": 0,
        "base_state": 0,
        "home_score": 0,
        "away_score": 0,
    }
    event.update(overrides)
    return event


def test_save_relay_data_events_only_writes_both_tables(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)
    _seed_game(SessionLocal, "20250401LGSS0")

    saved = game_repository.save_relay_data("20250401LGSS0", [_sample_event()])
    assert saved == 1

    with SessionLocal() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 1
        pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20250401LGSS0").one()
        assert pbp.play_description == "타자A : 좌전 안타"
        assert pbp.result == "안타"


def test_save_relay_data_pbp_only_preserves_existing_events(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)
    _seed_game(SessionLocal, "20250402LGSS0")

    game_repository.save_relay_data("20250402LGSS0", [_sample_event()])
    saved = game_repository.save_relay_data(
        "20250402LGSS0",
        events=None,
        raw_pbp_rows=[
            {
                "inning": 9,
                "inning_half": "bottom",
                "play_description": "끝내기 안타",
                "event_type": "batting",
                "result": "안타",
            }
        ],
    )
    assert saved == 1

    with SessionLocal() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250402LGSS0").count() == 1
        pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20250402LGSS0").one()
        assert pbp.inning == 9
        assert pbp.inning_half == "bottom"
        assert pbp.play_description == "끝내기 안타"


def test_backfill_game_play_by_play_from_existing_events(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)
    _seed_game(SessionLocal, "20250403LGSS0")

    game_repository.save_relay_data(
        "20250403LGSS0",
        [_sample_event(result_code=None, result="2루타", description="타자A : 우중간 2루타")],
    )
    with SessionLocal() as session:
        session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20250403LGSS0").delete()
        session.commit()

    saved = game_repository.backfill_game_play_by_play_from_existing_events("20250403LGSS0")
    assert saved == 1

    with SessionLocal() as session:
        pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20250403LGSS0").one()
        assert pbp.result == "2루타"
        assert pbp.play_description == "타자A : 우중간 2루타"


def test_import_adapter_reads_normalized_events_json(tmp_path):
    payload_path = tmp_path / "events.json"
    payload_path.write_text(
        """
        {
          "events": [
            {
              "event_seq": 1,
              "inning": 1,
              "inning_half": "top",
              "outs": 0,
              "description": "타자A : 좌전 안타",
              "event_type": "batting",
              "batter_name": "타자A",
              "pitcher_name": "투수B",
              "bases_before": "---",
              "bases_after": "1--",
              "base_state": 0,
              "home_score": 0,
              "away_score": 0,
              "score_diff": 0,
              "wpa": 0.1,
              "win_expectancy_before": 0.5,
              "win_expectancy_after": 0.6
            }
          ],
          "notes": "archived"
        }
        """.strip(),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text(
        "game_id,source_type,locator,format,priority,notes\n"
        f"20250404LGSS0,json_archive,{payload_path.name},normalized_events_json,1,test-note\n",
        encoding="utf-8",
    )

    entries = read_manifest_entries(manifest_path)
    adapter = ImportRelayAdapter(entries, manifest_base_dir=tmp_path)
    result = asyncio.run(adapter.fetch_game("20250404LGSS0"))

    assert len(result.events) == 1
    assert result.has_event_state is True
    assert result.notes == "test-note | archived"


def test_kbo_adapter_marks_auth_failure_as_unsupported():
    class _FakeCrawler:
        def __init__(self):
            self.last_failure_reason = "auth_required"

        async def crawl_game_events(self, game_id: str):
            return None

    adapter = KboRelayAdapter(_FakeCrawler())
    result = asyncio.run(adapter.fetch_game("20250405LGSS0"))

    assert result.is_empty is True
    assert result.notes == "unsupported: kbo relay auth required"


def test_orchestrator_skips_cached_unsupported_source(tmp_path):
    class _EmptyAdapter:
        def __init__(self, name: str):
            self.source_name = name
            self.calls = 0

        async def fetch_game(self, game_id: str):
            self.calls += 1
            return NormalizedRelayResult(game_id=game_id, source_name=self.source_name, notes="miss")

    class _SuccessAdapter:
        def __init__(self, name: str):
            self.source_name = name
            self.calls = 0

        async def fetch_game(self, game_id: str):
            self.calls += 1
            return NormalizedRelayResult(
                game_id=game_id,
                source_name=self.source_name,
                raw_pbp_rows=[{"inning": 1, "inning_half": "top", "play_description": "수동 복구"}],
                has_raw_pbp=True,
            )

    empty = _EmptyAdapter("kbo")
    success = _SuccessAdapter("manual")
    orchestrator = RelayRecoveryOrchestrator(
        {"kbo": empty, "manual": success},
        capability_path=tmp_path / "source_capability.csv",
    )

    asyncio.run(orchestrator.probe_bucket("2023_legacy", ["g1", "g2", "g3"], ["kbo", "manual"]))
    result, attempts = asyncio.run(orchestrator.fetch_game("g4", "2023_legacy", ["kbo", "manual"]))

    assert empty.calls == 3
    assert success.calls == 2
    assert attempts[0]["status"] == "cached_unsupported"
    assert result.source_name == "manual"
