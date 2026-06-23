from __future__ import annotations

import asyncio
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GameEvent, GamePlayByPlay, GameValidationMetrics
from src.models.season import KboSeason
from src.services import relay_recovery_service as service
from src.sources.relay import NormalizedRelayResult


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    KboSeason.__table__.create(bind=engine)
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    GamePlayByPlay.__table__.create(bind=engine)
    GameValidationMetrics.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_games(SessionLocal):
    with SessionLocal() as session:
        session.add(
            KboSeason(
                season_id=20250,
                season_year=2025,
                league_type_code=0,
                league_type_name="regular",
            )
        )
        session.add_all(
            [
                Game(
                    game_id="20250401LGSS0",
                    game_date=date(2025, 4, 1),
                    home_team="SS",
                    away_team="LG",
                    away_score=1,
                    home_score=0,
                    season_id=20250,
                    game_status="COMPLETED",
                ),
                Game(
                    game_id="20250402LGSS0",
                    game_date=date(2025, 4, 2),
                    home_team="SS",
                    away_team="LG",
                    away_score=2,
                    home_score=1,
                    season_id=20250,
                    game_status="COMPLETED",
                ),
                Game(
                    game_id="20250403LGSS0",
                    game_date=date(2025, 4, 3),
                    home_team="SS",
                    away_team="LG",
                    away_score=3,
                    home_score=2,
                    season_id=20250,
                    game_status="COMPLETED",
                ),
            ]
        )
        session.add_all(
            [
                GameEvent(game_id="20250402LGSS0", event_seq=1),
                GameEvent(
                    game_id="20250403LGSS0",
                    event_seq=1,
                    inning=1,
                    inning_half="top",
                    outs=0,
                    description="타자A : 좌전 안타",
                    bases_before="---",
                    bases_after="1--",
                    base_state=1,
                    home_score=0,
                    away_score=1,
                    wpa=0.01,
                    win_expectancy_before=0.5,
                    win_expectancy_after=0.51,
                ),
                GamePlayByPlay(game_id="20250403LGSS0", inning=1, inning_half="top"),
            ]
        )
        session.commit()


def _valid_event(**overrides):
    event = {
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
        "base_state": 1,
        "home_score": 0,
        "away_score": 1,
        "score_diff": -1,
        "wpa": 0.01,
        "win_expectancy_before": 0.5,
        "win_expectancy_after": 0.51,
    }
    event.update(overrides)
    return event


def test_load_relay_recovery_targets_skips_fully_recovered_games(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_games(SessionLocal)

    messages: list[str] = []
    targets = service.load_relay_recovery_targets(
        service.RecoveryTargetCriteria(
            season=2025,
            month=4,
            missing_only=True,
        ),
        log=messages.append,
    )

    assert [target.game_id for target in targets] == ["20250401LGSS0", "20250402LGSS0"]
    assert targets[0].needs_event_recovery is True
    assert targets[0].needs_pbp_recovery is True
    assert targets[1].has_events is True
    assert targets[1].has_event_state is False
    assert targets[1].needs_event_recovery is True
    assert targets[1].needs_pbp_recovery is True
    assert targets[0].bucket_id == "2025_regular_kbo"
    assert messages == ["[INFO] Missing-only mode: Skipped 1 games already fully recovered."]


def test_load_relay_recovery_targets_preserves_requested_order(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_games(SessionLocal)

    targets = service.load_relay_recovery_targets(
        service.RecoveryTargetCriteria(
            game_ids=["20250403LGSS0", "20250401LGSS0", "20250403LGSS0"],
            missing_only=False,
        ),
        log=lambda _msg: None,
    )

    assert [target.game_id for target in targets] == ["20250403LGSS0", "20250401LGSS0"]
    assert targets[0].has_events is True
    assert targets[0].has_event_state is True
    assert targets[0].has_pbp is True


def test_load_relay_recovery_targets_includes_existing_events_with_missing_wpa(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_games(SessionLocal)
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250404LGSS0",
                game_date=date(2025, 4, 4),
                home_team="SS",
                away_team="LG",
                away_score=1,
                home_score=0,
                season_id=20250,
                game_status="COMPLETED",
            )
        )
        session.add(
            GameEvent(
                game_id="20250404LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                outs=0,
                description="타자A : 좌전 안타",
                bases_before="---",
                bases_after="1--",
                base_state=1,
                home_score=0,
                away_score=1,
            )
        )
        session.add(GamePlayByPlay(game_id="20250404LGSS0", inning=1, inning_half="top"))
        session.commit()

    targets = service.load_relay_recovery_targets(
        service.RecoveryTargetCriteria(
            game_ids=["20250404LGSS0"],
            missing_only=True,
        ),
        log=lambda _msg: None,
    )

    assert [target.game_id for target in targets] == ["20250404LGSS0"]
    assert targets[0].has_events is True
    assert targets[0].has_event_state is False
    assert targets[0].has_pbp is True
    assert targets[0].needs_event_recovery is True
    assert targets[0].needs_pbp_recovery is False


def test_load_relay_recovery_targets_excludes_non_completed_requested_games(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_games(SessionLocal)
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250404LGSS0",
                game_date=date(2025, 4, 4),
                home_team="SS",
                away_team="LG",
                season_id=20250,
                game_status="CANCELLED",
            )
        )
        session.commit()

    targets = service.load_relay_recovery_targets(
        service.RecoveryTargetCriteria(
            game_ids=["20250404LGSS0", "20250401LGSS0"],
            missing_only=False,
        ),
        log=lambda _msg: None,
    )

    assert [target.game_id for target in targets] == ["20250401LGSS0"]


def test_recover_relay_data_saves_orchestrator_result(monkeypatch):
    saved_calls: list[tuple[str, list[dict], list[dict], str]] = []

    def _fake_save(game_id, events, raw_pbp_rows=None, source_name=None, **_kwargs):
        saved_calls.append((game_id, events, raw_pbp_rows, source_name))
        return 1

    monkeypatch.setattr(service, "save_relay_data", _fake_save)

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, bucket_id, game_ids, source_order):
            self.probe = (bucket_id, list(game_ids), list(source_order))

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name="fake",
                    raw_pbp_rows=[
                        {
                            "inning": 1,
                            "inning_half": "top",
                            "play_description": "test hit",
                        }
                    ],
                    has_raw_pbp=True,
                ),
                [
                    {
                        "game_id": game_id,
                        "bucket_id": bucket_id,
                        "source_name": source_order[0],
                        "status": "success",
                    }
                ],
            )

    result = asyncio.run(
        service.recover_relay_data(
            [
                service.RelayRecoveryTarget(
                    game_id="20250401LGSS0",
                    bucket_id="2025_regular_kbo",
                )
            ],
            service.RelayRecoveryConfig(
                source_order_override=["fake"],
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert result.total_targets == 1
    assert result.saved_games == 1
    assert result.saved_rows == 1
    assert saved_calls == [
        (
            "20250401LGSS0",
            [],
            [
                {
                    "inning": 1,
                    "inning_half": "top",
                    "pitcher_name": None,
                    "batter_name": None,
                    "play_description": "test hit",
                    "event_type": None,
                    "result": "hit",
                    "provider_log_id": None,
                    "source_row_index": None,
                    "source_name": None,
                }
            ],
            "fake",
        )
    ]
    assert result.report_rows[-1]["status"] == "saved"


def test_recover_relay_data_filters_malformed_rows_before_save(monkeypatch):
    saved_calls: list[tuple[list[dict], list[dict]]] = []

    def _fake_save(_game_id, events, raw_pbp_rows=None, **_kwargs):
        saved_calls.append((events, raw_pbp_rows))
        return len(events or []) + len(raw_pbp_rows or [])

    monkeypatch.setattr(service, "save_relay_data", _fake_save)

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name="fake",
                    events=[
                        _valid_event(),
                        {"inning": 1, "inning_half": "top", "description": "missing state"},
                    ],
                    raw_pbp_rows=[
                        {"inning": 1, "inning_half": "top", "play_description": "타자A : 좌전 안타"},
                        {"inning": None, "inning_half": "side", "play_description": ""},
                    ],
                    has_event_state=False,
                    has_raw_pbp=True,
                ),
                [],
            )

    result = asyncio.run(
        service.recover_relay_data(
            [service.RelayRecoveryTarget(game_id="20250401LGSS0", bucket_id="2025_regular_kbo")],
            service.RelayRecoveryConfig(
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert len(saved_calls) == 1
    assert len(saved_calls[0][0]) == 1
    assert len(saved_calls[0][1]) == 1
    assert result.saved_games == 1
    assert result.saved_rows == 2
    assert result.report_rows[-1]["status"] == "partial_relay"
    assert "filtered_event_rows:1" in result.report_rows[-1]["notes"]
    assert "filtered_pbp_rows:1" in result.report_rows[-1]["notes"]


def test_recover_relay_data_backfills_missing_wpa_before_save(monkeypatch):
    saved_events: list[dict] = []

    def _fake_save(_game_id, events, raw_pbp_rows=None, **_kwargs):
        saved_events.extend(events or [])
        return len(events or []) + len(raw_pbp_rows or [])

    monkeypatch.setattr(service, "save_relay_data", _fake_save)

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name="fake",
                    events=[
                        _valid_event(
                            wpa=None,
                            win_expectancy_before=None,
                            win_expectancy_after=None,
                        )
                    ],
                    has_event_state=False,
                ),
                [],
            )

    result = asyncio.run(
        service.recover_relay_data(
            [service.RelayRecoveryTarget(game_id="20250401LGSS0", bucket_id="2025_regular_kbo")],
            service.RelayRecoveryConfig(
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert result.saved_games == 1
    assert len(saved_events) == 1
    assert saved_events[0]["wpa"] is not None
    assert saved_events[0]["win_expectancy_before"] is not None
    assert saved_events[0]["win_expectancy_after"] is not None


def test_recover_relay_data_skips_when_all_rows_filtered(monkeypatch):
    saved_calls: list[str] = []
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, *_args, **_kwargs: saved_calls.append(game_id) or 1,
    )

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name="fake",
                    events=[{"description": "missing inning"}],
                    raw_pbp_rows=[{"inning": None, "inning_half": "top", "play_description": ""}],
                ),
                [],
            )

    result = asyncio.run(
        service.recover_relay_data(
            [service.RelayRecoveryTarget(game_id="20250401LGSS0", bucket_id="2025_regular_kbo")],
            service.RelayRecoveryConfig(
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert saved_calls == []
    assert result.saved_games == 0
    assert result.filtered_games == 1
    assert result.report_rows[-1]["status"] == "skipped_filtered"


def test_recover_relay_data_derives_missing_pbp_from_existing_events(monkeypatch):
    derived_calls: list[str] = []
    monkeypatch.setattr(
        service,
        "backfill_game_play_by_play_from_existing_events",
        lambda game_id: derived_calls.append(game_id) or 2,
    )

    class _UnusedOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, *_args):
            raise AssertionError("fetch_game should not run for derived PBP recovery")

    result = asyncio.run(
        service.recover_relay_data(
            [
                service.RelayRecoveryTarget(
                    game_id="20250402LGSS0",
                    bucket_id="2025_regular_kbo",
                    has_events=True,
                    has_event_state=True,
                    has_pbp=False,
                    needs_event_recovery=False,
                    needs_pbp_recovery=True,
                )
            ],
            service.RelayRecoveryConfig(
                allow_derived_pbp=True,
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_UnusedOrchestrator(),
        )
    )

    assert derived_calls == ["20250402LGSS0"]
    assert result.derived_pbp_games == 1
    assert result.saved_rows == 2


def test_recover_relay_data_skips_result_with_too_few_events(monkeypatch):
    saved_calls: list[str] = []
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, *_args, **_kwargs: saved_calls.append(game_id) or 1,
    )

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            result = NormalizedRelayResult(
                game_id=game_id,
                source_name="fake",
                events=[{"description": "홍길동 : 안타", "away_score": 1, "home_score": 0}],
                has_event_state=True,
            )
            validation_error = kwargs["validator"](result)
            if validation_error:
                return (
                    NormalizedRelayResult(game_id=game_id, source_name="none", notes=validation_error),
                    [
                        {
                            "game_id": game_id,
                            "bucket_id": bucket_id,
                            "source_name": source_order[0],
                            "status": "skipped_validation",
                            "notes": validation_error,
                        }
                    ],
                )
            return (result, [])

    result = asyncio.run(
        service.recover_relay_data(
            [service.RelayRecoveryTarget(game_id="20250401LGSS0", bucket_id="2025_regular_kbo")],
            service.RelayRecoveryConfig(
                min_result_events=2,
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert saved_calls == []
    assert result.saved_games == 0
    assert result.report_rows[-1]["status"] == "skipped_validation"
    assert result.report_rows[-1]["notes"] == "too_few_result_events:1<2"


def test_recover_relay_data_skips_score_mismatch_when_validation_enabled(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(service, "SessionLocal", SessionLocal)
    _seed_games(SessionLocal)
    saved_calls: list[str] = []
    monkeypatch.setattr(
        service,
        "save_relay_data",
        lambda game_id, *_args, **_kwargs: saved_calls.append(game_id) or 1,
    )

    class _FakeOrchestrator:
        def source_order_for_bucket(self, _bucket_id, override=None):
            return list(override or ["fake"])

        async def probe_bucket(self, *_args):
            return {}

        async def fetch_game(self, game_id, bucket_id, source_order, **kwargs):
            result = NormalizedRelayResult(
                game_id=game_id,
                source_name="fake",
                events=[
                    {"description": "홍길동 : 안타", "away_score": 0, "home_score": 0},
                    {"description": "이몽룡 : 삼진", "away_score": 0, "home_score": 0},
                ],
                has_event_state=True,
            )
            validation_error = kwargs["validator"](result)
            if validation_error:
                return (
                    NormalizedRelayResult(game_id=game_id, source_name="none", notes=validation_error),
                    [
                        {
                            "game_id": game_id,
                            "bucket_id": bucket_id,
                            "source_name": source_order[0],
                            "status": "skipped_validation",
                            "notes": validation_error,
                        }
                    ],
                )
            return (result, [])

    result = asyncio.run(
        service.recover_relay_data(
            [service.RelayRecoveryTarget(game_id="20250401LGSS0", bucket_id="2025_regular_kbo")],
            service.RelayRecoveryConfig(
                validate_final_score=True,
                sleep_seconds=0,
                log=lambda _msg: None,
            ),
            orchestrator=_FakeOrchestrator(),
        )
    )

    assert saved_calls == []
    assert result.saved_games == 0
    assert result.report_rows[-1]["status"] == "skipped_validation"
    assert result.report_rows[-1]["notes"] == "final_score_mismatch:events=0-0 game=1-0"
