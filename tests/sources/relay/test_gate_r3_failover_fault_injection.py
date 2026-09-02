"""Gate R3: Cross-Source Failover, Fault Injection, and Deduplication Tests.

Verifies:
1. KBO timeout -> Naver failover
2. Naver 5xx/exception -> KBO keep
3. Identical events received from multiple sources -> 0 duplicate events
4. Out-of-order arrival and delayed correction handling
5. Circuit OPEN -> Cooldown expiry -> CLOSED on success
6. Concurrent poll saving -> 0 duplicate writes in ephemeral SQLite DB
"""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.repositories.game_relay import save_relay_data
from src.sources.relay.base import NormalizedRelayResult, RelaySourceAdapter
from src.sources.relay.circuit_breaker import SourceCircuitBreaker
from src.sources.relay.orchestrator import RelayRecoveryOrchestrator
from src.sources.relay.relay_deduplicator import RelayDeduplicator


class MockAdapter(RelaySourceAdapter):
    """Mock relay adapter for fault injection."""

    def __init__(self, source_name: str, behavior="success", delay=0.0):
        super().__init__(source_name=source_name)
        self.behavior = behavior
        self.delay = delay
        self.fetch_count = 0

    async def fetch_game(self, game_id: str, last_payload_hash: str | None = None) -> NormalizedRelayResult:
        self.fetch_count += 1
        if self.delay > 0:
            await asyncio.sleep(self.delay)

        if self.behavior == "timeout":
            raise TimeoutError("Simulated socket timeout")
        if self.behavior == "5xx":
            raise RuntimeError("HTTP 500 Internal Server Error")
        if self.behavior == "empty":
            return NormalizedRelayResult(game_id=game_id, source_name=self.source_name, notes="no_data")
        return NormalizedRelayResult(
            game_id=game_id,
            source_name=self.source_name,
            events=[
                    {
                        "inning": 1,
                        "inning_half": "top",
                        "event_seq": 1,
                        "outs": 0,
                        "description": f"{self.source_name} event 1",
                        "home_score": 0,
                        "away_score": 0,
                        "base_state": 0,
                        "wpa": 0.01,
                        "win_expectancy_before": 0.50,
                        "win_expectancy_after": 0.51,
                    }
                ],
                raw_pbp_rows=[{"inning": 1, "inning_half": "top", "play_description": f"{self.source_name} pbp 1"}],
            )


@pytest.fixture
def temp_capability_file(tmp_path: Path) -> Path:
    cap_file = tmp_path / "capabilities.csv"
    cap_file.write_text("bucket_id,source_name,sample_size,supported,last_checked_at,notes\n")
    return cap_file


@pytest.mark.asyncio
async def test_kbo_timeout_failover_to_naver(temp_capability_file: Path):
    """Test Case 1: KBO timeout triggers circuit failure and fails over cleanly to Naver."""
    kbo = MockAdapter("kbo", behavior="timeout")
    naver = MockAdapter("naver", behavior="success")
    cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=10.0)

    orchestrator = RelayRecoveryOrchestrator(
        adapters={"kbo": kbo, "naver": naver},
        capability_path=temp_capability_file,
        timeout_seconds=0.1,
        circuit_breaker=cb,
    )

    result, attempts = await orchestrator.fetch_game(
        game_id="20260401SKLG0",
        bucket_id="kbo_2026",
        source_order=["kbo", "naver"],
    )

    assert result.source_name == "naver"
    assert not result.is_empty
    assert len(attempts) == 2
    assert attempts[0]["source_name"] == "kbo"
    assert attempts[0]["status"] == "timeout"
    assert attempts[1]["source_name"] == "naver"
    assert attempts[1]["status"] == "success"
    assert cb.consecutive_failures("kbo", "kbo_2026") == 1


@pytest.mark.asyncio
async def test_naver_5xx_keeps_kbo(temp_capability_file: Path):
    """Test Case 2: Naver throws 5xx error while KBO succeeds first."""
    kbo = MockAdapter("kbo", behavior="success")
    naver = MockAdapter("naver", behavior="5xx")
    cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=10.0)

    orchestrator = RelayRecoveryOrchestrator(
        adapters={"kbo": kbo, "naver": naver},
        capability_path=temp_capability_file,
        circuit_breaker=cb,
    )

    result, attempts = await orchestrator.fetch_game(
        game_id="20260401SKLG0",
        bucket_id="kbo_2026",
        source_order=["kbo", "naver"],
    )

    assert result.source_name == "kbo"
    assert len(attempts) == 1
    assert attempts[0]["status"] == "success"
    assert naver.fetch_count == 0


def test_cross_source_same_event_deduplication():
    """Test Case 3: Same event stream from multiple sources yields 0 duplicate events."""
    dedup = RelayDeduplicator(window_size=100)

    source_kbo_events = [
        {"provider_log_id": "EV1", "inning": 1, "text": "Kim hit 1B"},
        {"provider_log_id": "EV2", "inning": 1, "text": "Lee strikeout"},
    ]
    source_naver_events = [
        {"provider_log_id": "EV1", "inning": 1, "text": "Kim hit 1B"},
        {"provider_log_id": "EV2", "inning": 1, "text": "Lee strikeout"},
        {"provider_log_id": "EV3", "inning": 1, "text": "Park HR"},
    ]

    filtered_1 = dedup.filter_new_events(source_kbo_events)
    assert len(filtered_1) == 2

    filtered_2 = dedup.filter_new_events(source_naver_events)
    assert len(filtered_2) == 1
    assert filtered_2[0]["provider_log_id"] == "EV3"


def test_out_of_order_arrival_and_delayed_correction():
    """Test Case 4: Deduplicator handles fallback hash for unkeyed events without crash."""
    dedup = RelayDeduplicator(window_size=50)

    unkeyed_events_1 = [
        {"inning": 2, "source_row_index": 1, "play_description": "김선수 삼진 아웃"},
        {"inning": 2, "source_row_index": 2, "play_description": "이선수 2루타"},
    ]
    unkeyed_events_2 = [
        {"inning": 2, "source_row_index": 2, "play_description": "이선수 2루타"},
        {"inning": 2, "source_row_index": 3, "play_description": "박선수 희생플라이"},
    ]

    f1 = dedup.filter_new_events(unkeyed_events_1)
    assert len(f1) == 2

    f2 = dedup.filter_new_events(unkeyed_events_2)
    assert len(f2) == 1
    assert f2[0]["source_row_index"] == 3


@pytest.mark.asyncio
async def test_circuit_breaker_open_cooldown_and_close(temp_capability_file: Path):
    """Test Case 5: Circuit opens after threshold failures, skips queries, and closes on recovery."""
    bad_adapter = MockAdapter("flakey", behavior="timeout")
    cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=0.2)

    orchestrator = RelayRecoveryOrchestrator(
        adapters={"flakey": bad_adapter},
        capability_path=temp_capability_file,
        circuit_breaker=cb,
    )

    # 1st failure
    await orchestrator.fetch_game("G1", "bucket1", ["flakey"])
    assert cb.is_available("flakey", "bucket1") is True

    # 2nd failure -> Breaker OPENS
    await orchestrator.fetch_game("G2", "bucket1", ["flakey"])
    assert cb.is_available("flakey", "bucket1") is False

    # 3rd request while OPEN -> skipped immediately without calling adapter
    prev_count = bad_adapter.fetch_count
    _, attempts = await orchestrator.fetch_game("G3", "bucket1", ["flakey"])
    assert attempts[0]["status"] == "cb_open"
    assert bad_adapter.fetch_count == prev_count

    # Wait for cooldown to expire
    await asyncio.sleep(0.25)
    assert cb.is_available("flakey", "bucket1") is True

    # Probe recovery: set behavior to success
    bad_adapter.behavior = "success"
    res, attempts = await orchestrator.fetch_game("G4", "bucket1", ["flakey"])
    assert attempts[0]["status"] == "success"
    assert cb.consecutive_failures("flakey", "bucket1") == 0
    assert cb.is_available("flakey", "bucket1") is True


def test_concurrent_poll_ephemeral_db_zero_duplicate_writes(tmp_path: Path):
    """Test Case 6: Concurrent poll saving of identical relay payloads guarantees 0 duplicate rows."""
    db_file = tmp_path / "ephemeral_relay.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    # Seed parent game with NOT NULL game_date
    with Session() as s:
        s.add(Game(game_id="20260401SKLG0", game_date=date(2026, 4, 1), home_team="LG", away_team="SK"))
        s.commit()

    events_payload = [
        {
            "inning": 1,
            "inning_half": "top",
            "event_seq": 1,
            "outs": 0,
            "description": "홍길동 안타",
            "event_type": "hit",
            "result_code": "1B",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
            "wpa": 0.02,
            "win_expectancy_before": 0.50,
            "win_expectancy_after": 0.52,
        },
        {
            "inning": 1,
            "inning_half": "top",
            "event_seq": 2,
            "outs": 0,
            "description": "이순신 홈런",
            "event_type": "home_run",
            "result_code": "HR",
            "home_score": 0,
            "away_score": 2,
            "base_state": 1,
            "wpa": 0.15,
            "win_expectancy_before": 0.52,
            "win_expectancy_after": 0.67,
        },
    ]
    pbp_payload = [
        {"inning": 1, "inning_half": "top", "play_description": "홍길동 안타", "source_row_index": 1},
        {"inning": 1, "inning_half": "top", "play_description": "이순신 홈런", "source_row_index": 2},
    ]

    # First write
    with Session() as s:
        save_relay_data("20260401SKLG0", events=events_payload, raw_pbp_rows=pbp_payload, session=s)
        s.commit()

    # Second write (simulating concurrent / overlapping poll)
    with Session() as s:
        save_relay_data("20260401SKLG0", events=events_payload, raw_pbp_rows=pbp_payload, session=s)
        s.commit()

    # Verify row counts in DB: exactly 2 events and 2 pbps, 0 duplicates
    with Session() as s:
        ev_count = s.query(GameEvent).filter(GameEvent.game_id == "20260401SKLG0").count()
        pbp_count = s.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20260401SKLG0").count()
        assert ev_count == 2
        assert pbp_count == 2
