"""Gate R3-C: True Failover, Cross-Provider Semantic Dedup, Correction & Concurrency Closure.

Verifies:
1. Primary success short-circuits secondary (Naver uncalled).
2. KBO timeout -> Naver failover (KBO fails, Naver succeeds).
3. Naver 5xx -> KBO failover (Naver called, raises 5xx, KBO called and succeeds).
4. Cross-provider different IDs same event deduplicated (KBO-101 vs NAV-999 collapsed).
5. Same identity with changed content detected as in-place correction (_is_correction=True).
6. Out-of-order events canonically re-sequenced into baseball chronological order.
7. Concurrent pollers during half-open state allow EXACTLY one probe (thundering herd guard).
8. Real multi-threaded concurrent DB writers yield zero duplicate rows and zero duplicate keys.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
import threading
from typing import Any

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

    def __init__(self, source_name: str, behavior: str = "success", delay: float = 0.0) -> None:
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
async def test_primary_success_short_circuits_secondary(temp_capability_file: Path):
    """Test 1: Primary KBO succeeds, secondary Naver is never queried."""
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


@pytest.mark.asyncio
async def test_kbo_timeout_failover_to_naver(temp_capability_file: Path):
    """Test 2: KBO timeout triggers circuit failure and fails over cleanly to Naver."""
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
async def test_naver_5xx_fails_over_to_kbo(temp_capability_file: Path):
    """Test 3: Naver throws 5xx error, orchestrator records failure and fails over to KBO."""
    naver = MockAdapter("naver", behavior="5xx")
    kbo = MockAdapter("kbo", behavior="success")
    cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=10.0)

    orchestrator = RelayRecoveryOrchestrator(
        adapters={"naver": naver, "kbo": kbo},
        capability_path=temp_capability_file,
        circuit_breaker=cb,
    )

    result, attempts = await orchestrator.fetch_game(
        game_id="20260401SKLG0",
        bucket_id="kbo_2026",
        source_order=["naver", "kbo"],
    )

    assert result.source_name == "kbo"
    assert len(attempts) == 2
    assert attempts[0]["source_name"] == "naver"
    assert attempts[0]["status"] == "exception"
    assert attempts[1]["source_name"] == "kbo"
    assert attempts[1]["status"] == "success"
    assert naver.fetch_count == 1
    assert kbo.fetch_count == 1
    assert cb.consecutive_failures("naver", "kbo_2026") == 1


def test_cross_provider_different_ids_same_event_deduplicated():
    """Test 4: Different provider IDs representing the same baseball event are collapsed."""
    dedup = RelayDeduplicator(window_size=100)

    kbo_events = [
        {
            "provider_log_id": "KBO-101",
            "inning": 1,
            "inning_half": "top",
            "source_row_index": 1,
            "play_description": "홍길동 안타",
            "outs": 0,
            "home_score": 0,
            "away_score": 0,
        }
    ]

    naver_events = [
        # Same baseball event with completely different Naver ID
        {
            "provider_log_id": "NAV-999",
            "inning": 1,
            "inning_half": "top",
            "source_row_index": 1,
            "play_description": "홍길동 안타",
            "outs": 0,
            "home_score": 0,
            "away_score": 0,
        },
        # Distinct baseball event
        {
            "provider_log_id": "NAV-1000",
            "inning": 1,
            "inning_half": "top",
            "source_row_index": 2,
            "play_description": "이순신 홈런",
            "outs": 0,
            "home_score": 0,
            "away_score": 2,
        },
    ]

    res1 = dedup.filter_new_events(kbo_events, use_semantic_key=True)
    assert len(res1) == 1
    assert res1[0]["provider_log_id"] == "KBO-101"

    # Naver events: NAV-999 must be dropped as duplicate; NAV-1000 must be accepted
    res2 = dedup.filter_new_events(naver_events, use_semantic_key=True)
    assert len(res2) == 1
    assert res2[0]["provider_log_id"] == "NAV-1000"


def test_same_identity_changed_content_becomes_correction():
    """Test 5: Same event ID arriving with revised score/description is emitted as correction."""
    dedup = RelayDeduplicator(window_size=50)

    initial_event = {
        "provider_log_id": "EV-10",
        "inning": 1,
        "inning_half": "top",
        "result_code": "2B",
        "play_description": "홍길동 2루타",
        "home_score": 0,
        "away_score": 0,
    }

    correction_event = {
        "provider_log_id": "EV-10",
        "inning": 1,
        "inning_half": "top",
        "result_code": "E",
        "play_description": "홍길동 실책으로 2루 출루 (수비 실책으로 정정)",
        "home_score": 0,
        "away_score": 0,
    }

    res1 = dedup.filter_new_events([initial_event])
    assert len(res1) == 1
    assert not res1[0].get("_is_correction")

    # Revised event must be recognized as correction
    res2 = dedup.filter_new_events([correction_event])
    assert len(res2) == 1
    assert res2[0].get("_is_correction") is True
    assert res2[0]["result_code"] == "E"


def test_out_of_order_events_are_canonically_ordered():
    """Test 6: Shuffled/out-of-order events are restored into canonical baseball order."""
    unordered = [
        {"inning": 2, "inning_half": "top", "event_seq": 4, "outs": 1, "text": "2회초 아웃"},
        {"inning": 1, "inning_half": "top", "event_seq": 1, "outs": 0, "text": "1회초 안타"},
        {"inning": 1, "inning_half": "bottom", "event_seq": 3, "outs": 0, "text": "1회말 홈런"},
        {"inning": 1, "inning_half": "top", "event_seq": 2, "outs": 1, "text": "1회초 삼진"},
    ]

    ordered = RelayDeduplicator.order_events_canonically(unordered)
    assert [e["event_seq"] for e in ordered] == [1, 2, 3, 4]
    assert [(e["inning"], e["inning_half"]) for e in ordered] == [(1, "top"), (1, "top"), (1, "bottom"), (2, "top")]


@pytest.mark.asyncio
async def test_half_open_allows_exactly_one_concurrent_probe(temp_capability_file: Path):
    """Test 7: During half-open state, concurrent pollers allow EXACTLY one probe (thundering herd guard)."""
    probe_adapter = MockAdapter("probe_target", behavior="success", delay=0.05)
    cb = SourceCircuitBreaker(threshold=2, cooldown_seconds=0.1)

    orchestrator = RelayRecoveryOrchestrator(
        adapters={"probe_target": probe_adapter},
        capability_path=temp_capability_file,
        circuit_breaker=cb,
    )

    # 1. Trigger 2 failures to open breaker
    probe_adapter.behavior = "timeout"
    await orchestrator.fetch_game("G1", "bucket1", ["probe_target"])
    await orchestrator.fetch_game("G2", "bucket1", ["probe_target"])
    assert cb.is_available("probe_target", "bucket1") is False

    # 2. Wait for cooldown to expire -> enters half-open
    await asyncio.sleep(0.12)

    # 3. Launch 3 concurrent requests simultaneously
    probe_adapter.behavior = "success"
    probe_adapter.fetch_count = 0

    results = await asyncio.gather(
        orchestrator.fetch_game("G3_a", "bucket1", ["probe_target"]),
        orchestrator.fetch_game("G3_b", "bucket1", ["probe_target"]),
        orchestrator.fetch_game("G3_c", "bucket1", ["probe_target"]),
    )

    # Exactly ONE caller should have actually executed the probe
    assert probe_adapter.fetch_count == 1

    # Status breakdown across the 3 attempts
    all_attempts = [att for _, attempts in results for att in attempts]
    success_attempts = [a for a in all_attempts if a["status"] == "success"]
    probe_wait_attempts = [a for a in all_attempts if a["status"] == "half_open_probe_in_progress"]

    assert len(success_attempts) == 1
    assert len(probe_wait_attempts) == 2


def test_two_concurrent_db_writers_produce_zero_duplicates(tmp_path: Path):
    """Test 8: Two threads concurrently saving identical relay data produce 0 duplicate rows."""
    from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

    db_file = tmp_path / "concurrent_writers.db"
    db_url = f"sqlite:///{db_file}?timeout=30"
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    with Session() as s:
        s.add(
            Game(
                game_id="20260401SKLG0",
                game_date=date(2026, 4, 1),
                home_team="LG",
                away_team="SK",
                game_status="COMPLETED",
            )
        )
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

    barrier = threading.Barrier(2)
    stats = {
        "successful_transactions": 0,
        "handled_unique_conflicts": 0,
        "unexpected_exceptions": 0,
    }
    lock = threading.Lock()

    def worker(worker_id: int):
        try:
            with Session() as s:
                barrier.wait(timeout=5.0)
                saved = save_relay_data("20260401SKLG0", events=events_payload, raw_pbp_rows=pbp_payload, session=s)
                if saved > 0:
                    s.commit()
                    with lock:
                        stats["successful_transactions"] += 1
                else:
                    s.rollback()
                    with lock:
                        stats["handled_unique_conflicts"] += 1
        except (IntegrityError, OperationalError):
            with lock:
                stats["handled_unique_conflicts"] += 1
        except (SQLAlchemyError, RuntimeError, TimeoutError) as exc:
            _ = exc
            with lock:
                stats["unexpected_exceptions"] += 1

    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(worker, 1)
        f2 = executor.submit(worker, 2)
        f1.result()
        f2.result()

    assert stats["successful_transactions"] >= 1
    assert stats["successful_transactions"] + stats["handled_unique_conflicts"] == 2
    assert stats["unexpected_exceptions"] == 0

    with Session() as s:
        ev_count = s.query(GameEvent).filter(GameEvent.game_id == "20260401SKLG0").count()
        pbp_count = s.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20260401SKLG0").count()
        # Verify 0 duplicate rows and 0 duplicate keys
        assert ev_count == 2
        assert pbp_count == 2
        seqs = [e.event_seq for e in s.query(GameEvent).filter(GameEvent.game_id == "20260401SKLG0").all()]
        assert len(seqs) == len(set(seqs))
