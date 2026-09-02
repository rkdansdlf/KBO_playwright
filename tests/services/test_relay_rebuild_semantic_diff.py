"""Gate R1: Regression test for semantic rebuild differential and idempotency."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from src.cli.backfill.rebuild_relay_events import (
    _event_to_payload,
    _rebuild_events_for_game,
)
from src.models.game import Game, GameEvent
from src.services.wpa_calculator import WPACalculator


def _canonical_repr(ev: dict[str, Any]) -> str:
    keys = [
        "inning",
        "inning_half",
        "outs",
        "batter_id",
        "pitcher_id",
        "event_type",
        "result_code",
        "home_score",
        "away_score",
        "base_state",
        "wpa",
    ]
    return "|".join(str(ev.get(k)) for k in keys)


def _stream_hash(events: list[dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for ev in events:
        h.update(_canonical_repr(ev).encode("utf-8"))
    return h.hexdigest()


def test_semantic_rebuild_synthetic_game():
    """Verify semantic rebuild on a synthetic multi-inning game."""
    calc = WPACalculator()
    events = [
        GameEvent(
            game_id="20260401SKLG0",
            event_seq=1,
            inning=1,
            inning_half="top",
            outs=0,
            batter_name="홍길동",
            pitcher_name="김투수",
            description="홍길동 : 중전 안타",
            event_type="hit",
            result_code="1B",
            home_score=0,
            away_score=0,
        ),
        GameEvent(
            game_id="20260401SKLG0",
            event_seq=2,
            inning=1,
            inning_half="top",
            outs=1,
            batter_name="이순신",
            pitcher_name="김투수",
            description="이순신 : 삼진 아웃",
            event_type="strikeout",
            result_code="SO",
            home_score=0,
            away_score=0,
        ),
        GameEvent(
            game_id="20260401SKLG0",
            event_seq=3,
            inning=1,
            inning_half="top",
            outs=2,
            batter_name="강감찬",
            pitcher_name="김투수",
            description="강감찬 : 좌월 투런 홈런 (비거리 120m)",
            event_type="home_run",
            result_code="HR",
            home_score=0,
            away_score=2,
        ),
    ]

    # Pass 1
    rebuilt1 = _rebuild_events_for_game(events, calculator=calc)
    assert len(rebuilt1) == 3
    assert rebuilt1[0]["event_seq"] == 1
    assert rebuilt1[2]["home_score"] == 0
    assert rebuilt1[2]["away_score"] == 2

    # Invariants
    for ev in rebuilt1:
        assert ev["outs"] in (0, 1, 2, 3)
        assert ev["inning"] == 1

    # Pass 2 (Idempotency)
    mock_evs = [GameEvent(**{k: v for k, v in e.items() if hasattr(GameEvent, k)}) for e in rebuilt1]
    rebuilt2 = _rebuild_events_for_game(mock_evs, calculator=calc)
    assert _stream_hash(rebuilt1) == _stream_hash(rebuilt2)
